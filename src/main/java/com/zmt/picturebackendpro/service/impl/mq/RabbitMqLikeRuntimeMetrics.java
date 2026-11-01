package com.zmt.picturebackendpro.service.impl.mq;

import com.zmt.picturebackendpro.config.RabbitMqLikeProperties;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.springframework.amqp.rabbit.core.RabbitAdmin;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * RabbitMQ 点赞消费者的运行时指标组件。
 * <p>
 * 职责：
 * <ol>
 *   <li>向 Micrometer 注册消费相关的计数器、计时器与 Gauge；</li>
 *   <li>维护消费者在途计数 {@link #inFlight} 与最近完成时间 {@link #lastConsumerCompletedAtMs}，
 *       供对账任务判断"链路是否安静"；</li>
 *   <li>按需查询 RabbitMQ 队列的 ready 消息数（{@link RabbitAdmin#getQueueProperties}）。</li>
 * </ol>
 * <p>
 * 指标语义说明：
 * <ul>
 *   <li><b>queue.depth / retry.depth / dead.depth</b>：仅统计 <i>ready</i> 消息数
 *       （未被投递给消费者的），不包含 unacked/in-flight 的消息；</li>
 *   <li><b>inflight</b>：当前 consumer callback 内正在处理的消息数
 *       （已投递给消费者但尚未 ACK 的）；</li>
 *   <li><b>last_completed_epoch_ms</b>：最近一次 consumer callback 结束的时间戳；
 *       结合 queue.depth 与 inflight 可以解释"批量缓冲/消费中的消息"，
 *       避免仅凭 ready 数误判链路是否安静。</li>
 * </ul>
 * <p>
 * 与消费者实现的配套关系：本类额外提供持久化耗时/失败指标与重试队列深度 Gauge，
 * 与使用 {@code persistWithMetrics} 和延迟重试队列（{@code publishRetryRaw}）的
 * 批量消费者配套使用。
 * <p>
 * 通过 {@link ConditionalOnProperty} 限定只在 {@code community.like.mode=rabbitmq} 时装配。
 */
@Component
@ConditionalOnProperty(name = "community.like.mode", havingValue = "rabbitmq")
public class RabbitMqLikeRuntimeMetrics {

    /** RabbitAdmin 用于查询队列的 ready 消息数（被动查询，不修改队列） */
    private final RabbitAdmin rabbitAdmin;

    /** 点赞 MQ 配置：提供主队列、重试队列、死信队列名，用于注册队列深度 Gauge */
    private final RabbitMqLikeProperties properties;

    /**
     * 当前 consumer callback 内处理中的消息数。
     * <p>
     * 语义：已被投递给消费者、但尚未完成的批量消息总数。
     * 批量开始时加 size，结束时减 size；对账任务用它判断是否有在途消费。
     */
    private final AtomicInteger inFlight = new AtomicInteger();

    /**
     * 最近一次 consumer callback 完成的时间戳（epoch ms）。
     * <p>
     * 0 表示自启动以来从未完成过批次。对账任务用它判断"consumer callback 是否已静默"。
     */
    private final AtomicLong lastConsumerCompletedAtMs = new AtomicLong();

    /** 已成功持久化并 ACK 的消息总数（按消息条数计，非批次数） */
    private final Counter processedMessages;

    /** 批量事务失败次数（每次失败会触发降级逐条处理） */
    private final Counter batchFailures;

    /** 事件级重试次数（逐条降级中每次失败累加，消息随后投递到延迟重试队列） */
    private final Counter retryAttempts;

    /** 达到最大失败次数、已复制到死信队列的消息数 */
    private final Counter deadLetteredMessages;

    /** MySQL 持久化失败次数（事务回滚或抛异常） */
    private final Counter persistenceFailures;

    /**
     * 整个 consumer callback 的耗时（包含批量/逐条处理、降级、重试与死信交接）。
     * 使用 {@code publishPercentileHistogram()} 以便观测耗时分布。
     */
    private final Timer batchDuration;

    /**
     * 仅 MySQL 事务持久化服务的耗时（不含 MQ 发送、ACK、降级逻辑）。
     * 与 {@link #batchDuration} 分开统计，便于区分"DB 慢"与"MQ/序列化慢"。
     */
    private final Timer persistenceDuration;

    /**
     * 构造器：一次性注册所有指标。
     * <p>
     * 注意：Gauge 绑定的是本对象的 {@link AtomicInteger}/{@link AtomicLong} 引用，
     * 因此可以实时反映最新值，无需重复注册。
     *
     * @param rabbitAdmin RabbitAdmin（用于队列深度查询）
     * @param properties  点赞 MQ 配置（提供主/重试/死信队列名）
     * @param registry    Micrometer 注册表
     */
    public RabbitMqLikeRuntimeMetrics(RabbitAdmin rabbitAdmin, RabbitMqLikeProperties properties,
                                      MeterRegistry registry) {
        this.rabbitAdmin = rabbitAdmin;
        this.properties = properties;

        // ---- 计数器 ----
        this.processedMessages = Counter.builder("community.like.rabbitmq.consumer.messages")
                .description("RabbitMQ like events successfully persisted and ACKed")
                .register(registry);
        this.batchFailures = Counter.builder("community.like.rabbitmq.consumer.failures")
                .description("RabbitMQ like batch processing failures")
                .register(registry);
        this.retryAttempts = Counter.builder("community.like.rabbitmq.consumer.retry_attempts")
                .description("RabbitMQ like events sent to delayed retry queue")
                .register(registry);
        this.deadLetteredMessages = Counter.builder("community.like.rabbitmq.consumer.dead_lettered")
                .description("RabbitMQ like messages confirmed in the dead-letter queue")
                .register(registry);
        this.persistenceFailures = Counter.builder("community.like.rabbitmq.consumer.persistence.failures")
                .description("MySQL like persistence invocations that rolled back or failed")
                .register(registry);

        // ---- 计时器 ----
        // batchDuration：整个 callback 的耗时，开启直方图以观测 P50/P90/P99 分布
        this.batchDuration = Timer.builder("community.like.rabbitmq.consumer.batch.duration")
                .description("Whole consumer callback duration, including fallback and retry/dead-letter handoff")
                .publishPercentileHistogram()
                .register(registry);
        // persistenceDuration：仅 MySQL 事务耗时，同样开启直方图
        this.persistenceDuration = Timer.builder("community.like.rabbitmq.consumer.persistence.duration")
                .description("Time spent inside the transactional MySQL persistence service only")
                .publishPercentileHistogram()
                .register(registry);

        // ---- Gauge：直接读取原子字段，反映实时值 ----
        Gauge.builder("community.like.rabbitmq.consumer.inflight", inFlight, AtomicInteger::get)
                .description("Messages inside active consumer callbacks")
                .register(registry);
        Gauge.builder("community.like.rabbitmq.consumer.last_completed_epoch_ms", lastConsumerCompletedAtMs, AtomicLong::get)
                .description("Epoch milliseconds when the last consumer callback completed")
                .register(registry);

        // ---- Gauge：主队列、重试队列、死信队列的 ready 深度 ----
        registerQueueGauge(registry, "community.like.rabbitmq.queue.depth", properties.getQueue());
        registerQueueGauge(registry, "community.like.rabbitmq.dead.depth", properties.getDeadQueue());
        registerQueueGauge(registry, "community.like.rabbitmq.retry.depth", properties.getRetryQueue());
    }

    /**
     * 注册队列深度 Gauge（懒查询）。
     * <p>
     * 使用 {@link Gauge#builder} + 方法引用，使每次采集指标时才调用 {@link #queueDepth(String)}，
     * 而不是在注册时立即查询，避免启动阶段依赖 broker 可用性。
     * <p>
     * 描述中说明 {@code -1} 表示查询不可用（如 broker 暂不可达），
     * 便于监控告警区分"真实为空"与"查询失败"。
     *
     * @param registry Micrometer 注册表
     * @param metricName Gauge 名称
     * @param queueName  队列名
     */
    private void registerQueueGauge(MeterRegistry registry, String metricName, String queueName) {
        Gauge.builder(metricName, this, metrics -> metrics.queueDepth(queueName))
                .description("Ready messages in RabbitMQ queue " + queueName + "; -1 means unavailable")
                .register(registry);
    }

    /**
     * 查询指定队列的 ready 消息数。
     * <p>
     * 返回语义：
     * <ul>
     *   <li>{@code >= 0}：队列 ready 消息数（不含 unacked/in-flight）；</li>
     *   <li>{@code -1}：查询不可用（队列不存在、broker 不可达或返回非法值）。</li>
     * </ul>
     * 异常时返回 -1 而非抛出，避免指标采集异常影响业务或监控采集线程。
     *
     * @param queue 队列名
     * @return ready 消息数，或 -1 表示查询不可用
     */
    public int queueDepth(String queue) {
        try {
            Properties queueProperties = rabbitAdmin.getQueueProperties(queue);
            if (queueProperties == null) return -1;
            Object value = queueProperties.get(RabbitAdmin.QUEUE_MESSAGE_COUNT);
            return value instanceof Number ? ((Number) value).intValue() : -1;
        } catch (RuntimeException exception) {
            // 采集失败不向上抛出，避免影响监控采集线程或业务线程
            return -1;
        }
    }

    /**
     * 判断主队列是否为空（ready 数为 0）。
     * <p>
     * 对账只需要主队列 Ready 为空；重试/DLQ 中的旧版本会由 pairVersion fence 保护，
     * 因此这里不把 retry/DLQ 纳入判断，避免旧版本残留导致对账窗口长期不可用。
     *
     * @return true 表示主队列 ready 消息数为 0
     */
    public boolean mainQueueEmpty() {
        return queueDepth(properties.getQueue()) == 0;
    }

    /**
     * 判断主队列、延迟重试队列、死信队列是否都为空（ready 数均为 0）。
     * <p>
     * 保留全链路空判断，供人工诊断使用；它比 {@link #mainQueueEmpty()} 更严格，
     * 包含主队列、延迟重试队列和死信队列。
     * <p>
     * 注意：此方法只反映 ready 消息；完整判断链路是否安静还需结合
     * {@link #inFlight()} 与 {@link #lastConsumerCompletedAtMs()}。
     *
     * @return true 表示三个队列的 ready 消息数都为 0
     */
    public boolean allQueuesEmpty() {
        return queueDepth(properties.getQueue()) == 0
                && queueDepth(properties.getRetryQueue()) == 0
                && queueDepth(properties.getDeadQueue()) == 0;
    }

    /**
     * 批量处理开始：在途计数加上本批消息数。
     * <p>
     * 在 consumer callback 入口调用；与 {@link #endBatch(int)} 必须成对出现，
     * 由 {@code RabbitMqLikeBatchConsumer#consume} 的 finally 保证。
     *
     * @param size 本批消息数
     */
    public void beginBatch(int size) { inFlight.addAndGet(size); }

    /**
     * 批量处理结束：在途计数减去本批消息数，并记录完成时间戳。
     * <p>
     * 时间戳供对账任务判断"consumer callback 是否已静默"，
     * 防止 broker/监听器仍在攒批时误入对账窗口。
     *
     * @param size 本批消息数
     */
    public void endBatch(int size) {
        inFlight.addAndGet(-size);
        lastConsumerCompletedAtMs.set(System.currentTimeMillis());
    }

    /** 当前在途消息数（已投递未完成的），供对账窗口判断。 */
    public int inFlight() { return inFlight.get(); }

    /** 最近一次 consumer callback 完成的时间戳（epoch ms），0 表示从未完成。 */
    public long lastConsumerCompletedAtMs() { return lastConsumerCompletedAtMs.get(); }

    /**
     * 记录成功处理的消息数及批次耗时。
     * <p>
     * 在批量事务成功 ACK 后，或逐条降级完成后调用；
     * 每次调用累加 {@code count} 条消息并记录一次耗时样本。
     * <p>
     * {@code count <= 0} 时不向 Counter 累加，避免传入非法值。
     *
     * @param count 本次成功处理的消息数
     * @param nanos 本次处理耗时（纳秒）
     */
    public void recordProcessed(int count, long nanos) {
        if (count > 0) processedMessages.increment(count);
        batchDuration.record(nanos, TimeUnit.NANOSECONDS);
    }

    /** 记录一次批量事务失败（触发逐条降级）。 */
    public void recordFailure() { batchFailures.increment(); }

    /**
     * 记录一次 MySQL 持久化耗时。
     * <p>
     * 由 {@code persistWithMetrics} 的 finally 调用，成功与失败路径都会记录，
     * 便于定位 DB 慢查询。
     *
     * @param nanos 持久化耗时（纳秒）
     */
    public void recordPersistence(long nanos) { persistenceDuration.record(nanos, TimeUnit.NANOSECONDS); }

    /** 记录一次 MySQL 持久化失败（事务回滚或抛异常）。 */
    public void recordPersistenceFailure() { persistenceFailures.increment(); }

    /** 记录一次事件级重试（消息已投递到延迟重试队列）。 */
    public void recordRetryAttempt() { retryAttempts.increment(); }

    /** 记录一条消息达到最大重试次数并已复制到死信队列。 */
    public void recordDeadLettered() { deadLetteredMessages.increment(); }
}