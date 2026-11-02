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
 * RabbitMQ 点赞链路运行指标组件。
 *
 * <p>职责：</p>
 * <ol>
 *   <li>向 Micrometer 注册消费相关的计数器、计时器与 Gauge；</li>
 *   <li>维护消费者在途计数 {@link #inFlight} 与最近完成时间 {@link #lastConsumerCompletedAtMs}，
 *       供对账任务判断“链路是否安静”；</li>
 *   <li>按需查询 RabbitMQ 队列的 ready 消息数（{@link RabbitAdmin#getQueueProperties}）。</li>
 * </ol>
 *
 * <p>指标语义说明：</p>
 * <ul>
 *   <li><b>Counter</b>：记录累计事件数量（成功处理、失败、重试、死信发布、审计持久化等），
 *       只增不减；</li>
 *   <li><b>Timer</b>：记录批次耗时与持久化耗时分布，开启直方图便于观测 P50/P90/P99；</li>
 *   <li><b>Gauge</b>：只记录当前瞬时值，例如在途消息数、最近完成时间、队列 ready 深度；
 *       队列深度仅统计 <i>ready</i> 消息（未被投递给消费者的），不包含 unacked/in-flight 消息。</li>
 * </ul>
 *
 * <p>通过 {@link ConditionalOnProperty} 限定只在 {@code community.like.mode=rabbitmq} 时装配。</p>
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
     *
     * <p>语义：已被投递给消费者、但尚未完成的批量消息总数。
     * 批量开始时加 size，结束时减 size；对账任务用它判断是否有在途消费。</p>
     */
    private final AtomicInteger inFlight = new AtomicInteger();

    /**
     * 最近一次 consumer callback 完成的时间戳（epoch ms）。
     *
     * <p>0 表示自启动以来从未完成过批次。对账任务用它判断
     * “consumer callback 是否已静默”，避免 broker/监听器仍在攒批时误入对账窗口。</p>
     */
    private final AtomicLong lastConsumerCompletedAtMs = new AtomicLong();

    /** 已成功持久化并 ACK 的消息总数（按消息条数计，非批次数） */
    private final Counter processedMessages;

    /** 批量事务失败次数（每次失败会触发降级逐条处理） */
    private final Counter batchFailures;

    /** 事件级重试次数（逐条降级中每次失败累加，消息随后投递到延迟重试队列） */
    private final Counter retryAttempts;

    /** 达到最大失败次数、已成功发布到死信队列的消息数（推荐使用的新指标名） */
    private final Counter deadLetterPublished;

    /**
     * 旧名称保留，避免现有 Prometheus 查询突然失效。
     *
     * <p>与 {@link #deadLetterPublished} 同步递增，属于同一语义的历史别名：
     * {@code community_like_rabbitmq_consumer_dead_lettered_total}。</p>
     */
    private final Counter legacyDeadLetteredMessages;

    /** 死信审计消息成功落库并 ACK 的次数 */
    private final Counter deadLetterAuditPersisted;

    /** 死信审计消息落库或 ACK 失败的次数 */
    private final Counter deadLetterAuditFailures;

    /** MySQL 持久化失败次数（事务回滚或抛异常） */
    private final Counter persistenceFailures;

    /**
     * 整个 consumer callback 的耗时（包含批量/逐条处理、降级、重试与死信交接）。
     * 开启直方图以便观测耗时分布。
     */
    private final Timer batchDuration;

    /**
     * 仅 MySQL 事务持久化服务的耗时（不含 MQ 发送、ACK、降级逻辑）。
     * 与 {@link #batchDuration} 分开统计，便于区分“DB 慢”与“MQ/序列化慢”。
     */
    private final Timer persistenceDuration;

    /**
     * 构造器：一次性注册所有指标。
     *
     * <p>注意：Gauge 绑定的是本对象的 {@link AtomicInteger}/{@link AtomicLong} 引用，
     * 因此可以实时反映最新值，无需重复注册。</p>
     *
     * @param rabbitAdmin RabbitAdmin（用于队列深度查询）
     * @param properties  点赞 MQ 配置（提供主/重试/死信队列名）
     * @param registry    Micrometer 注册表
     */
    public RabbitMqLikeRuntimeMetrics(RabbitAdmin rabbitAdmin, RabbitMqLikeProperties properties,
                                      MeterRegistry registry) {
        this.rabbitAdmin = rabbitAdmin;
        this.properties = properties;

        // ---- 计数器：消费者处理链路 ----
        processedMessages = Counter.builder("community.like.rabbitmq.consumer.messages")
                .description("RabbitMQ like events successfully persisted and ACKed")
                .register(registry);
        batchFailures = Counter.builder("community.like.rabbitmq.consumer.failures")
                .description("RabbitMQ like batch processing failures")
                .register(registry);
        retryAttempts = Counter.builder("community.like.rabbitmq.consumer.retry_attempts")
                .description("RabbitMQ like events confirmed in delayed retry queue")
                .register(registry);

        // ---- 计数器：死信发布 ----
        // 推荐使用这个名称观察应用层成功发布到 DLQ 的累计数量
        deadLetterPublished = Counter.builder("community.like.rabbitmq.dead_letter.published")
                .description("Like events successfully published to RabbitMQ dead-letter queue")
                .register(registry);
        // 兼容旧名称：community_like_rabbitmq_consumer_dead_lettered_total
        legacyDeadLetteredMessages = Counter.builder("community.like.rabbitmq.consumer.dead_lettered")
                .description("Legacy alias of RabbitMQ like dead-letter published counter")
                .register(registry);

        // ---- 计数器：死信审计 ----
        deadLetterAuditPersisted = Counter.builder("community.like.rabbitmq.dead_letter.audit.persisted")
                .description("Dead-letter messages persisted to community_like_dead_letter_audit and ACKed")
                .register(registry);
        deadLetterAuditFailures = Counter.builder("community.like.rabbitmq.dead_letter.audit.failures")
                .description("Failures while persisting or acknowledging dead-letter audit messages")
                .register(registry);

        // ---- 计数器：MySQL 持久化失败 ----
        persistenceFailures = Counter.builder("community.like.rabbitmq.consumer.persistence.failures")
                .description("MySQL like persistence invocations that rolled back or failed")
                .register(registry);

        // ---- 计时器 ----
        // 整个 callback 的耗时，开启直方图以观测 P50/P90/P99 分布
        batchDuration = Timer.builder("community.like.rabbitmq.consumer.batch.duration")
                .description("Whole consumer callback duration")
                .publishPercentileHistogram()
                .register(registry);
        // 仅 MySQL 事务耗时，同样开启直方图
        persistenceDuration = Timer.builder("community.like.rabbitmq.consumer.persistence.duration")
                .description("Time spent inside transactional MySQL persistence")
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
     *
     * <p>使用 {@link Gauge#builder} + 方法引用，使每次采集指标时才调用 {@link #queueDepth(String)}，
     * 而不是在注册时立即查询，避免启动阶段强依赖 broker 可用性。</p>
     *
     * <p>描述中说明 {@code -1} 表示查询不可用（如 broker 暂不可达），
     * 便于监控告警区分“真实为空”与“查询失败”。</p>
     *
     * @param registry   Micrometer 注册表
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
     *
     * <p>返回语义：</p>
     * <ul>
     *   <li>{@code >= 0}：队列 ready 消息数（不含 unacked/in-flight）；</li>
     *   <li>{@code -1}：查询不可用（队列不存在、broker 不可达或返回非法值）。</li>
     * </ul>
     *
     * <p>异常时返回 -1 而非抛出，避免指标采集异常影响业务或监控采集线程。</p>
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
     *
     * <p>对账只需要主队列 ready 为空；重试/DLQ 中的旧版本会由 pairVersion fence 保护，
     * 因此这里不把 retry/DLQ 纳入判断，避免旧版本残留导致对账窗口长期不可用。</p>
     *
     * @return true 表示主队列 ready 消息数为 0
     */
    public boolean mainQueueEmpty() { return queueDepth(properties.getQueue()) == 0; }

    /**
     * 判断主队列、延迟重试队列、死信队列是否都为空（ready 数均为 0）。
     *
     * <p>保留全链路空判断，供人工诊断使用；它比 {@link #mainQueueEmpty()} 更严格。
     * 注意此方法只反映 ready 消息；完整判断链路是否安静还需结合
     * {@link #inFlight()} 与 {@link #lastConsumerCompletedAtMs()}。</p>
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
     *
     * <p>在 consumer callback 入口调用；与 {@link #endBatch(int)} 必须成对出现，
     * 由批量消费者的 {@code finally} 保证。</p>
     *
     * @param size 本批消息数
     */
    public void beginBatch(int size) { inFlight.addAndGet(size); }

    /**
     * 批量处理结束：在途计数减去本批消息数，并记录完成时间戳。
     *
     * <p>时间戳供对账任务判断“consumer callback 是否已静默”，
     * 防止 broker/监听器仍在攒批时误入对账窗口。</p>
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
     *
     * <p>在批量事务成功 ACK 后，或逐条降级完成后调用；
     * 每次调用累加 {@code count} 条消息并记录一次耗时样本。
     * {@code count <= 0} 时不向 Counter 累加，避免传入非法值。</p>
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
     *
     * <p>由持久化包装方法的 finally 调用，成功与失败路径都会记录，
     * 便于定位 DB 慢查询。</p>
     *
     * @param nanos 持久化耗时（纳秒）
     */
    public void recordPersistence(long nanos) { persistenceDuration.record(nanos, TimeUnit.NANOSECONDS); }

    /** 记录一次 MySQL 持久化失败（事务回滚或抛异常）。 */
    public void recordPersistenceFailure() { persistenceFailures.increment(); }

    /** 记录一次事件级重试（消息已投递到延迟重试队列）。 */
    public void recordRetryAttempt() { retryAttempts.increment(); }

    /**
     * 记录一条消息成功发布到死信队列。
     *
     * <p>只有 Publisher Confirm + mandatory return 检查通过后才调用；
     * 同时递增新指标 {@code dead_letter.published} 与旧别名
     * {@code consumer.dead_lettered}，保证新旧 Prometheus 查询都能命中。</p>
     */
    public void recordDeadLetterPublished() {
        deadLetterPublished.increment();
        legacyDeadLetteredMessages.increment();
    }

    /**
     * 兼容旧调用点，语义等价于 {@link #recordDeadLetterPublished()}。
     *
     * <p>新代码应使用 {@code recordDeadLetterPublished()}；本方法仅为兼容
     * 既有消费者代码而保留，内部委托到新方法，因此同样会同步递增新旧两个计数器。</p>
     */
    @Deprecated
    public void recordDeadLettered() { recordDeadLetterPublished(); }

    /**
     * 死信审计 SQL 成功且 RabbitMQ ACK 成功后调用。
     *
     * <p>表示一条死信消息已完整落库到审计表并完成确认，可作为死信审计链路的成功计数。</p>
     */
    public void recordDeadLetterAuditPersisted() { deadLetterAuditPersisted.increment(); }

    /**
     * 记录一次死信审计失败。
     *
     * <p>用于统计死信消息落库或 ACK 过程中的失败次数，便于告警与排障。</p>
     */
    public void recordDeadLetterAuditFailure() { deadLetterAuditFailures.increment(); }
}