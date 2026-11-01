package com.zmt.picturebackendpro.service.impl.mq;

import com.rabbitmq.client.AMQP;
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
 *   <li><b>queue.depth</b>：仅统计 <i>ready</i> 消息数（未被投递给消费者的），
 *       不包含 unacked/in-flight 的消息；</li>
 *   <li><b>inflight</b>：当前 consumer callback 内正在处理的消息数（即已投递给消费者但尚未 ACK 的）；</li>
 *   <li><b>last_completed_epoch_ms</b>：最近一次 consumer callback 结束的时间戳；
 *       结合 queue.depth 与 inflight 可以解释"批量缓冲/消费中的消息"，
 *       避免仅凭 ready 数误判链路是否安静。</li>
 * </ul>
 * <p>
 * 通过 {@link ConditionalOnProperty} 限定只在 {@code community.like.mode=rabbitmq} 时装配。
 */
@Component
@ConditionalOnProperty(name = "community.like.mode", havingValue = "rabbitmq")
public class RabbitMqLikeRuntimeMetrics {

    /** RabbitAdmin 用于查询队列的 ready 消息数（被动声明，不修改队列） */
    private final RabbitAdmin rabbitAdmin;
    /** 点赞 MQ 配置：提供主队列名与死信队列名，用于注册队列深度 Gauge */
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
    /** 事件级重试次数（逐条降级中每次失败累加） */
    private final Counter retryAttempts;
    /** 达到最大失败次数、已复制到死信队列的消息数 */
    private final Counter deadLetteredMessages;
    /** 批次处理耗时（包含 MySQL 事务与 ACK） */
    private final Timer batchDuration;

    /**
     * 构造器：一次性注册所有指标。
     * <p>
     * 注意：Gauge 绑定的是本对象的 {@link AtomicInteger}/{@link AtomicLong} 引用，
     * 因此可以实时反映最新值，无需重复注册。
     *
     * @param rabbitAdmin RabbitAdmin（用于队列深度查询）
     * @param properties  点赞 MQ 配置（提供队列名）
     * @param registry    Micrometer 注册表
     */
    public RabbitMqLikeRuntimeMetrics(RabbitAdmin rabbitAdmin, RabbitMqLikeProperties properties,
                                      MeterRegistry registry) {
        this.rabbitAdmin = rabbitAdmin;
        this.properties = properties;
        this.processedMessages = Counter.builder("community.like.rabbitmq.consumer.messages")
                .description("RabbitMQ like events successfully persisted and ACKed")
                .register(registry);
        this.batchFailures = Counter.builder("community.like.rabbitmq.consumer.failures")
                .description("RabbitMQ like batch processing failures")
                .register(registry);
        this.retryAttempts = Counter.builder("community.like.rabbitmq.consumer.retry_attempts")
                .description("RabbitMQ like event retry attempts")
                .register(registry);
        this.deadLetteredMessages = Counter.builder("community.like.rabbitmq.consumer.dead_lettered")
                .description("RabbitMQ like messages copied to the dead-letter queue after max failures")
                .register(registry);
        this.batchDuration = Timer.builder("community.like.rabbitmq.consumer.batch.duration")
                .description("MySQL transaction and acknowledgement batch duration")
                .register(registry);
        // Gauge 直接读取原子字段，反映实时值
        Gauge.builder("community.like.rabbitmq.consumer.inflight", inFlight, AtomicInteger::get)
                .description("Messages inside the current consumer callback")
                .register(registry);
        Gauge.builder("community.like.rabbitmq.consumer.last_completed_epoch_ms", lastConsumerCompletedAtMs, AtomicLong::get)
                .description("Epoch milliseconds when the last consumer batch completed")
                .register(registry);
        // 主队列与死信队列的 ready 深度 Gauge
        registerQueueGauge(registry, "community.like.rabbitmq.queue.depth", properties.getQueue());
        registerQueueGauge(registry, "community.like.rabbitmq.dead.depth", properties.getDeadQueue());
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
     * @param name     Gauge 名称
     * @param queue    队列名
     */
    private void registerQueueGauge(MeterRegistry registry, String name, String queue) {
        Gauge.builder(name, this, metrics -> metrics.queueDepth(queue))
                .description("Ready messages in RabbitMQ queue " + queue + "; -1 means query unavailable")
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
            Properties p = rabbitAdmin.getQueueProperties(queue);
            if (p == null) return -1;
            Object count = p.get(RabbitAdmin.QUEUE_MESSAGE_COUNT);
            return count instanceof Number ? ((Number) count).intValue() : -1;
        } catch (RuntimeException e) {
            return -1;
        }
    }

    /**
     * 判断主队列与死信队列是否都为空（ready 数均为 0）。
     * <p>
     * 注意：此方法只反映 ready 消息；还需结合 {@link #inFlight()} 与
     * {@link #lastConsumerCompletedAtMs()} 才能完整判断链路是否安静。
     * 对账服务 {@code isSafeWindow} 会同时使用这三者。
     *
     * @return true 表示两个队列的 ready 消息数都为 0
     */
    public boolean allQueuesEmpty() {
        return queueDepth(properties.getQueue()) == 0
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
     *
     * @param count 本次成功处理的消息数
     * @param nanos 本次处理耗时（纳秒）
     */
    public void recordProcessed(int count, long nanos) {
        processedMessages.increment(count);
        batchDuration.record(nanos, TimeUnit.NANOSECONDS);
    }

    /** 记录一次批量事务失败（触发逐条降级）。 */
    public void recordFailure() { batchFailures.increment(); }

    /** 记录一次事件级重试（逐条降级中发生）。 */
    public void recordRetryAttempt() { retryAttempts.increment(); }

    /** 记录一条消息达到最大重试次数并已复制到死信队列。 */
    public void recordDeadLettered() { deadLetteredMessages.increment(); }
}