package com.zmt.picturebackendpro.service.impl.mq;

import com.zmt.picturebackendpro.config.RabbitMqLikeProperties;
import com.zmt.picturebackendpro.service.redis.LikeRedisKeyFactory;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * 点赞数据定时对账服务（RabbitMQ 模式）。
 * <p>
 * 目的：在<b>安静窗口</b>内，按 Redis 当前状态修复 MySQL，并同步 pairVersion 水位。
 * 典型修复场景：消息丢失、消费失败进入死信、Redis 与 DB 短暂不一致等。
 * <p>
 * 执行时机（关键约束）——仅在链路安静时执行，避免与实时写操作竞争：
 * <ol>
 *   <li><b>API 静默</b>：最近一次点赞请求距今 ≥ quietPeriodMs
 *       （{@link RabbitMqLikeActivity#lastRequestAtMs()}）；</li>
 *   <li><b>Relay 已排空</b>：Stream relay 无待发布事件（{@code relay.isDrained()}）；</li>
 *   <li><b>无在途消费</b>：消费者 in-flight 计数为 0（{@code metrics.inFlight() == 0}）；</li>
 *   <li><b>主队列为空</b>：主队列 Ready 消息数为 0（{@code metrics.mainQueueEmpty()}）；</li>
 *   <li><b>Consumer callback 静默</b>：最近一次消费完成距今 ≥ quietPeriodMs
 *       （{@code metrics.lastConsumerCompletedAtMs()}）。</li>
 * </ol>
 * <p>
 * <b>与死信/延迟重试队列的关系（本版本的核心取舍）</b>：
 * 本实现<b>不把</b> retry/DLQ 的积压纳入静默判断，只要求主队列 Ready 为空。
 * 原因：死信/延迟重试队列可能长期保存"已被更高 pairVersion 覆盖的旧事件"，
 * 若要求它们也清空，对账可能被永久阻塞。
 * <p>
 * 安全性由 pairVersion fence 保证：对账会把 Redis 的 liked Set 与 versions Hash
 * 同步到 MySQL 并抬高 {@code community_like_pair_state} 水位；旧消息即使之后被重放，
 * 也会被 {@code LikePairVersionPolicy} 判为 {@code STALE} 而忽略，不会把状态改回去。
 * <p>
 * 并发控制：
 * <ul>
 *   <li>{@link #running} 保证同一 JVM 内对账任务不重叠；</li>
 *   <li>{@link RabbitMqLikeActivity#withExclusive} 获取写锁，阻塞所有点赞读请求；</li>
 *   <li>获得写锁后<b>二次确认</b>静默条件，防止在等待锁期间又有新请求进来。</li>
 * </ul>
 * <p>
 * 通过 {@link ConditionalOnProperty} 限定只在 {@code community.like.mode=rabbitmq} 时启用。
 */
@Slf4j
@Component
@ConditionalOnProperty(name = "community.like.mode", havingValue = "rabbitmq")
public class RabbitMqLikeReconciliationService {

    /** 点赞专用 Redis 模板（与全局模板隔离），用于读取分片 targets 集合 */
    @Resource(name = "likeStringRedisTemplate") private StringRedisTemplate redisTemplate;

    /** 点赞 MQ 配置：静默周期、对账初始延迟与间隔等 */
    @Resource private RabbitMqLikeProperties mqProperties;

    /** Redis key 工厂：提供分片数与分片 targets key 前缀 */
    @Resource private LikeRedisKeyFactory keyFactory;

    /** 请求/对账并发协调器：提供写锁 {@code withExclusive} 与最近请求时间 */
    @Resource private RabbitMqLikeActivity activity;

    /** Stream relay：提供"是否已排空"和"最近发布时间" */
    @Resource private RabbitMqLikeStreamRelay relay;

    /** 运行时指标：in-flight、主队列是否为空、最近消费完成时间 */
    @Resource private RabbitMqLikeRuntimeMetrics metrics;

    /** 对账 worker：实际执行单个 target 的关系/计数/水位修复 */
    @Resource private RabbitMqLikeReconciliationWorker worker;

    /** Micrometer 注册表：用于注册对账修复计数指标 */
    @Resource private MeterRegistry meterRegistry;

    /**
     * 对账任务互斥标志。
     * <p>
     * 使用 CAS 保证同一 JVM 内同一时刻只有一个对账 pass 在执行；
     * 即使调度线程重叠（上一轮耗时超过间隔），也不会并发对账。
     * 多实例部署时，这里不提供跨实例互斥——跨实例语义由写锁
     * {@link RabbitMqLikeActivity#withExclusive} 承担。
     */
    private final AtomicBoolean running = new AtomicBoolean(false);

    /**
     * 定时对账入口。
     * <p>
     * 调度配置：
     * <ul>
     *   <li>{@code initialDelay}：启动后延迟（默认 120s），避免应用刚启动就与预热流量冲突；</li>
     *   <li>{@code fixedDelay}：上一轮结束后再等待（默认 300s），保证轮次间有充足间隔。</li>
     * </ul>
     * <p>
     * 执行流程：
     * <ol>
     *   <li>CAS 抢占 {@link #running}，防止重叠；</li>
     *   <li>第一次静默检查（不持锁，快速失败，避免无谓地争抢写锁）；</li>
     *   <li>获取写锁（阻塞所有点赞请求）；</li>
     *   <li>持锁后<b>再次</b>检查静默条件，防止等待锁期间有新请求进入；</li>
     *   <li>遍历所有分片的 targets 集合，逐个 target 调用 worker 修复；</li>
     *   <li>汇总修复数量并记录指标与日志；</li>
     *   <li>finally 中释放 {@link #running}。</li>
     * </ol>
     * 单个 target 修复失败只记录日志，不中断整轮对账
     * （避免一个坏 target 阻塞全局修复）。
     */
    @Scheduled(initialDelayString = "${community.like.rabbitmq.reconcile-initial-delay-ms:120000}",
            fixedDelayString = "${community.like.rabbitmq.reconcile-interval-ms:300000}")
    public void reconcileIfIdle() {
        // CAS 保证同 JVM 内不重叠；已有对账在跑则直接跳过本轮
        if (!running.compareAndSet(false, true)) return;
        try {
            // 第一次静默检查：不持锁，快速判断是否需要进入对账
            if (!isSafeWindow()) return;
            // 用于在 lambda 中累加修复数量（lambda 捕获的局部变量必须 effectively final）
            final int[] repaired = {0};
            activity.withExclusive(() -> {
                // 获取独占锁后再确认，避免等待锁期间又有写请求进入。
                if (!isSafeWindow()) return;
                // 按分片遍历，避免单次遍历全部 target 造成长时间持锁
                for (int shard = 0; shard < keyFactory.getShardCount(); shard++) {
                    String registryKey = keyFactory.shardPrefix(shard) + ":targets";
                    Set<String> targets = redisTemplate.opsForSet().members(registryKey);
                    if (targets == null || targets.isEmpty()) continue;
                    for (String token : targets) {
                        // 单 target 失败不影响其它 target 的对账
                        try {
                            repaired[0] += worker.reconcileTarget(token);
                        } catch (Exception exception) {
                            log.error("Like reconciliation failed for target {}", token, exception);
                        }
                    }
                }
            });
            // 仅在有修复时记录指标，避免产生大量零值样本
            if (repaired[0] > 0) {
                Counter.builder("community.like.rabbitmq.reconciliation.repaired_relations")
                        .description("Like relations repaired by scheduled reconciliation")
                        .register(meterRegistry).increment(repaired[0]);
            }
            log.info("RabbitMQ like reconciliation completed; repaired relation changes={}", repaired[0]);
        } catch (Exception exception) {
            log.error("RabbitMQ like reconciliation pass failed", exception);
        } finally {
            // 无论成功失败都释放互斥标志，允许下一轮调度
            running.set(false);
        }
    }

    /**
     * 判断当前是否处于"安全窗口"（可以执行对账）。
     * <p>
     * 安全窗口需要同时满足以下全部条件：
     * <ol>
     *   <li><b>API / relay / consumer 三类活动均已静默</b>：三者最近时间戳距 now
     *       都 ≥ {@code quietPeriodMs}；</li>
     *   <li><b>Relay 已排空</b>：Stream relay 无待发布事件；</li>
     *   <li><b>无在途消息</b>：消费者 in-flight 计数为 0；</li>
     *   <li><b>主队列为空</b>：主队列 Ready 消息数为 0
     *       （{@link RabbitMqLikeRuntimeMetrics#mainQueueEmpty()}）；</li>
     *   <li><b>Consumer callback 静默</b>：最近一次消费完成距今 ≥ {@code quietPeriodMs}，
     *       防止批量监听器仍在攒批或 broker 正在推送。</li>
     * </ol>
     * <p>
     * <b>为什么只检查主队列、不检查死信/延迟重试队列</b>：
     * <ul>
     *   <li>死信/延迟重试队列可能长期保存"已被更高 pairVersion 覆盖的旧事件"，
     *       若要求它们也清空，对账可能被永久阻塞；</li>
     *   <li>本服务在 {@link RabbitMqLikeReconciliationWorker} 中会把 Redis 的
     *       liked Set 与 versions Hash 同步到 MySQL，并抬高
     *       {@code community_like_pair_state} 水位；</li>
     *   <li>旧消息之后即使被重放，也会被 {@code LikePairVersionPolicy} 判为
     *       {@code STALE} 而忽略，因此无需等待 retry/DLQ 清空。</li>
     * </ul>
     * 因此本方法使用 {@code metrics.mainQueueEmpty()}，而不是
     * {@code metrics.allQueuesEmpty()}；后者会把 retry/DLQ 纳入判断，
     * 与"不被旧消息阻塞"的设计目标冲突。
     * <p>
     * 为什么需要 consumer callback 静默这一条：即使队列为空、in-flight 为 0，
     * 批量监听器可能刚完成一批回调，下一批可能已在 broker 侧准备推送；
     * 要求最近一次 consumer callback 也安静一段时间，才能真正确认链路已停止活动。
     * <p>
     * 实现细节：
     * <ul>
     *   <li>{@code quietMs} 做 1000ms 兜底，防止配置为 0 导致误判为"永远安全"；</li>
     *   <li>{@code latestActivity} 取 API/relay/consumer 三者最大时间戳，
     *       任一活跃则不进入对账；</li>
     *   <li>时间戳为 0 视为"从未活动"，此时不因该维度阻塞。</li>
     * </ul>
     *
     * @return true 表示当前可以安全执行对账
     */
    private boolean isSafeWindow() {
        // 兜底 1s：配置为 0 或负数时不能把"永远安全"误判出来
        long quietMs = Math.max(1000L, mqProperties.getQuietPeriodMs());
        long now = System.currentTimeMillis();
        // 取 API 请求、relay 发布、consumer 完成三者中最近的时间戳
        long latestActivity = Math.max(activity.lastRequestAtMs(),
                Math.max(relay.lastPublishedAtMs(), metrics.lastConsumerCompletedAtMs()));
        if (latestActivity > 0L && now - latestActivity < quietMs) return false;
        // 链路必须排空：relay 无待发、无在途消费、主队列 Ready 为空。
        // 注意这里刻意使用 mainQueueEmpty() 而非 allQueuesEmpty()：
        // retry/DLQ 中的旧事件由 pairVersion fence 拦截，不应阻塞对账。
        if (!relay.isDrained() || metrics.inFlight() != 0 || !metrics.mainQueueEmpty()) return false;
        // 防止 broker/批量监听器仍在等待一批消息：最近一次 consumer callback 也必须安静一段时间。
        long lastConsumer = metrics.lastConsumerCompletedAtMs();
        return lastConsumer == 0L || now - lastConsumer >= quietMs;
    }
}