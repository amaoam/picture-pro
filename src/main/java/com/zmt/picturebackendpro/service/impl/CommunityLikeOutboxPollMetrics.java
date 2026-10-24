package com.zmt.picturebackendpro.service.impl;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.springframework.stereotype.Component;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Community Like Outbox 轮询调度层的指标监控组件。
 * 注意：worker 标签只有 worker-1..worker-N，保持低基数；禁止给指标添加用户 ID、帖子 ID。
 */
@Component
public class CommunityLikeOutboxPollMetrics {

    /** Micrometer 注册中心，用于统一注册、管理所有的监控指标 */
    private final MeterRegistry registry;

    /** 缓存每个 worker 对应的指标实例，确保线程安全并避免重复注册 */
    private final ConcurrentMap<String, WorkerMeters> workers = new ConcurrentHashMap<>();

    /**
     * 构造函数注入 MeterRegistry
     * @param registry Micrometer 注册表
     */
    public CommunityLikeOutboxPollMetrics(MeterRegistry registry) {
        this.registry = registry;
    }

    /**
     * 记录一次 Outbox 轮询的执行结果和相关性能指标
     *
     * @param workerName      工作线程名称（作为标签，如 worker-1）
     * @param processedEvents 本次轮询成功处理的事件数量
     * @param failed          本次轮询是否执行失败
     * @param durationNanos   本次轮询耗时（纳秒）
     * @param nextDelayMs     下一次轮询的延迟时间（毫秒）
     */
    public void recordPoll(String workerName, int processedEvents, boolean failed,
                           long durationNanos, long nextDelayMs) {
        // 获取或初始化对应 worker 的指标对象容器
        WorkerMeters meters = workers.computeIfAbsent(workerName, this::createWorkerMeters);

        // 1. 累计总轮询次数
        meters.polls.increment();

        if (failed) {
            // 2. 累计轮询失败次数
            meters.failures.increment();
        } else if (processedEvents == 0) {
            // 3. 累计空轮询次数（即没有拉取到可处理的事件）
            meters.emptyPolls.increment();
        }

        if (processedEvents > 0) {
            // 4. 累计成功处理的事件总数
            meters.processedEvents.increment(processedEvents);
        }

        // 5. 记录轮询耗时（确保时间非负）
        meters.pollDuration.record(Math.max(0L, durationNanos), TimeUnit.NANOSECONDS);

        // 6. 更新下一次轮询的延迟时间（供 Gauge 动态读取）
        meters.nextDelayMs.set(Math.max(0L, nextDelayMs));
    }

    /**
     * 为特定的 worker 创建并注册一组 Micrometer 监控指标
     *
     * @param workerName 工作线程名称
     * @return 封装了该 worker 各类指标的 WorkerMeters 实例
     */
    private WorkerMeters createWorkerMeters(String workerName) {
        // 定义低基数字签，严格控制标签维度，防止时序数据库爆内存
        String[] tags = new String[] {"worker", workerName};

        WorkerMeters meters = new WorkerMeters(
                // 计数器：总轮询尝试次数
                Counter.builder("community.like.outbox.polls")
                        .description("Outbox worker polling attempts")
                        .tags(tags).register(registry),

                // 计数器：未发现可处理事件的空轮询次数
                Counter.builder("community.like.outbox.polls.empty")
                        .description("Outbox polls that found no processable events")
                        .tags(tags).register(registry),

                // 计数器：轮询周期失败次数
                Counter.builder("community.like.outbox.polls.failed")
                        .description("Outbox poll cycles that failed")
                        .tags(tags).register(registry),

                // 计数器：每个 worker 处理的 Outbox 事件总数
                Counter.builder("community.like.outbox.poll.events.processed")
                        .description("Outbox events processed per worker")
                        .tags(tags).register(registry),

                // 计时器：单次 Outbox worker 轮询周期耗时分布
                Timer.builder("community.like.outbox.poll.duration")
                        .description("Duration of one Outbox worker cycle")
                        .tags(tags).register(registry),

                // 用于 Gauge 绑定的原子长整型变量
                new AtomicLong());

        // 仪表盘（Gauge）：监控下一次轮询的动态延迟时间（毫秒）
        Gauge.builder("community.like.outbox.poll.delay.ms", meters.nextDelayMs, AtomicLong::get)
                .description("Delay before the next Outbox poll in milliseconds")
                .tags(tags).register(registry);

        return meters;
    }

    /**
     * 内部静态类：聚合单个 worker 所需的所有指标组件，便于集中管理
     */
    private static final class WorkerMeters {
        private final Counter polls;           // 总轮询次数
        private final Counter emptyPolls;      // 空轮询次数
        private final Counter failures;        // 失败次数
        private final Counter processedEvents; // 处理事件总数
        private final Timer pollDuration;      // 轮询耗时
        private final AtomicLong nextDelayMs;  // 下一次轮询延迟值

        private WorkerMeters(Counter polls, Counter emptyPolls, Counter failures,
                             Counter processedEvents, Timer pollDuration, AtomicLong nextDelayMs) {
            this.polls = polls;
            this.emptyPolls = emptyPolls;
            this.failures = failures;
            this.processedEvents = processedEvents;
            this.pollDuration = pollDuration;
            this.nextDelayMs = nextDelayMs;
        }
    }
}