package com.zmt.picturebackendpro.service.impl;

import com.zmt.picturebackendpro.mapper.CommunityLikeOutboxMapper;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.springframework.stereotype.Component;

/**
 * Community Like Outbox 整体业务指标监控组件。
 * 负责收集批次耗时、处理总数、失败重试数，以及通过 Gauge 实时监控数据库中的积压状态。
 * 注意：Outbox 指标供 Actuator / Prometheus 抓取。切记不要把 metrics/prometheus 端点直接暴露到公网。
 */
@Component
public class CommunityLikeOutboxMetrics {

    private final MeterRegistry registry;
    private final Timer batchTimer;
    private final Counter processedEvents;
    private final Counter failedBatches;

    /**
     * 构造函数：初始化并注册各项 Outbox 核心指标
     *
     * @param registry Micrometer 注册中心
     * @param mapper   Outbox 数据访问层，用于提供 Gauge 的实时状态数据源
     */
    public CommunityLikeOutboxMetrics(MeterRegistry registry, CommunityLikeOutboxMapper mapper) {
        this.registry = registry;

        // 1. 注册批次耗时直方图/计时器（用于监控批次吞吐与耗时分布）
        this.batchTimer = Timer.builder("community.like.outbox.batch.duration")
                .description("点赞 Outbox 一批事件的处理耗时")
                .register(registry);

        // 2. 注册成功完成的事件计数器
        this.processedEvents = Counter.builder("community.like.outbox.events.processed")
                .description("成功完成的点赞 Outbox 事件总数")
                .register(registry);

        // 3. 注册批次失败需要重试的计数器
        this.failedBatches = Counter.builder("community.like.outbox.batches.failed")
                .description("失败并需要重试的点赞 Outbox 批次数")
                .register(registry);

        // 4. 注册仪表盘（Gauge）：监控当前数据库中待处理（Pending）的事件总数
        Gauge.builder("community.like.outbox.pending", mapper, CommunityLikeOutboxMapper::countPendingEvents)
                .description("待处理点赞 Outbox 事件数")
                .register(registry);

        // 5. 注册仪表盘（Gauge）：监控进入死信状态（Dead）的事件总数（需要人工介入或告警）
        Gauge.builder("community.like.outbox.dead", mapper, CommunityLikeOutboxMapper::countDeadEvents)
                .description("进入死信状态的点赞 Outbox 事件数")
                .register(registry);

        // 6. 注册仪表盘（Gauge）：监控最早一条未处理事件的积压时间（秒），防范队列严重滞后
        Gauge.builder("community.like.outbox.oldest.pending.seconds", mapper, CommunityLikeOutboxMapper::oldestPendingAgeSeconds)
                .description("最早待处理点赞 Outbox 事件的年龄（秒）")
                .register(registry);
    }

    /**
     * 启动一个批次处理的计时器采样
     *
     * @return Timer.Sample 采样对象，用于在批次结束时停止计时
     */
    public Timer.Sample startBatchTimer() {
        return Timer.start(registry);
    }

    /**
     * 记录当前批次成功完成
     *
     * @param sample         计时器采样对象
     * @param processedCount 本批次实际成功处理的事件数量
     */
    public void recordBatchCompleted(Timer.Sample sample, int processedCount) {
        // 停止计时并记录到 batchTimer 中
        sample.stop(batchTimer);
        if (processedCount > 0) {
            // 累加成功事件数
            processedEvents.increment(processedCount);
        }
    }

    /**
     * 记录当前批次执行失败（将触发重试）
     *
     * @param sample 计时器采样对象
     */
    public void recordBatchFailed(Timer.Sample sample) {
        // 即使失败也停止计时（记录失败批次的耗时分布）
        sample.stop(batchTimer);
        // 累加失败批次计数
        failedBatches.increment();
    }
}