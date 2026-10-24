    package com.zmt.picturebackendpro.service.impl;

import com.zmt.picturebackendpro.mapper.CommunityLikeOutboxMapper;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.springframework.stereotype.Component;

/** Outbox 指标供 Actuator / Prometheus 抓取。不要把 metrics/prometheus 端点直接暴露到公网。 */
@Component
public class CommunityLikeOutboxMetrics {

    private final MeterRegistry registry;
    private final Timer batchTimer;
    private final Counter processedEvents;
    private final Counter failedBatches;

    public CommunityLikeOutboxMetrics(MeterRegistry registry, CommunityLikeOutboxMapper mapper) {
        this.registry = registry;
        this.batchTimer = Timer.builder("community.like.outbox.batch.duration")
                .description("点赞 Outbox 一批事件的处理耗时")
                .register(registry);
        this.processedEvents = Counter.builder("community.like.outbox.events.processed")
                .description("成功完成的点赞 Outbox 事件总数")
                .register(registry);
        this.failedBatches = Counter.builder("community.like.outbox.batches.failed")
                .description("失败并需要重试的点赞 Outbox 批次数")
                .register(registry);

        Gauge.builder("community.like.outbox.pending", mapper, CommunityLikeOutboxMapper::countPendingEvents)
                .description("待处理点赞 Outbox 事件数")
                .register(registry);
        Gauge.builder("community.like.outbox.dead", mapper, CommunityLikeOutboxMapper::countDeadEvents)
                .description("进入死信状态的点赞 Outbox 事件数")
                .register(registry);
        Gauge.builder("community.like.outbox.oldest.pending.seconds", mapper, CommunityLikeOutboxMapper::oldestPendingAgeSeconds)
                .description("最早待处理点赞 Outbox 事件的年龄（秒）")
                .register(registry);
    }

    public Timer.Sample startBatchTimer() {
        return Timer.start(registry);
    }

    public void recordBatchCompleted(Timer.Sample sample, int processedCount) {
        sample.stop(batchTimer);
        if (processedCount > 0) {
            processedEvents.increment(processedCount);
        }
    }

    public void recordBatchFailed(Timer.Sample sample) {
        sample.stop(batchTimer);
        failedBatches.increment();
    }
}
