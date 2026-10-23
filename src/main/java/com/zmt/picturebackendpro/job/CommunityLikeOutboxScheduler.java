package com.zmt.picturebackendpro.job;

import com.zmt.picturebackendpro.exception.CommunityLikeBatchProcessingException;
import com.zmt.picturebackendpro.service.impl.CommunityLikeOutboxFailureService;
import com.zmt.picturebackendpro.service.impl.CommunityLikeOutboxProcessor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;

/**
 * 4 个独立调度任务共享一个 4 线程 TaskScheduler；MySQL SKIP LOCKED 确保同一时刻不重复领取已锁事件。
 * 每个 worker 每轮处理一批，batch-size 和 poll-delay 需通过压测调整。
 */
@Slf4j
@Component
public class CommunityLikeOutboxScheduler {

    @Resource private CommunityLikeOutboxProcessor processor;
    @Resource private CommunityLikeOutboxFailureService failureService;

    @Scheduled(fixedDelayString = "${community.like.outbox.poll-delay-ms:100}")
    public void worker1() { poll("worker-1"); }

    @Scheduled(fixedDelayString = "${community.like.outbox.poll-delay-ms:100}", initialDelay = 25)
    public void worker2() { poll("worker-2"); }

    @Scheduled(fixedDelayString = "${community.like.outbox.poll-delay-ms:100}", initialDelay = 50)
    public void worker3() { poll("worker-3"); }

    @Scheduled(fixedDelayString = "${community.like.outbox.poll-delay-ms:100}", initialDelay = 75)
    public void worker4() { poll("worker-4"); }

    private void poll(String workerName) {
        try {
            processor.processNextBatch();
        } catch (CommunityLikeBatchProcessingException e) {
            String detail = e.getCause() == null ? e.getMessage() : e.getCause().getMessage();
            try {
                failureService.recordFailure(e.getEventIds(), detail);
            } catch (Exception failureRecordException) {
                // 失败记录本身失败时不能吞掉告警；原事件事务已回滚，后续调度仍会重新读取它们。
                log.error("{} record Outbox failure failed, eventIds={}", workerName, e.getEventIds(), failureRecordException);
            }
            log.error("{} processed like Outbox batch failed, eventIds={}", workerName, e.getEventIds(), e);
        } catch (Exception e) {
            log.error("{} poll like Outbox failed before batch handling", workerName, e);
        }
    }
}
