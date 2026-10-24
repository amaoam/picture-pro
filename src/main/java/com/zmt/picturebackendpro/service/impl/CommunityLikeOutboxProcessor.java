package com.zmt.picturebackendpro.service.impl;

import com.zmt.picturebackendpro.exception.CommunityLikeBatchProcessingException;
import com.zmt.picturebackendpro.mapper.CommunityLikeOutboxMapper;
import com.zmt.picturebackendpro.model.entity.CommunityLikeOutboxEvent;
import com.zmt.picturebackendpro.service.CommunityExperienceService;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import io.micrometer.core.instrument.Timer;
import javax.annotation.Resource;
import java.util.*;
import java.util.stream.Collectors;

/**
 * 批量消费点赞 Outbox。
 * 锁定事件、更新计数、更新作者获赞统计/经验、标记事件完成都在同一个事务中。
 */
@Service
public class CommunityLikeOutboxProcessor {

    @Value("${community.like.outbox.target-batch-size:20}")
    private int configuredTargetBatchSize;
    @Value("${community.like.outbox.max-events-per-target:100}")
    private int configuredMaxEventsPerTarget;

    @Resource private CommunityLikeOutboxMapper outboxMapper;
    @Resource private CommunityExperienceService experienceService;
    @Resource private CommunityLikeOutboxMetrics metrics;

    @Transactional(rollbackFor = Exception.class)
    public int processNextBatch() {
        Timer.Sample timer = metrics.startBatchTimer();
        List<Long> retryIds = Collections.emptyList();
        try {
            int targetBatchSize = clamp(configuredTargetBatchSize, 1, 200);
            int maxEventsPerTarget = clamp(configuredMaxEventsPerTarget, 1, 1000);

            // 第一步：每个目标最多领取最早的一条到期事件。SKIP LOCKED 让其他 worker 处理其他目标。
            List<CommunityLikeOutboxEvent> heads = outboxMapper.selectPendingTargetHeads(targetBatchSize);
            if (heads == null || heads.isEmpty()) {
                metrics.recordBatchCompleted(timer, 0);
                return 0;
            }
            retryIds = idsOf(heads);

            Set<Long> postIdSet = new LinkedHashSet<>();
            Set<Long> pictureIdSet = new LinkedHashSet<>();
            for (CommunityLikeOutboxEvent head : heads) {
                if (CommunityLikeOutboxEvent.TARGET_POST.equals(head.getTargetType())) {
                    postIdSet.add(head.getTargetId());
                } else if (CommunityLikeOutboxEvent.TARGET_PICTURE.equals(head.getTargetType())) {
                    pictureIdSet.add(head.getTargetId());
                } else {
                    throw new IllegalStateException("未知的点赞目标类型，eventId=" + head.getId());
                }
            }

            // 第二步：为每个已锁定目标取一个有限的事件前缀。查询也带锁，避免读到过期事件状态。
            List<CommunityLikeOutboxEvent> candidates = outboxMapper.selectEventsForTargets(
                    new ArrayList<>(postIdSet), new ArrayList<>(pictureIdSet), maxEventsPerTarget);

            // 第三步：同 target 遇到未来重试时间或死信时，不能越过该事件处理后续事件。
            List<CommunityLikeOutboxEvent> events = filterProcessablePrefix(candidates);
            if (events.isEmpty()) {
                metrics.recordBatchCompleted(timer, 0);
                return 0;
            }
            retryIds = idsOf(events);

            Map<Long, Long> postDeltas = new LinkedHashMap<>();
            Map<Long, Long> pictureDeltas = new LinkedHashMap<>();
            for (CommunityLikeOutboxEvent event : events) {
                if (event.getTargetId() == null || event.getDelta() == null
                        || (event.getDelta() != 1 && event.getDelta() != -1)) {
                    throw new IllegalStateException("非法点赞事件，eventId=" + event.getId());
                }
                Map<Long, Long> deltas;
                if (CommunityLikeOutboxEvent.TARGET_POST.equals(event.getTargetType())) {
                    deltas = postDeltas;
                } else if (CommunityLikeOutboxEvent.TARGET_PICTURE.equals(event.getTargetType())) {
                    deltas = pictureDeltas;
                } else {
                    throw new IllegalStateException("未知的点赞目标类型，eventId=" + event.getId());
                }
                deltas.merge(event.getTargetId(), event.getDelta().longValue(), Long::sum);
            }

            // 同一批内 +1 和 -1 抵消后不执行无意义的 UPDATE，但事件仍然确认完成。
            postDeltas.entrySet().removeIf(entry -> entry.getValue() == 0L);
            pictureDeltas.entrySet().removeIf(entry -> entry.getValue() == 0L);

            if (!postDeltas.isEmpty()) {
                List<Long> ids = outboxMapper.selectExistingPostIds(new ArrayList<>(postDeltas.keySet()));
                if (ids.size() != postDeltas.size()) {
                    throw new IllegalStateException("点赞事件引用了不存在的帖子记录");
                }
                outboxMapper.batchAdjustPostLikeCounts(postDeltas);
            }
            if (!pictureDeltas.isEmpty()) {
                List<Long> ids = outboxMapper.selectExistingPictureIds(new ArrayList<>(pictureDeltas.keySet()));
                if (ids.size() != pictureDeltas.size()) {
                    throw new IllegalStateException("点赞事件引用了不存在的图片记录");
                }
                outboxMapper.batchAdjustPictureLikeCounts(pictureDeltas);
            }

            // 经验、作者获赞数量在同一个事务中批量维护，使用 eventKey 防止重试造成重复奖励。
            experienceService.processLikeEvents(events);

            int marked = outboxMapper.markProcessed(retryIds);
            if (marked != retryIds.size()) {
                throw new IllegalStateException("确认 Outbox 事件数量不匹配，预期="
                        + retryIds.size() + "，实际=" + marked);
            }

            metrics.recordBatchCompleted(timer, events.size());
            return events.size();
        } catch (RuntimeException e) {
            metrics.recordBatchFailed(timer);
            // 此异常在事务方法返回前抛出，确保计数和经验回滚；Scheduler 随后用 REQUIRES_NEW 记录重试。
            throw new CommunityLikeBatchProcessingException(retryIds, e);
        }
    }

    /**
     * 从全局按 ID 排序的候选事件中，取每个 target 的连续可处理前缀。
     * 一旦遇到死信或未到 nextRetryTime，该 target 的更晚事件全部留待之后处理。
     * 包级可见，方便测试，不要求启动 Spring 容器。
     */
    static List<CommunityLikeOutboxEvent> filterProcessablePrefix(List<CommunityLikeOutboxEvent> candidates) {
        if (candidates == null || candidates.isEmpty()) {
            return Collections.emptyList();
        }
        List<CommunityLikeOutboxEvent> ordered = new ArrayList<>(candidates);
        ordered.sort(Comparator.comparing(CommunityLikeOutboxEvent::getId));

        List<CommunityLikeOutboxEvent> result = new ArrayList<>();
        Set<String> blockedTargets = new HashSet<>();
        for (CommunityLikeOutboxEvent event : ordered) {
            String key = targetKey(event);
            if (blockedTargets.contains(key)) {
                continue;
            }
            if (!Objects.equals(event.getStatus(), CommunityLikeOutboxEvent.STATUS_PENDING)
                    || !Boolean.TRUE.equals(event.getRetryDue())) {
                blockedTargets.add(key);
                continue;
            }
            result.add(event);
        }
        return result;
    }

    private static String targetKey(CommunityLikeOutboxEvent event) {
        return String.valueOf(event.getTargetType()) + ":" + String.valueOf(event.getTargetId());
    }
    //如果value大于max取max，小于min取min，之间则取value
    private static int clamp(int value, int min, int max) {
        return Math.max(min, Math.min(value, max));
    }

    private static List<Long> idsOf(List<CommunityLikeOutboxEvent> events) {
        return events.stream()
                .filter(Objects::nonNull)             
                .map(CommunityLikeOutboxEvent::getId)
                .filter(Objects::nonNull)            
                .toList();
    }
}
