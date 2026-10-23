package com.zmt.picturebackendpro.service.impl;

import com.zmt.picturebackendpro.exception.CommunityLikeBatchProcessingException;
import com.zmt.picturebackendpro.mapper.CommunityLikeOutboxMapper;
import com.zmt.picturebackendpro.model.entity.CommunityLikeOutboxEvent;
import com.zmt.picturebackendpro.service.CommunityExperienceService;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.Resource;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * 批量消费点赞 Outbox。
 * 锁定事件、更新计数、更新作者获赞统计/经验、标记事件完成都在同一个事务中。
 */
@Service
public class CommunityLikeOutboxProcessor {

    @Value("${community.like.outbox.batch-size:200}")
    private int configuredBatchSize;

    @Resource private CommunityLikeOutboxMapper outboxMapper;
    @Resource private CommunityExperienceService experienceService;

    @Transactional(rollbackFor = Exception.class)
    public int processNextBatch() {
        int batchSize = Math.max(1, Math.min(configuredBatchSize, 1000));
        //分批拉取待处理的点赞事件
        List<CommunityLikeOutboxEvent> events = outboxMapper.selectPendingBatchForUpdate(batchSize);
        if (events == null || events.isEmpty()) {
            return 0;
        }
        List<Long> eventIds = events.stream().map(CommunityLikeOutboxEvent::getId).collect(Collectors.toList());

        try {
            Map<Long, Long> postDeltas = new LinkedHashMap<>();
            Map<Long, Long> pictureDeltas = new LinkedHashMap<>();
            for (CommunityLikeOutboxEvent event : events) {
                if (event.getTargetId() == null || event.getDelta() == null
                        || (event.getDelta() != 1 && event.getDelta() != -1)) {
                    throw new IllegalStateException("非法点赞事件，eventId=" + event.getId());
                }
                Map<Long, Long> targetDeltas;
                if (CommunityLikeOutboxEvent.TARGET_POST.equals(event.getTargetType())) {
                    targetDeltas = postDeltas;
                } else if (CommunityLikeOutboxEvent.TARGET_PICTURE.equals(event.getTargetType())) {
                    targetDeltas = pictureDeltas;
                } else {
                    throw new IllegalStateException("未知的点赞目标类型，eventId=" + event.getId());
                }
                targetDeltas.merge(event.getTargetId(), event.getDelta().longValue(), Long::sum);
            }

            // 合并后增减量为 0 的目标无需执行 UPDATE，避免一次无意义的热点行更新。
            postDeltas.entrySet().removeIf(entry -> entry.getValue() == 0L);
            pictureDeltas.entrySet().removeIf(entry -> entry.getValue() == 0L);

            // 先确认目标仍存在，防止物理删除的数据让事件永久重试。逻辑删除的数据仍保留主键，仍会正常计数。
            if (!postDeltas.isEmpty()) {
                List<Long> existingIds = outboxMapper.selectExistingPostIds(new ArrayList<>(postDeltas.keySet()));
                if (existingIds.size() != postDeltas.size()) {
                    throw new IllegalStateException("本批点赞事件引用了不存在的帖子记录");
                }
                // delta 可能被 GREATEST(0, ...) 截断为原值，因此不能用 UPDATE 的 affected-row 数量判断目标是否存在。
                outboxMapper.batchAdjustPostLikeCounts(postDeltas);
            }
            if (!pictureDeltas.isEmpty()) {
                List<Long> existingIds = outboxMapper.selectExistingPictureIds(new ArrayList<>(pictureDeltas.keySet()));
                if (existingIds.size() != pictureDeltas.size()) {
                    throw new IllegalStateException("本批点赞事件引用了不存在的图片记录");
                }
                outboxMapper.batchAdjustPictureLikeCounts(pictureDeltas);
            }

            // 此方法会把作者 ID 排序后逐个加锁，并将同一作者本批次的计数/经验变化合并后写一次。
            experienceService.processLikeEvents(events);

            int marked = outboxMapper.markProcessed(eventIds);
            if (marked != eventIds.size()) {
                throw new IllegalStateException("标记点赞事件完成的行数不匹配，预期="
                        + eventIds.size() + ", 实际=" + marked);
            }
            return events.size();
        } catch (RuntimeException e) {
            // 异常必须向外抛出，确保事务回滚；调度器会在回滚后用独立事务记录重试信息。
            throw new CommunityLikeBatchProcessingException(eventIds, e);
        }
    }
}
