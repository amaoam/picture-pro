package com.zmt.picturebackendpro.mapper;

import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import com.zmt.picturebackendpro.model.entity.CommunityLikeOutboxEvent;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;
import org.apache.ibatis.annotations.Update;

import java.util.List;
import java.util.Map;
/** Outbox 查询与批量计数 SQL。面向 MySQL 8.0+。 */
@Mapper
public interface CommunityLikeOutboxMapper extends BaseMapper<CommunityLikeOutboxEvent> {

    /**
     * 每个目标只领取最早的一条未完成事件。
     *
     * 关键点：状态 2 的死信也属于“未完成”。如果同一目标存在更早的待处理/死信，
     * 后续事件不能越过它；否则 +1、-1 的应用顺序可能与点赞关系变更顺序相反。
     * SKIP LOCKED 允许不同消费者并行领取不同目标，已被锁住的 head 会继续阻止同目标后续事件被领取。
     */
    @Select("SELECT e.id, e.targetType, e.targetId, e.actorUserId, e.ownerUserId, e.delta, e.status, " +
            "e.retryCount, e.nextRetryTime, e.lastError, e.createTime, e.processedTime " +
            "FROM like_event_outbox e " +
            "WHERE e.status = 0 " +
            "AND (e.nextRetryTime IS NULL OR e.nextRetryTime <= NOW()) " +
            "AND NOT EXISTS (" +
            "  SELECT 1 FROM like_event_outbox earlier " +
            "  WHERE earlier.targetType = e.targetType AND earlier.targetId = e.targetId " +
            "    AND earlier.id < e.id AND earlier.status <> 1" +
            ") " +
            "ORDER BY e.id ASC LIMIT #{limit} FOR UPDATE SKIP LOCKED")
    List<CommunityLikeOutboxEvent> selectPendingTargetHeads(@Param("limit") int limit);

    /**
     * 对刚领取到 head 的目标，取每个目标前 N 条未完成事件（包括死信和尚未到重试时间的事件）。
     *
     * ROW_NUMBER 保证每个 target 的事件按 ID 排序并限制单目标批量大小。retryDue 使用数据库 NOW() 计算。
     * Java 层只处理每个 target 从头开始的连续到期前缀；遇到未来重试事件或死信后，
     * 本次不处理该目标更晚的任何事件。head 锁使其他 worker 无法并行消费同一目标。
     */
    @Select({"<script>",
            "SELECT e.id, e.targetType, e.targetId, e.actorUserId, e.ownerUserId, e.delta, e.status, ",
            "e.retryCount, e.nextRetryTime, e.lastError, e.createTime, e.processedTime, ",
            "CASE WHEN e.nextRetryTime IS NULL OR e.nextRetryTime &lt;= NOW() THEN 1 ELSE 0 END AS retryDue ",
            "FROM like_event_outbox e ",
            "JOIN (",
            "  SELECT id, ROW_NUMBER() OVER (PARTITION BY targetType, targetId ORDER BY id ASC) AS targetRowNumber ",
            "  FROM like_event_outbox ",
            "  WHERE status &lt;&gt; 1 AND (",
            "    <if test='postIds != null and postIds.size() > 0'>",
            "      (targetType = 'POST' AND targetId IN ",
            "        <foreach collection='postIds' item='id' open='(' separator=',' close=')'>#{id}</foreach>)",
            "    </if>",
            "    <if test='postIds != null and postIds.size() > 0 and pictureIds != null and pictureIds.size() > 0'> OR </if>",
            "    <if test='pictureIds != null and pictureIds.size() > 0'>",
            "      (targetType = 'PICTURE' AND targetId IN ",
            "        <foreach collection='pictureIds' item='id' open='(' separator=',' close=')'>#{id}</foreach>)",
            "    </if>",
            "  )",
            ") ranked ON ranked.id = e.id ",
            "WHERE ranked.targetRowNumber &lt;= #{maxEventsPerTarget} ",
            "ORDER BY e.id ASC FOR UPDATE",
            "</script>"})
    List<CommunityLikeOutboxEvent> selectEventsForTargets(
            @Param("postIds") List<Long> postIds,
            @Param("pictureIds") List<Long> pictureIds,
            @Param("maxEventsPerTarget") int maxEventsPerTarget);

    @Select({"<script>",
            "SELECT id FROM community_post WHERE id IN ",
            "<foreach collection='ids' item='id' open='(' separator=',' close=')'>#{id}</foreach>",
            "</script>"})
    List<Long> selectExistingPostIds(@Param("ids") List<Long> ids);

    @Select({"<script>",
            "SELECT id FROM picture WHERE id IN ",
            "<foreach collection='ids' item='id' open='(' separator=',' close=')'>#{id}</foreach>",
            "</script>"})
    List<Long> selectExistingPictureIds(@Param("ids") List<Long> ids);

    /** 一条 UPDATE 按 CASE 将不同的 delta 应用到对应帖子，避免逐条更新。 */
    @Update({"<script>",
            "UPDATE community_post SET likeCount = GREATEST(0, COALESCE(likeCount, 0) + CASE id ",
            "<foreach collection='deltas' index='targetId' item='delta'> WHEN #{targetId} THEN #{delta} </foreach>",
            "ELSE 0 END WHERE id IN ",
            "<foreach collection='deltas' index='targetId' item='delta' open='(' separator=',' close=')'>#{targetId}</foreach>",
            "</script>"})
    int batchAdjustPostLikeCounts(@Param("deltas") Map<Long, Long> deltas);

    /** 一条 UPDATE 按 CASE 将不同的 delta 应用到对应公共图片。 */
    @Update({"<script>",
            "UPDATE picture SET likeCount = GREATEST(0, COALESCE(likeCount, 0) + CASE id ",
            "<foreach collection='deltas' index='targetId' item='delta'> WHEN #{targetId} THEN #{delta} </foreach>",
            "ELSE 0 END WHERE id IN ",
            "<foreach collection='deltas' index='targetId' item='delta' open='(' separator=',' close=')'>#{targetId}</foreach>",
            "</script>"})
    int batchAdjustPictureLikeCounts(@Param("deltas") Map<Long, Long> deltas);

    /** 计数、经验和状态标记同事务提交；只允许把当前待处理事件标记为完成。 */
    @Update({"<script>",
            "UPDATE like_event_outbox SET status = 1, processedTime = NOW(), nextRetryTime = NULL, lastError = NULL ",
            "WHERE status = 0 AND id IN ",
            "<foreach collection='ids' item='id' open='(' separator=',' close=')'>#{id}</foreach>",
            "</script>"})
    int markProcessed(@Param("ids") List<Long> ids);

    /** 外层批处理事务回滚之后，在 REQUIRES_NEW 事务里记录指数退避时间和死信状态。 */
    @Update({"<script>",
            "UPDATE like_event_outbox SET ",
            "status = CASE WHEN retryCount + 1 &gt;= #{maxRetries} THEN 2 ELSE 0 END, ",
            "nextRetryTime = CASE WHEN retryCount = 0 THEN DATE_ADD(NOW(), INTERVAL 1 SECOND) ",
            "WHEN retryCount = 1 THEN DATE_ADD(NOW(), INTERVAL 5 SECOND) ",
            "WHEN retryCount &lt; 5 THEN DATE_ADD(NOW(), INTERVAL 15 SECOND) ",
            "ELSE DATE_ADD(NOW(), INTERVAL 60 SECOND) END, ",
            "lastError = LEFT(#{errorMessage}, 500), ",
            "retryCount = retryCount + 1 ",
            "WHERE status = 0 AND id IN ",
            "<foreach collection='ids' item='id' open='(' separator=',' close=')'>#{id}</foreach>",
            "</script>"})
    int recordBatchFailure(@Param("ids") List<Long> ids,
                           @Param("errorMessage") String errorMessage,
                           @Param("maxRetries") int maxRetries);

    @Select("SELECT COUNT(*) FROM like_event_outbox WHERE status = 0")
    long countPendingEvents();

    @Select("SELECT COUNT(*) FROM like_event_outbox WHERE status = 2")
    long countDeadEvents();

    @Select("SELECT COALESCE(TIMESTAMPDIFF(SECOND, MIN(createTime), NOW()), 0) " +
            "FROM like_event_outbox WHERE status = 0")
    long oldestPendingAgeSeconds();
}
