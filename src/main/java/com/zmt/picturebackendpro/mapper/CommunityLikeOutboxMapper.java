package com.zmt.picturebackendpro.mapper;

import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import com.zmt.picturebackendpro.model.entity.CommunityLikeOutboxEvent;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;
import org.apache.ibatis.annotations.Update;

import java.util.List;
import java.util.Map;

@Mapper
public interface CommunityLikeOutboxMapper extends BaseMapper<CommunityLikeOutboxEvent> {

    /**
     * 分批拉取待处理的点赞事件（支持高并发分布式锁/多实例抢占）
     *
     * <p><strong>核心机制：</strong>
     * <ul>
     *   <li>利用 MySQL 8.0 的 {@code FOR UPDATE SKIP LOCKED} 语法，使多个工作线程或应用实例能够同时并发拉取不同的事件行，而不会发生行锁等待（Block）。</li>
     *   <li>配合 {@code status = 0}（待处理）以及重试时间过滤条件，确保只有到期或从未执行的事件会被选中。</li>
     * </ul>
     *
     * @param limit 本次拉取的最大批次数量
     * @return 待处理的社区点赞事件列表
     */
    @Select("SELECT id, targetType, targetId, actorUserId, ownerUserId, delta, status, retryCount, " +
            "nextRetryTime, lastError, createTime, processedTime " +
            "FROM like_event_outbox " +
            "WHERE status = 0 AND (nextRetryTime IS NULL OR nextRetryTime <= NOW()) " +
            "ORDER BY id ASC LIMIT #{limit} FOR UPDATE SKIP LOCKED")
    List<CommunityLikeOutboxEvent> selectPendingBatchForUpdate(@Param("limit") int limit);

    /**
     * 一条 SQL 合并更新本批次内多个帖子的点赞计数（批量更新优化）
     *
     * <p><strong>技术亮点：</strong>
     * <ul>
     *   <li>使用 MyBatis 的 {@code <foreach>} 动态拼接 SQL，在一条 SQL 中通过 {@code CASE WHEN} 为不同的帖子分发不同的增减量，避免循环单条执行造成的数据库性能开销。</li>
     *   <li>使用 {@code GREATEST(0, ...)} 防止点赞数在并发极端情况下出现负数。</li>
     * </ul>
     *
     * @param deltas Map 结构，Key 为帖子 ID（targetId），Value 为点赞增减量（如 +1 或 -1）
     * @受影响的行数
     */
    @Update({"<script>",
            "UPDATE community_post SET likeCount = GREATEST(0, COALESCE(likeCount, 0) + CASE id",
            "<foreach collection='deltas' index='targetId' item='delta'> WHEN #{targetId} THEN #{delta} </foreach>",
            "ELSE 0 END WHERE id IN",
            "<foreach collection='deltas' index='targetId' item='delta' open='(' separator=',' close=')'>#{targetId}</foreach>",
            "</script>"})
    int batchAdjustPostLikeCounts(@Param("deltas") Map<Long, Long> deltas);

    /**
     * 校验给定的帖子 ID 集合中哪些在数据库真实存在
     *
     * <p>用于在执行批量更新前过滤掉无效或已被物理删除的实体 ID，防止由于外键或业务异常导致整批事务回滚。
     *
     * @param ids 待校验的帖子 ID 列表
     * @return 数据库中真实存在的帖子 ID 列表
     */
    @Select({"<script>",
            "SELECT id FROM community_post WHERE id IN",
            "<foreach collection='ids' item='id' open='(' separator=',' close=')'>#{id}</foreach>",
            "</script>"})
    List<Long> selectExistingPostIds(@Param("ids") List<Long> ids);

    /**
     * 校验给定的公共图片 ID 集合中哪些在数据库真实存在
     *
     * @param ids 待校验的图片 ID 列表
     * @return 数据库中真实存在的图片 ID 列表
     */
    @Select({"<script>",
            "SELECT id FROM picture WHERE id IN",
            "<foreach collection='ids' item='id' open='(' separator=',' close=')'>#{id}</foreach>",
            "</script>"})
    List<Long> selectExistingPictureIds(@Param("ids") List<Long> ids);

    /**
     * 一条 SQL 合并更新本批次内多张公共图片的点赞计数
     *
     * @param deltas Map 结构，Key 为图片 ID（targetId），Value 为点赞增减量
     * @return 受影响的行数
     */
    @Update({"<script>",
            "UPDATE picture SET likeCount = GREATEST(0, COALESCE(likeCount, 0) + CASE id",
            "<foreach collection='deltas' index='targetId' item='delta'> WHEN #{targetId} THEN #{delta} </foreach>",
            "ELSE 0 END WHERE id IN",
            "<foreach collection='deltas' index='targetId' item='delta' open='(' separator=',' close=')'>#{targetId}</foreach>",
            "</script>"})
    int batchAdjustPictureLikeCounts(@Param("deltas") Map<Long, Long> deltas);

    /**
     * 标记事件批次为“已处理”状态
     *
     * <p><strong>事务边界：</strong>计数更新与经验值变更等业务事务全部成功提交后，才调用此方法。
     * 同时清空重试时间和错误信息，将 {@code status} 设为 {@code 1}（已完成）。
     *
     * @param ids 成功处理的事件 ID 列表
     * @return 更新成功的记录行数
     */
    @Update({"<script>",
            "UPDATE like_event_outbox SET status = 1, processedTime = NOW(), nextRetryTime = NULL, lastError = NULL",
            "WHERE status = 0 AND id IN",
            "<foreach collection='ids' item='id' open='(' separator=',' close=')'>#{id}</foreach>",
            "</script>"})
    int markProcessed(@Param("ids") List<Long> ids);

    /**
     * 记录批次处理失败并制定退避重试策略（支持死信队列机制）
     *
     * <p><strong>核心业务逻辑：</strong>
     * <ul>
     *   <li><strong>阶梯退避：</strong>根据当前的 {@code retryCount} 动态计算下一次重试时间（如第0次重试1秒后，第1次5秒后，小于5次15秒后，之后每60秒重试一次）。</li>
     *   <li><strong>死信处理：</strong>当累计重试次数达到 {@code maxRetries} 阈值时，自动将状态变更为 {@code status = 2}（死信状态），防止无限重试耗尽资源，需要人工或对账系统介入。</li>
     *   <li><strong>赋值顺序陷阱：</strong>为避免 MySQL 单表 {@code UPDATE} 从左向右赋值的副作用影响，{@code retryCount = retryCount + 1} 必须放置在 {@code SET} 子句的最后。</li>
     * </ul>
     *
     * @param ids          失败的事件 ID 列表
     * @param errorMessage 异常错误信息（会被截断至 500 字符内防止超出字段长度）
     * @param maxRetries   允许的最大重试次数阈值
     * @return 更新成功的记录行数
     */
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
}
