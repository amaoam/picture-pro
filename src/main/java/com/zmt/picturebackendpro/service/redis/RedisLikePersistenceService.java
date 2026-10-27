package com.zmt.picturebackendpro.service.redis;

import com.zmt.picturebackendpro.model.entity.CommunityLikeOutboxEvent;
import com.zmt.picturebackendpro.service.CommunityExperienceService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.jdbc.core.BatchPreparedStatementSetter;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.Resource;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import com.baomidou.mybatisplus.core.toolkit.IdWorker;

/**
 * Redis 点赞事件 MySQL 持久化服务
 * 核心设计：
 * 1. 一个 Stream 批次仅使用一个 MySQL 事务（事件流水账、关系表变更、计数聚合、经验副作用处于同一事务）。
 * 2. 严格遵循“事务未提交时绝不进行 Redis XACK”的原则，确保异常时能够整体回滚、消息保持 Pending 状态安全重试。
 */
@Slf4j
@Service
@ConditionalOnProperty(name = "community.like.mode", havingValue = "redis-stream")
public class RedisLikePersistenceService {

    @Resource
    private JdbcTemplate jdbcTemplate;

    @Resource
    private CommunityExperienceService experienceService;

    /**
     * 将一个 Stream 批次事件持久化到 MySQL。
     * 核心步骤：
     * 1. 批次格式校验 & 组内重复校验。
     * 2. 查询并过滤 MySQL 中已经处理过的 eventId（幂等防重）。
     * 3. 在当前事务内预登记事件到 Ledger（流水账表）。
     * 4. 按 Stream ID 原顺序应用关系变更（点赞/取消点赞），收集计数差值（Delta）。
     * 5. 批量合并更新目标实体的冗余计数（CASE WHEN 优化），防止计数负数。
     * 6. 触发经验服务副作用（积分/经验奖励）。
     *
     * @param events Redis Stream 读出的事件批次
     */
    @Transactional(rollbackFor = Exception.class)
    public void persistBatch(List<RedisLikeEvent> events) {
        if (events == null || events.isEmpty()) {
            return;
        }

        // 步骤零：消息来自 Redis Stream，但仍按不可信输入校验。
        // 先校验再拼 SQL，避免坏消息造成含糊的 SQL 或解析异常。
        // 坏消息会留在 PEL 并触发告警，不能被误当作成功事件 ACK。
        Set<String> eventIdsInBatch = new HashSet<>();
        for (RedisLikeEvent event : events) {
            validateEvent(event);
            if (!eventIdsInBatch.add(event.getEventId())) {
                throw new IllegalArgumentException("同一批 Stream 记录出现重复 eventId：" + event.getEventId());
            }
        }

        // 第一步：过滤 MySQL 已经应用过的 eventId。由于固定分片单 Worker 消费，可有效避免同 eventId 的并发竞争。
        Set<String> alreadyProcessed = findProcessedEventIds(events);
        List<RedisLikeEvent> freshEvents = events.stream()
                .filter(event -> !alreadyProcessed.contains(event.getEventId()))
                .collect(Collectors.toList());
        if (freshEvents.isEmpty()) {
            return; // 常见于 MySQL 已提交、但在发送 Redis XACK 前进程崩溃后的安全重投。
        }

        // 第二步：在当前事务内预登记事件。后续任意步骤失败都会一起回滚这些 ledger（流水）记录。
        insertEventLedger(freshEvents);

        // 第三步：按 Stream ID 的原顺序应用关系状态，只有真实的关系变化才进入计数/经验更新。
        Map<TargetKey, Long> counterDeltas = new LinkedHashMap<>();
        List<CommunityLikeOutboxEvent> actualTransitions = new ArrayList<>();

        for (RedisLikeEvent event : freshEvents) {
            int changedRows = applyRelationState(event);
            if (changedRows == 1) {
                // 关系发生真实变更，统计该目标的计数差值 (Delta)
                TargetKey targetKey = new TargetKey(event.getTargetType(), event.targetIdAsLong());
                counterDeltas.merge(targetKey, (long) event.deltaAsInt(), Long::sum);
                actualTransitions.add(toExperienceEvent(event));
            } else {
                // Redis 与 MySQL 状态不一致时不能盲目 ACK，否则会把漏计数永久固化。
                // 抛出异常让整个事务回滚，事件保持 pending 状态，等待人工/运维对账修复。
                throw new IllegalStateException("Redis/MySQL like projection mismatch: eventId=" + event.getEventId()
                        + ", type=" + event.getTargetType() + ", targetId=" + event.getTargetId()
                        + ", desiredState=" + event.getDesiredState() + ", userId=" + event.getUserId()
                        + ", changedRows=" + changedRows);
            }
        }

        // 第四步：先按 target 聚合 +1/-1，再批量执行 CASE UPDATE，大幅减少同一目标的重复 SQL UPDATE 交互。
        updateTargetCounters(counterDeltas);

        // 第五步：沿用现有经验处理逻辑。只把真实改变了 MySQL 关系的事件传进去，并处于同一事务。
        // 如果经验服务失败，本事务整体回滚，Stream 消息不会被 ACK，重试后不会重复应用关系/计数。
        if (!actualTransitions.isEmpty()) {
            experienceService.processLikeEvents(actualTransitions);
        }
    }

    /**
     * 批量查询哪些 event_id 已经在消费流水表中处理过（用于防重）
     */
    private Set<String> findProcessedEventIds(List<RedisLikeEvent> events) {
        List<String> ids = events.stream().map(RedisLikeEvent::getEventId).collect(Collectors.toList());
        String placeholders = String.join(",", Collections.nCopies(ids.size(), "?"));
        List<String> found = jdbcTemplate.query(
                "SELECT event_id FROM community_like_event_consume WHERE event_id IN (" + placeholders + ")",
                (rs, rowNum) -> rs.getString(1), ids.toArray());
        return new HashSet<>(found);
    }

    /**
     * 批量插入事件消费流水记录（Ledger）
     */
    private void insertEventLedger(List<RedisLikeEvent> events) {
        final String sql = "INSERT INTO community_like_event_consume " +
                "(event_id,target_type,target_id,actor_user_id,owner_user_id,desired_state,delta,pair_version,created_at,processed_at) " +
                "VALUES (?,?,?,?,?,?,?,?,?,CURRENT_TIMESTAMP(3))";
        jdbcTemplate.batchUpdate(sql, new BatchPreparedStatementSetter() {
            @Override
            public void setValues(PreparedStatement ps, int index) throws SQLException {
                RedisLikeEvent event = events.get(index);
                ps.setString(1, event.getEventId());
                ps.setString(2, event.getTargetType());
                ps.setLong(3, event.targetIdAsLong());
                ps.setLong(4, event.userIdAsLong());
                if (event.ownerUserIdAsLongOrNull() == null) {
                    ps.setNull(5, java.sql.Types.BIGINT);
                } else {
                    ps.setLong(5, event.ownerUserIdAsLongOrNull());
                }
                ps.setString(6, event.getDesiredState());
                ps.setInt(7, event.deltaAsInt());
                ps.setLong(8, event.pairVersionAsLong());
                ps.setTimestamp(9, new Timestamp(event.createdAtAsLong()));
            }

            @Override
            public int getBatchSize() {
                return events.size();
            }
        });
    }

    /**
     * 根据事件的目标类型（POST / PICTURE）与期望状态（LIKE / UNLIKE）应用点赞关系变更。
     * 运用 INSERT IGNORE 和 DELETE 保证幂等。
     */
    private int applyRelationState(RedisLikeEvent event) {
        long targetId = event.targetIdAsLong();
        long userId = event.userIdAsLong();

        if ("POST".equals(event.getTargetType())) {
            if ("LIKE".equals(event.getDesiredState())) {
                return jdbcTemplate.update(
                        "INSERT IGNORE INTO community_post_like " +
                                "(id, postId, userId, createTime) " +
                                "VALUES (?, ?, ?, CURRENT_TIMESTAMP)",
                        IdWorker.getId(), targetId, userId);
            }

            return jdbcTemplate.update(
                    "DELETE FROM community_post_like WHERE postId=? AND userId=?",
                    targetId, userId);
        }

        if ("PICTURE".equals(event.getTargetType())) {
            if ("LIKE".equals(event.getDesiredState())) {
                return jdbcTemplate.update(
                        "INSERT IGNORE INTO picture_like " +
                                "(id, pictureId, userId, createTime) " +
                                "VALUES (?, ?, ?, CURRENT_TIMESTAMP)",
                        IdWorker.getId(), targetId, userId);
            }

            return jdbcTemplate.update(
                    "DELETE FROM picture_like WHERE pictureId=? AND userId=?",
                    targetId, userId);
        }

        throw new IllegalArgumentException("Unsupported like target type: " + event.getTargetType());
    }

    /**
     * 分类聚合点赞计数差值，并批量更新目标表
     */
    private void updateTargetCounters(Map<TargetKey, Long> counterDeltas) {
        List<Map.Entry<TargetKey, Long>> postDeltas = new ArrayList<>();
        List<Map.Entry<TargetKey, Long>> pictureDeltas = new ArrayList<>();

        for (Map.Entry<TargetKey, Long> entry : counterDeltas.entrySet()) {
            if (entry.getValue() == 0L) {
                continue;
            }
            ("POST".equals(entry.getKey().type) ? postDeltas : pictureDeltas).add(entry);
        }

        // 每种目标类型至多执行一条优化后的 CASE UPDATE 语句；减少多目标批量操作对数据库的网络往返。
        // 同时在 WHERE 中校验目标存在性，且新计数绝对不会小于 0（防重负数保护）。任一条件不满足则事务整体回滚。
        applyCounterCaseUpdate("community_post", "id", "likeCount", postDeltas);
        applyCounterCaseUpdate("picture", "id", "likeCount", pictureDeltas);
    }

    /**
     * 使用 SQL 的 CASE WHEN 语法批量高效更新计数列，并配合防负数校验
     */
    private void applyCounterCaseUpdate(String table, String idColumn, String countColumn,
                                        List<Map.Entry<TargetKey, Long>> deltas) {
        if (deltas.isEmpty()) {
            return;
        }

        StringBuilder sql = new StringBuilder("UPDATE ").append(table)
                .append(" SET ").append(countColumn).append("=COALESCE(").append(countColumn).append(",0)+CASE ")
                .append(idColumn).append(' ');

        for (int i = 0; i < deltas.size(); i++) {
            sql.append("WHEN ? THEN ? ");
        }
        sql.append("ELSE 0 END WHERE ").append(idColumn).append(" IN (")
                .append(String.join(",", Collections.nCopies(deltas.size(), "?")))
                .append(") AND COALESCE(").append(countColumn).append(",0)+CASE ").append(idColumn).append(' ');

        for (int i = 0; i < deltas.size(); i++) {
            sql.append("WHEN ? THEN ? ");
        }
        sql.append("ELSE 0 END >= 0");

        int updated = jdbcTemplate.update(sql.toString(), ps -> {
            int index = 1;
            // 1. 填充 SET 部分的 CASE 参数
            for (Map.Entry<TargetKey, Long> entry : deltas) {
                ps.setLong(index++, entry.getKey().id);
                ps.setLong(index++, entry.getValue());
            }
            // 2. 填充 WHERE IN 列表的 ID 参数
            for (Map.Entry<TargetKey, Long> entry : deltas) {
                ps.setLong(index++, entry.getKey().id);
            }
            // 3. 填充 WHERE 防负数校验中 CASE 的参数
            for (Map.Entry<TargetKey, Long> entry : deltas) {
                ps.setLong(index++, entry.getKey().id);
                ps.setLong(index++, entry.getValue());
            }
        });

        // 如果更新行数与预期不符，说明存在目标数据缺失或like_count与实际关系不一致的脏数据，强制回滚
        if (updated != deltas.size()) {
            throw new IllegalStateException("点赞目标缺失或 like_count 与关系不一致；表=" + table
                    + ", expected=" + deltas.size() + ", updated=" + updated);
        }
    }

    /**
     * 将 RedisLikeEvent 转换为经验事件领域对象
     */
    private CommunityLikeOutboxEvent toExperienceEvent(RedisLikeEvent event) {
        CommunityLikeOutboxEvent converted = new CommunityLikeOutboxEvent();
        converted.setTargetType(event.getTargetType());
        converted.setTargetId(event.targetIdAsLong());
        converted.setActorUserId(event.userIdAsLong());
        converted.setOwnerUserId(event.ownerUserIdAsLongOrNull());
        converted.setDelta(event.deltaAsInt());
        converted.setCreateTime(new Date(event.createdAtAsLong()));
        converted.setStatus(CommunityLikeOutboxEvent.STATUS_PROCESSED);
        return converted;
    }

    /**
     * 严苛校验 Stream 事件的各个字段合法性
     */
    private void validateEvent(RedisLikeEvent event) {
        if (event == null || event.getEventId() == null || event.getEventId().trim().isEmpty()) {
            throw new IllegalArgumentException("Stream event 缺少 eventId");
        }
        if (!"POST".equals(event.getTargetType()) && !"PICTURE".equals(event.getTargetType())) {
            throw new IllegalArgumentException("Stream event targetType 不合法：" + event.getTargetType());
        }
        if (event.getRequestId() == null || event.getRequestId().trim().isEmpty()) {
            throw new IllegalArgumentException("Stream event 缺少 requestId：" + event.getEventId());
        }
        if (!"LIKE".equals(event.getDesiredState()) && !"UNLIKE".equals(event.getDesiredState())) {
            throw new IllegalArgumentException("Stream event desiredState 不合法：" + event.getDesiredState());
        }
        try {
            if (event.targetIdAsLong() <= 0 || event.userIdAsLong() <= 0
                    || event.pairVersionAsLong() <= 0 || event.createdAtAsLong() <= 0) {
                throw new IllegalArgumentException("Stream event 的 target/user/version/timestamp 必须是正数："
                        + event.getEventId());
            }
            // ownerUserId 可为空（例如尚未配置归属人的公共资源），但非空时必须可解析为 BIGINT。
            event.ownerUserIdAsLongOrNull();
        } catch (NumberFormatException exception) {
            throw new IllegalArgumentException("Stream event 中的 ID/version/timestamp 格式不合法："
                    + event.getEventId(), exception);
        }
        int expectedDelta;
        try {
            expectedDelta = "LIKE".equals(event.getDesiredState()) ? 1 : -1;
            if (event.deltaAsInt() != expectedDelta) {
                throw new IllegalArgumentException("Stream event delta 与 desiredState 不一致：" + event.getEventId());
            }
        } catch (NumberFormatException exception) {
            throw new IllegalArgumentException("Stream event delta 格式不合法：" + event.getEventId(), exception);
        }
    }

    /**
     * 用于聚合统计的内部组合主键封装类
     */
    private static final class TargetKey {
        private final String type;
        private final Long id;

        private TargetKey(String type, Long id) {
            this.type = type;
            this.id = id;
        }

        @Override
        public boolean equals(Object other) {
            if (this == other) return true;
            if (!(other instanceof TargetKey)) return false;
            TargetKey that = (TargetKey) other;
            return type.equals(that.type) && id.equals(that.id);
        }

        @Override
        public int hashCode() {
            return 31 * type.hashCode() + id.hashCode();
        }
    }
}