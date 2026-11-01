package com.zmt.picturebackendpro.service.impl.mq;

import com.baomidou.mybatisplus.core.toolkit.IdWorker;
import com.zmt.picturebackendpro.config.LikeRedisProperties;
import com.zmt.picturebackendpro.model.entity.CommunityLikeOutboxEvent;
import com.zmt.picturebackendpro.service.CommunityExperienceService;
import com.zmt.picturebackendpro.service.redis.LikeRedisKeyFactory;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.Resource;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * 单个点赞目标的对账 worker（RabbitMQ 模式）。
 * <p>
 * 对账模型：
 * <ul>
 *   <li><b>Redis 是状态来源</b>：Redis 中的点赞用户 Set（{@code likedUsersKey}）代表"当前真实点赞关系"；</li>
 *   <li><b>MySQL 是被修复方</b>：关系表（{@code community_post_like} / {@code picture_like}）、
 *       冗余计数（{@code likeCount}）以及作者收到的点赞统计，均以 Redis 为准进行差异修复；</li>
 *   <li><b>经验值事件</b>：对关系的新增/删除，会生成 {@link CommunityLikeOutboxEvent} 交给
 *       {@link CommunityExperienceService} 处理，保证作者经验值只在真实状态变化时增减一次。</li>
 * </ul>
 * <p>
 * 安全防线（关键）：
 * <ol>
 *   <li><b>缺失状态保护</b>：若 Redis 的 Set 与 countKey <i>都不存在</i>，不能视为"确实没人点赞"，
 *       否则 Redis 数据丢失时会误删 MySQL 全部关系；此时直接跳过对账并记录 error。</li>
 *   <li><b>已知空集合</b>：Set 不存在但 countKey 存在且为 0，表示"已初始化、最后一条点赞被取消"，
 *       属于可安全处理的空集合。</li>
 *   <li><b>不一致保护</b>：Set 不存在但 countKey 非 0，或 countKey 非数字，均视为异常状态，跳过对账。</li>
 * </ol>
 * <p>
 * 事务：整个对账在一个事务内完成（关系修复 + 计数校准 + 经验事件写入），
 * 任一环节异常则整体回滚，保证 Redis 与 MySQL 不会出现部分修复的中间态。
 * <p>
 * 通过 {@link ConditionalOnProperty} 限定只在 {@code community.like.mode=rabbitmq} 时装配。
 */
@Slf4j
@Service
@ConditionalOnProperty(name = "community.like.mode", havingValue = "rabbitmq")
public class RabbitMqLikeReconciliationWorker {

    /** JDBC 模板：直接执行对账 SQL（关系表、计数表） */
    @Resource private JdbcTemplate jdbcTemplate;
    /** 点赞专用 Redis 模板，读取当前点赞用户 Set 与计数 */
    @Resource(name = "likeStringRedisTemplate") private StringRedisTemplate likeStringRedisTemplate;
    /** Redis key 工厂：生成 likedUsersKey、likeCountKey */
    @Resource private LikeRedisKeyFactory keyFactory;
    /** 经验值服务：处理对账产生的点赞状态变更事件，保证作者经验值同步 */
    @Resource private CommunityExperienceService experienceService;

    /**
     * 对单个点赞目标执行对账修复。
     * <p>
     * 执行步骤：
     * <ol>
     *   <li>解析 targetToken（形如 {@code POST:123} / {@code PICTURE:456}），校验类型与 ID；</li>
     *   <li>读取 Redis 状态，执行"缺失状态保护"判断（见类注释）；</li>
     *   <li>加载目标元数据（作者 ID、目标是否可用），不可用则跳过；</li>
     *   <li>对比 Redis 用户集合与 MySQL 关系表：
     *     <ul>
     *       <li>Redis 有、MySQL 无：INSERT IGNORE 补齐，并生成 +1 经验事件；</li>
     *       <li>MySQL 有、Redis 无：DELETE 删除，并生成 -1 经验事件；</li>
     *     </ul>
     *   </li>
     *   <li>以 Redis Set 的 SCARD 为准，校准 Redis 计数器与 MySQL 冗余计数；</li>
     *   <li>批量提交经验事件；</li>
     *   <li>返回本次修复的关系变更数量。</li>
     * </ol>
     * <p>
     * 事务语义：方法上的 {@code @Transactional} 保证关系修复、计数校准、经验事件写入要么全部成功，
     * 要么全部回滚。经验事件虽然由外部服务处理，但其入库在本事务内完成，避免状态不一致。
     *
     * @param targetToken 目标标识，形如 {@code POST:123}
     * @return 本次修复的关系变更数量（新增 + 删除），0 表示无需修复或已跳过
     * @throws IllegalArgumentException targetToken 格式非法
     */
    @Transactional(rollbackFor = Exception.class)
    public int reconcileTarget(String targetToken) {
        // ---------- 1. 解析并校验 targetToken ----------
        String[] parts = targetToken.split(":", -1);
        if (parts.length != 2 || (!"POST".equals(parts[0]) && !"PICTURE".equals(parts[0]))) {
            throw new IllegalArgumentException("Invalid reconcile target token: " + targetToken);
        }
        String type = parts[0];
        long targetId;
        try { targetId = Long.parseLong(parts[1]); }
        catch (NumberFormatException e) { throw new IllegalArgumentException("Invalid target id: " + targetToken, e); }
        if (targetId <= 0) throw new IllegalArgumentException("Invalid target id: " + targetToken);

        // ---------- 2. 读取 Redis 状态并执行缺失保护 ----------
        String usersKey = keyFactory.likedUsersKey(type, targetId);
        String countKey = keyFactory.likeCountKey(type, targetId);
        boolean usersKeyExists = likeStringRedisTemplate.hasKey(usersKey);
        String countRaw = likeStringRedisTemplate.opsForValue().get(countKey);
        // 不能把“Redis 两个状态 Key 都不存在”误判为“这个目标确实没人点赞”，否则 Redis 数据丢失时会删掉 MySQL 全部关系。
        // 已初始化但最后一条点赞被取消的状态通常是 Set 不存在、countKey=0；这种情况可安全视为已知空集合。
        if (!usersKeyExists && countRaw == null) {
            log.error("Skip reconciliation because Redis like state is missing, not known-empty: target={}", targetToken);
            return 0;
        }
        if (!usersKeyExists && countRaw != null) {
            long storedCount;
            try { storedCount = Long.parseLong(countRaw); }
            catch (NumberFormatException e) {
                log.error("Skip reconciliation because Redis count is not numeric: target={}, value={}", targetToken, countRaw);
                return 0;
            }
            if (storedCount != 0L) {
                // Set 不存在但计数非 0：状态自相矛盾，可能是数据损坏，跳过修复以免误删
                log.error("Skip reconciliation because Redis Set is missing but count is nonzero: target={}, count={}",
                        targetToken, storedCount);
                return 0;
            }
            // 此时为"已知空集合"（count=0），继续走正常对账流程
        }
        // 读取 Redis 当前点赞用户集合，并校验每个 userId 合法性
        Set<String> redisUsersRaw = likeStringRedisTemplate.opsForSet().members(usersKey);
        Set<Long> redisUsers = new HashSet<>();
        if (redisUsersRaw != null) {
            for (String value : redisUsersRaw) {
                long userId = Long.parseLong(value);
                if (userId <= 0) throw new IllegalStateException("Invalid Redis liker id " + value + " on " + targetToken);
                redisUsers.add(userId);
            }
        }

        // ---------- 3. 加载目标元数据（作者、可用性） ----------
        TargetMetadata metadata = loadTargetMetadata(type, targetId);
        if (metadata == null) {
            log.warn("Skip reconciliation for unavailable/deleted target {}", targetToken);
            return 0;
        }

        // ---------- 4. 对比 Redis 与 MySQL 关系表 ----------
        String relationSql = "POST".equals(type)
                ? "SELECT userId FROM community_post_like WHERE postId=?"
                : "SELECT userId FROM picture_like WHERE pictureId=?";
        List<Long> mysqlRows = jdbcTemplate.query(relationSql, (rs, rowNum) -> rs.getLong("userId"), targetId);
        Set<Long> mysqlUsers = new HashSet<>(mysqlRows);
        // 本次对账产生的经验值变更事件（新增 +1 / 删除 -1）
        List<CommunityLikeOutboxEvent> experienceEvents = new ArrayList<>();
        int correctedRelations = 0;

        // Redis has a relation that MySQL lacks: add it and account the genuine transition exactly once.
        // Redis 有、MySQL 无：补插关系，并生成 +1 经验事件（仅在真正插入成功时计数，避免 INSERT IGNORE 重复触发）
        for (Long userId : redisUsers) {
            if (mysqlUsers.contains(userId)) continue;
            int inserted;
            if ("POST".equals(type)) {
                inserted = jdbcTemplate.update("INSERT IGNORE INTO community_post_like (id, postId, userId, createTime) VALUES (?, ?, ?, CURRENT_TIMESTAMP)",
                        IdWorker.getId(), targetId, userId);
            } else {
                inserted = jdbcTemplate.update("INSERT IGNORE INTO picture_like (id, pictureId, userId, createTime) VALUES (?, ?, ?, CURRENT_TIMESTAMP)",
                        IdWorker.getId(), targetId, userId);
            }
            if (inserted == 1) {
                experienceEvents.add(toExperienceEvent(type, targetId, userId, metadata.ownerUserId, 1));
                correctedRelations++;
            }
        }

        // MySQL has a relation no longer present in Redis: remove it and reverse receivedLikeCount.
        // MySQL 有、Redis 无：删除关系，并生成 -1 经验事件（仅删除成功时计数）
        for (Long userId : mysqlUsers) {
            if (redisUsers.contains(userId)) continue;
            String deleteSql = "POST".equals(type)
                    ? "DELETE FROM community_post_like WHERE postId=? AND userId=?"
                    : "DELETE FROM picture_like WHERE pictureId=? AND userId=?";
            int deleted = jdbcTemplate.update(deleteSql, targetId, userId);
            if (deleted == 1) {
                experienceEvents.add(toExperienceEvent(type, targetId, userId, metadata.ownerUserId, -1));
                correctedRelations++;
            }
        }

        // ---------- 5. 校准计数（Redis 计数器 + MySQL 冗余计数） ----------
        // Redis Set 是当前状态的来源；先将其计数器恢复为 SCARD，再校准 MySQL 冗余计数。
        long exactCount = redisUsers.size();
        likeStringRedisTemplate.opsForValue().set(countKey, String.valueOf(exactCount));
        String countSql = "POST".equals(type)
                ? "UPDATE community_post SET likeCount=? WHERE id=?"
                : "UPDATE picture SET likeCount=? WHERE id=?";
        jdbcTemplate.update(countSql, exactCount, targetId);

        // ---------- 6. 提交经验事件 ----------
        // 经验事件写入在本事务内，保证与关系修复同生共死
        if (!experienceEvents.isEmpty()) experienceService.processLikeEvents(experienceEvents);
        if (correctedRelations > 0) {
            log.warn("Like reconciliation repaired target={}, relationChanges={}, redisCount={}",
                    targetToken, correctedRelations, exactCount);
        }
        return correctedRelations;
    }

    /**
     * 加载目标元数据（仅作者 ID），并校验目标是否可用。
     * <p>
     * 可用性条件：
     * <ul>
     *   <li>帖子：已发布（status=1）且未删除；</li>
     *   <li>图片：公共可见（spaceId IS NULL）、审核通过（reviewStatus=1）且未删除。</li>
     * </ul>
     * 不可用的目标返回 null，调用方会跳过对账（避免对已删除/下线内容做无意义的修复）。
     * <p>
     * 作者 userId 可能为 null（历史数据），使用 {@code rs.wasNull()} 显式判断。
     *
     * @param type     目标类型（POST / PICTURE）
     * @param targetId 目标 ID
     * @return 目标元数据；目标不可用或不存在时返回 null
     */
    private TargetMetadata loadTargetMetadata(String type, long targetId) {
        String sql = "POST".equals(type)
                ? "SELECT userId FROM community_post WHERE id=? AND status=1 AND isDelete=0"
                : "SELECT userId FROM picture WHERE id=? AND spaceId IS NULL AND reviewStatus=1 AND isDelete=0";
        List<Long> owners = jdbcTemplate.query(sql, (rs, rowNum) -> {
            long value = rs.getLong("userId");
            return rs.wasNull() ? null : value;
        }, targetId);
        if (owners.isEmpty()) return null;
        return new TargetMetadata(owners.get(0));
    }

    /**
     * 构造一条经验值变更事件（outbox 风格）。
     * <p>
     * 该事件直接标记为 {@link CommunityLikeOutboxEvent#STATUS_PROCESSED}，
     * 因为它是在对账事务内同步生成的"已确认变更"，由 {@link CommunityExperienceService}
     * 在同事务中处理，不需要再走异步 outbox 投递流程。
     *
     * @param type     目标类型
     * @param targetId 目标 ID
     * @param actorId  点赞/取消点赞的用户 ID
     * @param ownerId  目标作者 ID（可能为 null）
     * @param delta    经验值变化量：+1 表示新增点赞，-1 表示取消点赞
     * @return 经验值事件
     */
    private CommunityLikeOutboxEvent toExperienceEvent(String type, long targetId, long actorId,
                                                       Long ownerId, int delta) {
        CommunityLikeOutboxEvent event = new CommunityLikeOutboxEvent();
        event.setTargetType(type);
        event.setTargetId(targetId);
        event.setActorUserId(actorId);
        event.setOwnerUserId(ownerId);
        event.setDelta(delta);
        event.setCreateTime(new Date());
        event.setStatus(CommunityLikeOutboxEvent.STATUS_PROCESSED);
        return event;
    }

    /**
     * 目标元数据（内部值对象）。
     * 只保留对账需要的作者 ID，避免加载整个实体。
     */
    private static final class TargetMetadata {
        private final Long ownerUserId;
        private TargetMetadata(Long ownerUserId) { this.ownerUserId = ownerUserId; }
    }
}