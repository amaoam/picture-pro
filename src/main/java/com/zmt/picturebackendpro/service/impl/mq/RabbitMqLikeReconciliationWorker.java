package com.zmt.picturebackendpro.service.impl.mq;

import com.baomidou.mybatisplus.core.toolkit.IdWorker;
import com.zmt.picturebackendpro.model.entity.CommunityLikeOutboxEvent;
import com.zmt.picturebackendpro.service.CommunityExperienceService;
import com.zmt.picturebackendpro.service.redis.LikeRedisKeyFactory;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.Resource;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * 单个点赞目标的静默对账 worker（RabbitMQ 模式）。
 * <p>
 * 对账模型：
 * <ul>
 *   <li><b>Redis 是状态来源</b>：Redis 的 liked-users Set 代表"当前真实点赞关系"，
 *       versions Hash 代表每个 actor 的 pairVersion 水位；</li>
 *   <li><b>MySQL 是被修复方</b>：关系表（{@code community_post_like} / {@code picture_like}）、
 *       冗余计数（{@code likeCount}）、以及 pairVersion 水位表
 *       {@code community_like_pair_state}，均以 Redis 为准进行差异修复；</li>
 *   <li><b>经验值事件</b>：对关系的新增/删除，会生成 {@link CommunityLikeOutboxEvent}
 *       交给 {@link CommunityExperienceService} 处理，保证作者经验值只在真实状态变化时增减一次。</li>
 * </ul>
 * <p>
 * 安全防线（关键）：
 * <ol>
 *   <li><b>缺失状态保护</b>：若 Redis 的 Set 与 countKey <i>都不存在</i>，不能视为"确实没人点赞"，
 *       否则 Redis 数据丢失时会误删 MySQL 全部关系；此时直接跳过对账并记录 error；</li>
 *   <li><b>已知空集合</b>：Set 不存在但 countKey 存在且为 0，表示"已初始化、最后一条点赞被取消"，
 *       属于可安全处理的空集合；</li>
 *   <li><b>不一致保护</b>：Set 不存在但 countKey 非 0，或 countKey 非数字，均视为异常状态，跳过对账。</li>
 * </ol>
 * <p>
 * pairVersion 兜底：
 * 迁移脚本从 MySQL CSV 回填时只初始化 users Set 和 count，不会为历史点赞创建 versions Hash。
 * 因此 versions Hash 缺失或个别 userId 缺少版本时不能直接跳过：先用消费 Ledger
 * （{@code community_like_event_consume}）的最大已消费版本兜底，再由现存 pair-state 的
 * GREATEST 保留更高水位，避免重放旧死信覆盖当前状态。
 * <p>
 * 事务：整个对账在一个事务内完成（关系修复 + pair state 同步 + 计数校准 + 经验事件写入），
 * 任一环节异常则整体回滚。注意 Redis count 写入不在 MySQL 事务内，因此顺序上先写 MySQL、
 * 后写 Redis，保证 MySQL 回滚时 Redis 计数尚未被修改。
 * <p>
 * 通过 {@link ConditionalOnProperty} 限定只在 {@code community.like.mode=rabbitmq} 时装配。
 */
@Slf4j
@Service
@ConditionalOnProperty(name = "community.like.mode", havingValue = "rabbitmq")
public class RabbitMqLikeReconciliationWorker {

    /** JDBC 模板：直接执行对账 SQL（关系表、计数表、pair state、ledger 查询） */
    @Resource private JdbcTemplate jdbcTemplate;

    /** 点赞专用 Redis 模板：读取 liked-users Set、count、pairVersions Hash */
    @Resource(name = "likeStringRedisTemplate") private StringRedisTemplate likeStringRedisTemplate;

    /** Redis key 工厂：生成 likedUsersKey、likeCountKey、pairVersionsKey */
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
     *   <li>读取 Redis liked-users Set 与 pairVersions Hash；</li>
     *   <li>加载目标元数据（作者 ID、目标是否可用），不可用则跳过；</li>
     *   <li>对比 Redis 用户集合与 MySQL 关系表：
     *     <ul>
     *       <li>Redis 有、MySQL 无：INSERT IGNORE 补齐，并生成 +1 经验事件；</li>
     *       <li>MySQL 有、Redis 无：DELETE 删除，并生成 -1 经验事件；</li>
     *     </ul>
     *   </li>
     *   <li>以 Redis 快照为准同步 {@code community_like_pair_state} 水位；</li>
     *   <li>以 Redis Set 的精确基数校准 MySQL 冗余计数与 Redis count key；</li>
     *   <li>批量提交经验事件；</li>
     *   <li>返回本次修复的关系变更数量。</li>
     * </ol>
     * <p>
     * 事务语义：方法上的 {@code @Transactional} 保证关系修复、pair state 同步、计数校准、
     * 经验事件写入要么全部成功，要么全部回滚。Redis count 写入在 MySQL 操作之后，
     * 因此 MySQL 回滚时 Redis 计数尚未被修改。
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
        try {
            targetId = Long.parseLong(parts[1]);
        } catch (NumberFormatException exception) {
            throw new IllegalArgumentException("Invalid target id: " + targetToken, exception);
        }
        if (targetId <= 0) throw new IllegalArgumentException("Invalid target id: " + targetToken);

        // ---------- 2. 读取 Redis 状态并执行缺失保护 ----------
        String usersKey = keyFactory.likedUsersKey(type, targetId);
        String countKey = keyFactory.likeCountKey(type, targetId);
        String versionsKey = keyFactory.pairVersionsKey(type, targetId);
        // hasKey 显式做 null 防御，避免拆箱 NPE
        boolean usersKeyExists = Boolean.TRUE.equals(likeStringRedisTemplate.hasKey(usersKey));
        String countRaw = likeStringRedisTemplate.opsForValue().get(countKey);

        // 两个 key 都不存在，不能擅自解释为“确实没有点赞”，否则 Redis 数据丢失会导致 MySQL 关系被清空。
        if (!usersKeyExists && countRaw == null) {
            log.error("Skip reconciliation because Redis like state is missing, not known-empty: target={}", targetToken);
            return 0;
        }
        if (!usersKeyExists) {
            // Set 不存在但 countKey 存在：只有 count == 0 才是“已知空集合”，可安全继续；
            // 非数字或非 0 都说明状态自相矛盾，跳过修复以免误删。
            long storedCount;
            try {
                storedCount = Long.parseLong(countRaw);
            } catch (NumberFormatException exception) {
                log.error("Skip reconciliation because Redis count is not numeric: target={}, value={}",
                        targetToken, countRaw);
                return 0;
            }
            if (storedCount != 0L) {
                log.error("Skip reconciliation because Redis Set is missing but count is nonzero: target={}, count={}",
                        targetToken, storedCount);
                return 0;
            }
        }

        // 读取 Redis 当前点赞用户集合与 pairVersion 水位
        Set<Long> redisUsers = readRedisUsers(usersKey, targetToken);
        Map<Long, Long> redisVersions = readRedisVersions(versionsKey, targetToken);
        // 迁移脚本从 MySQL CSV 回填时只初始化 users Set 和 count，不会为历史点赞创建 versions Hash。
        // 因此版本 Hash 缺失/个别 userId 缺少版本时，不能直接跳过：先用消费 Ledger 的最大版本兜底，
        // 再由现存 pair-state 的 GREATEST 保留更高水位。

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
        List<Long> mysqlRows = jdbcTemplate.query(relationSql,
                (rs, rowNum) -> rs.getLong("userId"), targetId);
        Set<Long> mysqlUsers = new HashSet<>(mysqlRows);
        // 本次对账产生的经验值变更事件（新增 +1 / 删除 -1）
        List<CommunityLikeOutboxEvent> experienceEvents = new ArrayList<>();
        int correctedRelations = 0;

        // Redis 有、MySQL 无：补关系，只有真实 INSERT 才调整经验。
        // 用 INSERT IGNORE 防止并发下重复插入；只有 inserted == 1 才说明发生真实状态跃迁。
        for (Long userId : redisUsers) {
            if (mysqlUsers.contains(userId)) continue;
            int inserted;
            if ("POST".equals(type)) {
                inserted = jdbcTemplate.update("INSERT IGNORE INTO community_post_like "
                                + "(id,postId,userId,createTime) VALUES (?,?,?,CURRENT_TIMESTAMP(3))",
                        IdWorker.getId(), targetId, userId);
            } else {
                inserted = jdbcTemplate.update("INSERT IGNORE INTO picture_like "
                                + "(id,pictureId,userId,createTime) VALUES (?,?,?,CURRENT_TIMESTAMP(3))",
                        IdWorker.getId(), targetId, userId);
            }
            if (inserted == 1) {
                experienceEvents.add(toExperienceEvent(type, targetId, userId, metadata.ownerUserId, 1));
                correctedRelations++;
            }
        }

        // MySQL 有、Redis 无：删除关系，只有真实 DELETE 才扣减经验。
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

        // ---------- 5. 同步 pair state 水位 ----------
        // Redis 快照为准更新 pair state 水位。已有高水位保留，防止重放旧死信覆盖当前状态。
        synchronizePairStates(type, targetId, redisUsers, redisVersions);

        // ---------- 6. 校准计数（MySQL 冗余计数 + Redis 计数器） ----------
        long exactCount = redisUsers.size();
        String countSql = "POST".equals(type)
                ? "UPDATE community_post SET likeCount=? WHERE id=?"
                : "UPDATE picture SET likeCount=? WHERE id=?";
        // 先写 MySQL：若后续事务回滚，Redis count 尚未修改，状态保持一致
        jdbcTemplate.update(countSql, exactCount, targetId);
        // Redis count key 在静默窗口修复为 Set 的精确基数；点赞 API 在同实例内被独占锁阻塞。
        likeStringRedisTemplate.opsForValue().set(countKey, String.valueOf(exactCount));

        // ---------- 7. 提交经验事件 ----------
        // 经验事件写入在本事务内，保证与关系修复同生共死
        if (!experienceEvents.isEmpty()) experienceService.processLikeEvents(experienceEvents);
        if (correctedRelations > 0) {
            log.warn("Like reconciliation repaired target={}, relationChanges={}, redisCount={}",
                    targetToken, correctedRelations, exactCount);
        }
        return correctedRelations;
    }

    /**
     * 读取 Redis liked-users Set，并校验每个 userId 合法性。
     * <p>
     * 任何非数字或非正数的 userId 都视为数据损坏，抛 {@link IllegalStateException}，
     * 让事务回滚、对账中止，避免用损坏数据去修复 MySQL。
     * 异常信息携带 targetToken，便于定位是哪个目标的数据有问题。
     *
     * @param usersKey    liked-users Set 的 key
     * @param targetToken 目标标识（仅用于异常信息）
     * @return 解析后的 userId 集合（Set 不存在时返回空集合）
     */
    private Set<Long> readRedisUsers(String usersKey, String targetToken) {
        Set<String> rawUsers = likeStringRedisTemplate.opsForSet().members(usersKey);
        Set<Long> users = new HashSet<>();
        if (rawUsers != null) {
            for (String value : rawUsers) {
                long userId;
                try {
                    userId = Long.parseLong(value);
                } catch (NumberFormatException exception) {
                    throw new IllegalStateException("Invalid Redis liker id " + value + " on " + targetToken, exception);
                }
                if (userId <= 0) throw new IllegalStateException("Invalid Redis liker id " + value + " on " + targetToken);
                users.add(userId);
            }
        }
        return users;
    }

    /**
     * 读取 Redis pairVersions Hash（actorId -> pairVersion 水位）。
     * <p>
     * 任何非数字、非正数的 key 或 value 都视为数据损坏，抛 {@link IllegalStateException}。
     * Hash 不存在时返回空 Map。
     * <p>
     * 注意：返回空 Map 并不等于"该目标没有版本水位"——迁移回填的历史用户可能没有版本，
     * 后续 {@link #synchronizePairStates} 会用 Ledger 最大版本兜底。
     *
     * @param versionsKey pairVersions Hash 的 key
     * @param targetToken 目标标识（仅用于异常信息）
     * @return actorId -> pairVersion 的映射
     */
    private Map<Long, Long> readRedisVersions(String versionsKey, String targetToken) {
        Map<Object, Object> raw = likeStringRedisTemplate.opsForHash().entries(versionsKey);
        Map<Long, Long> versions = new HashMap<>();
        if (raw == null) return versions;
        for (Map.Entry<Object, Object> entry : raw.entrySet()) {
            try {
                long userId = Long.parseLong(String.valueOf(entry.getKey()));
                long version = Long.parseLong(String.valueOf(entry.getValue()));
                if (userId <= 0 || version <= 0) throw new NumberFormatException("non-positive value");
                versions.put(userId, version);
            } catch (NumberFormatException exception) {
                throw new IllegalStateException("Invalid Redis pair version on " + targetToken
                        + ": " + entry.getKey() + "=" + entry.getValue(), exception);
            }
        }
        return versions;
    }

    /**
     * 以 Redis 快照为准同步 {@code community_like_pair_state} 水位表。
     * <p>
     * 步骤：
     * <ol>
     *   <li>先把该目标下所有历史 pair 标记为未点赞（liked=0）；</li>
     *   <li>合并 redisVersions 与 redisUsers 的 actor 集合；</li>
     *   <li>从消费 Ledger 查每个 actor 的最大已消费版本，作为历史用户的兜底水位；</li>
     *   <li>按 userId 排序后批量 upsert，使用 {@code GREATEST} 保留更高水位，
     *       防止重放旧死信把水位回退。</li>
     * </ol>
     * <p>
     * 排序写入的目的：固定对账批次写 pair 行的顺序，降低与消费者并发写入时的死锁概率。
     * <p>
     * {@code liked=VALUES(liked)} 直接以 Redis 快照为准，覆盖第一步的 liked=0。
     *
     * @param type          目标类型
     * @param targetId      目标 ID
     * @param redisUsers    Redis 当前点赞用户集合
     * @param redisVersions Redis pairVersion 水位
     */
    private void synchronizePairStates(String type, long targetId,
                                       Set<Long> redisUsers, Map<Long, Long> redisVersions) {
        // 先将目标下所有历史 pair 标记为未点赞，再把 Redis versions/liked 快照逐项 upsert。
        jdbcTemplate.update("UPDATE community_like_pair_state SET liked=0,updated_at=CURRENT_TIMESTAMP(3) "
                + "WHERE target_type=? AND target_id=?", type, targetId);

        // 合并 versions 与 users：只有 versions 的用户（已取消点赞）也需要写 pair state，
        // 只有 users 的用户（历史回填、无版本）也需要写 liked=1。
        Set<Long> allUsers = new HashSet<>(redisVersions.keySet());
        allUsers.addAll(redisUsers);
        if (allUsers.isEmpty()) return;

        // 历史回填用户可能没有 pairVersions，用 Ledger 最大已消费版本兜底
        Map<Long, Long> ledgerVersions = findLedgerVersions(type, targetId, allUsers);
        String sql = "INSERT INTO community_like_pair_state "
                + "(target_type,target_id,actor_user_id,last_pair_version,liked,updated_at) "
                + "VALUES (?,?,?,?,?,CURRENT_TIMESTAMP(3)) "
                + "ON DUPLICATE KEY UPDATE "
                + "last_pair_version=GREATEST(last_pair_version,VALUES(last_pair_version)), "
                + "liked=VALUES(liked),updated_at=CURRENT_TIMESTAMP(3)";
        List<Long> users = new ArrayList<>(allUsers);
        Collections.sort(users); // 对账批次固定顺序写 pair 行，降低与消费者并发时的死锁概率。
        jdbcTemplate.batchUpdate(sql, users, users.size(), (ps, userId) -> {
            ps.setString(1, type);
            ps.setLong(2, targetId);
            ps.setLong(3, userId);
            long redisVersion = redisVersions.getOrDefault(userId, 0L);
            long ledgerVersion = ledgerVersions.getOrDefault(userId, 0L);
            // 取 max：版本 Hash 存在时用 Redis 版本；缺失时用 Ledger 版本兜底
            ps.setLong(4, Math.max(redisVersion, ledgerVersion));
            ps.setBoolean(5, redisUsers.contains(userId));
        });
    }

    /**
     * 从消费 Ledger 查询每个 actor 的最大已消费 pairVersion。
     * <p>
     * 用途：Redis 基线回填不含 pairVersions，历史用户没有版本水位；
     * 用已消费 Ledger 的最大版本为这些用户建立初始水位，
     * 避免对账把水位写回 0 导致后续乱序保护失效。
     * <p>
     * 使用 IN 占位符动态拼接，users 为空时直接返回空 Map（避免生成非法 SQL）。
     *
     * @param type     目标类型
     * @param targetId 目标 ID
     * @param users    需要查询版本的 actor 集合
     * @return actorId -> 最大已消费版本；Ledger 无记录的用户不在返回 Map 中
     */
    private Map<Long, Long> findLedgerVersions(String type, long targetId, Set<Long> users) {
        if (users.isEmpty()) return Collections.emptyMap();
        String placeholders = String.join(",", Collections.nCopies(users.size(), "?"));
        String sql = "SELECT actor_user_id,MAX(pair_version) AS max_version "
                + "FROM community_like_event_consume WHERE target_type=? AND target_id=? "
                + "AND actor_user_id IN (" + placeholders + ") GROUP BY actor_user_id";
        return jdbcTemplate.query(sql, ps -> {
            ps.setString(1, type);
            ps.setLong(2, targetId);
            int index = 3;
            for (Long userId : users) ps.setLong(index++, userId);
        }, rs -> {
            Map<Long, Long> versions = new HashMap<>();
            while (rs.next()) {
                versions.put(rs.getLong("actor_user_id"), rs.getLong("max_version"));
            }
            return versions;
        });
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
        return owners.isEmpty() ? null : new TargetMetadata(owners.get(0));
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