package com.zmt.picturebackendpro.service.impl.mq;

import com.baomidou.mybatisplus.core.toolkit.IdWorker;
import com.zmt.picturebackendpro.service.redis.RedisLikeEvent;
import com.zmt.picturebackendpro.service.redis.RedisLikePersistenceService;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.Resource;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

/**
 * RabbitMQ 模式专用持久化入口。
 *
 * <p><b>核心约束：</b></p>
 * 1. 一个事件批次仍由父类放在同一 MySQL 事务中提交；
 * 2. 消费者先按固定顺序锁定 pair-state 行，再读取关系和版本水位；
 * 3. pairVersion 小于当前水位的事件只写消费 Ledger，不得改变关系、计数或经验；
 * 4. 对账 worker 使用同一组 pair-state 行锁，防止对账与 Retry Queue 消费互相覆盖；
 * 5. 如果旧版本消费者曾经造成关系投影偏离已知最高版本，先修复投影，再应用新事件。
 *
 * <p><b>为什么需要本类：</b></p>
 * Redis Stream 模式天然按流顺序消费，消息不会乱序；而 RabbitMQ 重试队列可能把旧事件
 * 重新投递到消费者，导致"旧版本事件晚到反而覆盖新状态"的经典问题。本类通过
 * {@code community_like_pair_state} 记录每个 (target, actor) 二元组的已知最高版本水位，
 * 把版本判断、关系投影、计数/经验副作用全部收敛到同一个事务里，保证幂等和乱序安全。
 *
 * <p><b>职责边界：</b></p>
 * - 本类只覆盖"落库语义"相关钩子，Ledger 写入、计数与经验副作用仍由父类完成；
 * - Redis Stream 模式仍使用父类原来的无版本策略，本类通过
 *   {@link ConditionalOnProperty} 仅在 {@code community.like.mode=rabbitmq} 时生效，
 *   不会影响其他模式。
 *
 * @see RedisLikePersistenceService
 */
@Service
@ConditionalOnProperty(name = "community.like.mode", havingValue = "rabbitmq")
public class RabbitMqLikePersistenceService extends RedisLikePersistenceService {

    /**
     * Micrometer 指标注册表。
     * 用于记录各类 pairVersion 决策（STALE / DUPLICATE / ADVANCE_NOOP / CHANGE）
     * 与投影修复次数，便于观测线上乱序/重投递的比例是否异常。
     */
    @Resource
    private MeterRegistry meterRegistry;

    /**
     * 当前调用批次的 pair 上下文。
     *
     * <p><b>为什么使用 ThreadLocal 而不是成员变量：</b></p>
     * 本类是 Spring Singleton，若使用普通 {@code Map} 成员变量，多线程（如多个 Rabbit
     * 监听容器线程、或并发对账线程）会共享同一 Map，产生线程安全问题。
     * 而 Spring 的 {@code @Transactional} 绑定在调用线程上，一个批次内的
     * {@code persistBatch -> beforeApplyEvents -> applyRelationStateDeltas}
     * 都在同一线程内同步执行，因此 ThreadLocal 正好能让上下文在批次期间存活、
     * 在批次结束后立即释放（finally 中 {@code remove()} 防止内存泄漏）。
     *
     * <p><b>为什么要单独维护一份上下文：</b></p>
     * {@link #applyRelationStateDeltas} 在遍历事件时会被父类逐条回调，
     * 父类并不知道我们加了 pair-state 这层结构。因此需要在
     * {@link #beforeApplyEvents} 阶段把每个 pair 的基线版本与基线状态缓存下来，
     * 事件应用阶段直接读取，避免重复查询数据库。
     */
    private final ThreadLocal<Map<PairKey, PairContext>> pairContexts = new ThreadLocal<>();

    /**
     * 批次持久化入口。
     *
     * <p>工作流程：</p>
     * 1. 初始化本批次线程级上下文，保证 {@link #beforeApplyEvents} 与
     *    {@link #applyRelationStateDeltas} 能在同一批次内共享 pair 基线；
     * 2. 调用父类 {@code super.persistBatch(events)}，由父类在一个事务中完成：
     *    Ledger 写入、目标 likeCount 更新、receivedLikeCount 更新、经验累加等。
     *    父类模板方法会动态回调本类覆盖的两个钩子，因此无需本类重复实现；
     * 3. finally 中清除 ThreadLocal，防止线程复用造成上下文污染或内存泄漏。
     *
     * <p><b>事务边界：</b></p>
     * 本方法上的 {@code @Transactional(rollbackFor = Exception.class)} 与父类保持一致，
     * 保证 pair-state、关系投影、Ledger、计数、经验全部是同一事务，任何一步失败都整体回滚，
     * 不会出现"投影已改但计数未改"的中间态。
     */
    @Override
    @Transactional(rollbackFor = Exception.class)
    public void persistBatch(List<RedisLikeEvent> events) {
        pairContexts.set(new HashMap<PairKey, PairContext>());
        try {
            // 通过父类的事务批处理框架完成 Ledger、目标计数与经验副作用。
            // 父类会动态调用本类覆盖的 beforeApplyEvents / applyRelationStateDeltas。
            super.persistBatch(events);
        } finally {
            // 线程复用场景（如 Rabbit 监听容器线程池）下必须显式清理，
            // 否则下个批次可能读到上一个批次残留的上下文。
            pairContexts.remove();
        }
    }

    /**
     * 在应用任何事件之前，先为本批涉及的所有 pair 建立并按稳定顺序加锁。
     *
     * <p><b>关键设计点：</b></p>
     *
     * <h4>1. 稳定顺序加锁，降低死锁概率</h4>
     * 用 {@link TreeSet} 对 PairKey 排序（targetType -> targetId -> actorUserId），
     * 保证任意批次、任意对账任务都以同一顺序请求行锁，避免两个事务相互等待对方持有的
     * pair-state 行锁造成死锁。
     *
     * <h4>2. INSERT IGNORE + SELECT ... FOR UPDATE</h4>
     * pair-state 行可能尚不存在（比如第一次收到某 pair 的事件），因此先用
     * {@code INSERT IGNORE} 保证行存在（已存在则无影响），再 {@code FOR UPDATE}
     * 拿排他锁。两步放在同一事务中，且唯一键 (target_type, target_id, actor_user_id)
     * 保证并发 insert 最多只有一条成功。
     *
     * <h4>3. 与 Ledger 做一次"水位对齐"</h4>
     * 上线前/旧版本消费者可能已经往 Ledger 里写入了更高版本的事件，但 pair-state 水位没同步。
     * 这里通过查询 Ledger 中该 pair 的最高 pair_version 与对应 desired_state，将水位
     * 补齐到"已消费过的真实最高版本"，防止旧事件被误认为新事件而覆盖真实状态。
     * 同时校验最高版本上是否出现多个不同 desired_state——若存在说明历史数据已经被
     * 并发写坏，直接抛异常让消息进死信，避免继续扩散。
     *
     * <h4>4. version=0 的历史基线</h4>
     * 某些历史数据（如 CSV 导入）没有版本信息，此时 pair-state 初始版本为 0。
     * 这种情况下不能假定状态为 UNLIKE，否则会把历史点赞误清空。此时应读取关系表当前状态
     * 作为可信基线。此后再来新事件时，只要 eventVersion > 0 就能正常判定 CHANGE / STALE。
     *
     * <h4>5. 把对齐结果写回 pair-state</h4>
     * 如果对齐得到的水位/状态与 DB 中 pair-state 不一致（比如 Ledger 上有更高版本，
     * 或 version=0 下用关系表校正了初始值），则立即更新 pair-state，
     * 使后续批次（包括对账 worker）看到一致的基线。
     */
    @Override
    protected void beforeApplyEvents(List<RedisLikeEvent> events) {
        Map<PairKey, PairContext> contexts = pairContexts.get();
        if (contexts == null) {
            // 理论上不会发生：persistBatch 一定会先设置上下文。
            // 若出现说明有子类绕过 persistBatch 直接调用本方法，属于编程错误。
            throw new IllegalStateException("RabbitMQ pair-version 上下文未初始化");
        }

        // TreeSet 依赖 PairKey 的 compareTo，实现固定顺序加锁，避免死锁。
        TreeSet<PairKey> keys = new TreeSet<>();
        for (RedisLikeEvent event : events) {
            keys.add(new PairKey(event.getTargetType(), event.targetIdAsLong(), event.userIdAsLong()));
        }

        for (PairKey key : keys) {
            // 唯一键必须是 (target_type,target_id,actor_user_id)。
            // INSERT IGNORE 在并发场景下是安全的：并发 insert 中最多一条生效，
            // 其余被唯一键冲突忽略，不会报错也不会破坏已有数据。
            jdbcTemplate.update(
                    "INSERT IGNORE INTO community_like_pair_state " +
                            "(target_type,target_id,actor_user_id,last_pair_version,liked,updated_at) " +
                            "VALUES (?,?,?,0,0,CURRENT_TIMESTAMP(3))",
                    key.targetType, key.targetId, key.actorUserId);

            // 立即对 pair-state 行加排他锁。此后本事务与任何对账 worker 对该 pair 的操作
            // 都互斥，防止对账结果与消费者结果互相覆盖。
            List<PairStateRow> stateRows = jdbcTemplate.query(
                    "SELECT last_pair_version,liked FROM community_like_pair_state " +
                            "WHERE target_type=? AND target_id=? AND actor_user_id=? FOR UPDATE",
                    (rs, rowNum) -> new PairStateRow(rs.getLong("last_pair_version"), rs.getBoolean("liked")),
                    key.targetType, key.targetId, key.actorUserId);
            if (stateRows.size() != 1) {
                // 唯一键保证只有一行；行不存在意味着 INSERT IGNORE 失败，属于数据/并发异常，
                // 直接抛出交由事务回滚，避免后续逻辑基于错误状态运行。
                throw new IllegalStateException("无法锁定唯一点赞 pair-state 行: " + key);
            }
            PairStateRow persistedState = stateRows.get(0);

            // 兼容修复上线前已写入 Ledger、但没有同步 pair-state 水位的事件。
            // 通过 Ledger 中最高版本反推真实水位，避免遗漏历史消费进度。
            // FOR UPDATE 强制读取最高已提交版本，防止读到未提交数据造成误判。
            List<Long> latestVersions = jdbcTemplate.query(
                    "SELECT pair_version FROM community_like_event_consume " +
                            "WHERE target_type=? AND target_id=? AND actor_user_id=? " +
                            "ORDER BY pair_version DESC LIMIT 1 FOR UPDATE",
                    (rs, rowNum) -> rs.getLong("pair_version"),
                    key.targetType, key.targetId, key.actorUserId);

            // 基线初值取自 pair-state，必要时用 Ledger 中的最新版本覆盖。
            long baselineVersion = persistedState.version;
            boolean baselineLiked = persistedState.liked;
            if (!latestVersions.isEmpty()) {
                long ledgerVersion = latestVersions.get(0);
                // 检查 Ledger 最高版本上是否出现多个不同的 desired_state。
                // 正常情况下同一 pair 同一版本只可能是同一种状态（一次事件只对应一个状态）；
                // 若出现多个状态，说明历史上有并发写入或数据被篡改，继续执行会导致关系投影
                // 不可预测，因此直接抛异常让消息进死信，交由人工介入。
                List<String> latestStates = jdbcTemplate.query(
                        "SELECT desired_state FROM community_like_event_consume " +
                                "WHERE target_type=? AND target_id=? AND actor_user_id=? AND pair_version=? FOR UPDATE",
                        (rs, rowNum) -> rs.getString("desired_state"),
                        key.targetType, key.targetId, key.actorUserId, ledgerVersion);
                Set<String> stateVariants = new HashSet<>(latestStates);
                if (stateVariants.size() != 1) {
                    throw new IllegalStateException("Ledger 最高 pairVersion 对应多个状态: " + key
                            + ", version=" + ledgerVersion + ", states=" + stateVariants);
                }
                boolean ledgerLiked = toLiked(stateVariants.iterator().next(), "Ledger", key);
                if (ledgerVersion > baselineVersion) {
                    // Ledger 水位高于 pair-state：说明 pair-state 落后，需要用 Ledger 水位纠偏。
                    baselineVersion = ledgerVersion;
                    baselineLiked = ledgerLiked;
                } else if (ledgerVersion == baselineVersion && baselineVersion > 0
                        && ledgerLiked != baselineLiked) {
                    // 同版本却状态不同，说明 pair-state 与 Ledger 出现数据不一致，
                    // 无法判断谁是真的，直接失败让消息进死信，等待人工核查。
                    throw new IllegalStateException("pair-state 与消费 Ledger 同版本状态冲突: " + key
                            + ", version=" + baselineVersion);
                }
            }

            // version=0 表示还没有可信的版本历史（例如 CSV 导入的历史关系）。
            // 此时以关系表当前状态作为基线，不能把历史点赞误清空。
            // 注意：这里对关系表加 FOR UPDATE，避免与对账或其他 writer 并发修改。
            boolean actualLiked = relationExistsForUpdate(key);
            if (baselineVersion == 0L) {
                baselineLiked = actualLiked;
            }

            // 把从 Ledger 恢复出的较高水位，或 version=0 的历史基线，落回 pair-state。
            // 只有真正需要变更时才 UPDATE，减少无谓写入与锁竞争。
            if (baselineVersion != persistedState.version || baselineLiked != persistedState.liked) {
                updatePairState(key, baselineVersion, baselineLiked);
            }

            // 将基线缓存在线程级上下文，供本批次后续事件应用阶段直接读取，
            // 避免重复查询数据库。
            // actualLiked 也要缓存：事件应用前需要先把关系投影修复到基线状态。
            contexts.put(key, new PairContext(baselineVersion, baselineLiked, actualLiked));
        }
    }

    /**
     * 应用单条事件。
     *
     * <p><b>返回值语义：</b></p>
     * 父类只根据返回值累加 likeCount 与 receivedLikeCount，绝不盲信事件自带 delta。
     * 返回值是本次事务实际发生的关系跃迁 delta 列表：
     * <ul>
     *   <li>空列表：STALE / DUPLICATE_VERSION / ADVANCE_NOOP，均未改变关系投影；</li>
     *   <li>单个 delta：正常 CHANGE，返回 +1（点赞）或 -1（取消）；</li>
     *   <li>可能先包含投影修复 delta，再包含当前事件 delta（两元素列表）：
     *       说明修复前关系表与已知最高版本不一致，需要先把投影修正到基线状态
     *       （例如补记一条 -1），再应用当前事件的实际跃迁（例如 +1）。</li>
     * </ul>
     *
     * <p><b>执行顺序：</b></p>
     * 1. 先做投影修复（最多一次），保证关系表与已知最高版本状态一致；</p>
     * 2. 再用 {@link LikePairVersionPolicy#evaluate} 对本事件做版本判定，
     *    返回 STALE / DUPLICATE_VERSION / ADVANCE_NOOP / CHANGE 四种决策；</p>
     * 3. 依据决策落库并维护 pair-state 水位。</p>
     */
    @Override
    protected List<Integer> applyRelationStateDeltas(RedisLikeEvent event) {
        PairKey key = new PairKey(event.getTargetType(), event.targetIdAsLong(), event.userIdAsLong());
        Map<PairKey, PairContext> contexts = pairContexts.get();
        PairContext context = contexts == null ? null : contexts.get(key);
        if (context == null) {
            // 正常情况下 beforeApplyEvents 已经为本批次所有 pair 建立上下文；
            // 走到这里说明父类回调的事件未在 beforeApplyEvents 中出现（编程错误）
            // 或者有人绕过 beforeApplyEvents 直接调用本方法。
            throw new IllegalStateException("当前批次缺少已锁定的 pair 上下文: " + key);
        }

        List<Integer> actualDeltas = new ArrayList<>(2);

        // 先把历史代码可能留下的关系投影偏差修正到当前已知最高版本状态。
        // 场景示例：Ledger 已记录 pairVersion=5 且 liked=true，但关系表里没有这条记录，
        // 说明旧消费者写 Ledger 成功、写关系失败（或反过来），导致关系表与最高版本状态不一致。
        // 此时先在当前事务内把关系表修正到 liked=true，并记 +1 让上层计数同步纠正，
        // 然后再应用本次事件。这一修复与后续事件、Ledger、计数和经验仍处于同一事务。
        if (!context.projectionReconciled) {
            if (context.actualLiked != context.liked) {
                int changed = writeRelationState(key, context.liked);
                if (changed != 1) {
                    // INSERT IGNORE / DELETE 均预期影响 1 行；
                    // 若为 0 说明并发或数据异常，继续执行会让计数与实际关系进一步偏离，
                    // 因此直接失败让事务回滚。
                    throw new IllegalStateException("修复关系投影失败: " + key + ", expectedRows=1, actualRows=" + changed);
                }
                // 把修复动作记入 delta，让父类同步调整 likeCount / receivedLikeCount。
                actualDeltas.add(context.liked ? 1 : -1);
                context.actualLiked = context.liked;
                count("projection_repaired");
            }
            // 标记已尝试修复，后续事件不再重复修复；
            // 因为同一事务内关系表只会被本方法修改，一旦修复一致就不会再偏离。
            context.projectionReconciled = true;
        }

        long eventVersion = event.pairVersionAsLong();
        boolean desiredLiked = "LIKE".equals(event.getDesiredState());

        // 用独立的策略类做版本判定，便于单元测试覆盖各种边界：
        // - STALE：eventVersion < context.version，旧事件，丢弃
        // - DUPLICATE_VERSION：eventVersion == context.version，重复投递，丢弃
        // - ADVANCE_NOOP：eventVersion > context.version 但状态一致，仅推进水位
        // - CHANGE：eventVersion > context.version 且状态不一致，需要切换关系
        LikePairVersionPolicy.Decision decision = LikePairVersionPolicy.evaluate(
                context.version, context.liked, eventVersion, desiredLiked);

        switch (decision.getAction()) {
            case STALE:
                // 旧事件仍会由父类写入消费 Ledger 并由 Rabbit listener ACK，避免无限重试。
                // 注意：写 Ledger 是必要的，否则每次重投递都会被再次处理，浪费资源；
                // 写 Ledger 后配合 pair-state 版本号，可以保证消息最终被"消费"完并 ACK。
                count("stale");
                return actualDeltas;
            case DUPLICATE_VERSION:
                // 同一版本重复投递，视为已处理，直接 ACK。
                count("duplicate");
                return actualDeltas;
            case ADVANCE_NOOP:
                // 状态已经一致，但版本更新。必须推进水位，后续更旧事件才能被拦截。
                // 水位推进本身不产生关系 delta，因此不修改 likeCount / receivedLikeCount。
                context.version = eventVersion;
                context.liked = desiredLiked;
                updatePairState(key, context.version, context.liked);
                count("advance_noop");
                return actualDeltas;
            case CHANGE:
                // 真实状态跃迁：写关系表 -> 更新上下文 -> 更新 pair-state 水位。
                int changed = writeRelationState(key, desiredLiked);
                if (changed != 1) {
                    // 判定了需要跃迁但实际影响 0 行，说明关系表与预期不一致（并发或被其他路径修改），
                    // 继续执行会让计数和关系进一步偏离，因此直接失败回滚。
                    throw new IllegalStateException("pairVersion 判定需要状态跃迁，但关系表影响行数异常: "
                            + key + ", version=" + eventVersion + ", desiredLiked=" + desiredLiked
                            + ", changedRows=" + changed);
                }
                int actualDelta = decision.getActualDelta();
                actualDeltas.add(actualDelta);
                context.actualLiked = desiredLiked;
                context.liked = desiredLiked;
                context.version = eventVersion;
                updatePairState(key, context.version, context.liked);
                count("changed");
                return actualDeltas;
            default:
                // 防御性兜底：若策略类新增枚举而此处未覆盖，直接失败以避免静默丢弃事件。
                throw new IllegalStateException("未知 pairVersion action: " + decision.getAction());
        }
    }

    /**
     * 查询关系表中 pair 当前是否存在，并对该行加锁。
     *
     * <p>加锁目的：</p>
     * 1. 与对账 worker、其他 writer 互斥，防止读取到并发修改中的状态；
     * 2. 后续 {@link #writeRelationState} 的 INSERT/DELETE 与这里是同一事务，
     *    保持锁语义一致，避免中间状态被外界读到。
     *
     * <p>返回 true 表示关系存在（已点赞），false 表示关系不存在（未点赞）。</p>
     */
    private boolean relationExistsForUpdate(PairKey key) {
        String sql = "POST".equals(key.targetType)
                ? "SELECT 1 FROM community_post_like WHERE postId=? AND userId=? LIMIT 1 FOR UPDATE"
                : "SELECT 1 FROM picture_like WHERE pictureId=? AND userId=? LIMIT 1 FOR UPDATE";
        List<Integer> rows = jdbcTemplate.query(sql, (rs, rowNum) -> 1, key.targetId, key.actorUserId);
        return !rows.isEmpty();
    }

    /**
     * 将指定 pair 的关系表状态设置为 desiredLiked，返回真实影响行数。
     *
     * <p>实现说明：</p>
     * - 使用 INSERT IGNORE：避免两个事务同时尝试插入同一唯一键导致冲突；
     *   若已存在，则影响 0 行——但本方法调用前已通过上下文判断状态不同，
     *   因此返回 0 会被调用方视为异常。
     * - 使用 DELETE：影响 0 行说明关系不存在，同样会被调用方视为异常。
     * - 主键 id 用 {@link IdWorker} 生成，避免分库分表/分布式场景下的 ID 冲突。
     *
     * <p>返回值通常应为 1；返回 0 或 >1 说明状态与预期不一致，由调用方抛异常回滚。</p>
     */
    private int writeRelationState(PairKey key, boolean desiredLiked) {
        if ("POST".equals(key.targetType)) {
            if (desiredLiked) {
                return jdbcTemplate.update(
                        "INSERT IGNORE INTO community_post_like (id,postId,userId,createTime) " +
                                "VALUES (?,?,?,CURRENT_TIMESTAMP(3))",
                        IdWorker.getId(), key.targetId, key.actorUserId);
            }
            return jdbcTemplate.update(
                    "DELETE FROM community_post_like WHERE postId=? AND userId=?",
                    key.targetId, key.actorUserId);
        }
        if ("PICTURE".equals(key.targetType)) {
            if (desiredLiked) {
                return jdbcTemplate.update(
                        "INSERT IGNORE INTO picture_like (id,pictureId,userId,createTime) " +
                                "VALUES (?,?,?,CURRENT_TIMESTAMP(3))",
                        IdWorker.getId(), key.targetId, key.actorUserId);
            }
            return jdbcTemplate.update(
                    "DELETE FROM picture_like WHERE pictureId=? AND userId=?",
                    key.targetId, key.actorUserId);
        }
        throw new IllegalArgumentException("Unsupported like target type: " + key.targetType);
    }

    /**
     * 更新 pair-state 水位与状态。
     *
     * <p>更新影响行数必须为 1：beforeApplyEvents 中已经 INSERT IGNORE 并 FOR UPDATE 锁定该行，
     * 因此这里不应该出现 0 行；若出现说明数据异常或事务隔离级别被改动，
     * 继续执行会基于错误的水位运行，因此直接抛异常。</p>
     */
    private void updatePairState(PairKey key, long version, boolean liked) {
        int updated = jdbcTemplate.update(
                "UPDATE community_like_pair_state SET last_pair_version=?,liked=?,updated_at=CURRENT_TIMESTAMP(3) " +
                        "WHERE target_type=? AND target_id=? AND actor_user_id=?",
                version, liked ? 1 : 0, key.targetType, key.targetId, key.actorUserId);
        if (updated != 1) {
            throw new IllegalStateException("更新 pair-state 失败: " + key + ", updatedRows=" + updated);
        }
    }

    /**
     * 把 Ledger 中的 desired_state 字符串转换为布尔值。
     *
     * <p>使用严格的白名单而不是 {@code "LIKE".equals(...)} 之类的宽松判断：
     * 一旦出现非法状态（如空值、拼写错误、历史遗留数据），立即抛异常让消息进死信，
     * 避免用错误的状态推进水位造成后续事件全部被误判。</p>
     */
    private boolean toLiked(String desiredState, String source, PairKey key) {
        if ("LIKE".equals(desiredState)) return true;
        if ("UNLIKE".equals(desiredState)) return false;
        throw new IllegalStateException(source + " 中存在非法 desired_state: " + key + ", state=" + desiredState);
    }

    /**
     * 记录一次 pairVersion 决策结果（或投影修复）指标。
     *
     * <p>通过 outcome 标签区分：
     * stale / duplicate / advance_noop / changed / projection_repaired。
     * 这些指标帮助定位"旧事件重投递过多""关系投影发生漂移"等线上问题。</p>
     */
    private void count(String outcome) {
        Counter.builder("community.like.rabbitmq.consumer.pair_version")
                .tag("outcome", outcome)
                .description("RabbitMQ like pair-version fence decisions and projection repairs")
                .register(meterRegistry)
                .increment();
    }

    /**
     * 点赞二元组的唯一标识：{@code targetType + targetId + actorUserId}。
     *
     * <p><b>为什么实现 {@link Comparable}：</b></p>
     * {@link #beforeApplyEvents} 使用 {@link TreeSet} 以固定顺序对多个 pair 加锁。
     * 固定加锁顺序是避免多事务死锁的经典手段——任意两个事务都按同一顺序请求行锁，
     * 不会出现循环等待，因此不会死锁。
     *
     * <p>同时实现 {@link #equals(Object)} / {@link #hashCode()}，保证在
     * {@link HashMap} 中能正确去重和查找。</p>
     */
    private static final class PairKey implements Comparable<PairKey> {
        private final String targetType;
        private final long targetId;
        private final long actorUserId;

        private PairKey(String targetType, long targetId, long actorUserId) {
            this.targetType = targetType;
            this.targetId = targetId;
            this.actorUserId = actorUserId;
        }

        /**
         * 字典序：先按 targetType，再按 targetId，最后按 actorUserId。
         * 三段全比较，保证 total order，使 TreeSet 稳定排序。
         */
        @Override
        public int compareTo(PairKey other) {
            int typeCompare = targetType.compareTo(other.targetType);
            if (typeCompare != 0) return typeCompare;
            int targetCompare = Long.compare(targetId, other.targetId);
            if (targetCompare != 0) return targetCompare;
            return Long.compare(actorUserId, other.actorUserId);
        }

        @Override
        public boolean equals(Object other) {
            if (this == other) return true;
            if (!(other instanceof PairKey)) return false;
            PairKey that = (PairKey) other;
            return targetId == that.targetId && actorUserId == that.actorUserId
                    && targetType.equals(that.targetType);
        }

        @Override
        public int hashCode() {
            int result = targetType.hashCode();
            result = 31 * result + Long.hashCode(targetId);
            result = 31 * result + Long.hashCode(actorUserId);
            return result;
        }

        /** 便于日志与异常信息定位具体是哪个 pair。 */
        @Override
        public String toString() {
            return targetType + ":" + targetId + ":" + actorUserId;
        }
    }

    /**
     * pair-state 行查询结果的不可变载体，仅用于方法内部传递，避免返回裸数组。
     */
    private static final class PairStateRow {
        private final long version;
        private final boolean liked;
        private PairStateRow(long version, boolean liked) {
            this.version = version;
            this.liked = liked;
        }
    }

    /**
     * 单个 pair 在当前批次中的上下文。
     *
     * <p>字段含义：</p>
     * <ul>
     *   <li>{@code version}：当前已知最高 pairVersion 水位（由 pair-state 与 Ledger 对齐后得出）；</li>
     *   <li>{@code liked}：上述水位对应的"期望状态"（关系表应当呈现的状态）；</li>
     *   <li>{@code actualLiked}：关系表当前实际状态，用于检测投影漂移；</li>
     *   <li>{@code projectionReconciled}：本批次是否已完成投影修复，保证一个 pair 只修复一次。</li>
     * </ul>
     *
     * <p>作用：</p>
     * 将"DB 查询 + 版本判定"的结果缓存到内存，事件应用阶段无需再查库，
     * 同时保证同一批次内对同一 pair 的多次事件按版本顺序正确推进。
     */
    private static final class PairContext {
        private long version;
        private boolean liked;
        private boolean actualLiked;
        private boolean projectionReconciled;

        private PairContext(long version, boolean liked, boolean actualLiked) {
            this.version = version;
            this.liked = liked;
            this.actualLiked = actualLiked;
        }
    }
}