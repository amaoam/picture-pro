package com.zmt.picturebackendpro.service.impl.mq;

/**
 * Pair-version 状态机（点赞关系的幂等与乱序保护策略）。
 * <p>
 * 设计语义：每个事件表达的是<b>期望状态</b>（desiredLiked），而不是需要无条件累加的 delta。
 * 这样即使同一事件被重复消费、或在延迟重试队列中乱序到达，也不会把点赞数反复 +1/-1：
 * 关系表最终只按"当前是否点赞"收敛，计数变化只发生在真实状态跃迁时。
 * <p>
 * 版本语义：
 * <ul>
 *   <li>每个 actor 对每个 target 都有一个单调递增的 {@code pairVersion}；</li>
 *   <li>版本<b>允许跳号</b>：事件不要求连续，高版本事件可以覆盖低版本事件，
 *       因为事件本身承载了完整的 desiredState，不依赖中间版本；</li>
 *   <li>同版本事件必须对应同一状态，否则说明数据损坏或上游生成逻辑有 bug，直接抛异常暴露问题。</li>
 * </ul>
 * <p>
 * 该策略是纯函数：不读写数据库、不依赖线程状态，判定结果由 {@link #evaluate} 返回，
 * 由调用方（消费者/对账 worker）根据 {@link Decision} 决定是否落库、如何调整计数。
 */
final class LikePairVersionPolicy {

    /**
     * 事件相对当前持久化状态的判定结果。
     */
    enum Action {
        /** 事件版本低于数据库已应用版本：属于过期消息，忽略，不改变任何状态。 */
        STALE,
        /** 同版本、同状态：属于重复事件（重复投递/重试），忽略，不改变任何状态。 */
        DUPLICATE_VERSION,
        /** 新版本，但当前关系状态已经等于期望状态：只需推进版本水位，不改变关系。 */
        ADVANCE_NOOP,
        /** 新版本，且确实需要改变关系：需要执行 INSERT/DELETE，并按 actualDelta 调整计数。 */
        CHANGE
    }

    /**
     * 一次判定的结果。
     * <p>
     * {@code action} 告诉调用方"该怎么处理"，{@code actualDelta} 给出
     * 只有 {@link Action#CHANGE} 时才非零的计数变化量：
     * <ul>
     *   <li>{@code +1}：由未点赞变为点赞，需要增加计数；</li>
     *   <li>{@code -1}：由已点赞变为未点赞，需要减少计数；</li>
     *   <li>{@code 0}：其余所有 Action，计数不变。</li>
     * </ul>
     * 注意：{@code actualDelta} 是"真实状态跃迁"的 delta，不是事件携带的 delta，
     * 因此重复消费、乱序到达都不会导致计数被重复累加。
     */
    static final class Decision {
        /** 判定类型 */
        private final Action action;
        /** 真实计数变化量：仅 CHANGE 时为 +1/-1，其余为 0 */
        private final int actualDelta;

        private Decision(Action action, int actualDelta) {
            this.action = action;
            this.actualDelta = actualDelta;
        }

        /** 返回判定类型，调用方据此决定是否落库、是否推进版本。 */
        Action getAction() { return action; }

        /** 返回真实计数变化量（仅 CHANGE 非零）。 */
        int getActualDelta() { return actualDelta; }
    }

    /** 工具类，禁止实例化。 */
    private LikePairVersionPolicy() {
    }

    /**
     * 根据当前持久化状态与事件期望状态，计算本次事件应如何处理。
     * <p>
     * 判定顺序（短路返回，顺序很重要）：
     * <ol>
     *   <li><b>参数校验</b>：数据库当前版本不能为负（{@code currentVersion < 0} 视为损坏），
     *       事件版本必须为正（{@code eventVersion <= 0} 视为非法事件），否则抛
     *       {@link IllegalArgumentException}；</li>
     *   <li><b>STALE</b>：{@code eventVersion < currentVersion}，过期事件，忽略；</li>
     *   <li><b>同版本</b>：{@code eventVersion == currentVersion}：
     *     <ul>
     *       <li>状态一致 → {@link Action#DUPLICATE_VERSION}（重复事件）；</li>
     *       <li>状态冲突 → 抛 {@link IllegalStateException}，因为同一版本不可能合法地对应两种状态，
     *           这通常意味着上游生成逻辑或存储被破坏，必须暴露而不是静默吞掉；</li>
     *     </ul>
     *   </li>
     *   <li><b>新版本</b>：{@code eventVersion > currentVersion}：
     *     <ul>
     *       <li>当前状态已等于期望状态 → {@link Action#ADVANCE_NOOP}，只推进水位；</li>
     *       <li>状态不同 → {@link Action#CHANGE}，actualDelta 为 {@code +1}（点赞）或 {@code -1}（取消）。</li>
     *     </ul>
     *   </li>
     * </ol>
     * <p>
     * 纯函数：无副作用，可安全地在事务内、对账中或单元测试里重复调用。
     *
     * @param currentVersion 数据库中该 pair 已应用的最大 pairVersion（不能为负）
     * @param currentLiked   数据库中该 pair 当前是否点赞
     * @param eventVersion   事件携带的 pairVersion（必须为正）
     * @param desiredLiked   事件期望的点赞状态
     * @return 判定结果（Action + 真实计数变化量）
     * @throws IllegalArgumentException 版本参数非法（currentVersion 为负或 eventVersion 非正）
     * @throws IllegalStateException    同版本对应互相冲突的点赞状态（数据损坏）
     */
    static Decision evaluate(long currentVersion, boolean currentLiked,
                             long eventVersion, boolean desiredLiked) {
        // 版本合法性校验：数据库版本为 -1 等负值代表未初始化/损坏；
        // 事件版本必须为正，否则无法与 currentVersion 做可靠比较。
        if (currentVersion < 0 || eventVersion <= 0) {
            throw new IllegalArgumentException("pairVersion 必须为正数，数据库版本不能为负数");
        }
        // 过期事件：乱序到达或重放旧死信，直接忽略，不改变状态、不调整计数
        if (eventVersion < currentVersion) {
            return new Decision(Action.STALE, 0);
        }
        // 同版本：只允许状态一致（重复事件）；状态冲突说明数据损坏，必须暴露
        if (eventVersion == currentVersion) {
            if (desiredLiked != currentLiked) {
                throw new IllegalStateException("同一个 pairVersion 对应了互相冲突的点赞状态：version="
                        + eventVersion + ", currentLiked=" + currentLiked + ", desiredLiked=" + desiredLiked);
            }
            return new Decision(Action.DUPLICATE_VERSION, 0);
        }
        // 新版本（允许跳号）：若状态已一致，只需推进水位，不调整计数
        if (currentLiked == desiredLiked) {
            return new Decision(Action.ADVANCE_NOOP, 0);
        }
        // 新版本且状态确实需要改变：actualDelta 由"期望状态"决定，而非事件携带的 delta
        return new Decision(Action.CHANGE, desiredLiked ? 1 : -1);
    }
}