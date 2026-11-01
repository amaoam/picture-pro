package com.zmt.picturebackendpro.service.impl.mq;

import org.springframework.stereotype.Component;

import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Supplier;

/**
 * 同实例内协调「点赞写请求」与「全量对账」的并发控制器。
 * <p>
 * 设计背景：
 * 点赞请求会高频读写 Redis 中的计数与集合，而全量对账任务需要对 Redis 与 MySQL 做一致性扫描。
 * 若两者并发执行，对账可能读到"写了一半"的中间态，进而误判并修正错误数据。
 * 本类通过读写锁让二者互斥：正常请求走读锁（可并发），对账走写锁（独占）。
 * <p>
 * 锁语义：
 * <ul>
 *   <li><b>读锁</b>（{@link #withRequest}）：所有点赞写请求、状态查询共享，允许多个并发；</li>
 *   <li><b>写锁</b>（{@link #withExclusive}）：全量对账等独占操作，与所有读锁互斥；</li>
 *   <li>公平模式（fair=true）：避免写锁被持续到达的读请求"饿死"，保证对账能及时获得执行机会。</li>
 * </ul>
 * <p>
 * 静默检测：通过 {@link #lastRequestAtMs} 记录最近一次请求时间，
 * 配合 {@link #hasBeenQuietFor(long)} 判断是否处于业务低峰，
 * 供定时对账任务在"无人写"时再获取写锁，减少对线上请求的阻塞。
 * <p>
 * <b>重要限制</b>：本协调器仅在<b>单应用实例</b>内有效。
 * 多实例部署时，各实例的读写锁彼此独立，无法阻止实例 A 的请求与实例 B 的对账并发，
 * 此时必须升级为分布式 fencing / 租约（如 Redis 分布式锁 + fencing token）方案。
 */
@Component
public class RabbitMqLikeActivity {

    /**
     * 协调点赞请求与对账的读写锁。
     * <p>
     * 使用公平模式（fair=true）：
     * - 读锁可并发获取，满足点赞高吞吐；
     * - 写锁获取时，后续读请求会排队，避免对账被源源不断的读请求饿死。
     */
    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock(true);

    /**
     * 最近一次点赞请求的时间戳（毫秒），0 表示从未有请求。
     * <p>
     * 使用 {@link AtomicLong} 而非普通字段，保证多线程下写入与读取的可见性；
     * 供 {@link #hasBeenQuietFor(long)} 判断业务是否处于静默期。
     */
    private final AtomicLong lastRequestAtMs = new AtomicLong(0L);

    /**
     * 在「请求上下文」中执行动作（持有读锁）。
     * <p>
     * 用于所有点赞写请求与状态查询：
     * <ul>
     *   <li>持读锁期间，独占操作（对账）无法进入，保证请求侧看到的数据一致；</li>
     *   <li>多个请求可并发执行，不互相阻塞；</li>
     *   <li>执行前刷新 {@code lastRequestAtMs}，标记"业务仍在活跃"，供对账任务判断静默期。</li>
     * </ul>
     * 无论动作是否抛异常，读锁都会在 finally 中释放，避免锁泄漏。
     *
     * @param action 业务动作
     * @param <T>    返回值类型
     * @return 业务动作的返回值
     */
    public <T> T withRequest(Supplier<T> action) {
        lock.readLock().lock();
        try {
            lastRequestAtMs.set(System.currentTimeMillis());
            return action.get();
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * 在「独占上下文」中执行动作（持有写锁）。
     * <p>
     * 用于全量对账等需要与所有请求互斥的操作：
     * <ul>
     *   <li>写锁与读锁互斥，也与其它写锁互斥；</li>
     *   <li>由于是公平锁，写锁请求会阻止后续读请求"插队"，避免写锁长期饥饿。</li>
     * </ul>
     * 无论动作是否抛异常，写锁都会在 finally 中释放。
     * <p>
     * 注意：独占期间所有点赞请求都会被阻塞，因此对账任务应尽量缩短持锁时间，
     * 或结合 {@link #hasBeenQuietFor(long)} 在业务低峰期执行。
     *
     * @param action 独占动作
     */
    public void withExclusive(Runnable action) {
        lock.writeLock().lock();
        try {
            action.run();
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * 获取最近一次点赞请求的时间戳（毫秒）。
     * <p>
     * 0 表示自应用启动以来尚未有请求。调用方可用它与当前时间比较，
     * 判断距离上次活跃已过去多久。
     *
     * @return 最近一次请求时间戳（毫秒），无请求时为 0
     */
    public long lastRequestAtMs() {
        return lastRequestAtMs.get();
    }

    /**
     * 判断业务是否已静默（无请求）达到指定时长。
     * <p>
     * 语义：
     * <ul>
     *   <li>若从未有请求（last == 0），视为静默（返回 true）；</li>
     *   <li>否则比较当前时间与上次请求时间的差值是否 ≥ {@code quietMs}。</li>
     * </ul>
     * 典型用法：定时对账任务在发起对账前先检查该方法，确认处于低峰再获取写锁，
     * 以减少对在线点赞请求的阻塞。
     *
     * @param quietMs 期望的静默时长（毫秒）
     * @return 是否已静默足够时长
     */
    public boolean hasBeenQuietFor(long quietMs) {
        long last = lastRequestAtMs.get();
        return last == 0L || System.currentTimeMillis() - last >= quietMs;
    }
}