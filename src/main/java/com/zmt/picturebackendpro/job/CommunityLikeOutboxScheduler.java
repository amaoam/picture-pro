package com.zmt.picturebackendpro.job;

import com.zmt.picturebackendpro.exception.CommunityLikeBatchProcessingException;
import com.zmt.picturebackendpro.service.impl.CommunityLikeOutboxFailureService;
import com.zmt.picturebackendpro.service.impl.CommunityLikeOutboxPollMetrics;
import com.zmt.picturebackendpro.service.impl.CommunityLikeOutboxProcessor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicReference;

/**
 * V2.2 自适应 Outbox 调度器。
 * 核心特性：
 * 1. 不改变 Processor 的事务/顺序语义：只根据上一轮的处理结果（是否成功、是否空转）动态决定下一次唤醒延迟。
 * 2. 具备抖动（Jitter）机制，防止多个 Worker 在空闲时发生“惊群效应”（Thundering Herd）。
 * 3. 支持工作线程错峰启动与优雅停机（DisposableBean）。
 */
@Slf4j
@Component
public class CommunityLikeOutboxScheduler implements InitializingBean, DisposableBean {

    // ================= 配置参数（支持通过 Nacos/Application.yml 动态调节） =================
    @Value("${community.like.outbox.workers:4}")
    private int configuredWorkers;                 // 工作线程数（默认 4 个）
    @Value("${community.like.outbox.idle-min-poll-delay-ms:25}")
    private long idleMinDelayMs;                   // 空转时的最小延迟（毫秒）
    @Value("${community.like.outbox.idle-max-poll-delay-ms:500}")
    private long idleMaxDelayMs;                   // 空转时的最大延迟（毫秒）
    @Value("${community.like.outbox.busy-poll-delay-ms:1}")
    private long busyDelayMs;                      // 有数据持续处理（高吞吐）时的激进轮询延迟（毫秒）
    @Value("${community.like.outbox.error-poll-delay-ms:1000}")
    private long errorDelayMs;                     // 发生异常时的熔断冷却延迟（毫秒）
    @Value("${community.like.outbox.poll-jitter-percent:10}")
    private int configuredJitterPercent;           // 空转延迟的随机抖动百分比（防并发撞车）
    @Value("${community.like.outbox.initial-worker-stagger-ms:25}")
    private long initialWorkerStaggerMs;           // 多 Worker 启动时的初始错峰时间间隔（毫秒）

    // ================= 依赖组件 =================
    @Resource(name = "communityLikeTaskScheduler")
    private ThreadPoolTaskScheduler taskScheduler; // 专用调度线程池
    @Resource
    private CommunityLikeOutboxProcessor processor;        // Outbox 批次处理器
    @Resource
    private CommunityLikeOutboxFailureService failureService;// 失败重试/死信记录服务
    @Resource
    private CommunityLikeOutboxPollMetrics pollMetrics;      // 监控指标收集器

    // ================= 运行态状态变量 =================
    private final List<WorkerState> states = new ArrayList<>(); // 保存所有 Worker 的运行状态句柄
    private AdaptivePollDelayPolicy delayPolicy;                // 自适应策略计算器
    private int jitterPercent;                                  // 实际生效的抖动比例

    /**
     * Bean 初始化完成后执行：校验参数、初始化策略、注册并拉起所有的 Worker
     */
    @Override
    public void afterPropertiesSet() {
        // 限制 worker 数量在 [1, 16] 范围内，防止配置过大耗尽线程池
        int workerCount = Math.max(1, Math.min(configuredWorkers, 16));
        delayPolicy = new AdaptivePollDelayPolicy(idleMinDelayMs, idleMaxDelayMs, busyDelayMs, errorDelayMs);
        jitterPercent = Math.max(0, Math.min(configuredJitterPercent, 25));

        // 循环创建指定数量的 worker 并错峰调度
        for (int i = 0; i < workerCount; i++) {
            WorkerState state = new WorkerState("worker-" + (i + 1));
            // 错峰计算：每个 worker 初始延迟递增，避免同时打满数据库
            state.nextDelayMs = Math.max(1L, i * Math.max(0L, initialWorkerStaggerMs));
            states.add(state);
            schedule(state, state.nextDelayMs);
        }

        log.info("Community like Outbox adaptive scheduler started: workers={}, idleDelay={}..{}ms, busyDelay={}ms, errorDelay={}ms",
                workerCount, delayPolicy.getIdleMinMs(), delayPolicy.getIdleMaxMs(),
                Math.max(0L, busyDelayMs), Math.max(1L, errorDelayMs));
    }

    /**
     * 单个 Worker 的核心轮询执行逻辑
     *
     * @param state 当前 worker 的运行状态
     */
    private void pollOnce(WorkerState state) {
        long startedAt = System.nanoTime(); // 记录开始耗时的纳秒时间戳
        int processed = 0;
        boolean failed = false;

        try {
            // 执行核心批次处理，返回本次成功消费的事件数量
            processed = processor.processNextBatch();
            if (processed > 0) {
                log.debug("{} processed {} Outbox events", state.name, processed);
            }
        } catch (CommunityLikeBatchProcessingException e) {
            // 业务批量处理异常（说明某批次内部分或全部事件失败）
            failed = true;
            String detail = e.getCause() == null ? e.getMessage() : e.getCause().getMessage();
            try {
                // 记录重试次数或转移至死信队列
                failureService.recordFailure(e.getEventIds(), detail);
            } catch (Exception failureRecordException) {
                // 防御性捕获：记录失败状态本身出错了不能吞掉异常，确保后续 poll 仍能再次尝试补偿
                log.error("{} failed to record Outbox retry state, eventIds={}",
                        state.name, e.getEventIds(), failureRecordException);
            }
            log.error("{} failed to process like Outbox batch, eventIds={}", state.name, e.getEventIds(), e);
        } catch (Exception e) {
            // 系统级或未捕获的严重异常
            failed = true;
            log.error("{} failed while polling like Outbox", state.name, e);
        } finally {
            // 计算本次执行耗时
            long durationNanos = System.nanoTime() - startedAt;

            // 根据自适应策略计算下一次的标称延迟时间
            long nominalDelay = delayPolicy.nextDelayMs(state.nextDelayMs, processed, failed);

            // 只有在“非失败且无事件处理（纯空转）”时才施加 Jitter 抖动，避免高吞吐时引入额外延迟
            long nextDelay = (!failed && processed == 0) ? applyJitter(nominalDelay) : nominalDelay;
            state.nextDelayMs = nextDelay;

            try {
                // 上报监控指标（如总次数、耗时直方图、空转计数等）
                pollMetrics.recordPoll(state.name, processed, failed, durationNanos, nextDelay);
            } catch (Exception metricException) {
                // 监控上报异常绝不能导致轮询消费者线程永久停止
                log.warn("{} failed to record poll metrics", state.name, metricException);
            }

            // 如果线程未被中断，继续安排下一次调度
            if (!Thread.currentThread().isInterrupted()) {
                schedule(state, nextDelay);
            }
        }
    }

    /**
     * 为空转轮询引入随机抖动（Jitter），防止多个 Worker 在同一时刻被唤醒并发访问数据库（惊群）
     *
     * @param delayMs 原始标称延迟
     * @return 施加随机抖动后的延迟时间
     */
    private long applyJitter(long delayMs) {
        if (delayMs <= 0 || jitterPercent <= 0) {
            return delayMs;
        }
        long spread = Math.max(1L, delayMs * jitterPercent / 100L);
        long low = Math.max(delayPolicy.getIdleMinMs(), delayMs - spread);
        long high = Math.min(delayPolicy.getIdleMaxMs(), delayMs + spread);
        if (high <= low) {
            return low;
        }
        // 在 [low, high] 区间内随机取一个延迟值
        return ThreadLocalRandom.current().nextLong(low, high + 1L);
    }

    /**
     * 将指定的 Worker 任务交给 Spring 的 ThreadPoolTaskScheduler 进行延迟调度
     *
     * @param state   Worker 状态句柄
     * @param delayMs 延迟毫秒数
     */
    private void schedule(WorkerState state, long delayMs) {
        if (taskScheduler == null) {
            return;
        }
        try {
            ScheduledFuture<?> future = taskScheduler.schedule(
                    () -> pollOnce(state), new Date(System.currentTimeMillis() + Math.max(0L, delayMs)));
            // 原子更新当前 worker 持有的任务 Future，方便停机时取消
            state.future.set(future);
        } catch (Exception e) {
            log.error("Unable to schedule {}. Check communityLikeTaskScheduler pool size and shutdown status.", state.name, e);
        }
    }

    /**
     * 容器销毁（DisposableBean）：优雅停机，取消所有未执行或正在等待的定时任务
     */
    @Override
    public void destroy() {
        for (WorkerState state : states) {
            ScheduledFuture<?> future = state.future.getAndSet(null);
            if (future != null) {
                future.cancel(false); // false 表示不强行中断正在运行中的任务，等待当前批次安全收尾
            }
        }
    }

    /**
     * 内部静态类：用于封装和维护单个 Worker 线程的生命周期状态与动态延迟
     */
    private static final class WorkerState {
        private final String name;                                  // 工作线程标识，例如 "worker-1"（低基数标签，符合 Metrics 规范）
        private final AtomicReference<ScheduledFuture<?>> future = new AtomicReference<>(); // 定时任务句柄
        private volatile long nextDelayMs;                          // 记录下一次的延迟时间

        private WorkerState(String name) {
            this.name = name;
        }
    }
}