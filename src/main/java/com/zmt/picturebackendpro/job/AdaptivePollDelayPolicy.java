package com.zmt.picturebackendpro.job;

/**
 * 无外部依赖的自适应轮询退避策略，可单元测试。
 * 有工作时快速继续；空轮询时指数退避；异常时使用单独的错误退避。
 */
public final class AdaptivePollDelayPolicy {
    private final long idleMinMs;
    private final long idleMaxMs;
    private final long busyDelayMs;
    private final long errorDelayMs;

    public AdaptivePollDelayPolicy(long idleMinMs, long idleMaxMs,
                                   long busyDelayMs, long errorDelayMs) {
        this.idleMinMs = Math.max(1L, idleMinMs);
        this.idleMaxMs = Math.max(this.idleMinMs, idleMaxMs);
        this.busyDelayMs = Math.max(1L, busyDelayMs);
        this.errorDelayMs = Math.max(1L, errorDelayMs);
    }

    /**
     * @param previousDelayMs 上一轮实际等待时间；首次轮询前可传 0
     * @param processedEvents 本轮成功处理的事件数
     * @param failed          本轮是否发生异常
     */
    public long nextDelayMs(long previousDelayMs, int processedEvents, boolean failed) {
        if (failed) {
            return errorDelayMs;
        }
        if (processedEvents > 0) {
            return busyDelayMs;
        }
        if (previousDelayMs <= 0) {
            return idleMinMs;
        }
        long baseline = Math.max(idleMinMs, previousDelayMs);
        if (baseline >= idleMaxMs || baseline > Long.MAX_VALUE / 2L) {
            return idleMaxMs;
        }
        return Math.min(idleMaxMs, baseline * 2L);
    }

    public long getIdleMinMs() { return idleMinMs; }
    public long getIdleMaxMs() { return idleMaxMs; }
}
