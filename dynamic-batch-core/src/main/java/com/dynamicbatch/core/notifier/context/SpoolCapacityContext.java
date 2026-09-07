package com.dynamicbatch.core.notifier.context;

/**
 * Spool 容量告警场景上下文（检测型）：巡检时刻的磁盘占用与预算拆分。
 */
public class SpoolCapacityContext extends NotifyContext {

    /** 巡检时刻目录总占用（字节） */
    private final long currentBytes;
    /** 磁盘预算（字节），Long.MAX_VALUE = 不限 */
    private final long maxBytes;
    /** 当前占用占预算的百分比 */
    private final int percent;
    /** 已读完未删除（字节） */
    private final long consumedBytes;
    /** 还未读（字节） */
    private final long pendingBytes;

    public SpoolCapacityContext(String key, long currentBytes, long maxBytes, int percent,
                                long consumedBytes, long pendingBytes) {
        super(key);
        this.currentBytes = currentBytes;
        this.maxBytes = maxBytes;
        this.percent = percent;
        this.consumedBytes = consumedBytes;
        this.pendingBytes = pendingBytes;
    }

    public long getCurrentBytes() {
        return currentBytes;
    }

    public long getMaxBytes() {
        return maxBytes;
    }

    public int getPercent() {
        return percent;
    }

    public long getConsumedBytes() {
        return consumedBytes;
    }

    public long getPendingBytes() {
        return pendingBytes;
    }
}
