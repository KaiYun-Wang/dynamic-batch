package com.dynamicbatch.spool;

/**
 * 磁盘占用即时拆分快照：实时列目录，只统计数据滚动文件（.cq4）按 {@code Files.size} 求和，无缓存。
 *
 * <p>拆分以共享 tailer 当前所在文件为界（文件名字典序 = 时间序，与清理同口径）：
 * 早于它的数据文件已读完、待清理删除；其余（含在读文件整体）未读。
 * 文件级估算，误差 ≤ 一个滚动文件，且只会把已读部分多算入未读。
 * metadata 等内部文件一律不计，{@code totalBytes = consumedBytes + pendingBytes} 严格成立。
 */
public final class DiskUsage {

    /** 目录全部文件总占用（字节） */
    private final long totalBytes;
    /** 已读完未删除（字节）：早于 tailer 当前文件的数据文件 */
    private final long consumedBytes;
    /** 还未读（字节）：tailer 尚未越过的数据文件 */
    private final long pendingBytes;
    /** 磁盘预算（字节），Long.MAX_VALUE = 不限 */
    private final long maxSizeBytes;

    public DiskUsage(long totalBytes, long consumedBytes, long pendingBytes, long maxSizeBytes) {
        this.totalBytes = totalBytes;
        this.consumedBytes = consumedBytes;
        this.pendingBytes = pendingBytes;
        this.maxSizeBytes = maxSizeBytes;
    }

    public long getTotalBytes() {
        return totalBytes;
    }

    public long getConsumedBytes() {
        return consumedBytes;
    }

    public long getPendingBytes() {
        return pendingBytes;
    }

    public long getMaxSizeBytes() {
        return maxSizeBytes;
    }

    @Override
    public String toString() {
        return "DiskUsage{totalBytes=" + totalBytes + ", consumedBytes=" + consumedBytes
                + ", pendingBytes=" + pendingBytes + ", maxSizeBytes=" + maxSizeBytes + "}";
    }
}
