package com.dynamicbatch.core.vo;

import java.util.Arrays;

/**
 * Worker 组累计统计快照：提交/回调累计与 RT 分桶计数的瞬时只读值；
 * 各字段独立读取，无原子性保证。
 */
public final class CumulativeStatsVO {

    /** 提交成功累计 */
    private final long submitTotal;
    /** 回调完成累计（含 flush 失败的批次） */
    private final long callbackTotal;
    /** RT 桶上界数组（严格递增，毫秒）；溢出桶上界 +∞ 不在数组内 */
    private final long[] rtBucketBoundaries;
    /** RT 各桶计数，长度 = rtBucketBoundaries.length + 1：首桶 [0, 首边界)，末位溢出桶 [末边界, +∞) */
    private final long[] rtBucketCounts;

    public CumulativeStatsVO(long submitTotal, long callbackTotal,
                             long[] rtBucketBoundaries, long[] rtBucketCounts) {
        this.submitTotal = submitTotal;
        this.callbackTotal = callbackTotal;
        this.rtBucketBoundaries = rtBucketBoundaries.clone();
        this.rtBucketCounts = rtBucketCounts.clone();
    }

    public long getSubmitTotal() {
        return submitTotal;
    }

    public long getCallbackTotal() {
        return callbackTotal;
    }

    public long[] getRtBucketBoundaries() {
        return rtBucketBoundaries.clone();
    }

    public long[] getRtBucketCounts() {
        return rtBucketCounts.clone();
    }

    @Override
    public String toString() {
        return "CumulativeStatsVO{submitTotal=" + submitTotal + ", callbackTotal=" + callbackTotal
                + ", rtBucketBoundaries=" + Arrays.toString(rtBucketBoundaries)
                + ", rtBucketCounts=" + Arrays.toString(rtBucketCounts) + "}";
    }
}
