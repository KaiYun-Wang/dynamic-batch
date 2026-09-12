package com.dynamicbatch.core.stats;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.LongAdder;

/**
 * 组级累计统计：提交成功数 / 回调完成数 / RT 分桶计数，全 {@link LongAdder}。
 * 单调递增、从不复位、重启归零。
 *
 * <p>RT 桶由边界数组派生，桶总数 = 边界数 + 1：第一桶 [0, 首边界)，中间桶 [前边界, 本边界)，
 * 末位为溢出桶 [末边界, +∞)；桶计数与回调计数同径写入严格相等，弱一致读取允许 ±几条漂移。
 */
public final class CumulativeStats {

    /** 默认边界生成上界（毫秒，≈23 天） */
    private static final long DEFAULT_BOUNDARY_UPPER_MILLIS = 2_000_000_000L;

    /** 提交成功累计 */
    private final LongAdder submitTotal = new LongAdder();
    /** 回调完成累计（含 flush 失败的批次） */
    private final LongAdder callbackTotal = new LongAdder();
    /** RT 桶计数：下标 0 = 第一桶，末位 = 溢出桶 */
    private final LongAdder[] rtBuckets;
    /** RT 桶上界数组（严格递增，毫秒），长度 = rtBuckets.length - 1 */
    private final long[] rtBucketBoundaries;

    /**
     * @param rtBucketBoundaries RT 桶上界数组，须全正且严格递增，非法抛 {@link IllegalArgumentException}
     */
    public CumulativeStats(long[] rtBucketBoundaries) {
        this.rtBucketBoundaries = validated(rtBucketBoundaries);
        this.rtBuckets = new LongAdder[this.rtBucketBoundaries.length + 1];
        for (int i = 0; i < rtBuckets.length; i++) {
            rtBuckets[i] = new LongAdder();
        }
    }

    /** 默认桶边界：1-2-5 序列生成至 {@value #DEFAULT_BOUNDARY_UPPER_MILLIS} ms */
    public static long[] defaultBoundaries() {
        List<Long> list = new ArrayList<>();
        for (long magnitude = 1L; magnitude <= DEFAULT_BOUNDARY_UPPER_MILLIS; magnitude *= 10L) {
            for (long multiple : new long[]{1L, 2L, 5L}) {
                long boundary = magnitude * multiple;
                if (boundary <= DEFAULT_BOUNDARY_UPPER_MILLIS) {
                    list.add(boundary);
                }
            }
        }
        long[] result = new long[list.size()];
        for (int i = 0; i < result.length; i++) {
            result[i] = list.get(i);
        }
        return result;
    }

    /** 提交成功（spool.append 成功）+1 */
    public void recordSubmit() {
        submitTotal.increment();
    }

    /**
     * 回调完成 +1 并按 RT 落桶：负 rt 按 0 计（时钟回拨），rt=0 落第一桶，超出末边界落溢出桶。
     */
    public void recordCallback(long rtMillis) {
        rtBuckets[bucketIndex(Math.max(0L, rtMillis))].increment();
        callbackTotal.increment();
    }

    public long submitTotal() {
        return submitTotal.sum();
    }

    public long callbackTotal() {
        return callbackTotal.sum();
    }

    /** RT 桶上界数组副本（严格递增，毫秒）；溢出桶上界 +∞ 不在数组内 */
    public long[] rtBucketBoundaries() {
        return rtBucketBoundaries.clone();
    }

    /** RT 各桶计数副本，下标 i 的区间见 {@link #rtBucketBoundaries()}；长度 = 边界数 + 1 */
    public long[] rtBucketCounts() {
        long[] counts = new long[rtBuckets.length];
        for (int i = 0; i < counts.length; i++) {
            counts[i] = rtBuckets[i].sum();
        }
        return counts;
    }

    private int bucketIndex(long rtMillis) {
        // 命中边界（rt 恰等于上界）与落在两边界之间统一为二分插入点：区间左闭右开，
        // rt == b[i] 落下一桶 [b[i], b[i+1])；插入点 0 = 第一桶，插入点 = 边界数 = 溢出桶
        int idx = Arrays.binarySearch(rtBucketBoundaries, rtMillis);
        return idx >= 0 ? idx + 1 : -(idx + 1);
    }

    private static long[] validated(long[] boundaries) {
        if (boundaries == null || boundaries.length == 0) {
            throw new IllegalArgumentException("rtBucketBoundaries must not be empty");
        }
        long previous = 0L;
        for (long boundary : boundaries) {
            if (boundary <= previous) {
                throw new IllegalArgumentException("rtBucketBoundaries must be positive and strictly"
                        + " increasing, got " + Arrays.toString(boundaries));
            }
            previous = boundary;
        }
        return boundaries.clone();
    }
}
