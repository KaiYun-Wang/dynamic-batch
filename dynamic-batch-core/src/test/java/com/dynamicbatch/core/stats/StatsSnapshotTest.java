package com.dynamicbatch.core.stats;

import java.util.Map;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import org.junit.Test;

/**
 * StatsSnapshot 组装算术：tp 线性插值（含首桶下界 0 与溢出桶报末边界）、
 * qps 分母 clamp、tp 单一空态（未配分位与 0 条回调同形为空 map）。
 */
public class StatsSnapshotTest {

    /** 4 边界 → 5 桶：[0,1) [1,2) [2,5) [5,10) [10,+∞) */
    private static final long[] BOUNDARIES = {1, 2, 5, 10};

    private static final double[] TP_50 = {0.5};

    private static StatsSnapshot snapshot(long[] bucketCounts, double[] percentiles) {
        long submitTotal = 0;
        long callbackTotal = 0;
        for (long count : bucketCounts) {
            callbackTotal += count;
        }
        return StatsSnapshot.create("g", 1000L, 3000L, 1L, callbackTotal,
                bucketCounts, submitTotal, callbackTotal, bucketCounts.clone(),
                BOUNDARIES, percentiles);
    }

    /** 常规桶插值：目标条数落在桶内按占比线性映射到区间 */
    @Test
    public void tpInterpolatesInsideBucket() {
        // count = 7：tp50 → idx = ceil(3.5) = 4 → 落桶2 [2,5) 的第 2 条 → 2 + 3×(2/2) = 5.0
        StatsSnapshot snapshot = snapshot(new long[]{1, 1, 2, 1, 2}, TP_50);
        Map<String, Double> tp = snapshot.getTp();
        assertEquals(1, tp.size());
        assertEquals(5.0, tp.get("tp50"), 1e-9);
    }

    /** 首桶插值区间 [0, 首边界)：下界是 0，不是首边界 */
    @Test
    public void tpFirstBucketInterpolatesFromZero() {
        // count = 2：tp50 → idx = 1 → 桶0 [0,1) 的第 1 条 → 0 + 1×(1/2) = 0.5
        StatsSnapshot snapshot = snapshot(new long[]{2, 0, 0, 0, 0}, TP_50);
        assertEquals(0.5, snapshot.getTp().get("tp50"), 1e-9);
    }

    /** 目标落溢出桶一律报末边界（超出量程全按末边界算，+Inf 桶同款行为） */
    @Test
    public void tpOverflowBucketReportsLastBoundary() {
        StatsSnapshot single = snapshot(new long[]{0, 0, 0, 0, 1}, TP_50);
        assertEquals(10.0, single.getTp().get("tp50"), 1e-9);

        // tp95/tp100 的目标都落在溢出桶
        StatsSnapshot mixed = snapshot(new long[]{1, 1, 2, 1, 2}, new double[]{0.95, 1.0});
        assertEquals(10.0, mixed.getTp().get("tp95"), 1e-9);
        assertEquals(10.0, mixed.getTp().get("tp100"), 1e-9);
    }

    /** 边界值分位与多分位 key 命名：tp50 / tp95 / tp99 */
    @Test
    public void tpKeysFollowPercentileConfig() {
        StatsSnapshot snapshot = snapshot(new long[]{7, 0, 0, 0, 0}, new double[]{0.5, 0.95, 0.99});
        Map<String, Double> tp = snapshot.getTp();
        assertEquals(3, tp.size());
        // count=7 全落 [0,1)：tp50 → 0 + 1×(3.5→4/7) ≈ 0.571
        assertEquals(4.0 / 7.0, tp.get("tp50"), 1e-9);
        assertTrue(tp.containsKey("tp95"));
        assertTrue(tp.containsKey("tp99"));
    }

    /** 非整数百分位保留必要小数：0.999 → tp99.9，不四舍五入成 tp100 */
    @Test
    public void tpKeyKeepsFractionalPercentile() {
        // count=10 全落 [0,1)：tp99.9 → idx = ceil(10×0.999) = 10 → 桶0 第 10 条 → 0 + 1×(10/10) = 1.0
        StatsSnapshot snapshot = snapshot(new long[]{10, 0, 0, 0, 0}, new double[]{0.999});
        assertTrue("0.999 应命名为 tp99.9", snapshot.getTp().containsKey("tp99.9"));
        assertEquals(1.0, snapshot.getTp().get("tp99.9"), 1e-9);
    }

    /** tp 单一空态：未配分位与区间 0 条回调统一为空 map，不区分原因 */
    @Test
    public void tpEmptyWhenNoPercentileOrNoSamples() {
        assertTrue("未配分位应为空 map",
                snapshot(new long[]{1, 1, 1, 0, 0}, new double[0]).getTp().isEmpty());
        assertTrue("0 条回调应为空 map",
                snapshot(new long[]{0, 0, 0, 0, 0}, TP_50).getTp().isEmpty());
    }

    /** qps = 增量 ÷ 实际间隔秒数 */
    @Test
    public void qpsUsesActualInterval() {
        StatsSnapshot snapshot = StatsSnapshot.create("g", 1000L, 3000L, 10L, 6L,
                new long[]{6}, 10L, 6L, new long[]{6}, BOUNDARIES, new double[0]);
        assertEquals(5.0, snapshot.getSubmitQps(), 1e-9);
        assertEquals(3.0, snapshot.getCallbackQps(), 1e-9);
    }

    /** 分母 clamp 1：时钟回拨导致区间非正时防除零 */
    @Test
    public void qpsDenominatorClampedToAvoidDivideByZero() {
        StatsSnapshot snapshot = StatsSnapshot.create("g", 3000L, 3000L, 10L, 0L,
                new long[5], 10L, 0L, new long[5], BOUNDARIES, new double[0]);
        assertEquals("区间 0ms 时按 1ms 计（×1000）", 10_000.0, snapshot.getSubmitQps(), 1e-9);
    }

    /** 数组字段返回防御性副本，外部修改不影响快照 */
    @Test
    public void arraysAreDefensivelyCopied() {
        StatsSnapshot snapshot = snapshot(new long[]{1, 1, 1, 0, 0}, new double[0]);
        snapshot.getBucketCounts()[0] = 99;
        snapshot.getBucketTotals()[0] = 99;
        snapshot.getRtBucketBoundaries()[0] = 99;
        assertArrayEquals(new long[]{1, 1, 1, 0, 0}, snapshot.getBucketCounts());
        assertArrayEquals(new long[]{1, 1, 1, 0, 0}, snapshot.getBucketTotals());
        assertArrayEquals(BOUNDARIES, snapshot.getRtBucketBoundaries());
    }
}
