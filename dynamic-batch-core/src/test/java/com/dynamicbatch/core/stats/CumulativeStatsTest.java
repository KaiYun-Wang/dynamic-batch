package com.dynamicbatch.core.stats;

import org.junit.Test;

import java.util.Arrays;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * CumulativeStats 计数与落桶语义：边界校验 fail fast、两端兜底桶归属、防御性副本、桶总和等式。
 */
public class CumulativeStatsTest {

    /** 4 边界 → 5 桶：[0,1) [1,2) [2,5) [5,10) [10,+∞) */
    private static final long[] BOUNDARIES = {1, 2, 5, 10};

    @Test
    public void invalidBoundariesRejected() {
        long[][] invalid = {
                null,
                new long[0],
                {0, 1, 2},        // 非正
                {-1, 1, 2},       // 负数
                {5, 1, 10},       // 非递增
                {1, 2, 2},        // 相等（非严格递增）
        };
        for (long[] boundaries : invalid) {
            try {
                new CumulativeStats(boundaries);
                fail("expected IllegalArgumentException for " + Arrays.toString(boundaries));
            } catch (IllegalArgumentException expected) {
                // 边界非法 fail fast
            }
        }
    }

    @Test
    public void defaultBoundariesStrictlyIncreasing() {
        long[] boundaries = CumulativeStats.defaultBoundaries();
        assertEquals("首边界应为 1ms", 1L, boundaries[0]);
        assertEquals("末边界应为 2e9 ms", 2_000_000_000L, boundaries[boundaries.length - 1]);
        for (int i = 1; i < boundaries.length; i++) {
            assertTrue("边界应严格递增: " + Arrays.toString(boundaries),
                    boundaries[i] > boundaries[i - 1]);
        }
    }

    @Test
    public void boundaryValuesFallToNextBucket() {
        CumulativeStats stats = new CumulativeStats(BOUNDARIES);
        // {rt, 期望桶下标}：恰等上界落下一桶（区间左闭右开），超出末边界落溢出桶
        long[][] cases = {{0, 0}, {1, 1}, {2, 2}, {4, 2}, {5, 3}, {10, 4}, {999_999, 4}};
        for (long[] c : cases) {
            stats.recordCallback(c[0]);
        }

        long[] counts = stats.rtBucketCounts();
        assertEquals("桶总数应 = 边界数 + 1", BOUNDARIES.length + 1, counts.length);
        // 逐桶聚合期望：桶 2 聚了 rt=2 与 rt=4 两条，桶 4（溢出）聚了 rt=10 与 rt=999_999
        assertArrayEquals(new long[]{1, 1, 2, 1, 2}, counts);
        assertEquals("callbackTotal 应与落桶条数一致", cases.length, stats.callbackTotal());
    }

    @Test
    public void negativeRtClampedToFirstBucket() {
        CumulativeStats stats = new CumulativeStats(BOUNDARIES);
        stats.recordCallback(-100);

        assertEquals("负 rt 应按 0 计落第一桶", 1L, stats.rtBucketCounts()[0]);
        assertEquals(1L, stats.callbackTotal());
    }

    @Test
    public void submitAndCallbackCountIndependently() {
        CumulativeStats stats = new CumulativeStats(BOUNDARIES);
        stats.recordSubmit();
        stats.recordSubmit();
        stats.recordCallback(3);

        assertEquals(2L, stats.submitTotal());
        assertEquals(1L, stats.callbackTotal());
    }

    @Test
    public void bucketSumEqualsCallbackTotal() {
        CumulativeStats stats = new CumulativeStats(BOUNDARIES);
        stats.recordCallback(0);
        stats.recordCallback(-5);
        stats.recordCallback(3);
        stats.recordCallback(1000);

        long sum = 0;
        for (long count : stats.rtBucketCounts()) {
            sum += count;
        }
        assertEquals("桶计数总和应与回调计数严格相等", stats.callbackTotal(), sum);
    }

    @Test
    public void countsAccumulate() {
        CumulativeStats stats = new CumulativeStats(BOUNDARIES);
        for (int i = 0; i < 7; i++) {
            stats.recordCallback(3);   // 全落 [2,5) 桶
        }

        assertEquals(7L, stats.rtBucketCounts()[2]);
        assertEquals(7L, stats.callbackTotal());
    }

    @Test
    public void readsReturnDefensiveCopies() {
        CumulativeStats stats = new CumulativeStats(BOUNDARIES);
        stats.recordCallback(0);

        long[] counts = stats.rtBucketCounts();
        counts[0] = 99;
        assertEquals("桶计数应不受外部修改影响", 1L, stats.rtBucketCounts()[0]);

        long[] boundaries = stats.rtBucketBoundaries();
        boundaries[0] = 999;
        assertEquals("边界应不受外部修改影响", 1L, stats.rtBucketBoundaries()[0]);
    }
}
