package com.dynamicbatch.core.pojo;

import com.dynamicbatch.common.constants.BatchWorkerConstant;
import org.junit.Test;

import java.util.Arrays;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * StatsConfigPOJO 默认值与校验语义：四字段默认形态、非法参数 fail fast、防御性副本。
 */
public class StatsConfigPOJOTest {

    @Test
    public void defaultsMatchContract() {
        StatsConfigPOJO config = StatsConfigPOJO.builder().build();
        assertTrue("采集默认开", config.isEnabled());
        assertEquals("默认采集周期 5s", BatchWorkerConstant.DEFAULT_STATS_COLLECT_INTERVAL_MS,
                config.getCollectIntervalMillis());
        assertEquals("默认不配分位", 0, config.getPercentiles().length);
        assertNull("未配桶边界走框架默认", config.getRtBuckets());
    }

    @Test
    public void invalidIntervalRejected() {
        for (long interval : new long[]{0, -1}) {
            try {
                StatsConfigPOJO.builder().collectIntervalMillis(interval).build();
                fail("expected IllegalArgumentException for interval " + interval);
            } catch (IllegalArgumentException expected) {
                // 周期非法 fail fast
            }
        }
    }

    @Test
    public void invalidPercentileRejected() {
        for (double percentile : new double[]{0, -0.5, 1.0001, 2}) {
            try {
                StatsConfigPOJO.builder().percentiles(0.5, percentile).build();
                fail("expected IllegalArgumentException for percentile " + percentile);
            } catch (IllegalArgumentException expected) {
                // 分位越界 fail fast
            }
        }
    }

    @Test
    public void invalidRtBucketsRejected() {
        long[][] invalid = {
                new long[0],
                {0, 1, 2},        // 非正
                {-1, 1, 2},       // 负数
                {5, 1, 10},       // 非递增
                {1, 2, 2},        // 相等（非严格递增）
        };
        for (long[] buckets : invalid) {
            try {
                StatsConfigPOJO.builder().rtBuckets(buckets).build();
                fail("expected IllegalArgumentException for " + Arrays.toString(buckets));
            } catch (IllegalArgumentException expected) {
                // 边界非法 fail fast
            }
        }
    }

    @Test
    public void arraysAreDefensivelyCopied() {
        double[] percentiles = {0.5, 0.95};
        long[] rtBuckets = {1, 2, 5};
        StatsConfigPOJO config = StatsConfigPOJO.builder()
                .percentiles(percentiles)
                .rtBuckets(rtBuckets)
                .build();

        percentiles[0] = 9;
        rtBuckets[0] = 99;
        assertArrayEquals("入参数组应不受外部修改影响", new double[]{0.5, 0.95}, config.getPercentiles(), 0);
        assertArrayEquals("入参数组应不受外部修改影响", new long[]{1, 2, 5}, config.getRtBuckets());

        config.getPercentiles()[0] = 9;
        config.getRtBuckets()[0] = 99;
        assertArrayEquals("读出数组应不受外部修改影响", new double[]{0.5, 0.95}, config.getPercentiles(), 0);
        assertArrayEquals("读出数组应不受外部修改影响", new long[]{1, 2, 5}, config.getRtBuckets());
    }

    @Test
    public void disabledFlagSurvives() {
        StatsConfigPOJO config = StatsConfigPOJO.builder().enabled(false).build();
        assertFalse(config.isEnabled());
    }
}
