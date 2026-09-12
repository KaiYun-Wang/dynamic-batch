package com.dynamicbatch.core.stats;

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * 统计采集快照（不可变）：一个采集间隔的增量字段 + 启动至今的累计字段，
 * 及派生的 submitQps / callbackQps / tp。由 {@link StatsCollector} 差分产出，字段即对外契约。
 *
 * <p>各字段独立读取无原子性保证；tp 只含有值条目（未配分位或 0 条回调均为空 map），
 * 为本间隔近似值，不可跨间隔平均。
 */
public final class StatsSnapshot {

    /** 组 key */
    private final String groupKey;
    /** 采集区间起点（毫秒 epoch），即上次快照的区间终点 */
    private final long intervalStartMillis;
    /** 采集区间终点（毫秒 epoch，本次差分时刻） */
    private final long intervalEndMillis;
    /** 区间内提交成功增量 */
    private final long submitCount;
    /** 区间内回调完成增量（成功失败合并计数） */
    private final long callbackCount;
    /** RT 各桶增量计数，长度 = 边界数 + 1：首桶 [0, 首边界)，末位溢出桶 [末边界, +∞) */
    private final long[] bucketCounts;
    /** 提交成功累计（重启归零） */
    private final long submitTotal;
    /** 回调完成累计（含 flush 失败的批次） */
    private final long callbackTotal;
    /** RT 各桶累计计数（启动至今） */
    private final long[] bucketTotals;
    /** RT 桶上界数组（严格递增，毫秒）；溢出桶上界 +∞ 不在数组内 */
    private final long[] rtBucketBoundaries;
    /** 区间提交 QPS = submitCount ÷ 实际间隔秒（分母 clamp 1ms 防除零） */
    private final double submitQps;
    /** 区间回调 QPS = callbackCount ÷ 实际间隔秒 */
    private final double callbackQps;
    /** 可配 tp 值（key 形如 tp50/tp95）；未配分位或区间 0 条回调为空 map */
    private final Map<String, Double> tp;

    private StatsSnapshot(String groupKey, long intervalStartMillis, long intervalEndMillis,
                          long submitCount, long callbackCount, long[] bucketCounts,
                          long submitTotal, long callbackTotal, long[] bucketTotals,
                          long[] rtBucketBoundaries, double submitQps, double callbackQps,
                          Map<String, Double> tp) {
        this.groupKey = groupKey;
        this.intervalStartMillis = intervalStartMillis;
        this.intervalEndMillis = intervalEndMillis;
        this.submitCount = submitCount;
        this.callbackCount = callbackCount;
        this.bucketCounts = bucketCounts;
        this.submitTotal = submitTotal;
        this.callbackTotal = callbackTotal;
        this.bucketTotals = bucketTotals;
        this.rtBucketBoundaries = rtBucketBoundaries;
        this.submitQps = submitQps;
        this.callbackQps = callbackQps;
        this.tp = tp;
    }

    /** 组装快照并派生 qps / tp：算术单一来源，采集 tick 与 live 查询两处共用 */
    static StatsSnapshot create(String groupKey, long intervalStartMillis, long intervalEndMillis,
                                long submitCount, long callbackCount, long[] bucketCounts,
                                long submitTotal, long callbackTotal, long[] bucketTotals,
                                long[] rtBucketBoundaries, double[] percentiles) {
        // 分母 clamp 1ms 防除零
        long interval = Math.max(1L, intervalEndMillis - intervalStartMillis);
        double seconds = interval / 1000.0d;
        Map<String, Double> tp = calculateTp(bucketCounts, rtBucketBoundaries, percentiles);
        return new StatsSnapshot(groupKey, intervalStartMillis, intervalEndMillis,
                submitCount, callbackCount, bucketCounts.clone(),
                submitTotal, callbackTotal, bucketTotals.clone(),
                rtBucketBoundaries.clone(), submitCount / seconds, callbackCount / seconds, tp);
    }

    public String getGroupKey() {
        return groupKey;
    }

    public long getIntervalStartMillis() {
        return intervalStartMillis;
    }

    public long getIntervalEndMillis() {
        return intervalEndMillis;
    }

    public long getSubmitCount() {
        return submitCount;
    }

    public long getCallbackCount() {
        return callbackCount;
    }

    /** RT 各桶增量计数副本，下标 i 的区间见 {@link #getRtBucketBoundaries()} */
    public long[] getBucketCounts() {
        return bucketCounts.clone();
    }

    public long getSubmitTotal() {
        return submitTotal;
    }

    public long getCallbackTotal() {
        return callbackTotal;
    }

    /** RT 各桶累计计数副本（启动至今），长度与区间语义同 {@link #getBucketCounts()} */
    public long[] getBucketTotals() {
        return bucketTotals.clone();
    }

    /** RT 桶上界数组副本（严格递增，毫秒）；溢出桶上界 +∞ 不在数组内 */
    public long[] getRtBucketBoundaries() {
        return rtBucketBoundaries.clone();
    }

    public double getSubmitQps() {
        return submitQps;
    }

    public double getCallbackQps() {
        return callbackQps;
    }

    /** 可配 tp 值（不可变，key 形如 tp50/tp95）；空 map = 本区间无可用 tp 值 */
    public Map<String, Double> getTp() {
        return tp;
    }

    @Override
    public String toString() {
        return "StatsSnapshot{groupKey=" + groupKey
                + ", interval=[" + intervalStartMillis + "," + intervalEndMillis + ")"
                + ", submitCount=" + submitCount + ", callbackCount=" + callbackCount
                + ", bucketCounts=" + Arrays.toString(bucketCounts)
                + ", submitTotal=" + submitTotal + ", callbackTotal=" + callbackTotal
                + ", bucketTotals=" + Arrays.toString(bucketTotals)
                + ", submitQps=" + submitQps + ", callbackQps=" + callbackQps
                + ", tp=" + tp + "}";
    }

    /**
     * 增量桶上算 tp：目标条数 idx = ceil(count × q)，从左定位所在桶后桶内线性插值
     * （首桶下界 0，溢出桶报末边界）；count = 0 或未配分位返回空 map。
     */
    private static Map<String, Double> calculateTp(long[] bucketCounts, long[] boundaries,
                                                   double[] percentiles) {
        if (percentiles.length == 0) {
            return Collections.emptyMap();
        }
        long count = 0L;
        for (long bucketCount : bucketCounts) {
            count += bucketCount;
        }
        if (count == 0L) {
            return Collections.emptyMap();
        }
        Map<String, Double> result = new LinkedHashMap<>();
        for (double q : percentiles) {
            long target = Math.round(Math.ceil(count * q));
            if (target < 1L) {
                target = 1L;
            }
            if (target > count) {
                target = count;
            }
            Double value = locate(bucketCounts, boundaries, target);
            if (value != null) {
                result.put(tpKey(q), value);
            }
        }
        return result;
    }

    /** tp key 命名：百分数整数倍直出（0.5→tp50），非整数保留必要小数（0.999→tp99.9），不四舍五入丢位 */
    private static String tpKey(double q) {
        BigDecimal percent = BigDecimal.valueOf(q * 100).stripTrailingZeros();
        return percent.scale() <= 0
                ? "tp" + percent.longValueExact()
                : "tp" + percent.toPlainString();
    }

    /** 定位第 target 条（1-based）所在桶并线性插值；空桶跳过，桶计数总和 ≥ target 时必有解 */
    private static Double locate(long[] bucketCounts, long[] boundaries, long target) {
        long cumulative = 0L;
        for (int i = 0; i < bucketCounts.length; i++) {
            long bucketCount = bucketCounts[i];
            if (bucketCount <= 0L) {
                continue;   // 空桶跳过：目标不可能落在 0 条样本的桶内，也防除零
            }
            cumulative += bucketCount;
            if (cumulative < target) {
                continue;
            }
            if (i == boundaries.length) {
                return (double) boundaries[boundaries.length - 1];   // 溢出桶报末边界
            }
            long low = i == 0 ? 0L : boundaries[i - 1];
            long high = boundaries[i];
            long offset = target - (cumulative - bucketCount);   // 桶内第 offset 条，1-based
            return low + (high - low) * (double) offset / bucketCount;
        }
        return null;
    }
}
