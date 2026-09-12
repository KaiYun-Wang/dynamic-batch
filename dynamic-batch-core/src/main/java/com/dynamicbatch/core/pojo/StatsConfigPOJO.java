package com.dynamicbatch.core.pojo;

import com.dynamicbatch.common.constants.BatchWorkerConstant;

import java.util.Arrays;

/**
 * 统计体系配置（不可变，经 {@link Builder} 构建）：经 {@code BatchWorkerGroup.Builder#statsConfig}
 * 可选接入，不配 = 默认实例（采集开、5s 周期、无 tp、默认 1-2-5 桶边界）。
 */
public final class StatsConfigPOJO {

    /** 采集总开关。false = 采集线程不跑、endpoint stats 为 null、listener 永不触发 */
    private final boolean enabled;

    /** 采集周期（毫秒），即差分间隔 */
    private final long collectIntervalMillis;

    /** 分位数（0,1]，如 0.5/0.95/0.99；空 = 不产 tp 键 */
    private final double[] percentiles;

    /**
     * RT 桶上界数组（毫秒，Prometheus le 语义），须全正且严格递增，桶总数 = 数组长度 + 1；
     * null = 框架按 1-2-5 序列生成默认数组。
     */
    private final long[] rtBuckets;

    private StatsConfigPOJO(Builder builder) {
        this.enabled = builder.enabled;
        this.collectIntervalMillis = builder.collectIntervalMillis;
        this.percentiles = builder.percentiles.clone();
        this.rtBuckets = builder.rtBuckets == null ? null : builder.rtBuckets.clone();
    }

    /** 创建配置构建器，全部字段走默认值，链式方法覆盖 */
    public static Builder builder() {
        return new Builder();
    }

    /** @return 采集总开关 */
    public boolean isEnabled() {
        return enabled;
    }

    /** @return 采集周期（毫秒） */
    public long getCollectIntervalMillis() {
        return collectIntervalMillis;
    }

    /** @return 分位数配置副本，空数组 = 不产 tp 键 */
    public double[] getPercentiles() {
        return percentiles.clone();
    }

    /** @return RT 桶上界数组副本（严格递增，毫秒）；null 表示用框架默认 1-2-5 序列 */
    public long[] getRtBuckets() {
        return rtBuckets == null ? null : rtBuckets.clone();
    }

    // ======================== Builder ========================

    /**
     * {@code StatsConfigPOJO} 构建器：全部字段可选，不设置即用默认值；
     * build() 时统一校验，非法抛 {@link IllegalArgumentException}（启动期 fail fast）。
     */
    public static final class Builder {

        private boolean enabled = true;
        private long collectIntervalMillis = BatchWorkerConstant.DEFAULT_STATS_COLLECT_INTERVAL_MS;
        private double[] percentiles = new double[0];
        private long[] rtBuckets;

        private Builder() {
        }

        /** 采集总开关：关 = 无采集、endpoint stats 为 null、listener 永不触发 */
        public Builder enabled(boolean enabled) {
            this.enabled = enabled;
            return this;
        }

        /** 采集周期（毫秒），必须 &gt; 0 */
        public Builder collectIntervalMillis(long collectIntervalMillis) {
            this.collectIntervalMillis = collectIntervalMillis;
            return this;
        }

        /** 分位数（0,1]，配了才产 tp 键（key 形如 tp50/tp95）；不配 = tp 恒空 map */
        public Builder percentiles(double... percentiles) {
            this.percentiles = percentiles == null ? new double[0] : percentiles.clone();
            return this;
        }

        /** RT 桶上界数组（毫秒，le 语义）；不配 = 框架按 1-2-5 序列生成默认数组 */
        public Builder rtBuckets(long... rtBuckets) {
            this.rtBuckets = rtBuckets == null ? null : rtBuckets.clone();
            return this;
        }

        /** 构建不可变配置实例，非法参数抛 {@link IllegalArgumentException} */
        public StatsConfigPOJO build() {
            if (collectIntervalMillis <= 0) {
                throw new IllegalArgumentException("collectIntervalMillis must be > 0, got "
                        + collectIntervalMillis);
            }
            for (double percentile : percentiles) {
                if (percentile <= 0 || percentile > 1) {
                    throw new IllegalArgumentException("percentile must be in (0, 1], got " + percentile);
                }
            }
            if (rtBuckets != null) {
                if (rtBuckets.length == 0) {
                    throw new IllegalArgumentException("rtBuckets must not be empty"
                            + " (omit for framework default 1-2-5 boundaries)");
                }
                long previous = 0L;
                for (long boundary : rtBuckets) {
                    if (boundary <= previous) {
                        throw new IllegalArgumentException("rtBuckets must be positive and strictly"
                                + " increasing, got " + Arrays.toString(rtBuckets));
                    }
                    previous = boundary;
                }
            }
            return new StatsConfigPOJO(this);
        }
    }
}
