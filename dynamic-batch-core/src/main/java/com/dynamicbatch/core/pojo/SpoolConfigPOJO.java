package com.dynamicbatch.core.pojo;

import com.dynamicbatch.spool.Serializer;

/**
 * Spool 磁盘缓冲配置（不可变，经 {@link Builder} 构建；非泛型——载荷类型由
 * {@code BatchWorkerGroup} 的 type 统一决定，本类只是传参载体）。
 *
 * <p>由 {@code BatchWorkerGroup.Builder#builder(type, flushCallback, spoolConfig)} 必传
 * （编译期强制），框架内部据此在组启动时构建 {@code Spool}。spoolDir 必填
 * （{@link Builder} 构造器强制，blank 直接抛 {@link IllegalArgumentException}），
 * 其余字段 {@code null} 表示使用 Spool 默认值（本类不做填充，两边默认值不重复维护）。
 *
 * <p>{@code serializer} 只声明载荷编解码意图（可传任何 {@code Serializer}），载荷的真实
 * 类型由 Group 的 {@code type} 提供（SpoolEntrySerializer 构造参数），类型错配由
 * Group.submit / Worker.submit 的双重 isInstance 校验兜底。
 */
public final class SpoolConfigPOJO {

    /** Spool 队列目录（必填；每 Group 独占一个目录，共用则启动 fail fast） */
    private final String spoolDir;

    /** 磁盘预算上限（字节）。null → Spool 默认 {@code Long.MAX_VALUE}（不限） */
    private final Long maxSizeBytes;

    /** 定时刷盘间隔（毫秒），影响断电丢失/重复消费窗口。null → Spool 默认 5000 */
    private final Long flushIntervalMs;

    /**
     * Chronicle 数据文件滚动周期（毫秒）。暴露时长而非 {@code RollCycle} 枚举，
     * 避免框架 API 泄漏 Chronicle 类型。null → Spool 默认 FIVE_MINUTELY；&gt; 0 时映射
     * {@code Spool.Builder#rollCycleMillis}。
     */
    private final Long rollCycleMillis;

    /** 清理已消费旧文件的间隔（毫秒），滚动周期大时建议单独调小。null → Spool 默认 0（跟滚动周期） */
    private final Long cleanupIntervalMs;

    /** 内存暂存队列满时 append 最多阻塞等待多久（毫秒），超时返回 false。null → Spool 默认 100 */
    private final Long offerTimeoutMs;

    /** 写路径内存暂存队列容量（条数），后台单写线程落盘。null → Spool 默认 10000 */
    private final Integer stagingCapacity;

    /** 目录占用字节数刷新间隔（毫秒），用于磁盘预算判断。null → Spool 默认 5000 */
    private final Long dirSizeRefreshIntervalMs;

    /**
     * 载荷序列化器（JDK / Jackson / Kryo 等）。
     * null → 框架默认 {@link com.dynamicbatch.spool.JdkSerializer}
     * （载荷须实现 {@link java.io.Serializable}，Group build 期校验拦截）。
     * routingKey 编解码由框架信封序列化器统一处理，不在此配置。
     */
    private final Serializer<?> serializer;

    private SpoolConfigPOJO(Builder builder) {
        this.spoolDir = builder.spoolDir;
        this.maxSizeBytes = builder.maxSizeBytes;
        this.flushIntervalMs = builder.flushIntervalMs;
        this.rollCycleMillis = builder.rollCycleMillis;
        this.cleanupIntervalMs = builder.cleanupIntervalMs;
        this.offerTimeoutMs = builder.offerTimeoutMs;
        this.stagingCapacity = builder.stagingCapacity;
        this.dirSizeRefreshIntervalMs = builder.dirSizeRefreshIntervalMs;
        this.serializer = builder.serializer;
    }

    /**
     * 创建配置构建器，spoolDir 必填（blank 抛 {@link IllegalArgumentException}）。
     *
     * @param spoolDir Spool 队列目录，每 Group 独占
     */
    public static Builder builder(String spoolDir) {
        return new Builder(spoolDir);
    }

    /** @return Spool 队列目录 */
    public String getSpoolDir() {
        return spoolDir;
    }

    /** @return 磁盘预算上限（字节），null 表示用 Spool 默认 */
    public Long getMaxSizeBytes() {
        return maxSizeBytes;
    }

    /** @return 刷盘间隔（毫秒），null 表示用 Spool 默认 */
    public Long getFlushIntervalMs() {
        return flushIntervalMs;
    }

    /** @return 文件滚动周期（毫秒），null 表示用 Spool 默认 */
    public Long getRollCycleMillis() {
        return rollCycleMillis;
    }

    /** @return 清理旧文件间隔（毫秒），null 表示用 Spool 默认 */
    public Long getCleanupIntervalMs() {
        return cleanupIntervalMs;
    }

    /** @return 暂存队列满时 append 超时（毫秒），null 表示用 Spool 默认 */
    public Long getOfferTimeoutMs() {
        return offerTimeoutMs;
    }

    /** @return 内存暂存队列容量（条数），null 表示用 Spool 默认 */
    public Integer getStagingCapacity() {
        return stagingCapacity;
    }

    /** @return 目录占用刷新间隔（毫秒），null 表示用 Spool 默认 */
    public Long getDirSizeRefreshIntervalMs() {
        return dirSizeRefreshIntervalMs;
    }

    /** @return 载荷序列化器，null 表示框架默认 JDK */
    public Serializer<?> getSerializer() {
        return serializer;
    }

    // ======================== Builder ========================

    /**
     * {@code SpoolConfigPOJO} 构建器：spoolDir 为必传构造参数（编译期强制），
     * 其余字段链式可选，不设置即用 Spool 默认值。
     */
    public static final class Builder {

        private final String spoolDir;
        private Long maxSizeBytes;
        private Long flushIntervalMs;
        private Long rollCycleMillis;
        private Long cleanupIntervalMs;
        private Long offerTimeoutMs;
        private Integer stagingCapacity;
        private Long dirSizeRefreshIntervalMs;
        private Serializer<?> serializer;

        private Builder(String spoolDir) {
            if (spoolDir == null || spoolDir.trim().isEmpty()) {
                throw new IllegalArgumentException("spoolDir must not be blank");
            }
            this.spoolDir = spoolDir;
        }

        /** 磁盘预算上限（字节），如 {@code 1024L * 1024 * 1024} = 1GB；超限后 append 返回 false */
        public Builder maxSizeBytes(long maxSizeBytes) {
            this.maxSizeBytes = maxSizeBytes;
            return this;
        }

        /** 刷盘间隔（毫秒）。值越小断电丢失越少，写吞吐略降 */
        public Builder flushIntervalMs(long flushIntervalMs) {
            this.flushIntervalMs = flushIntervalMs;
            return this;
        }

        /** 文件滚动周期（毫秒），最小 1000，低于自动提升。不设置用 Spool 默认（5 分钟） */
        public Builder rollCycleMillis(long rollCycleMillis) {
            this.rollCycleMillis = rollCycleMillis;
            return this;
        }

        /** 清理已消费旧文件的间隔（毫秒）。滚动周期大（如 1 小时）时建议单独调小 */
        public Builder cleanupIntervalMs(long cleanupIntervalMs) {
            this.cleanupIntervalMs = cleanupIntervalMs;
            return this;
        }

        /** 内存暂存队列满时 append 最多阻塞等待多久（毫秒），超时返回 false */
        public Builder offerTimeoutMs(long offerTimeoutMs) {
            this.offerTimeoutMs = offerTimeoutMs;
            return this;
        }

        /** 写路径内存暂存队列容量（条数）。崩溃时暂存中数据会丢失 */
        public Builder stagingCapacity(int stagingCapacity) {
            this.stagingCapacity = stagingCapacity;
            return this;
        }

        /** 目录占用字节数刷新间隔（毫秒） */
        public Builder dirSizeRefreshIntervalMs(long dirSizeRefreshIntervalMs) {
            this.dirSizeRefreshIntervalMs = dirSizeRefreshIntervalMs;
            return this;
        }

        /**
         * 载荷序列化器（JDK / Jackson / Kryo 等），不设置用框架默认 JDK
         * （载荷须实现 {@link java.io.Serializable}）。
         */
        public Builder serializer(Serializer<?> serializer) {
            this.serializer = serializer;
            return this;
        }

        /** 构建不可变配置实例 */
        public SpoolConfigPOJO build() {
            return new SpoolConfigPOJO(this);
        }
    }
}
