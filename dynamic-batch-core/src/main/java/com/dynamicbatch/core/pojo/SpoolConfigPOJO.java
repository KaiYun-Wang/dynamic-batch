package com.dynamicbatch.core.pojo;

import com.dynamicbatch.spool.Serializer;

/**
 * Spool 磁盘缓冲配置 POJO。
 *
 * <p>由 {@code BatchWorkerGroup.Builder} 必传，框架内部据此构建 {@code Spool}。
 * 包装类型字段 {@code null} 表示该项未设置，构建时由 Spool 默认值兜底（本类不做填充）。
 *
 * <p>{@code serializer} 粒度为载荷 {@code T}：框架用 {@link com.dynamicbatch.core.serializer.SpoolEntrySerializer}
 * 包一层 routingKey 后落盘，使用方只需关心业务 data 的编解码方式。
 */
public class SpoolConfigPOJO<T> {

    /**
     * Spool 队列目录（每 Group 独占一个目录，共用则启动 fail fast）。
     * 必填，构建期校验 blank → {@link IllegalArgumentException}。
     */
    private String spoolDir;

    /**
     * 磁盘预算上限（字节）。目录占用 ≥ 上限时 {@code append} 返回 false。
     * null → Spool 默认 {@code Long.MAX_VALUE}（不限）。
     */
    private Long maxSizeBytes;

    /**
     * 定时刷盘间隔（毫秒）。数据与读位置一并 sync，影响断电丢失/重复消费窗口。
     * null → Spool 默认 5000。
     */
    private Long flushIntervalMs;

    /**
     * Chronicle 数据文件滚动周期（毫秒）。暴露时长而非 {@code RollCycle} 枚举，避免 API 泄漏 Chronicle 类型。
     * null → Spool 默认 FIVE_MINUTELY；&gt; 0 时映射为 {@code Spool.Builder#rollCycleMillis}。
     */
    private Long rollCycleMillis;

    /**
     * 清理已消费旧文件的间隔（毫秒）。滚动周期大时建议单独调小。
     * null → Spool 默认 0（跟滚动周期一致）。
     */
    private Long cleanupIntervalMs;

    /**
     * 内存暂存队列满时，{@code append} 最多阻塞等待多久（毫秒），超时返回 false。
     * null → Spool 默认 100。
     */
    private Long offerTimeoutMs;

    /**
     * 写路径内存暂存队列容量（条数）。生产者 {@code append} 先入此队列，后台单写线程落盘。
     * null → Spool 默认 10000。
     */
    private Integer stagingCapacity;

    /**
     * 目录占用字节数刷新间隔（毫秒），用于 {@code maxSizeBytes} 预算判断。
     * null → Spool 默认 5000。
     */
    private Long dirSizeRefreshIntervalMs;

    /**
     * 业务载荷 {@code T} 的序列化器（JDK / Jackson / Kryo 等）。
     * null → 框架默认 {@link com.dynamicbatch.spool.JdkSerializer}（payload 须实现 {@link java.io.Serializable}）。
     * routingKey 编解码由信封序列化器统一处理，不在此配置。
     */
    private Serializer<T> serializer;

    /** @return Spool 队列目录 */
    public String getSpoolDir() {
        return spoolDir;
    }

    public void setSpoolDir(String spoolDir) {
        this.spoolDir = spoolDir;
    }

    /** @return 磁盘预算上限（字节），null 表示用 Spool 默认 */
    public Long getMaxSizeBytes() {
        return maxSizeBytes;
    }

    public void setMaxSizeBytes(Long maxSizeBytes) {
        this.maxSizeBytes = maxSizeBytes;
    }

    /** @return 刷盘间隔（毫秒），null 表示用 Spool 默认 */
    public Long getFlushIntervalMs() {
        return flushIntervalMs;
    }

    public void setFlushIntervalMs(Long flushIntervalMs) {
        this.flushIntervalMs = flushIntervalMs;
    }

    /** @return 文件滚动周期（毫秒），null 表示用 Spool 默认 */
    public Long getRollCycleMillis() {
        return rollCycleMillis;
    }

    public void setRollCycleMillis(Long rollCycleMillis) {
        this.rollCycleMillis = rollCycleMillis;
    }

    /** @return 清理旧文件间隔（毫秒），null 表示用 Spool 默认 */
    public Long getCleanupIntervalMs() {
        return cleanupIntervalMs;
    }

    public void setCleanupIntervalMs(Long cleanupIntervalMs) {
        this.cleanupIntervalMs = cleanupIntervalMs;
    }

    /** @return 暂存队列满时 append 超时（毫秒），null 表示用 Spool 默认 */
    public Long getOfferTimeoutMs() {
        return offerTimeoutMs;
    }

    public void setOfferTimeoutMs(Long offerTimeoutMs) {
        this.offerTimeoutMs = offerTimeoutMs;
    }

    /** @return 内存暂存队列容量（条数），null 表示用 Spool 默认 */
    public Integer getStagingCapacity() {
        return stagingCapacity;
    }

    public void setStagingCapacity(Integer stagingCapacity) {
        this.stagingCapacity = stagingCapacity;
    }

    /** @return 目录占用刷新间隔（毫秒），null 表示用 Spool 默认 */
    public Long getDirSizeRefreshIntervalMs() {
        return dirSizeRefreshIntervalMs;
    }

    public void setDirSizeRefreshIntervalMs(Long dirSizeRefreshIntervalMs) {
        this.dirSizeRefreshIntervalMs = dirSizeRefreshIntervalMs;
    }

    /** @return 载荷序列化器，null 表示框架默认 JDK */
    public Serializer<T> getSerializer() {
        return serializer;
    }

    public void setSerializer(Serializer<T> serializer) {
        this.serializer = serializer;
    }
}
