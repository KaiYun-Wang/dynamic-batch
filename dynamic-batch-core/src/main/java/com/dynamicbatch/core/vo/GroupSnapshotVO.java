package com.dynamicbatch.core.vo;

import com.dynamicbatch.core.stats.StatsSnapshot;
import com.dynamicbatch.spool.DiskUsage;

import java.util.List;

/**
 * Worker 组运行快照：配置、分区、调度器相位、队列水位、磁盘占用与统计的瞬时只读值。
 *
 * <p>各字段为独立时点读取，持续变化的字段（水位 / 占用 / 相位 / 统计）之间无原子性保证。
 * 未启动组或未启用采集（{@code statsConfig.enabled=false}）时 {@code dispatcherPhase} /
 * {@code stagingSize} / {@code spoolUsage} / {@code stats} 为 null。
 */
public final class GroupSnapshotVO {

    /** 组 key */
    private final String groupKey;
    /** 是否已启动 */
    private final boolean running;
    /** 内存队列容量 */
    private final Integer queueCapacity;
    /** 攒批条数 */
    private final Integer batchSize;
    /** 最大等待毫秒 */
    private final Long maxWaitMs;
    /** 组级投递限速（条/秒）；null = 未配置不限流 */
    private final Integer rateLimitPerSecond;
    /** 分区数 */
    private final int partitionCount;
    /** 调度器相位：RUNNING / PAUSE_PENDING / PAUSED / RUN_PENDING；未启动为 null */
    private final String dispatcherPhase;
    /** 各分区内存队列当前水位，下标即分区号 */
    private final List<Integer> queueSizes;
    /** Spool 暂存积压条数；未启动为 null */
    private final Integer stagingSize;
    /** Spool 磁盘占用拆分；未启动为 null */
    private final DiskUsage spoolUsage;
    /** 统计快照（增量 + 累计与派生 qps/tp 的 live 差分值）；未启用采集或组未启动为 null */
    private final StatsSnapshot stats;

    public GroupSnapshotVO(String groupKey, boolean running, Integer queueCapacity, Integer batchSize,
                           Long maxWaitMs, Integer rateLimitPerSecond, int partitionCount,
                           String dispatcherPhase,
                           List<Integer> queueSizes, Integer stagingSize, DiskUsage spoolUsage,
                           StatsSnapshot stats) {
        this.groupKey = groupKey;
        this.running = running;
        this.queueCapacity = queueCapacity;
        this.batchSize = batchSize;
        this.maxWaitMs = maxWaitMs;
        this.rateLimitPerSecond = rateLimitPerSecond;
        this.partitionCount = partitionCount;
        this.dispatcherPhase = dispatcherPhase;
        this.queueSizes = queueSizes;
        this.stagingSize = stagingSize;
        this.spoolUsage = spoolUsage;
        this.stats = stats;
    }

    public String getGroupKey() {
        return groupKey;
    }

    public boolean isRunning() {
        return running;
    }

    public Integer getQueueCapacity() {
        return queueCapacity;
    }

    public Integer getBatchSize() {
        return batchSize;
    }

    public Long getMaxWaitMs() {
        return maxWaitMs;
    }

    public Integer getRateLimitPerSecond() {
        return rateLimitPerSecond;
    }

    public int getPartitionCount() {
        return partitionCount;
    }

    public String getDispatcherPhase() {
        return dispatcherPhase;
    }

    public List<Integer> getQueueSizes() {
        return queueSizes;
    }

    public Integer getStagingSize() {
        return stagingSize;
    }

    public DiskUsage getSpoolUsage() {
        return spoolUsage;
    }

    public StatsSnapshot getStats() {
        return stats;
    }

    @Override
    public String toString() {
        return "GroupSnapshotVO{groupKey=" + groupKey + ", running=" + running
                + ", queueCapacity=" + queueCapacity + ", batchSize=" + batchSize
                + ", maxWaitMs=" + maxWaitMs + ", rateLimitPerSecond=" + rateLimitPerSecond
                + ", partitionCount=" + partitionCount
                + ", dispatcherPhase=" + dispatcherPhase + ", queueSizes=" + queueSizes
                + ", stagingSize=" + stagingSize + ", spoolUsage=" + spoolUsage
                + ", stats=" + stats + "}";
    }
}
