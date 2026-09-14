package com.dynamicbatch.core.stats;

import com.dynamicbatch.core.pojo.StatsConfigPOJO;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 * 组级统计采集任务：每次 tick 读累计器 − 上次快照 = 增量快照，替换 latest 并推 listener。
 * 由 {@code BatchWorkerGroup} 启动期创建持有，业务方不应直接使用。
 *
 * <p>调度挂 {@linkplain #SCHEDULER 静态共享线程}（一条 daemon 线程服务全部组），
 * start 自挂、组关时 stop 自撤；基线即上次快照（累计字段 + intervalEndMillis），
 * 首份快照 = 组启动至今；tick 体整体 catch Throwable——scheduleWithFixedDelay 的任务
 * 抛未捕获异常会被 JDK 静默取消。
 */
public final class StatsCollector {

    private static final Logger log = LoggerFactory.getLogger(StatsCollector.class);

    /** 静态共享调度线程：一条 daemon 线程挂全部组的采集任务，进程生命周期常驻 */
    private static final ScheduledExecutorService SCHEDULER = Executors.newSingleThreadScheduledExecutor(r -> {
        Thread t = new Thread(r, "batch-stats-collector");
        t.setDaemon(true);
        return t;
    });

    private final String groupKey;
    private final CumulativeStats stats;
    private final StatsConfigPOJO config;
    /** 组启动时刻：首份快照（尚无上次快照基线）的区间起点，即"组启动至今"口径的锚点 */
    private final long createdAtMillis;
    /** 最新成品快照（volatile：采集线程写替换，endpoint 查询线程读做 live 差分基线） */
    private volatile StatsSnapshot latest;
    /** 本组任务在共享调度器上的句柄；未 start 为 null，stop 后置 null 可重启接线 */
    private ScheduledFuture<?> scheduled;

    public StatsCollector(String groupKey, CumulativeStats stats, StatsConfigPOJO config) {
        this.groupKey = groupKey == null ? "" : groupKey;
        this.stats = stats;
        this.config = config;
        this.createdAtMillis = System.currentTimeMillis();
    }

    /** 自挂共享调度器：fixedDelay，上一轮结束后才计时下一轮 */
    public synchronized void start() {
        if (scheduled != null) {
            return;
        }
        long interval = config.getCollectIntervalMillis();
        scheduled = SCHEDULER.scheduleWithFixedDelay(this::collectOnce, interval, interval, TimeUnit.MILLISECONDS);
        log.debug("[{}] stats collector started, intervalMillis={}, percentiles={}",
                groupKey, interval, Arrays.toString(config.getPercentiles()));
    }

    /** cancel 本组任务（不打断执行中的 tick），调度线程常驻 */
    public synchronized void stop() {
        if (scheduled != null) {
            scheduled.cancel(false);
            scheduled = null;
            log.debug("[{}] stats collector stopped", groupKey);
        }
    }

    /**
     * 现场差分“上次快照 → 当前”的 live 快照，供 endpoint 查询即时值；
     * 只读不推进（不替换 latest）。
     */
    public StatsSnapshot liveSnapshot() {
        return diffAndBuild();
    }

    /** 采集 tick：差分产出快照 → 替换 latest → 推 listener；整体 catch 防任务被静默取消。package-private 供测试直接驱动 */
    void collectOnce() {
        try {
            StatsSnapshot snapshot = diffAndBuild();
            this.latest = snapshot;
            StatsListeners.notifySnapshot(snapshot);
        } catch (Throwable t) {
            log.error("[{}] stats collect failed, next tick continues", groupKey, t);
        }
    }

    /** 差分组装快照：基线 = 上次快照累计 + intervalEnd；尚无快照时基线为零累计 + 创建时刻 */
    private StatsSnapshot diffAndBuild() {
        StatsSnapshot last = this.latest;
        long now = System.currentTimeMillis();
        long intervalStart = last != null ? last.getIntervalEndMillis() : createdAtMillis;
        long submitTotal = stats.submitTotal();
        long callbackTotal = stats.callbackTotal();
        long[] bucketTotals = stats.rtBucketCounts();
        long baseSubmit = last != null ? last.getSubmitTotal() : 0L;
        long baseCallback = last != null ? last.getCallbackTotal() : 0L;
        long[] baseBuckets = last != null ? last.getBucketTotals() : new long[bucketTotals.length];
        long[] bucketCounts = new long[bucketTotals.length];
        for (int i = 0; i < bucketCounts.length; i++) {
            bucketCounts[i] = bucketTotals[i] - baseBuckets[i];
        }
        return StatsSnapshot.create(groupKey, intervalStart, now,
                submitTotal - baseSubmit, callbackTotal - baseCallback, bucketCounts,
                submitTotal, callbackTotal, bucketTotals,
                stats.rtBucketBoundaries(), config.getPercentiles());
    }
}
