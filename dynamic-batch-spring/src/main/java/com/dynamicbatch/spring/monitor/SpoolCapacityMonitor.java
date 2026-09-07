package com.dynamicbatch.spring.monitor;

import com.dynamicbatch.common.pojo.NotifyItemPOJO;
import com.dynamicbatch.core.BatchProcessor;
import com.dynamicbatch.core.notifier.manager.NotifyManager;
import com.dynamicbatch.spring.properties.NotifyProperties;
import com.dynamicbatch.spool.DiskUsage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Spool 容量定时巡检：周期遍历全部组占用，达到 notify-items 的 threshold
 * （占预算百分比）即投递 SPOOL_CAPACITY 告警。
 *
 * <p>notify-items 登记 {@code type: spool_capacity} 且 {@code enabled: true}
 * 时启动巡检线程，否则 Bean 空转不起线程；自带单 daemon 调度线程，
 * 容器关闭经 destroyMethod="stop" 停止。
 */
public class SpoolCapacityMonitor {

    private static final Logger log = LoggerFactory.getLogger(SpoolCapacityMonitor.class);

    /** 巡检周期默认（秒）：notify-items 未配 intervalSeconds 时生效 */
    private static final int DEFAULT_INTERVAL_SECONDS = 60;
    /** 告警阈值默认（%）：notify-items 未配 threshold 时生效 */
    private static final int DEFAULT_THRESHOLD_PERCENT = 70;

    /** 巡检目标：组占用查询入口 */
    private final BatchProcessor processor;
    /** 单 daemon 调度线程；未启用巡检时为 null（Bean 空转，零线程零开销） */
    private final ScheduledExecutorService scheduler;
    /** 生效的巡检配置（threshold / silencePeriod 来源） */
    private final NotifyItemPOJO item;

    public SpoolCapacityMonitor(BatchProcessor processor, NotifyProperties properties) {
        this.processor = processor;
        this.item = findEnabledItem(properties);
        if (item == null) {
            this.scheduler = null;
            log.info("spool capacity monitor disabled, notify-items has no enabled spool_capacity item");
            return;
        }
        long intervalSeconds = item.getIntervalSeconds() != null && item.getIntervalSeconds() > 0
                ? item.getIntervalSeconds() : DEFAULT_INTERVAL_SECONDS;
        this.scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "batch-spool-capacity-monitor");
            t.setDaemon(true);
            return t;
        });
        // fixedDelay：上一轮巡检完成后再计时，多组串行查询不叠加延迟
        this.scheduler.scheduleWithFixedDelay(this::checkAllGroups,
                intervalSeconds, intervalSeconds, TimeUnit.SECONDS);
        log.info("spool capacity monitor started, intervalSeconds={}, threshold={}%",
                intervalSeconds, effectiveThreshold());
    }

    /** 从 notify-items 找登记且启用的 SPOOL_CAPACITY 项；没有返回 null（巡检关闭） */
    private static NotifyItemPOJO findEnabledItem(NotifyProperties properties) {
        if (properties.getNotifyItems() == null) {
            return null;
        }
        for (NotifyItemPOJO item : properties.getNotifyItems()) {
            if ("spool_capacity".equalsIgnoreCase(item.getType()) && item.isEnabled()) {
                return item;
            }
        }
        return null;
    }

    /** 生效阈值（%）：未配置默认 70，最小 1 */
    private int effectiveThreshold() {
        int threshold = item.getThreshold() != null ? item.getThreshold() : DEFAULT_THRESHOLD_PERCENT;
        return Math.max(1, threshold);
    }

    /**
     * 一轮巡检：遍历全部组占用，超阈值投递告警；异常只记日志不终止调度。
     */
    void checkAllGroups() {
        try {
            int threshold = effectiveThreshold();
            Map<String, DiskUsage> usages = processor.listSpoolUsages();
            for (Map.Entry<String, DiskUsage> entry : usages.entrySet()) {
                DiskUsage usage = entry.getValue();
                int percent = percentOf(usage);
                if (percent >= threshold) {
                    log.warn("spool capacity threshold hit, group={}, percent={}%, usage={}",
                            entry.getKey(), percent, usage);
                    NotifyManager.getInstance().tryNoticeSpoolCapacityAsync(entry.getKey(),
                            usage.getTotalBytes(), usage.getMaxSizeBytes(), percent,
                            usage.getConsumedBytes(), usage.getPendingBytes());
                }
            }
        } catch (Exception e) {
            log.warn("spool capacity check failed", e);
        }
    }

    /** 当前占用占预算的百分比（0~100）；预算未配置（Long.MAX_VALUE）恒为 0，永不触发 */
    private static int percentOf(DiskUsage usage) {
        if (usage.getMaxSizeBytes() <= 0 || usage.getMaxSizeBytes() == Long.MAX_VALUE) {
            return 0;
        }
        long percent = usage.getTotalBytes() * 100 / usage.getMaxSizeBytes();
        return (int) Math.min(100, percent);
    }

    /** 巡检线程是否已启动（notify-items 登记且 enabled=true 才为 true） */
    public boolean isRunning() {
        return scheduler != null;
    }

    /** 停止巡检线程（容器关闭回调） */
    public void stop() {
        if (scheduler != null) {
            scheduler.shutdownNow();
            log.info("spool capacity monitor stopped");
        }
    }
}
