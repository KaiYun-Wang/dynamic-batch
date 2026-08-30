package com.dynamicbatch.spool;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Spool 定时任务：定期刷盘（sync）、清理旧文件、刷新目录占用。
 * <p>自身不持有数据，只引用 {@link SpoolWriter} / {@link SpoolReader} / {@link Spool}
 * 并定期调用其方法。使用单线程调度器。</p>
 *
 * <p>offset 持久化由 Chronicle 命名 tailer 自动完成，不在此处理。</p>
 */
class SpoolTimer implements Closeable {

    private static final Logger log = LoggerFactory.getLogger(SpoolTimer.class);

    /** 单线程调度器：刷盘、清理、刷新占用 */
    private final ScheduledExecutorService scheduler;

    SpoolTimer(SpoolWriter writer, long flushIntervalMs,
               SpoolReader reader, long cleanupIntervalMs,
               Spool<?> spool, long dirSizeRefreshIntervalMs) {
        this.scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "spool-timer");
            t.setDaemon(true);
            return t;
        });

        // 定时刷盘
        scheduler.scheduleAtFixedRate(() -> {
            try {
                writer.sync();
            } catch (Exception e) {
                log.error("spool timer sync error", e);
            }
        }, flushIntervalMs, flushIntervalMs, TimeUnit.MILLISECONDS);

        // 定时清理已消费的旧 cycle 文件，删完后刷新目录占用
        if (cleanupIntervalMs > 0) {
            scheduler.scheduleAtFixedRate(() -> {
                try {
                    reader.deleteConsumedCycles();
                    spool.refreshDiskUsage();
                } catch (Exception e) {
                    log.error("spool timer cleanup error", e);
                }
            }, cleanupIntervalMs, cleanupIntervalMs, TimeUnit.MILLISECONDS);
        }

        // 定时刷新目录大小
        if (dirSizeRefreshIntervalMs > 0) {
            scheduler.scheduleAtFixedRate(() -> {
                try {
                    spool.refreshDiskUsage();
                } catch (Exception e) {
                    log.error("spool timer dir size refresh error", e);
                }
            }, dirSizeRefreshIntervalMs, dirSizeRefreshIntervalMs, TimeUnit.MILLISECONDS);
        }

        log.debug("spool timer started, flushIntervalMs={}, cleanupIntervalMs={}, dirSizeRefreshIntervalMs={}",
                flushIntervalMs, cleanupIntervalMs, dirSizeRefreshIntervalMs);
    }

    @Override
    public void close() {
        scheduler.shutdown();
        try {
            if (!scheduler.awaitTermination(3, TimeUnit.SECONDS)) {
                scheduler.shutdownNow();
            }
        } catch (InterruptedException e) {
            scheduler.shutdownNow();
            Thread.currentThread().interrupt();
        }
        log.debug("spool timer closed");
    }
}
