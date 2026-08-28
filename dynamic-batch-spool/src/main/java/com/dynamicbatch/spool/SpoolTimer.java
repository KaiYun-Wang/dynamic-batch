package com.dynamicbatch.spool;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Spool 定时任务：定期刷盘（sync），让后台线程承担定时工作。
 * <p>自身不持有数据，只引用 {@link SpoolWriter} 和 {@link SpoolReader} 并定期 sync。
 * 使用单线程调度器，sync 是轻量操作（只刷当前 mmap 文件脏页），不会阻塞数据读写。</p>
 */
class SpoolTimer implements Closeable {

    private static final Logger log = LoggerFactory.getLogger(SpoolTimer.class);

    private final ScheduledExecutorService scheduler;

    SpoolTimer(SpoolWriter writer, long flushIntervalMs,
               SpoolReader reader, long cleanupIntervalMs) {
        this.scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "spool-timer");
            t.setDaemon(true);
            return t;
        });

        // 定时刷盘：数据 + offset 一起刷，保持同一节奏
        scheduler.scheduleAtFixedRate(() -> {
            try {
                writer.sync();
            } catch (Exception e) {
                log.error("spool timer sync error", e);
            }
            try {
                reader.syncIndex();
            } catch (Exception e) {
                log.error("spool timer syncIndex error", e);
            }
        }, flushIntervalMs, flushIntervalMs, TimeUnit.MILLISECONDS);

        // 定时清理已消费的旧 cycle 文件
        if (cleanupIntervalMs > 0) {
            scheduler.scheduleAtFixedRate(() -> {
                try {
                    reader.deleteConsumedCycles();
                } catch (Exception e) {
                    log.error("spool timer cleanup error", e);
                }
            }, cleanupIntervalMs, cleanupIntervalMs, TimeUnit.MILLISECONDS);
        }

        log.debug("spool timer started, flushIntervalMs={}, cleanupIntervalMs={}",
                flushIntervalMs, cleanupIntervalMs);
    }

    /** 兼容旧构造器（无清理），不单独启动 schedule */
    SpoolTimer(SpoolWriter writer, long flushIntervalMs) {
        this(writer, flushIntervalMs, null, 0);
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