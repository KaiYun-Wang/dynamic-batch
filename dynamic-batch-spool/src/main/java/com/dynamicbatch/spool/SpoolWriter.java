package com.dynamicbatch.spool;

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.queue.ChronicleQueue;
import net.openhft.chronicle.queue.ExcerptAppender;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * Spool 写路径：单写线程 + 有界暂存队列。
 * <p>生产者调用 {@link #append(byte[])} 入暂存队列（线程安全，零锁），
 * 后台单写线程串行写入 Chronicle-Queue。定时 sync 由 {@link SpoolTimer} 承担。
 */
class SpoolWriter implements Closeable {

    private static final Logger log = LoggerFactory.getLogger(SpoolWriter.class);

    /** 写线程无数据时轮询间隔 */
    private static final long POLL_INTERVAL_MS = 100;
    /** 每次批量 drain 上限 */
    private static final int DRAIN_MAX = 128;

    private final LinkedBlockingQueue<byte[]> stagingQueue;
    private final ExcerptAppender appender;
    private final ChronicleQueue chronicleQueue;
    private final SpoolConfig config;

    private volatile boolean running;
    private Thread writeThread;

    SpoolWriter(SpoolConfig config, ChronicleQueue chronicleQueue) {
        this.config = config;
        this.chronicleQueue = chronicleQueue;
        this.stagingQueue = new LinkedBlockingQueue<>(config.stagingCapacity);
        this.appender = chronicleQueue.createAppender();
    }

    /** 启动写线程 */
    synchronized void start() {
        if (running) return;
        running = true;
        writeThread = new Thread(this::writeLoop, "spool-writer");
        writeThread.setDaemon(true);
        writeThread.start();
        log.info("spool writer started, stagingCapacity={}, flushIntervalMs={}",
                config.stagingCapacity, config.flushIntervalMs);
    }

    /**
     * 入暂存队列，线程安全。
     * @return true 入队成功；false 队列满且超时
     */
    boolean append(byte[] data) {
        if (!running) return false;
        try {
            return stagingQueue.offer(data, config.offerTimeoutMs, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
        }
    }

    int stagingSize() {
        return stagingQueue.size();
    }

    @Override
    public synchronized void close() {
        if (!running) return;
        running = false;

        if (writeThread != null) {
            try {
                writeThread.join(TimeUnit.SECONDS.toMillis(5));
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        log.info("spool writer closed");
    }

    // ======================== 写循环 ========================

    /** 刷盘：由 {@link SpoolTimer} 定时调用 */
    void sync() {
        appender.sync();
    }

    private void writeLoop() {
        List<byte[]> batch = new ArrayList<>();

        while (running) {
            try {
                byte[] data = stagingQueue.poll(POLL_INTERVAL_MS, TimeUnit.MILLISECONDS);
                if (data == null) continue;

                batch.add(data);
                stagingQueue.drainTo(batch, DRAIN_MAX);
                for (byte[] b : batch) {
                    appender.writeBytes(Bytes.wrapForRead(b));
                }
                batch.clear();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            } catch (Exception e) {
                log.error("spool write loop error", e);
            }
        }

        // 关闭前排空暂存队列 + 刷盘
        List<byte[]> remaining = new ArrayList<>();
        stagingQueue.drainTo(remaining);
        if (!remaining.isEmpty()) {
            log.info("draining {} remaining entries on close", remaining.size());
            for (byte[] b : remaining) {
                appender.writeBytes(Bytes.wrapForRead(b));
            }
        }
        appender.sync();
    }
}