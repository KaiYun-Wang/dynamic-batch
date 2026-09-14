package com.dynamicbatch.spool;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.queue.ChronicleQueue;
import net.openhft.chronicle.queue.ExcerptAppender;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Spool 写路径：单写线程 + 有界暂存队列。
 * <p>生产者调用 {@link #append(byte[])} 入暂存队列（线程安全，零锁），
 * 后台单写线程串行写入 Chronicle-Queue。定时 sync 由 {@link SpoolTimer} 承担。
 */
class SpoolWriter implements Closeable {

    private static final Logger log = LoggerFactory.getLogger(SpoolWriter.class);

    /** 写线程无数据时轮询间隔（毫秒） */
    private static final long POLL_INTERVAL_MS = 100;
    /** 每次批量 drain 上限（条） */
    private static final int DRAIN_MAX = 128;

    /** 构建期配置（只读） */
    private final SpoolConfig config;
    /** 内存暂存队列：生产者入队，写线程批量落盘 */
    private final LinkedBlockingQueue<byte[]> stagingQueue;
    /** Chronicle 追加写入口 */
    private final ExcerptAppender appender;
    /** 写循环是否运行中 */
    private volatile boolean running;
    /** 后台单写线程 */
    private Thread writeThread;
    /** 新数据信号：写侧边沿触发（无未消费信号时才按铃），许可恒 ≤1，取数侧挂起等待 */
    private final Semaphore newDataSignal = new Semaphore(0);

    SpoolWriter(SpoolConfig config, ChronicleQueue chronicleQueue) {
        this.config = config;
        /* Chronicle-Queue 实例（创建 appender 用） */
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
        log.debug("spool writer started, stagingCapacity={}, flushIntervalMs={}",
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
        log.debug("spool writer closed");
    }

    // ======================== 写循环 ========================

    /** 刷盘：由 {@link SpoolTimer} 定时调用 */
    void sync() {
        appender.sync();
    }

    /** 落盘单条并按响新数据信号：全部写路径唯一收口 */
    private void writeOne(byte[] data) {
        appender.writeBytes(Bytes.wrapForRead(data));
        // 写侧边沿触发：无未消费信号才按铃（单写线程判定精确），许可恒 ≤1 杜绝积压溢出
        if (newDataSignal.availablePermits() == 0) {
            newDataSignal.release();
        }
    }
    
    /**
     * 等待新落盘信号（取数侧经 {@link Spool#awaitData} 转发）：挂起至多 timeoutMs，
     * true = 有新落盘应立即 poll，false = 超时（调用方兜底真读）或被中断（恢复标志）。
     * 信号为写侧边沿触发（恒 ≤1 个未消费许可），消费即拿走唯一许可，无需读侧清理存量。
     */
    boolean awaitData(long timeoutMs) {
        try {
            return newDataSignal.tryAcquire(timeoutMs, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
        }
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
                    writeOne(b);
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
            log.debug("draining {} remaining entries on close", remaining.size());
            for (byte[] b : remaining) {
                writeOne(b);
            }
        }
        appender.sync();
    }
}