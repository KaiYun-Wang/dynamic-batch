package com.dynamicbatch.spool;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Stream;
import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.bytes.MappedBytes;
import net.openhft.chronicle.bytes.MappedBytesStore;
import net.openhft.chronicle.queue.ChronicleQueue;
import net.openhft.chronicle.queue.ExcerptTailer;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Spool 读路径：锁保护共享 tailer + 文件清理。
 * <p>使用 Chronicle 命名 tailer（持久化读位置），重启后从断点续读。
 * 多线程并发 poll 由一把非公平 {@link ReentrantLock} 串行化。
 */
class SpoolReader implements Closeable {

    private static final Logger log = LoggerFactory.getLogger(SpoolReader.class);

    /** 串行化并发 poll 的非公平锁 */
    private final ReentrantLock lock = new ReentrantLock(false);
    /** 构建期配置（只读） */
    private final SpoolConfig config;
    /** Chronicle-Queue 实例（syncIndex 刷 metadata 用） */
    private final ChronicleQueue chronicleQueue;
    /** 命名 tailer：读位置自动持久化，重启可续读 */
    private final ExcerptTailer tailer;
    /** 读缓冲复用，避免每条 allocateElasticDirect 泄漏直内存 */
    private final Bytes<?> readBuffer = Bytes.allocateElasticDirect();

    SpoolReader(SpoolConfig config, ChronicleQueue chronicleQueue) {
        this.config = config;
        this.chronicleQueue = chronicleQueue;
        // 命名 tailer：Chronicle 自动持久化读位置到 metadata 文件
        this.tailer = chronicleQueue.createTailer("spool-tailer");
    }

    /**
     * 读取一条数据，线程安全（锁内串行化）。
     * <p>语义：{@code lockTimeoutMs} 是获取锁的最长等待；拿到锁后读一次——
     * 有数据返回，队列空立即返回 null（不等待）。锁超时抛 {@link java.util.concurrent.TimeoutException}，
     * 调用方据此区分"队列空"与"并发读过载"。</p>
     * <p>中断（仅进程关闭时发生）在内部吞掉并恢复中断标志：读位置未推进、不丢数据，返回 null 即可。</p>
     *
     * @param lockTimeoutMs 获取锁的最长等待（毫秒）
     * @return 数据字节数组；队列为空返回 null
     * @throws TimeoutException 获取锁超时（并发读压力过大）
     */
    byte[] poll(long lockTimeoutMs) throws TimeoutException {
        boolean locked;
        try {
            locked = lock.tryLock(lockTimeoutMs, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt(); // 恢复中断标志，上层可感知退出信号
            return null;                       // 中断场景读不到数据不丢，等同无数据
        }
        if (!locked) {
            throw new TimeoutException("lock acquire timeout after " + lockTimeoutMs + "ms");
        }
        try {
            readBuffer.clear();
            if (tailer.readBytes(readBuffer)) {
                return readBuffer.toByteArray();
            }
            return null; // 队列空，立即返回
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void close() {
        // 关闭前把 index 刷下去
        syncIndex();
        try {
            readBuffer.releaseLast();
        } catch (Exception e) {
            log.warn("release readBuffer failed", e);
        }
        log.info("spool reader closed");
    }

    /**
     * 将命名 tailer 的读位置（index）强制刷到磁盘。
     * <p>Chronicle 命名 tailer 的 index 存储在 {@code metadata.cq4t} 文件（TableStore）的 mmap 区域，
     * 默认靠 OS 异步刷盘（最长 ~30 秒）。主动调用此方法可将脏页立即刷下去，
     * 把进程崩溃时的重复消费范围从 OS 默认值降到 sync 间隔。</p>
     * <p>{@link SpoolTimer} 定期调用此方法，和数据 sync 保持同一节奏。</p>
     */
    void syncIndex() {
        // 走 Chronicle 自己的 MappedBytesStore.syncUpTo() 刷 metadata 文件
        try {
            SingleChronicleQueue scq = (SingleChronicleQueue) chronicleQueue;
            MappedBytes mb = scq.metaStore().bytes();
            if (mb.bytesStore() instanceof MappedBytesStore) {
                ((MappedBytesStore) mb.bytesStore()).syncUpTo(mb.writePosition());
            }
            mb.releaseLast();
        } catch (Exception e) {
            log.warn("sync metadata failed", e);
        }
    }

    /**
     * 删除 tailer 已消费完的旧 cycle 文件。由 {@link SpoolTimer} 定期调用。
     * <p>以 tailer 当前正在读的文件为界（{@code tailer.currentFile()}），
     * 文件名比它小的（时间更早）都说明已读完，删；遇到它就停。</p>
     */
    void deleteConsumedCycles() {
        lock.lock();
        try {
            doDeleteConsumedCycles();
        } finally {
            lock.unlock();
        }
    }

    private void doDeleteConsumedCycles() {
        File current = tailer.currentFile();
        if (current == null) return;
        String currentName = current.getName();

        Path dir = config.dir;
        if (!Files.isDirectory(dir)) return;

        // 文件名按时间排序 = 字典序，比当前文件小的都删，遇到当前文件就停
        try (Stream<Path> stream = Files.list(dir)) {
            // ponytail: JDK8 无 takeWhile，用循环
            java.util.List<Path> sorted = new java.util.ArrayList<>();
            stream.filter(p -> p.toString().endsWith(Spool.DATA_FILE_SUFFIX))
                    .sorted()
                    .forEach(sorted::add);
            for (Path p : sorted) {
                if (p.getFileName().toString().compareTo(currentName) >= 0) break;
                deleteQuietly(p);
            }
        } catch (IOException e) {
            log.warn("cleanup list failed", e);
        }
    }

    // ======================== 文件删除（无锁，外部已持锁） ========================

    private void deleteQuietly(Path file) {
        try {
            Files.deleteIfExists(file);
            log.debug("deleted old cycle file: {}", file.getFileName());
        } catch (IOException e) {
            log.warn("failed to delete old cycle file: {}", file.getFileName());
        }
    }

    // ======================== 文件清理 ========================

    /**
     * 即时统计目录占用拆分，拿读锁（与 poll / 清理互斥），拆分语义见 {@link DiskUsage}。
     * tailer 从未 poll 过时 currentFile() 为 null，全部数据文件计入未读侧。
     *
     * @return 占用拆分；目录不可读或列目录失败返回 null
     */
    DiskUsage breakdownDiskUsage() {
        lock.lock();
        try {
            return doBreakdownDiskUsage();
        } finally {
            lock.unlock();
        }
    }

    private DiskUsage doBreakdownDiskUsage() {
        File current = tailer.currentFile();
        String currentName = current == null ? null : current.getName();

        Path dir = config.dir;
        if (!Files.isDirectory(dir)) return null;

        long total = 0L, consumed = 0L, pending = 0L;
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(dir)) {
            for (Path p : stream) {
                String name = p.getFileName().toString();
                if (!name.endsWith(Spool.DATA_FILE_SUFFIX) || !Files.isRegularFile(p)) {
                    continue;
                }
                long size;
                try {
                    size = Files.size(p);
                } catch (IOException e) {
                    continue; // 文件恰好被清理线程删除等竞态，按 0 计
                }
                total += size;
                if (currentName != null && name.compareTo(currentName) < 0) {
                    consumed += size;
                } else {
                    pending += size;
                }
            }
        } catch (IOException e) {
            log.warn("breakdown disk usage list failed, dir={}", dir, e);
            return null;
        }
        return new DiskUsage(total, consumed, pending, config.maxSizeBytes);
    }
}
