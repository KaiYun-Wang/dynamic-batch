package com.dynamicbatch.spool;

import net.openhft.chronicle.queue.ChronicleQueue;
import net.openhft.chronicle.queue.RollCycle;
import net.openhft.chronicle.queue.RollCycles;
import net.openhft.chronicle.queue.rollcycles.TestRollCycles;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileLock;
import java.nio.channels.OverlappingFileLockException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Objects;
import java.util.concurrent.TimeoutException;

/**
 * 磁盘级 FIFO 缓冲队列（Spool）：基于 Chronicle-Queue 的薄封装。
 * <p>
 * 写路径：生产者 {@link #append} 入有界暂存队列（线程安全，零锁）
 * → 后台单写线程串行落盘 + 定期 sync。
 * 读路径：{@link #poll} 由单锁串行化共享命名 tailer，读位置自动持久化。
 * </p>
 *
 * <p>目录独占锁防止多进程操作同一目录。作为一个可独立复用的缓冲模块设计。</p>
 *
 * @param <T> 数据类型
 */
public class Spool<T> implements Closeable {

    private static final Logger log = LoggerFactory.getLogger(Spool.class);

    /** 目录独占锁文件名 */
    private static final String LOCK_FILE = ".spool.lock";

    // Chronicle 全局开关：关启动公告；允许多线程通过同一 queue 创建 appender/tailer（本模块自行串行化写/读）
    static {
        System.setProperty("chronicle.announcer.disable", "true");
        System.setProperty("disable.single.threaded.check", "true");
    }

    /** 数据类型，反序列化时使用 */
    private final Class<T> type;
    /** 序列化器：对象 ↔ byte[] */
    private final Serializer<T> serializer;
    /** 构建期配置（只读） */
    private final SpoolConfig config;
    /** 队列目录路径（与 config.dir 相同，便于对外暴露） */
    private final Path dir;
    /** Chronicle-Queue 实例 */
    private final ChronicleQueue chronicleQueue;
    /** 写路径：暂存队列 + 单写线程 */
    private final SpoolWriter writer;
    /** 读路径：共享 tailer + 清理 */
    private final SpoolReader reader;
    /** 定时刷盘 / 清理 / 刷新占用 */
    private final SpoolTimer timer;
    /** 目录独占文件锁 */
    private final FileLock dirLock;
    /** 目录锁对应的 RandomAccessFile，关闭时一并释放 */
    private final RandomAccessFile lockFile;
    /** 目录当前占用（字节），由 {@link #refreshDiskUsage()} 刷新 */
    private volatile long currentSizeBytes;
    /** 是否已关闭 */
    private volatile boolean closed;

    private Spool(Builder<T> builder) {
        this.type = Objects.requireNonNull(builder.type, "type must not be null");
        this.serializer = Objects.requireNonNull(builder.serializer, "serializer must not be null");
        this.config = builder.toConfig();
        this.dir = this.config.dir;

        // 确保目录存在
        try {
            Files.createDirectories(this.config.dir);
        } catch (IOException e) {
            throw new RuntimeException("failed to create spool directory: " + this.config.dir, e);
        }

        // 目录独占锁：同目录第二个进程/实例直接 fail fast
        Path lockPath = this.config.dir.resolve(LOCK_FILE);
        FileLock tmpLock = null;
        RandomAccessFile tmpLockFile = null;
        try {
            tmpLockFile = new RandomAccessFile(lockPath.toFile(), "rw");
            tmpLock = tmpLockFile.getChannel().tryLock();
            if (tmpLock == null) {
                tmpLockFile.close();
                throw new IllegalStateException(
                        "spool directory already locked by another process: " + this.config.dir);
            }
        } catch (OverlappingFileLockException e) {
            throw new IllegalStateException(
                    "spool directory locked within same JVM: " + this.config.dir, e);
        } catch (IOException e) {
            throw new RuntimeException("failed to lock spool directory: " + this.config.dir, e);
        }
        this.dirLock = tmpLock;
        this.lockFile = tmpLockFile;

        // 创建 Chronicle-Queue
        try {
            this.chronicleQueue = ChronicleQueue.singleBuilder(this.config.dir.toFile())
                    .rollCycle(this.config.rollCycle)
                    .build();
        } catch (Exception e) {
            releaseLock();
            throw new RuntimeException("failed to create ChronicleQueue at: " + this.config.dir, e);
        }

        // 内部组件
        this.writer = new SpoolWriter(this.config, chronicleQueue);
        this.reader = new SpoolReader(this.config, chronicleQueue);
        this.writer.start();
        // cleanupIntervalMs=0 时默认跟滚动周期一致
        long cleanupMs = this.config.cleanupIntervalMs > 0
                ? this.config.cleanupIntervalMs
                : this.config.rollCycle.lengthInMillis();
        this.timer = new SpoolTimer(writer, this.config.flushIntervalMs, reader, cleanupMs,
                this, this.config.dirSizeRefreshIntervalMs);
        // 启动时刷新一次目录占用（Timer 首跑有 delay）
        refreshDiskUsage();
    }

    /**
     * 入队，线程安全。
     * <p>返回 true 仅代表已进入内存暂存队列，后台异步落盘，进程崩溃可能丢失暂存中的数据。</p>
     *
     * @param data 待缓冲的数据
     * @return true 入暂存队列成功；false 磁盘预算超限、暂存队列满且超时，或已关闭
     */
    public boolean append(T data) {
        if (closed) return false;
        if (currentSizeBytes >= config.maxSizeBytes) {
            log.warn("spool disk budget exceeded, append rejected: currentSizeBytes={}, maxSizeBytes={}, dir={}",
                    currentSizeBytes, config.maxSizeBytes, dir);
            return false;
        }
        byte[] bytes = serializer.serialize(data);
        return writer.append(bytes);
    }

    /**
     * 扫描队列目录，更新 {@link #currentSizeBytes}。
     * <p>由定时任务与清理旧文件后调用；统计目录内数据文件总长度（忽略 {@code .spool.lock}）。</p>
     */
    public void refreshDiskUsage() {
        long total = 0L;
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(dir)) {
            for (Path p : stream) {
                String name = p.getFileName().toString();
                if (LOCK_FILE.equals(name)) {
                    continue;
                }
                if (!Files.isRegularFile(p)) {
                    continue;
                }
                total += Files.size(p);
            }
        } catch (IOException e) {
            log.warn("failed to refresh disk usage, dir={}", dir, e);
            return;
        }
        currentSizeBytes = total;
        log.debug("spool disk usage refreshed: currentSizeBytes={}, maxSizeBytes={}, dir={}",
                currentSizeBytes, config.maxSizeBytes, dir);
    }

    /** 目录当前占用字节数（最近一次 {@link #refreshDiskUsage()} 的结果） */
    public long getCurrentSizeBytes() {
        return currentSizeBytes;
    }

    /**
     * 读取一条数据，线程安全（内部锁串行化）。
     * <p>语义：{@code lockTimeoutMs} 是获取锁的最长等待；拿到锁后读一次——
     * 有数据返回，队列空立即返回 null（不等待）。锁超时抛 {@link java.util.concurrent.TimeoutException}，
     * 调用方据此区分"队列空"与"并发读过载"。中断在内部处理，调用方无需关心。</p>
     *
     * @param lockTimeoutMs 获取锁的最长等待（毫秒）
     * @return 反序列化后的数据；队列为空返回 null
     * @throws TimeoutException 获取锁超时（并发读压力过大）
     */
    public T poll(long lockTimeoutMs) throws TimeoutException {
        byte[] bytes = reader.poll(lockTimeoutMs);
        if (bytes == null) return null;
        return serializer.deserialize(bytes, type);
    }

    /** 暂存队列当前积压条数 */
    public int stagingSize() {
        return writer.stagingSize();
    }

    @Override
    public void close() {
        if (closed) return;
        closed = true;

        // 关闭顺序：timer → writer（排空暂存 + sync）→ reader → Chronicle → 释放锁
        try {
            timer.close();
        } catch (Exception e) {
            log.error("error closing timer", e);
        }
        try {
            writer.close();
        } catch (Exception e) {
            log.error("error closing writer", e);
        }
        try {
            reader.close();
        } catch (Exception e) {
            log.error("error closing reader", e);
        }
        try {
            chronicleQueue.close();
        } catch (Exception e) {
            log.error("error closing ChronicleQueue", e);
        }
        releaseLock();
        log.info("spool closed, dir={}", dir());
    }

    public Path dir() {
        return dir;
    }

    private void releaseLock() {
        try {
            if (dirLock != null) dirLock.release();
        } catch (IOException ignored) {
        }
        try {
            if (lockFile != null) lockFile.close();
        } catch (IOException ignored) {
        }
    }

    // ======================== Builder ========================

    /**
     * 创建 Spool 构建器。type / dir 必填，序列化器默认 JDK 原生，可用 {@link Builder#serializer(Serializer)} 覆盖。
     */
    public static <T> Builder<T> builder(Class<T> type, String dir, Serializer<T> serializer) {
        return new Builder<>(type, Paths.get(dir), serializer);
    }

    /** 创建 Spool 构建器，序列化器默认 JDK 原生（对象需实现 {@link java.io.Serializable}） */
    public static <T> Builder<T> builder(Class<T> type, String dir) {
        return new Builder<>(type, Paths.get(dir), new JdkSerializer<T>());
    }

    public static <T> Builder<T> builder(Class<T> type, Path dir, Serializer<T> serializer) {
        return new Builder<>(type, dir, serializer);
    }

    /** 创建 Spool 构建器，序列化器默认 JDK 原生（对象需实现 {@link java.io.Serializable}） */
    public static <T> Builder<T> builder(Class<T> type, Path dir) {
        return new Builder<>(type, dir, new JdkSerializer<T>());
    }

    public static class Builder<T> {
        /** 数据类型 */
        private final Class<T> type;
        /** 队列目录 */
        private final Path dir;
        /** 序列化器，默认 JDK 原生 */
        private Serializer<T> serializer;

        /** 磁盘预算上限（字节），默认 Long.MAX_VALUE=不限 */
        private long maxSizeBytes = SpoolConfig.DEFAULT_MAX_SIZE_BYTES;
        /** 刷盘间隔（毫秒） */
        private long flushIntervalMs = SpoolConfig.DEFAULT_FLUSH_INTERVAL_MS;
        /** 文件滚动周期 */
        private RollCycle rollCycle = SpoolConfig.DEFAULT_ROLL_CYCLE;
        /** 清理旧文件间隔（毫秒），0=跟滚动周期一致 */
        private long cleanupIntervalMs = SpoolConfig.DEFAULT_CLEANUP_INTERVAL_MS;
        /** 入暂存队列超时（毫秒） */
        private long offerTimeoutMs = SpoolConfig.DEFAULT_OFFER_TIMEOUT_MS;
        /** 暂存队列容量（条数） */
        private int stagingCapacity = SpoolConfig.DEFAULT_STAGING_CAPACITY;
        /** 定时刷新目录大小的间隔（毫秒） */
        private long dirSizeRefreshIntervalMs = SpoolConfig.DEFAULT_DIR_SIZE_REFRESH_INTERVAL_MS;

        private Builder(Class<T> type, Path dir, Serializer<T> serializer) {
            this.type = Objects.requireNonNull(type, "type must not be null");
            this.dir = Objects.requireNonNull(dir, "dir must not be null");
            this.serializer = Objects.requireNonNull(serializer, "serializer must not be null");
        }

        /**
         * 指定序列化器，覆盖默认的 JDK 原生序列化。
         * <p>像 RedisTemplate 一样：不指定用默认（JDK），有特殊需求（JSON / Kryo / Protobuf）就自己传。</p>
         */
        public Builder<T> serializer(Serializer<T> serializer) {
            this.serializer = Objects.requireNonNull(serializer, "serializer must not be null");
            return this;
        }

       /**
         * 磁盘预算上限（字节）。队列目录内数据文件总占用达到或超过此值后，{@link Spool#append}
         * 拒绝入队并返回 false。默认 {@link Long#MAX_VALUE}（不限制）。
         * 注意：这是包含已消费但尚未删除的历史文件的总磁盘占用，不等于"未消费数据量"。
         * @param maxSizeBytes 字节数，如 1024L*1024*1024 = 1GB
         */
        public Builder<T> maxSizeBytes(long maxSizeBytes) {
            this.maxSizeBytes = maxSizeBytes;
            return this;
        }

        /**
         * 定时刷新目录大小的间隔（毫秒）。后台 {@link SpoolTimer} 按此周期调用
         * {@link Spool#refreshDiskUsage()}。默认 5000ms。
         */
        public Builder<T> dirSizeRefreshIntervalMs(long dirSizeRefreshIntervalMs) {
            this.dirSizeRefreshIntervalMs = dirSizeRefreshIntervalMs;
            return this;
        }

        /**
         * 刷盘间隔（毫秒）。后台 {@link SpoolTimer} 线程按此间隔调用 sync()，
         * 将 mmap 缓冲的脏页刷到磁盘。值越小断电丢失越少，但写吞吐略微下降。
         * 默认 5000ms（5 秒），断电最多丢 5 秒的已落盘但未 sync 数据。
         */
        public Builder<T> flushIntervalMs(long flushIntervalMs) {
            this.flushIntervalMs = flushIntervalMs;
            return this;
        }

        /**
         * 文件滚动周期。Chronicle-Queue 按时间切割文件，每个周期一个独立 .cq4 文件。
         * 可选值：{@link net.openhft.chronicle.queue.RollCycles#FIVE_MINUTELY}（5分钟，默认）、
         * FAST_HOURLY（1小时）、DAILY（1天）等。周期越短单文件越小，但文件数越多。
         * tailer 读完一个周期的所有数据后，Spool 会删除对应的旧文件。
         */
        public Builder<T> rollCycle(RollCycle rollCycle) {
            this.rollCycle = rollCycle;
            return this;
        }

        /**
         * 按毫秒指定文件滚动周期。内部自动构造 RollCycle 实现，用户无需关心索引参数。
         * <p>示例：{@code .rollCycleMillis(30_000)} = 30 秒滚动一个文件。<br>
         * 原理：基于 {@link TestRollCycles#TEST_SECONDLY} 包装，仅替换滚动间隔。</p>
         * @param millis 滚动间隔（毫秒），最小 1000ms。低于 1000 会自动提升到 1000。
         */
        public Builder<T> rollCycleMillis(long millis) {
            final long safe = Math.max(millis, 1000);
            this.rollCycle = new MillisRollCycle((int) safe);
            return this;
        }

        /**
         * 清理已消费旧文件的间隔（毫秒）。默认 0 = 跟滚动周期一致。
         * 后台线程定期扫目录，从最旧的文件开始检查，已消费完的删除。
         * 如果滚动周期很大（如 1 小时），建议单独设一个更短的清理间隔。
         */
        public Builder<T> cleanupIntervalMs(long cleanupIntervalMs) {
            this.cleanupIntervalMs = cleanupIntervalMs;
            return this;
        }

        /**
         * 入暂存队列超时（毫秒）。生产者 {@link Spool#append(Object)} 时，
         * 如果暂存队列已满，最多阻塞等待这么久。超时后返回 false。
         * 默认 100ms。
         */
        public Builder<T> offerTimeoutMs(long offerTimeoutMs) {
            this.offerTimeoutMs = offerTimeoutMs;
            return this;
        }

        /**
         * 暂存队列容量（条数）。生产者 append 的数据先进入此内存队列，
         * 后台单写线程串行落盘。超出此容量后 append 会阻塞等待（最多等
         * {@link #offerTimeoutMs}）。默认 10000 条。
         * <p>注意：配置时需确保 {@code stagingCapacity × 平均序列化大小 ≤ 内存预算}。
         * 进程崩溃时暂存队列中数据会丢失。</p>
         */
        public Builder<T> stagingCapacity(int stagingCapacity) {
            this.stagingCapacity = stagingCapacity;
            return this;
        }

        public Spool<T> build() {
            return new Spool<>(this);
        }

        private SpoolConfig toConfig() {
            return new SpoolConfig(dir, maxSizeBytes, flushIntervalMs,
                    rollCycle, cleanupIntervalMs, offerTimeoutMs, stagingCapacity,
                    dirSizeRefreshIntervalMs);
        }
    }

    // ======================== 内部 RollCycle 包装 ========================

    /**
     * 按毫秒间隔滚动的 RollCycle，包装 {@link TestRollCycles#TEST_SECONDLY}，仅替换滚动时长。
     * 用户通过 {@link Builder#rollCycleMillis(long)} 创建，无需手动实现接口。
     */
    private static class MillisRollCycle implements RollCycle {

        /** 委托索引/格式实现，仅替换滚动时长 */
        private final RollCycle base = TestRollCycles.TEST_SECONDLY;
        /** 滚动间隔（毫秒） */
        private final int lengthInMillis;

        MillisRollCycle(int lengthInMillis) {
            this.lengthInMillis = lengthInMillis;
        }

        @Override
        public String format() { return base.format(); }

        @Override
        public int lengthInMillis() { return lengthInMillis; }

        @Override
        public int defaultIndexCount() { return base.defaultIndexCount(); }

        @Override
        public int defaultIndexSpacing() { return base.defaultIndexSpacing(); }

        @Override
        public long toIndex(int cycle, long sequenceNumber) {
            return base.toIndex(cycle, sequenceNumber);
        }

        @Override
        public long toSequenceNumber(long index) { return base.toSequenceNumber(index); }

        @Override
        public int toCycle(long index) { return base.toCycle(index); }

        @Override
        public long maxMessagesPerCycle() { return base.maxMessagesPerCycle(); }
    }
}