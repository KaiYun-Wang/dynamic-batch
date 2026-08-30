package com.dynamicbatch.spool;

import java.nio.file.Path;

import net.openhft.chronicle.queue.RollCycle;
import net.openhft.chronicle.queue.RollCycles;

/**
 * Spool 配置参数。由 {@link Spool.Builder} 填充，构建后对 Writer / Reader 只读。
 */
class SpoolConfig {

    /** 默认滚动周期：5 分钟一个文件 */
    static final RollCycle DEFAULT_ROLL_CYCLE = RollCycles.FIVE_MINUTELY;
    /** 默认刷盘间隔：5000ms（5 秒），断电最多丢 5 秒数据 */
    static final long DEFAULT_FLUSH_INTERVAL_MS = 5000;
    /** 默认入队超时：100ms，暂存队列满时最多等这么久 */
    static final long DEFAULT_OFFER_TIMEOUT_MS = 100;
    /** 默认暂存队列容量：10000 条 */
    static final int DEFAULT_STAGING_CAPACITY = 10000;
    /** 默认磁盘预算上限：Long.MAX_VALUE = 不限制 */
    static final long DEFAULT_MAX_SIZE_BYTES = Long.MAX_VALUE;
    /** 默认目录大小刷新间隔：5000ms */
    static final long DEFAULT_DIR_SIZE_REFRESH_INTERVAL_MS = 5000;
    /** 默认清理间隔：0 = 默认跟滚动周期一致，用户可单独指定 */
    static final long DEFAULT_CLEANUP_INTERVAL_MS = 0;

    /** 队列目录路径 */
    final Path dir;
    /** 磁盘预算上限（字节），默认 Long.MAX_VALUE=不限。超限后 append 拒绝 */
    final long maxSizeBytes;
    /** 刷盘间隔（毫秒），后台线程定期调 sync 把 mmap 脏页刷到磁盘 */
    final long flushIntervalMs;
    /** 文件滚动周期，默认 FIVE_MINUTELY（5 分钟一个文件） */
    final RollCycle rollCycle;
    /** 清理旧文件间隔（毫秒），0=跟滚动周期一致，由 timer 定期触发 */
    final long cleanupIntervalMs;
    /** 入暂存队列超时（毫秒），队列满时生产者最多等多久 */
    final long offerTimeoutMs;
    /** 暂存队列容量（条数），超过此值 append 会阻塞等待 */
    final int stagingCapacity;
    /** 定时刷新目录大小的间隔（毫秒） */
    final long dirSizeRefreshIntervalMs;

    SpoolConfig(Path dir, long maxSizeBytes, long flushIntervalMs,
                RollCycle rollCycle, long cleanupIntervalMs,
                long offerTimeoutMs, int stagingCapacity,
                long dirSizeRefreshIntervalMs) {
        this.dir = dir;
        this.maxSizeBytes = maxSizeBytes;
        this.flushIntervalMs = flushIntervalMs;
        this.rollCycle = rollCycle;
        this.cleanupIntervalMs = cleanupIntervalMs;
        this.offerTimeoutMs = offerTimeoutMs;
        this.stagingCapacity = stagingCapacity;
        this.dirSizeRefreshIntervalMs = dirSizeRefreshIntervalMs;
    }
}
