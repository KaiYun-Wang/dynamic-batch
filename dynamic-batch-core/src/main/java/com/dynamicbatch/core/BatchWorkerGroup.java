package com.dynamicbatch.core;

import com.dynamicbatch.common.constants.BatchWorkerConstant;
import com.dynamicbatch.common.pojo.EnvelopePOJO;
import com.dynamicbatch.core.notifier.manager.NotifyManager;
import com.dynamicbatch.core.pojo.BatchWorkerGroupConfigPOJO;
import com.dynamicbatch.core.pojo.SpoolConfigPOJO;
import com.dynamicbatch.core.pojo.StatsConfigPOJO;
import com.dynamicbatch.core.serializer.EnvelopeSerializer;
import com.dynamicbatch.core.stats.CumulativeStats;
import com.dynamicbatch.core.stats.StatsCollector;
import com.dynamicbatch.core.stats.StatsSnapshot;
import com.dynamicbatch.core.vo.GroupSnapshotVO;
import com.dynamicbatch.spool.DiskUsage;
import com.dynamicbatch.spool.JdkSerializer;
import com.dynamicbatch.spool.Serializer;
import com.dynamicbatch.spool.Spool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;

/**
 * 批处理 Worker 组：磁盘缓冲 + 分发线程 + 分区 Worker 的编排者。
 *
 * <p>数据链路 {@code submit → Spool(磁盘) → Dispatcher → Worker 队列 → flushCallback}；
 * submit 只写 Spool 削峰，分发线程异步搬运。同一 routingKey 恒入同一分区（hashCode 取模），
 * 分区内 FIFO + 单线程消费 = 严格有序。
 *
 * <p>生命周期：启动 Spool → Workers → Dispatcher；关闭逆序——Dispatcher 先彻底停，
 * Workers 广播并行收尾（限时、到点弃权），最后关 Spool。
 *
 * <p>Spool / Dispatcher 在 {@link #start()} 构建，{@code build()} 仅做配置校验，
 * 未注册的组不持有磁盘资源；启动失败 fail fast，重启后同目录续读。
 * 组由 {@link BatchProcessor} 统一管理，业务方不直接操作分区 Worker 与分发线程。
 */
public class BatchWorkerGroup<T> {
    private static final Logger log = LoggerFactory.getLogger(BatchWorkerGroup.class);

    /** 组 key，registerGroup 时由 {@link BatchProcessor} 注入，分区名称为 "{key}-{序号}" */
    private String key;
    /** 数据类型，submit 时由分区 Worker 做运行时校验 */
    private final Class<T> type;
    private final Consumer<List<T>> flushCallback;
    private final Consumer<List<T>> failureHandler;
    /** 组共享配置：全部分区 Worker 持有同一实例 */
    private final BatchWorkerGroupConfigPOJO config;

    /**
     * 分区列表：序号即分区号，路由目标；分区数 = size()。
     * 引用易变（volatile），分区数变更采用「构建全新列表 + 单步整体替换」，
     * 读方先取局部快照再取模，永不感知中间态。
     */
    private volatile List<BatchWorker<T>> partitions = new ArrayList<>();

    private volatile boolean running;
    /** shutdown 入口置 true，resize 入口据此拒绝；与 resize 共用对象锁 */
    private volatile boolean shuttingDown;

    /** 磁盘缓冲（削峰蓄水池），start() 时按 spoolConfig 构建；未 start 的组为 null */
    private Spool<EnvelopePOJO<T>> spool;

    /** 分发线程：从 Spool poll 并路由投递到 Worker 队列，start() 时构建接线；未 start 的组为 null */
    private Dispatcher<T> dispatcher;

    /** Spool 配置（必传，编译期强制，spoolDir 必填；非泛型——载荷类型由本类 type 统一决定），start() 时据此构建 Spool */
    private final SpoolConfigPOJO spoolConfig;

    /** 统计配置，不配 = 默认实例（采集开、5s、无 tp、默认 1-2-5 桶边界） */
    private final StatsConfigPOJO statsConfig;

    /** 组级累计统计，全部分区 Worker 共享同一实例；桶边界来自 statsConfig */
    private final CumulativeStats stats;

    /** 组级统计采集任务，enabled 才在 start() 创建；未启用为 null */
    private volatile StatsCollector statsCollector;

    private BatchWorkerGroup(Builder<T> builder) {
        this.type = builder.type;
        this.flushCallback = builder.flushCallback;
        this.failureHandler = builder.failureHandler;
        this.config = builder.config;
        this.spoolConfig = builder.spoolConfig;
        this.statsConfig = builder.statsConfig != null
                ? builder.statsConfig : StatsConfigPOJO.builder().build();
        this.stats = new CumulativeStats(statsConfig.getRtBuckets() != null
                ? statsConfig.getRtBuckets() : CumulativeStats.defaultBoundaries());
        // 共享同一 config 实例：分区 Worker 不复制配置，组级公共字段经此对象读取
        List<BatchWorker<T>> initial = new ArrayList<>(builder.partitionCount);
        for (int i = 0; i < builder.partitionCount; i++) {
            initial.add(new BatchWorker<>(type, flushCallback, failureHandler, config, stats));
        }
        this.partitions = Collections.unmodifiableList(initial);
    }

    public void setKey(String key) {
        this.key = key;
    }

    public int getPartitionCount() {
        return partitions.size();
    }

    /**
     * 启动组：构建 Spool 与 Dispatcher，再启动全部分区与分发线程。
     *
     * <p>顺序定死（资源齐备才置 running）：构建 Spool → 构建 Dispatcher → 置 running
     * → 启动 Workers → 启动 Dispatcher。Spool 构建是唯一的启动期失败点（目录锁冲突 /
     * 磁盘 IO），失败时组保持未启动态、零资源残留，异常由 {@code BatchProcessor.registerGroup}
     * 清理注册表后向上传播（应用启动期 fail fast）。
     */
    public synchronized void start() {
        if (running) {
            log.warn("[{}] worker group already started", key);
            return;
        }
        // ① 构建 Spool（削峰蓄水池）：拿目录锁 + 起写线程，构造器失败路径自清理
        this.spool = buildSpool();
        // ② 构建 Dispatcher（纯内存接线，不抛）：取数 = spool::poll，投递 = 私有 submitWorker，
        //    限速每轮读共享配置（热更生效）；config 为 null 时 Dispatcher 不限流，本组必有 config；
        //    空转等待 = spool::awaitData（写线程落盘按铃、取数侧挂起听铃，微秒级唤醒免空转轮询）
        this.dispatcher = new Dispatcher<>(spool::poll, this::submitWorker, config, spool::awaitData);
        // ③ 资源齐备才置 running：此后 start 序列不再有失败点，shutdown 的 running 检查不会碰 null
        running = true;
        // ④ 启动全部分区
        for (int i = 0; i < partitions.size(); i++) {
            BatchWorker<T> worker = partitions.get(i);
            worker.setName(key + "-" + i);
            worker.start();
        }
        // ⑤ 启动分发线程：key 在 registerGroup 时才注入，线程名只能在此设置
        dispatcher.setName(key + "-dispatcher");
        dispatcher.start();
        // ⑥ 启动统计采集（enabled 才建 Collector 自挂共享调度器）
        if (statsConfig.isEnabled()) {
            StatsCollector collector = new StatsCollector(key, stats, statsConfig);
            collector.start();
            this.statsCollector = collector;
        }
        log.info("[{}] worker group started, partitions={}, queueCapacity={}, batchSize={}, spoolDir={}",
                key, partitions.size(), config.getQueueCapacity(), config.getBatchSize(), spool.dir());
    }

    /**
     * 按 spoolConfig 构建 {@code Spool<EnvelopePOJO<T>>}（信封序列化器外封 routingKey 与
     * 提交时间戳，使用方序列化器只管载荷）。非空字段才调对应 Builder 链式方法，null 走 Spool
     * 默认值，两边默认值不重复维护；泛型擦除的 unchecked 强转收敛到本方法。
     * spoolConfig 非泛型：载荷类型由本类 type 统一提供，serializer 类型错配由双重 isInstance 兜底。
     */
    @SuppressWarnings("unchecked")
    private Spool<EnvelopePOJO<T>> buildSpool() {
        Class<EnvelopePOJO<T>> entryType = (Class<EnvelopePOJO<T>>) (Class<?>) EnvelopePOJO.class;
        Serializer<T> payload = (Serializer<T>) (spoolConfig.getSerializer() != null
                ? spoolConfig.getSerializer() : new JdkSerializer<T>());
        Serializer<EnvelopePOJO<T>> envelope = new EnvelopeSerializer<>(type, payload);
        Spool.Builder<EnvelopePOJO<T>> builder = Spool.builder(entryType, spoolConfig.getSpoolDir(), envelope);
        if (spoolConfig.getMaxSizeBytes() != null) {
            builder.maxSizeBytes(spoolConfig.getMaxSizeBytes());
        }
        if (spoolConfig.getFlushIntervalMs() != null) {
            builder.flushIntervalMs(spoolConfig.getFlushIntervalMs());
        }
        if (spoolConfig.getRollCycleMillis() != null && spoolConfig.getRollCycleMillis() > 0) {
            builder.rollCycleMillis(spoolConfig.getRollCycleMillis());
        }
        if (spoolConfig.getCleanupIntervalMs() != null) {
            builder.cleanupIntervalMs(spoolConfig.getCleanupIntervalMs());
        }
        if (spoolConfig.getOfferTimeoutMs() != null) {
            builder.offerTimeoutMs(spoolConfig.getOfferTimeoutMs());
        }
        if (spoolConfig.getStagingCapacity() != null) {
            builder.stagingCapacity(spoolConfig.getStagingCapacity());
        }
        if (spoolConfig.getDirSizeRefreshIntervalMs() != null) {
            builder.dirSizeRefreshIntervalMs(spoolConfig.getDirSizeRefreshIntervalMs());
        }
        return builder.build();
    }

    /**
     * 提交一条数据到磁盘缓冲（削峰入口）：类型校验后包装为落盘条目写入 Spool，由
     * 分发线程异步路由投递到分区 Worker。本方法不做路由——路由在 {@link #submitWorker}。
     *
     * <p>同一 routingKey 的数据永远进入同一分区，分区内严格有序；不同 routingKey
     * 可能落在同一分区（哈希碰撞/分区数少），互相排队属预期行为。
     *
     * @param routingKey 业务 key（路由依据），不可为 null（null 无法取模；Processor 入口已拦截）
     * @return true 已写入磁盘缓冲；false 组未启动/已关闭、类型不匹配、Spool 拒绝
     *         （磁盘预算满或暂存队列满超时）或写入异常（如载荷不可序列化）
     */
    public boolean submit(String routingKey, T data) {
        if (!running) {
            log.warn("[{}] submit rejected, group not running", key);
            NotifyManager.getInstance().tryNoticeOfferFailedAsync(key, "组未启动或已关闭",
                    spool == null ? 0 : spool.stagingSize());
            return false;
        }
        // 类型校验（fail fast）：错误类型的数据在业务线程就拒绝，避免落盘后才暴露；
        // Worker.submit 保留同款校验，作为反序列化后数据的最后防线
        // （序列化器 bug / 升级后旧数据不兼容，命中即毒丸丢弃，见 Worker 侧注释）
        if (!type.isInstance(data)) {
            log.error("[{}] submit rejected, type mismatch: expected={}, got={}",
                    key, type.getName(), data == null ? "null" : data.getClass().getName());
            NotifyManager.getInstance().tryNoticeOfferFailedAsync(key,
                    "类型不匹配: expected=" + type.getName() + ", got="
                            + (data == null ? "null" : data.getClass().getName()),
                    spool.stagingSize());
            return false;
        }
        try {
            // 信封构造即打提交时间戳
            boolean ok = spool.append(new EnvelopePOJO<>(routingKey, data));
            if (ok) {
                stats.recordSubmit();
            } else {
                log.warn("[{}] spool append rejected, stagingSize={}", key, spool.stagingSize());
                NotifyManager.getInstance().tryNoticeOfferFailedAsync(key,
                        "spool 拒绝（磁盘预算满或暂存队列满超时）",
                        spool.stagingSize());
            }
            return ok;
        } catch (Exception e) {
            // 序列化失败（如载荷未实现 Serializable）等异常：保持 submit 不抛异常的契约
            log.error("[{}] spool append failed", key, e);
            NotifyManager.getInstance().tryNoticeOfferFailedAsync(key,
                    "spool append 异常: " + e, spool.stagingSize());
            return false;
        }
    }

    /** 分发线程投递入口：按信封 routingKey 路由到固定分区，整封阻塞投递（Dispatcher 接线 {@code this::submitWorker}） */
    boolean submitWorker(EnvelopePOJO<T> envelope) {
        List<BatchWorker<T>> snapshot = partitions;   // 局部快照：size 与 get 必然同代，替换中间态不可见
        return snapshot.get(Math.floorMod(envelope.getRoutingKey().hashCode(), snapshot.size())).submit(envelope);
    }

    /**
     * 优雅关闭：分发线程先彻底停（在途阻塞投递中断终结，Worker 收尾时队列封闭），
     * 再广播全部分区停止信号并行收尾（总时长 = max 而非 sum），限时等待、到点弃权，最后关 Spool。
     * 预算 = shutdownTimeoutMs（组级配置）。
     *
     * <p>弃权只丢内存未刷数据（error 汇总日志），磁盘已落盘数据下次启动续读。
     */
    public synchronized void shutdown() {
        Long configured = config.getShutdownTimeoutMs();
        shutdown(configured != null ? configured : BatchWorkerConstant.SHUTDOWN_WAIT_MS);
    }

    /** 指定总预算（毫秒）的关闭：deadline 在分发线程与各分区间共享，先到先得 */
    public synchronized void shutdown(long timeoutMs) {
        shuttingDown = true;
        if (!running) {
            return;
        }
        running = false;
        long deadlineNanos = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeoutMs);
        int abandoned = 0;
        // 分发线程先停：先发停止信号再限时等待；在途阻塞投递中断终结后，Worker 收尾时队列封闭，无迟到投递竞态
        if (dispatcher != null) {
            dispatcher.requestStop();
            if (!dispatcher.awaitStop(deadlineNanos)) {
                abandoned++;
            }
        }
        // 广播分区停止信号（各 Worker 退出路径自行排空刷盘），再逐个限时等待，到点弃权
        for (BatchWorker<T> worker : partitions) {
            worker.requestStop(deadlineNanos);
        }
        for (BatchWorker<T> worker : partitions) {
            if (!worker.awaitStop(deadlineNanos)) {
                abandoned++;
            }
        }
        spool.close();
        // 采集最后停，采到排空尾巴
        if (statsCollector != null) {
            statsCollector.stop();
        }
        if (abandoned == 0) {
            log.info("[{}] worker group stopped", key);
        } else {
            log.error("[{}] worker group stopped, {} consumer(s) abandoned, unflushed items dropped "
                    + "(on-disk data untouched, resumed on next start)", key, abandoned);
        }
    }

    // ======================== 调度器暂停/恢复 ========================

    /**
     * 暂停调度器：置暂停意图后立即返回，分发线程回到循环顶部即停止取数；
     * 分区 Worker 自然消化手头批次至队列清空，submit 照常写入 Spool（削峰语义保留），
     * 恢复后自动消化积压。是否已暂停见 {@link #getDispatcherPhase()}。
     *
     * @throws IllegalStateException 组未启动
     */
    synchronized void pauseDispatcher() {
        requireRunning();
        dispatcher.requestPause();
    }

    /**
     * 恢复调度器：置运行意图后立即返回，分发线程自行翻回运行态续投。
     *
     * @throws IllegalStateException 组未启动
     */
    synchronized void resumeDispatcher() {
        requireRunning();
        dispatcher.requestRun();
    }

    /** 调度器当前运行状态：RUNNING / PAUSE_PENDING / PAUSED / RUN_PENDING；组未启动返回 null */
    Dispatcher.PausePhase getDispatcherPhase() {
        return dispatcher == null ? null : dispatcher.getPhase();
    }

    /** 组已启动校验 */
    private void requireRunning() {
        if (!running) {
            throw new IllegalStateException("[" + key + "] group not started");
        }
    }

    // ======================== 磁盘占用查询 ========================

    /**
     * 组 Spool 磁盘占用即时拆分（见 {@link DiskUsage}）。查询不抛异常：
     * 未启动 / 关闭中返回 null 不触碰 Spool，异常也兜底返回 null。
     */
    DiskUsage getSpoolUsage() {
        if (!running || spool == null) {
            return null;
        }
        try {
            return spool.diskUsage();
        } catch (Exception e) {
            log.warn("[{}] spool usage query failed", key, e);
            return null;
        }
    }

    // ======================== 运行快照 ========================

    /**
     * 组运行快照：配置、分区、相位、队列水位与磁盘占用（字段语义见 {@link GroupSnapshotVO}）。
     * 各字段独立读取，未启动组相位与磁盘字段为 null。
     */
    GroupSnapshotVO snapshot() {
        Dispatcher.PausePhase phase = getDispatcherPhase();
        return new GroupSnapshotVO(key, running, config.getQueueCapacity(), config.getBatchSize(),
                config.getMaxWaitMs(), config.getRateLimitPerSecond(), partitions.size(),
                phase == null ? null : phase.name(),
                getPartitionQueueSizes(), getStagingSize(), getSpoolUsage(), statsVo());
    }

    /** 统计 live 快照（现场差分，只读不推进）；未启用采集 / 组未启动为 null */
    private StatsSnapshot statsVo() {
        StatsCollector collector = this.statsCollector;
        return collector == null ? null : collector.liveSnapshot();
    }

    /** 各分区内存队列当前水位，下标即分区号 */
    private List<Integer> getPartitionQueueSizes() {
        List<BatchWorker<T>> snapshot = partitions;
        List<Integer> sizes = new ArrayList<>(snapshot.size());
        for (BatchWorker<T> worker : snapshot) {
            sizes.add(worker.getQueueSize());
        }
        return sizes;
    }

    /** Spool 暂存积压条数；未启动或统计失败返回 null */
    private Integer getStagingSize() {
        if (!running || spool == null) {
            return null;
        }
        try {
            return spool.stagingSize();
        } catch (Exception e) {
            log.warn("[{}] spool staging size query failed", key, e);
            return null;
        }
    }

    // ======================== 热更新：分区数 ========================

    /**
     * 改变分区数：阻塞等待全员排空后，以「构建全新分区组 + 单步原子替换」完成拓扑变更。
     * 前置要求调度器已暂停（未暂停直接拒绝）；排空即所有 Worker 空闲且队列清空，
     * 调度器已暂停后不再有新投递，轮询必然收敛。
     * 失败时未动分区、组保持暂停态，可直接重试。
     *
     * @param newSize   目标分区数，必须 &gt;= 1
     * @param timeoutMs 排空等待的超时预算（毫秒）
     * @throws IllegalStateException    组未启动、正在 shutdown，或调度器未暂停
     * @throws IllegalArgumentException newSize &lt; 1
     * @throws TimeoutException         预算内未排空（未动分区，组保持暂停态）
     */
    synchronized void resizePartitions(int newSize, long timeoutMs) throws TimeoutException {
        if (shuttingDown) {
            throw new IllegalStateException("[" + key + "] resize rejected, group is shutting down");
        }
        requireRunning();
        if (newSize < 1) {
            throw new IllegalArgumentException("newSize must be >= 1, got " + newSize);
        }
        List<BatchWorker<T>> oldPartitions = this.partitions;
        if (dispatcher.getPhase() != Dispatcher.PausePhase.PAUSED) {
            throw new IllegalStateException("[" + key
                    + "] dispatcher not paused, call pauseGroup before resizePartitions");
        }
        if (newSize == oldPartitions.size()) {
            return;   // 幂等
        }
        awaitAllDrained(oldPartitions, System.currentTimeMillis() + timeoutMs);

        // 静止点之后变更不可失败：全新分区组启动后单步替换，旧组关闭不参与失败路径
        List<BatchWorker<T>> replacement = new ArrayList<>(newSize);
        for (int i = 0; i < newSize; i++) {
            BatchWorker<T> worker = new BatchWorker<>(type, flushCallback, failureHandler, config, stats);
            worker.setName(key + "-" + i);
            worker.start();
            replacement.add(worker);
        }
        this.partitions = Collections.unmodifiableList(replacement);
        for (BatchWorker<T> worker : oldPartitions) {
            worker.requestStop();
        }
        for (BatchWorker<T> worker : oldPartitions) {
            worker.awaitStop();
        }
        log.info("[{}] partitions resized {} -> {}", key, oldPartitions.size(), newSize);
    }

    /**
     * 阻塞等待所有 Worker 空闲且队列清空：调度器已暂停后不再有新投递，
     * idle 一旦为 true 即保持，轮询必然收敛；超时抛出且未动任何状态。
     */
    private void awaitAllDrained(List<BatchWorker<T>> workers, long deadline) throws TimeoutException {
        while (true) {
            boolean allIdle = true;
            for (BatchWorker<T> worker : workers) {
                if (!worker.isIdle() || worker.getQueueSize() > 0) {
                    allIdle = false;
                    break;
                }
            }
            if (allIdle) {
                return;
            }
            if (System.currentTimeMillis() > deadline) {
                throw new TimeoutException("[" + key + "] drain wait timed out, partitions=" + workers.size());
            }
            try {
                Thread.sleep(BatchWorkerConstant.PAUSE_POLL_INTERVAL_MS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new TimeoutException("[" + key + "] drain wait interrupted");
            }
        }
    }

    // ======================== 热更新：配置参数 ========================

    /** 当前攒批条数（组共享配置） */
    int getBatchSize() {
        return config.getBatchSize();
    }

    /** 当前最大等待毫秒（组共享配置） */
    long getMaxWaitMs() {
        return config.getMaxWaitMs();
    }

    /** 当前内存队列容量（组共享配置） */
    int getQueueCapacity() {
        return config.getQueueCapacity();
    }

    /** 当前组级投递限速（条/秒，组共享配置；null = 未配置即不限流） */
    Integer getRateLimitPerSecond() {
        return config.getRateLimitPerSecond();
    }

    /**
     * 运行时调整组配置：batchSize / maxWaitMs / rateLimitPerSecond，null = 不改。
     * 合并后整体校验，任一非法则配置不变；消费/分发线程每轮读共享配置，
     * 下一批 / 下一投递起自动生效。
     *
     * @throws IllegalStateException    组未启动
     * @throws IllegalArgumentException batchSize &lt;= 0、&gt; queueCapacity，maxWaitMs &lt; 0，或 rateLimitPerSecond &lt;= 0
     */
    synchronized void resizeGroupConfig(Integer batchSize, Long maxWaitMs, Integer rateLimitPerSecond) {
        requireRunning();
        int newBatchSize = batchSize != null ? batchSize : config.getBatchSize();
        long newMaxWaitMs = maxWaitMs != null ? maxWaitMs : config.getMaxWaitMs();
        Integer newRateLimit = rateLimitPerSecond != null ? rateLimitPerSecond : config.getRateLimitPerSecond();
        if (newBatchSize <= 0) {
            throw new IllegalArgumentException("batchSize must be > 0, got " + newBatchSize);
        }
        if (newBatchSize > config.getQueueCapacity()) {
            throw new IllegalArgumentException("batchSize must be <= queueCapacity, got batchSize:"
                    + newBatchSize + " > queueCapacity:" + config.getQueueCapacity());
        }
        if (newMaxWaitMs < 0) {
            throw new IllegalArgumentException("maxWaitMs must be >= 0, got " + newMaxWaitMs);
        }
        if (newRateLimit != null && newRateLimit <= 0) {
            throw new IllegalArgumentException("rateLimit must be > 0, got " + newRateLimit);
        }
        config.setBatchSize(newBatchSize);
        config.setMaxWaitMs(newMaxWaitMs);
        config.setRateLimitPerSecond(newRateLimit);
        log.info("[{}] batch config updated, batchSize={}, maxWaitMs={}, rateLimitPerSecond={}",
                key, newBatchSize, newMaxWaitMs, newRateLimit);
    }

    // ======================== Builder ========================

    /**
     * 创建 {@code BatchWorkerGroup} 构建器，三个必传参数在编译期强制：
     * {@code type} / {@code flushCallback} / {@code spoolConfig}；其余参数链式可选。
     *
     * <p>用法：
     * <pre>{@code
     * BatchWorkerGroup<MyData> group = BatchWorkerGroup.builder(MyData.class,
     *                 SpoolConfigPOJO.builder("/data/batch/my-group").build(),
     *                 records -> insertBatch(records))
     *         .partitionCount(4)
     *         .batchSize(100)
     *         .build();
     * }</pre>
     */
    public static <T> Builder<T> builder(Class<T> type, SpoolConfigPOJO spoolConfig,
                                         Consumer<List<T>> flushCallback) {
        return new Builder<>(type, spoolConfig, flushCallback);
    }

    /**
     * {@code BatchWorkerGroup} 构建器。
     *
     * <p>通过 {@link #builder(Class, SpoolConfigPOJO, Consumer)} 创建，三个必传参数
     * （type / flushCallback / spoolConfig）在构造时传入保证编译期强制，其余参数有默认值
     * （见 {@link BatchWorkerConstant}），可按需覆盖。链式方法写入共享
     * {@link BatchWorkerGroupConfigPOJO}，build() 后全部分区 Worker 共享。
     */
    public static class Builder<T> {

        /** 数据类型，submit 时做运行时校验，必填 */
        private final Class<T> type;
        /** 刷盘回调，攒满一批或超时触发时调用，必填 */
        private final Consumer<List<T>> flushCallback;
        /** 分区数：每分区一个队列 + 单线程消费者，并行度与有序性的平衡点 */
        private int partitionCount = BatchWorkerConstant.DEFAULT_PARTITION_COUNT;
        /** 组共享配置，builder 填默认值，链式方法覆盖 */
        private final BatchWorkerGroupConfigPOJO config = new BatchWorkerGroupConfigPOJO();
        /** 失败回调，flush 抛异常时调用，可为 null（此时只打 error 日志） */
        private Consumer<List<T>> failureHandler;
        /** Spool 配置（必传，编译期强制，spoolDir 必填），start() 时由框架内部构建 Spool */
        private final SpoolConfigPOJO spoolConfig;
        /** 统计配置，null = 默认实例 */
        private StatsConfigPOJO statsConfig;

        private Builder(Class<T> type, SpoolConfigPOJO spoolConfig, Consumer<List<T>> flushCallback) {
            this.type = Objects.requireNonNull(type, "type must not be null");
            this.spoolConfig = Objects.requireNonNull(spoolConfig, "spoolConfig must not be null");
            this.flushCallback = Objects.requireNonNull(flushCallback, "flushCallback must not be null");
            config.setQueueCapacity(BatchWorkerConstant.DEFAULT_QUEUE_CAPACITY);
            config.setBatchSize(BatchWorkerConstant.DEFAULT_BATCH_SIZE);
            config.setMaxWaitMs(BatchWorkerConstant.DEFAULT_MAX_WAIT_MS);
            config.setShutdownTimeoutMs(BatchWorkerConstant.SHUTDOWN_WAIT_MS);
        }

        public Builder<T> partitionCount(int partitionCount) {
            this.partitionCount = partitionCount;
            return this;
        }

        public Builder<T> queueCapacity(int queueCapacity) {
            config.setQueueCapacity(queueCapacity);
            return this;
        }

        public Builder<T> batchSize(int batchSize) {
            config.setBatchSize(batchSize);
            return this;
        }

        public Builder<T> maxWaitMs(long maxWaitMs) {
            config.setMaxWaitMs(maxWaitMs);
            return this;
        }

        /**
         * 组级投递限速（条/秒）：分发线程取数前睡足与上次取数的间隔，空闲期不积累突发；
         * 运行时可用 {@code resizeGroupConfig} 热更。
         *
         * @param rateLimitPerSecond 每秒最多取数条数，必须 &gt; 0；不配置 = 不限流
         */
        public Builder<T> rateLimit(int rateLimitPerSecond) {
            config.setRateLimitPerSecond(rateLimitPerSecond);
            return this;
        }

        /**
         * 关闭总预算（毫秒）：从广播停止信号到弃权卡死分区的硬上限；
         * 预算内未收尾完成的分区被弃权，未刷的内存数据丢弃（磁盘已落盘数据不受影响）。
         * 不配 = 默认 {@code SHUTDOWN_WAIT_MS}。
         */
        public Builder<T> shutdownTimeoutMs(long shutdownTimeoutMs) {
            config.setShutdownTimeoutMs(shutdownTimeoutMs);
            return this;
        }

        public Builder<T> failureHandler(Consumer<List<T>> failureHandler) {
            this.failureHandler = failureHandler;
            return this;
        }

        /**
         * 统计配置（enabled / collectIntervalMillis / percentiles / rtBuckets），
         * 不配 = 默认实例。
         */
        public Builder<T> statsConfig(StatsConfigPOJO statsConfig) {
            this.statsConfig = statsConfig;
            return this;
        }

        /**
         * 构建 {@code BatchWorkerGroup} 实例：纯配置校验 + 纯内存组装（Spool 与 Dispatcher
         * 在 {@link BatchWorkerGroup#start()} 期构建，未注册的组不持有磁盘资源）。
         * spoolConfig 与 spoolDir 已由 {@link SpoolConfigPOJO#builder} 编译期/构造期强制，
         * 此处不再重复校验；校验失败抛 {@link IllegalArgumentException}（启动期 fail fast）。
         */
        public BatchWorkerGroup<T> build() {
            if (partitionCount <= 0) {
                throw new IllegalArgumentException("partitionCount must be > 0, got " + partitionCount);
            }
            if (config.getRateLimitPerSecond() != null && config.getRateLimitPerSecond() <= 0) {
                throw new IllegalArgumentException("rateLimit must be > 0, got " + config.getRateLimitPerSecond());
            }
            // L1：默认 JDK 序列化要求载荷可序列化，把失败从第一次 append 提前到启动期
            if (spoolConfig.getSerializer() == null && !Serializable.class.isAssignableFrom(type)) {
                throw new IllegalArgumentException(
                        "payload type " + type.getName()
                                + " is not Serializable: default JDK serializer requires it;"
                                + " implement Serializable or provide a custom serializer via SpoolConfigPOJO.serializer");
            }
            return new BatchWorkerGroup<>(this);
        }
    }
}
