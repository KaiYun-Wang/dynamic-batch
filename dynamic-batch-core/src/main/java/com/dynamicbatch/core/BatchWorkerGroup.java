package com.dynamicbatch.core;

import com.dynamicbatch.common.constants.BatchWorkerConstant;
import com.dynamicbatch.common.enums.PausePhase;
import com.dynamicbatch.common.pojo.BatchWorkerGroupConfigPOJO;
import com.dynamicbatch.common.pojo.SpoolEntryPOJO;
import com.dynamicbatch.core.notifier.manager.NotifyManager;
import com.dynamicbatch.core.pojo.SpoolConfigPOJO;
import com.dynamicbatch.core.serializer.SpoolEntrySerializer;
import com.dynamicbatch.spool.JdkSerializer;
import com.dynamicbatch.spool.Serializer;
import com.dynamicbatch.spool.Spool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;
import java.util.function.Supplier;

/**
 * 批处理 Worker 组：磁盘缓冲 + 分发线程 + 分区 Worker 组的编排者。
 *
 * <p>数据链路：{@code submit → Spool(磁盘) → Dispatcher → Worker 内存队列 → flushCallback}。
 * submit 仅将数据写入 Spool 削峰，由分发线程异步搬运并按 routingKey 路由；
 * 路由规则：{@code routingKey.hashCode() % 分区数} 取模——同一 routingKey 永远进入
 * 同一分区，分区内 FIFO + 单线程消费 = 严格有序。
 *
 * <p>生命周期（启动顺序 Spool → Workers → Dispatcher，关闭逆序逐个彻底关闭）：
 * Spool 与 Dispatcher 在 {@link #start()}（即 {@link BatchProcessor#registerGroup} 期）构建，
 * {@code build()} 仅做配置校验——未注册的组不持有任何磁盘资源；构建失败异常传播阻断启动
 * （fail fast），进程重启后同目录重建 Spool 从持久化读进度续读。
 *
 * <p>组内全部分区共享同一份 {@link BatchWorkerGroupConfigPOJO} 实例：分区 Worker 不持有
 * 独立配置，全部读取该共享对象（构建期锁定）。组由 {@link BatchProcessor} 统一管理，
 * 业务方不直接操作分区 Worker 与分发线程。
 */
public class BatchWorkerGroup<T> {
    private static final Logger log = LoggerFactory.getLogger(BatchWorkerGroup.class);

    /** 组 key，registerGroup 时由 {@link BatchProcessor} 注入，分区名称为 "{key}-{序号}" */
    private String key;
    /** 数据类型，submit 时由分区 Worker 做运行时校验 */
    private final Class<T> type;
    private final int partitionCount;
    /** 组共享配置：全部分区 Worker 持有同一实例 */
    private final BatchWorkerGroupConfigPOJO config;

    /** 分区列表：序号即分区号，路由目标 */
    private final List<BatchWorker<T>> partitions = new ArrayList<>();

    private volatile boolean running;

    /** 磁盘缓冲（削峰蓄水池），start() 时按 spoolConfig 构建；未 start 的组为 null */
    private Spool<SpoolEntryPOJO<T>> spool;

    /** 分发线程：从 Spool poll 并路由投递到 Worker 队列，start() 时构建接线；未 start 的组为 null */
    private Dispatcher<T> dispatcher;

    /** Spool 配置（必传，编译期强制，spoolDir 必填；非泛型——载荷类型由本类 type 统一决定），start() 时据此构建 Spool */
    private final SpoolConfigPOJO spoolConfig;

    private BatchWorkerGroup(Builder<T> builder) {
        this.type = builder.type;
        this.partitionCount = builder.partitionCount;
        this.config = builder.config;
        this.spoolConfig = builder.spoolConfig;
        // 共享同一 config 实例：分区 Worker 不复制配置，组级公共字段经此对象读取
        for (int i = 0; i < partitionCount; i++) {
            partitions.add(new BatchWorker<>(type, builder.flushCallback, builder.failureHandler, config));
        }
    }

    public void setKey(String key) {
        this.key = key;
    }

    public int getPartitionCount() {
        return partitionCount;
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
        // ② 构建 Dispatcher（纯内存接线，不抛）：取数 = spool::poll，投递 = 私有 submitWorker
        this.dispatcher = new Dispatcher<>(spool::poll, this::submitWorker);
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
        log.info("[{}] worker group started, partitions={}, queueCapacity={}, batchSize={}, spoolDir={}",
                key, partitionCount, config.getQueueCapacity(), config.getBatchSize(), spool.dir());
    }

    /**
     * 按 spoolConfig 构建 {@code Spool<SpoolEntryPOJO<T>>}（信封序列化器外封 routingKey，
     * 使用方序列化器只管载荷）。非空字段才调对应 Builder 链式方法，null 走 Spool 默认值，
     * 两边默认值不重复维护；泛型擦除的 unchecked 强转收敛到本方法。
     * spoolConfig 非泛型：载荷类型由本类 type 统一提供，serializer 类型错配由双重 isInstance 兜底。
     */
    @SuppressWarnings("unchecked")
    private Spool<SpoolEntryPOJO<T>> buildSpool() {
        Class<SpoolEntryPOJO<T>> entryType = (Class<SpoolEntryPOJO<T>>) (Class<?>) SpoolEntryPOJO.class;
        Serializer<T> payload = (Serializer<T>) (spoolConfig.getSerializer() != null
                ? spoolConfig.getSerializer() : new JdkSerializer<T>());
        Serializer<SpoolEntryPOJO<T>> envelope = new SpoolEntrySerializer<>(type, payload);
        Spool.Builder<SpoolEntryPOJO<T>> builder = Spool.builder(entryType, spoolConfig.getSpoolDir(), envelope);
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
                    config.getOfferTimeoutMs(), spool == null ? 0 : spool.stagingSize());
            return false;
        }
        // 类型校验（fail fast）：错误类型的数据在业务线程就拒绝，避免落盘后才暴露；
        // Worker.submit 保留同款校验，作为反序列化后数据的最后防线（序列化器 bug 场景）
        if (!type.isInstance(data)) {
            log.error("[{}] submit rejected, type mismatch: expected={}, got={}",
                    key, type.getName(), data == null ? "null" : data.getClass().getName());
            NotifyManager.getInstance().tryNoticeOfferFailedAsync(key,
                    "类型不匹配: expected=" + type.getName() + ", got="
                            + (data == null ? "null" : data.getClass().getName()),
                    config.getOfferTimeoutMs(), spool.stagingSize());
            return false;
        }
        try {
            boolean ok = spool.append(new SpoolEntryPOJO<>(routingKey, data));
            if (!ok) {
                log.warn("[{}] spool append rejected, stagingSize={}", key, spool.stagingSize());
                NotifyManager.getInstance().tryNoticeOfferFailedAsync(key,
                        "spool 拒绝（磁盘预算满或暂存队列满超时）",
                        config.getOfferTimeoutMs(), spool.stagingSize());
            }
            return ok;
        } catch (Exception e) {
            // 序列化失败（如载荷未实现 Serializable）等异常：保持 submit 不抛异常的契约
            log.error("[{}] spool append failed", key, e);
            NotifyManager.getInstance().tryNoticeOfferFailedAsync(key,
                    "spool append 异常: " + e, config.getOfferTimeoutMs(), spool.stagingSize());
            return false;
        }
    }

    /** 分发线程投递入口：按 routingKey 路由到固定分区并入队（Dispatcher 接线 {@code this::submitWorker}） */
    boolean submitWorker(String routingKey, T data) {
        return partitions.get(partitionIndex(routingKey)).submit(data);
    }

    /** 路由：hash 取模；routingKey 不可为 null（Processor 入口 requireNonNull + 落盘条目构造器 fail fast 兜底） */
    private int partitionIndex(String routingKey) {
        return Math.floorMod(routingKey.hashCode(), partitionCount);
    }

    /**
     * 优雅关闭：先停分发线程（已 poll 条目投递成功才退），再关闭全部分区
     * （每个分区：等消费线程自然退出 → 超时强制中断 → 排空剩余数据刷盘），最后关 Spool。
     *
     * <p>关闭顺序与启动相反（Dispatcher → Workers → Spool），下一环等上一环彻底关闭：
     * Dispatcher 停止后 Worker 仍接收投递（在途条目不丢）；Spool close 排空暂存落盘并
     * 释放目录锁。磁盘上未消费的数据不删除，下次同目录启动续读。
     * 未启动的组无任何资源（spool/dispatcher 均为 null），直接返回。
     */
    public synchronized void shutdown() {
        if (!running) {
            return;
        }
        running = false;
        // 关闭顺序与启动相反（Dispatcher → Workers → Spool）：先停分发线程（已 poll 条目
        // 投递成功才退，此时 Worker 仍在接收），再关分区（等自然退出 → 超时中断 → flushRemaining），
        // 最后关 Spool（排空暂存落盘 + 释放目录锁）；漏掉任一环都会导致 Dispatcher 泄漏重试或锁不释放
        dispatcher.stop();
        for (BatchWorker<T> worker : partitions) {
            worker.shutdown();
        }
        spool.close();
        log.info("[{}] worker group stopped", key);
    }

    // ======================== 暂停/恢复 ========================

    /**
     * 暂停整组：分发线程停止取数，各 Worker 排空手头批次与队列残留后待命。
     * 顺序：先分发线程后 Worker；整段共用超时预算，超时自动回滚（整组保持消费）后抛出。
     * 暂停期间 submit 照常写入 Spool，恢复后自动消化积压。
     *
     * @param timeoutMs 整段超时预算（毫秒，含等确认与排空）
     * @throws IllegalStateException 组未启动
     * @throws TimeoutException      预算内未完成（已自动回滚）
     */
    void pauseAll(long timeoutMs) throws TimeoutException {
        if (!running) {
            throw new IllegalStateException("[" + key + "] group not started");
        }
        long deadline = System.currentTimeMillis() + timeoutMs;
        try {
            // ① 暂停分发线程并等待确认
            dispatcher.requestPause();
            awaitPhase(dispatcher::getPausePhase, PausePhase.PAUSED, deadline, "dispatcher");
            // ② 暂停全部 Worker 并等待确认
            for (BatchWorker<T> worker : partitions) {
                worker.requestPause();
            }
            int index = 0;
            for (BatchWorker<T> worker : partitions) {
                awaitPhase(worker::getPausePhase, PausePhase.PAUSED, deadline, "worker-" + index++);
            }
            // ③ 逐个排空队列残留
            index = 0;
            for (BatchWorker<T> worker : partitions) {
                long remaining = deadline - System.currentTimeMillis();
                if (remaining <= 0) {
                    throw new TimeoutException("[" + key + "] pause budget exhausted before drain worker-" + index);
                }
                worker.flushRemainingWithTimeout(remaining);
                index++;
            }
        } catch (TimeoutException e) {
            // 超时回滚：整组置「请恢复」，保持消费
            dispatcher.requestRun();
            for (BatchWorker<T> worker : partitions) {
                worker.requestRun();
            }
            log.warn("[{}] pauseAll timed out, rolled back to running", key, e);
            throw e;
        }
        log.info("[{}] worker group paused, partitions={}", key, partitions.size());
    }

    /**
     * 恢复整组：各 Worker 恢复运行后分发线程续投。幂等可重试；超时不回滚，可直接重试。
     *
     * @param timeoutMs 整段超时预算（毫秒）
     * @throws IllegalStateException 组未启动
     * @throws TimeoutException      预算内未全部恢复
     */
    void resumeAll(long timeoutMs) throws TimeoutException {
        if (!running) {
            throw new IllegalStateException("[" + key + "] group not started");
        }
        long deadline = System.currentTimeMillis() + timeoutMs;
        // ① 全部 Worker 置待运行（先全部置、再逐个等）→ 等全部 RUNNING
        for (BatchWorker<T> worker : partitions) {
            worker.requestRun();
        }
        int index = 0;
        for (BatchWorker<T> worker : partitions) {
            awaitPhase(worker::getPausePhase, PausePhase.RUNNING, deadline, "worker-" + index++);
        }
        // ② 分发线程置待运行 → 等 RUNNING
        dispatcher.requestRun();
        awaitPhase(dispatcher::getPausePhase, PausePhase.RUNNING, deadline, "dispatcher");
        log.info("[{}] worker group resumed", key);
    }

    /** 短轮询等待相位翻到期望值，超时抛 {@link TimeoutException} */
    private static void awaitPhase(Supplier<PausePhase> phase, PausePhase expected,
                                   long deadline, String target) throws TimeoutException {
        while (phase.get() != expected) {
            if (System.currentTimeMillis() > deadline) {
                throw new TimeoutException("wait phase " + expected + " timed out, target=" + target);
            }
            try {
                Thread.sleep(BatchWorkerConstant.PAUSE_POLL_INTERVAL_MS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new TimeoutException("wait phase " + expected + " interrupted, target=" + target);
            }
        }
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

        private Builder(Class<T> type, SpoolConfigPOJO spoolConfig, Consumer<List<T>> flushCallback) {
            this.type = Objects.requireNonNull(type, "type must not be null");
            this.spoolConfig = Objects.requireNonNull(spoolConfig, "spoolConfig must not be null");
            this.flushCallback = Objects.requireNonNull(flushCallback, "flushCallback must not be null");
            config.setQueueCapacity(BatchWorkerConstant.DEFAULT_QUEUE_CAPACITY);
            config.setBatchSize(BatchWorkerConstant.DEFAULT_BATCH_SIZE);
            config.setMaxWaitMs(BatchWorkerConstant.DEFAULT_MAX_WAIT_MS);
            config.setOfferTimeoutMs(BatchWorkerConstant.DEFAULT_OFFER_TIMEOUT_MS);
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

        public Builder<T> offerTimeoutMs(long offerTimeoutMs) {
            config.setOfferTimeoutMs(offerTimeoutMs);
            return this;
        }

        public Builder<T> failureHandler(Consumer<List<T>> failureHandler) {
            this.failureHandler = failureHandler;
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
