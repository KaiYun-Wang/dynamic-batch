package com.dynamicbatch.core;

import com.dynamicbatch.common.constants.BatchWorkerConstant;
import com.dynamicbatch.common.pojo.BatchWorkerGroupConfigPOJO;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.function.Consumer;

/**
 * 批处理 Worker 组：一组分区 Worker，每个分区一个内存队列 + 单线程消费者。
 *
 * <p>路由：submit 携带业务 key（routingKey），按 {@code routingKey.hashCode() % 分区数} 取模
 * 路由到固定分区——同一 routingKey 永远进入同一分区，分区内 FIFO + 单线程消费 = 严格有序。
 * 并行度由分区数提供，顺序由分区保证（替代旧版「多消费线程竞争同一队列」的乱序模式）。
 *
 * <p>组内全部分区共享同一份 {@link BatchWorkerGroupConfigPOJO} 实例：分区 Worker 不持有
 * 独立配置，全部读取该共享对象（构建期锁定）。组由 {@link BatchProcessor} 统一管理，
 * 业务方不直接操作分区 Worker。
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

    private BatchWorkerGroup(Builder<T> builder) {
        this.type = builder.type;
        this.partitionCount = builder.partitionCount;
        this.config = builder.config;
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
     * 启动全部分区：设置分区名并启动各自消费线程。
     */
    public synchronized void start() {
        if (running) {
            log.warn("[{}] worker group already started", key);
            return;
        }
        running = true;
        for (int i = 0; i < partitions.size(); i++) {
            BatchWorker<T> worker = partitions.get(i);
            worker.setName(key + "-" + i);
            worker.start();
        }
        log.info("[{}] worker group started, partitions={}, queueCapacity={}, batchSize={}",
                key, partitionCount, config.getQueueCapacity(), config.getBatchSize());
    }

    /**
     * 提交一条数据，按 routingKey 路由到固定分区。
     *
     * <p>同一 routingKey 的数据永远进入同一分区，分区内严格有序；不同 routingKey
     * 可能落在同一分区（哈希碰撞/分区数少），互相排队属预期行为。
     *
     * @param routingKey 业务 key（路由依据），null 时固定路由到分区 0
     * @return true 入队成功；false 队列满且超时（或组已关闭）
     */
    public boolean submit(String routingKey, T data) {
        return partitions.get(partitionIndex(routingKey)).submit(data);
    }

    /** 路由：hash 取模；routingKey 为 null 时固定分区 0（由调用方保证不常传 null） */
    private int partitionIndex(String routingKey) {
        if (routingKey == null) {
            return 0;
        }
        return Math.floorMod(routingKey.hashCode(), partitionCount);
    }

    /**
     * 优雅关闭全部分区（每个分区：等消费线程自然退出 → 超时强制中断 → 排空剩余数据刷盘）。
     */
    public synchronized void shutdown() {
        if (!running) {
            return;
        }
        running = false;
        for (BatchWorker<T> worker : partitions) {
            worker.shutdown();
        }
        log.info("[{}] worker group stopped", key);
    }

    // ======================== Builder ========================

    /**
     * 创建 {@code BatchWorkerGroup} 构建器，{@code type} 与 {@code flushCallback} 为必填参数。
     *
     * <p>用法：
     * <pre>{@code
     * BatchWorkerGroup<MyData> group = BatchWorkerGroup.builder(MyData.class, records -> insertBatch(records))
     *         .partitionCount(4)
     *         .batchSize(100)
     *         .build();
     * }</pre>
     */
    public static <T> Builder<T> builder(Class<T> type, Consumer<List<T>> flushCallback) {
        return new Builder<>(type, flushCallback);
    }

    /**
     * {@code BatchWorkerGroup} 构建器。
     *
     * <p>通过 {@link #builder(Class, Consumer)} 创建，{@code type} 与 {@code flushCallback}
     * 在构造时传入保证必填，其余参数有默认值（见 {@link BatchWorkerConstant}），可按需覆盖。
     * 链式方法写入共享 {@link BatchWorkerGroupConfigPOJO}，build() 后全部分区 Worker 共享。
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

        private Builder(Class<T> type, Consumer<List<T>> flushCallback) {
            this.type = Objects.requireNonNull(type, "type must not be null");
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
         * 构建 {@code BatchWorkerGroup} 实例：校验分区数，并按共享配置逐个构建分区 Worker
         * （Worker 构造器统一校验 batchSize <= queueCapacity 等约束）。
         */
        public BatchWorkerGroup<T> build() {
            if (partitionCount <= 0) {
                throw new IllegalArgumentException("partitionCount must be > 0, got " + partitionCount);
            }
            return new BatchWorkerGroup<>(this);
        }
    }
}
