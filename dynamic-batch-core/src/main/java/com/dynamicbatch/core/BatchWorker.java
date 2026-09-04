package com.dynamicbatch.core;

import com.dynamicbatch.common.constants.BatchWorkerConstant;
import com.dynamicbatch.common.pojo.BatchWorkerGroupConfigPOJO;
import com.dynamicbatch.core.notifier.manager.NotifyManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;


/**
 * 单个批处理 Worker：一个内存队列 + 一条消费线程（单线程消费，保证有序）。
 *
 * <p>并行度由 {@link BatchWorkerGroup} 的分区数提供：每个分区一个 Worker，
 * 同路由 key 的数据永远进入同一分区，分区内 FIFO + 单线程 = 严格有序。
 * 业务方不应直接构造/持有 Worker，统一通过 Worker 组与 {@link BatchProcessor} 交互。
 *
 * <p>配置不落在本类：全部读取组共享的 {@link BatchWorkerGroupConfigPOJO}（同一实例），
 * 本类只保留 name 与 queue 两个自身状态。
 *
 * <p>空闲标志：队列无数据且无在途批次时为 true（批次边界置位），分区组替换据此判定静止点。
 */
public class BatchWorker<T> {
    private static final Logger log = LoggerFactory.getLogger(BatchWorker.class);

    private String name;
    /** 数据类型，submit 时做运行时校验（fail fast），避免错误类型混入队列 */
    private final Class<T> type;
    private final LinkedBlockingQueue<T> queue;
    /** 组共享配置（与 BatchWorkerGroup 同一实例），构建期锁定 */
    private final BatchWorkerGroupConfigPOJO config;
    private final Consumer<List<T>> flushCallback;
    private final Consumer<List<T>> failureHandler;

    private volatile boolean running;
    private Thread consumerThread;

    /** 空闲标志：队列无数据且无在途批次时为 true（批次边界置位），分区组替换据此判定静止点 */
    private volatile boolean idle;

    /**
     * 由 {@link BatchWorkerGroup} 构造分区时直接调用，共享同一 config 实例；
     * {@link Builder#build()} 也走本构造器。校验失败抛 {@link IllegalArgumentException}。
     */
    BatchWorker(Class<T> type, Consumer<List<T>> flushCallback, Consumer<List<T>> failureHandler,
                 BatchWorkerGroupConfigPOJO config) {
        validateConfig(config);
        this.type = Objects.requireNonNull(type, "type must not be null");
        this.queue = new LinkedBlockingQueue<>(config.getQueueCapacity());
        this.flushCallback = Objects.requireNonNull(flushCallback, "flushCallback must not be null");
        this.failureHandler = failureHandler;
        this.config = config;
    }

    public void setName(String name) {
        this.name = name;
    }

    // ======================== 停止与空闲 ========================

    /** 空闲标志：队列无数据且无在途批次时为 true，协调方据此判定分区组已到静止点 */
    boolean isIdle() {
        return idle;
    }

    public synchronized void start() {
        running = true;
        consumerThread = new Thread(this::consumeLoop, "batch-processor-" + name);
        consumerThread.setDaemon(true);
        consumerThread.start();
        log.info("[{}] worker started, queueCapacity={}, batchSize={}",
                name, config.getQueueCapacity(), config.getBatchSize());
    }

    public int getQueueSize() {
        return queue.size();
    }

    /**
     * 投递一条数据到内存队列：队列满时阻塞等待空位，有位即投。
     * 生产链路由 Dispatcher 调用，业务方一般不直接使用。
     *
     * <p>返回 true = 本条已终结（入队成功，或类型不匹配毒丸已丢弃），投递方可继续下一条；
     * false = 本条未投递（worker 已关闭，或强制关闭中断打断阻塞 put，在途条目丢弃）。
     *
     * @return true 本条已终结；false 未投递，投递方应结束
     */
    public boolean submit(T data) {
        // 已关闭则直接拒绝：避免「返回 true 但数据入队后无人消费」的静默丢失
        if (!running) {
            log.warn("[{}] submit rejected, worker not running", name);
            NotifyManager.getInstance().tryNoticeOfferFailedAsync(name, "worker 已关闭", queue.size());
            return false;
        }
        // 毒丸丢弃：错误类型重试无意义，error 留痕后丢弃、继续下一条；
        // 不发运维告警（内部防线，业务入口已校验过类型）
        if (!type.isInstance(data)) {
            log.error("[{}] poison entry dropped, type mismatch: expected={}, got={}",
                    name, type.getName(), data == null ? "null" : data.getClass().getName());
            return true;
        }
        try {
            queue.put(data);
            return true;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.warn("[{}] put interrupted, entry dropped on forced shutdown", name);
            return false;
        }
    }

    private void consumeLoop() {
        List<T> batch = new ArrayList<>(config.getBatchSize());
        while (running) {
            try {
                // 用限时 poll 代替阻塞 take：running=false 后最迟 WAKEUP_INTERVAL_MS 感知到停止信号，
                // 不会被攒批窗口（maxWaitMs 可能很大）卡住，关闭响应与攒批时长彻底解耦
                T first = queue.poll(BatchWorkerConstant.WAKEUP_INTERVAL_MS, TimeUnit.MILLISECONDS);
                if (first == null) {
                    idle = true;   // poll 超时 = 无数据且无在途批次：批次边界上的静止点
                    continue;
                }
                idle = false;      // 手里已有数据，攒批/flush 完成前非空闲
                batch.add(first);

                long startTime = System.nanoTime();
                while (batch.size() < config.getBatchSize() && running) {
                    // 用 nanoTime 计时：单调递增，不受系统校时（NTP/手动改时间）影响；
                    // currentTimeMillis 在时钟回拨时会把攒批窗口无限拉长，前跳时又会瞬间截断
                    long elapsed = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startTime);
                    if (elapsed >= config.getMaxWaitMs()) {
                        break;
                    }
                    T next = queue.poll(Math.min(BatchWorkerConstant.WAKEUP_INTERVAL_MS,
                            config.getMaxWaitMs() - elapsed), TimeUnit.MILLISECONDS);
                    if (next == null) {
                        continue;
                    }
                    batch.add(next);
                }

                flushBatch(batch);
                batch.clear();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                log.warn("[{}] consumer interrupted", name);
                break;
            } catch (Exception e) {
                log.error("[{}] consume loop error", name, e);
            }
        }

        // 退出前兜底刷掉批次中残留数据（主要是中断打断攒批的场景）
        if (!batch.isEmpty()) {
            log.info("[{}] flushing {} remaining records before exit", name, batch.size());
            flushBatch(batch);
        }
    }

    private void flushBatch(List<T> batch) {
        if (batch.isEmpty()) {
            return;
        }
        try {
            flushCallback.accept(batch);
            if (log.isDebugEnabled()) {
                log.debug("[{}] flushed {} items", name, batch.size());
            }
        } catch (Exception e) {
            log.error("[{}] flush failed, size={}, err={}", name, batch.size(), e.toString(), e);
            boolean dataLossRisk = false;
            if (failureHandler != null) {
                try {
                    failureHandler.accept(batch);
                    log.warn("[{}] failureHandler handled {} items", name, batch.size());
                } catch (Exception ex) {
                    log.error("[{}] failureHandler also failed, size={}", name, batch.size(), ex);
                    dataLossRisk = true;
                }
            } else {
                log.error("[{}] no failureHandler, {} items may be lost", name, batch.size());
                dataLossRisk = true;
            }
            // 通知放在 failureHandler 处理完之后，消息才能带上数据丢失风险
            NotifyManager.getInstance().tryNoticeFlushFailedAsync(name, batch.size(), e.toString(), dataLossRisk);
        }
    }

    /**
     * 优雅关闭（生命周期关闭）：置停止信号 → 等消费线程退出 → 排空队列剩余数据分批刷盘
     */
    public synchronized void shutdown() {
        requestStop();
        awaitStop();
        flushRemaining();
        log.info("[{}] worker stopped", name);
    }

    /** 置停止信号，不等消费线程退出（与 {@link #awaitStop()} 配对） */
    void requestStop() {
        running = false;
    }

    /**
     * 等消费线程退出：自然退出超时则强制中断再短等。
     * 不刷盘队列——刷盘是 {@link #shutdown()} 的收尾，分区组替换场景队列必空、无刷盘需求。
     */
    void awaitStop() {
        if (consumerThread == null) {
            return;
        }
        // 消费线程最迟 WAKEUP_INTERVAL_MS 内感知停止信号并自然退出，
        // 这里只需等它把手头批次处理完（含 flushCallback 耗时），与攒批窗口 maxWaitMs 无关
        try {
            consumerThread.join(BatchWorkerConstant.SHUTDOWN_WAIT_MS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.warn("[{}] await stop interrupted while waiting for consumer", name);
        }

        // 超时仍存活：说明卡在慢回调/异常中，强制中断兜底
        if (consumerThread.isAlive()) {
            log.warn("[{}] consumer still alive after {}ms, forcing interrupt; " +
                    "flushCallback may still be running, ensure it is thread-safe", name, BatchWorkerConstant.SHUTDOWN_WAIT_MS);
            consumerThread.interrupt();
            // 中断后再短暂等待，确保消费线程真正退出，避免与调用方后续动作并发执行回调
            try {
                consumerThread.join(1000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    /**
     * 排空队列中剩余数据，并按批次大小分批刷盘
     */
    private void flushRemaining() {
        List<T> remaining = new ArrayList<>();
        queue.drainTo(remaining);
        if (!remaining.isEmpty()) {
            log.info("[{}] flushing {} remaining items on shutdown", name, remaining.size());
            for (int i = 0; i < remaining.size(); i += config.getBatchSize()) {
                int end = Math.min(i + config.getBatchSize(), remaining.size());
                flushBatch(remaining.subList(i, end));
            }
        }
    }

    // ======================== 参数校验 ========================

    /**
     * 配置合法性校验（与 Worker 组共享同一份配置，构建期统一执行）：
     * 非法参数抛 {@link IllegalArgumentException}。
     */
    private static void validateConfig(BatchWorkerGroupConfigPOJO config) {
        int queueCapacity = config.getQueueCapacity();
        int batchSize = config.getBatchSize();
        long maxWaitMs = config.getMaxWaitMs();
        if (batchSize <= 0) {
            throw new IllegalArgumentException("batchSize must be > 0, got " + batchSize);
        }
        if (queueCapacity <= 0) {
            throw new IllegalArgumentException("queueCapacity must be > 0, got " + queueCapacity);
        }
        if (batchSize > queueCapacity) {
            throw new IllegalArgumentException("batchSize must be <= queueCapacity, got batchSize:" + batchSize + " > queueCapacity:" + queueCapacity);
        }
        if (maxWaitMs < 0) {
            throw new IllegalArgumentException("maxWaitMs must be >= 0, got " + maxWaitMs);
        }
    }

    // ======================== Builder ========================

    /**
     * 创建 {@code BatchWorker} 构建器，{@code type} 与 {@code flushCallback} 为必填参数。
     *
     * <p>通常不直接使用：由 {@link BatchWorkerGroup} 内部按分区构建，业务方只接触 Worker 组。
     */
    public static <T> Builder<T> builder(Class<T> type, Consumer<List<T>> flushCallback) {
        return new Builder<>(type, flushCallback);
    }

    /**
     * {@code BatchWorker} 构建器。
     *
     * <p>通过 {@link #builder(Class, Consumer)} 创建，{@code type} 与 {@code flushCallback}
     * 在构造时传入保证必填，其余参数有默认值（见 {@link BatchWorkerConstant}），可按需覆盖。
     */
    public static class Builder<T> {

        /** 数据类型，submit 时做运行时校验，必填 */
        private final Class<T> type;
        /** 刷盘回调，攒满一批或超时触发时调用，必填 */
        private final Consumer<List<T>> flushCallback;
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

        public Builder<T> failureHandler(Consumer<List<T>> failureHandler) {
            this.failureHandler = failureHandler;
            return this;
        }

        /**
         * 构建 {@code BatchWorker} 实例，执行参数校验。
         */
        public BatchWorker<T> build() {
            return new BatchWorker<>(type, flushCallback, failureHandler, config);
        }
    }
}
