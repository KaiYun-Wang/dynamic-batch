package com.dynamicbatch.core;

import com.dynamicbatch.core.constants.BatchWorkerConstant;
import com.dynamicbatch.core.pojo.BatchWorkerConfigPOJO;
import com.dynamicbatch.core.queue.VariableLinkedBlockingQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;


/**
 * 单个批处理 Worker
 */
public class BatchWorker<T> {
    private static final Logger log = LoggerFactory.getLogger(BatchWorker.class);

    private String name;
    /** 数据类型，submit 时做运行时校验（fail fast），避免错误类型混入队列 */
    private final Class<T> type;
    private final VariableLinkedBlockingQueue<T> queue;
    /** 当前队列容量，热更新时通过 queue.setCapacity 动态调整；新容量必须 >= batchSize（见 refresh 校验） */
    private volatile int queueCapacity;
    /** 攒批条数，热更新后下一批即生效 */
    private volatile int batchSize;
    /** 最大等待毫秒，热更新后攒批窗口即时生效 */
    private volatile long maxWaitMs;
    /** 入队超时毫秒，热更新后新的 submit 调用生效 */
    private volatile long offerTimeoutMs;
    private final Consumer<List<T>> flushCallback;
    private final Consumer<List<T>> failureHandler;
    /** 期望消费线程数：多条线程从同一队列竞争取数据、各自攒批各自 flush（分片并行，不保证顺序） */
    private volatile int consumers;

    private volatile boolean running;
    private final List<Thread> consumerThreads = new ArrayList<>();
    /** 与 consumerThreads 平行的每线程停止标志：refresh 缩线程时置位，线程感知后自然退出 */
    private final List<AtomicBoolean> consumerStopFlags = new ArrayList<>();

    private BatchWorker(Builder<T> builder) {
        this.type = builder.type;
        this.queueCapacity = builder.queueCapacity;
        this.batchSize = builder.batchSize;
        this.consumers = builder.consumers;
        this.maxWaitMs = builder.maxWaitMs;
        this.offerTimeoutMs = builder.offerTimeoutMs;
        this.flushCallback = builder.flushCallback;
        this.failureHandler = builder.failureHandler;
        this.queue = new VariableLinkedBlockingQueue<>(queueCapacity);
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }

    public synchronized void start() {
        running = true;
        for (int i = 0; i < consumers; i++) {
            startConsumerThread(i);
        }
        log.info("[{}] worker started, consumers={}, queueCapacity={}, batchSize={}", name, consumers, queueCapacity, batchSize);
    }

    /**
     * 热更新配置，{@code null} 字段表示不更新。
     *
     * <p>batchSize/maxWaitMs/offerTimeoutMs 即时生效；queueCapacity 通过
     * {@link VariableLinkedBlockingQueue#setCapacity(int)} 动态调整；consumers 增则新建消费线程，
     * 减则等待多余线程自然退出（复用优雅关闭模式）。校验规则与 {@link Builder#build()} 一致，
     * 失败抛 {@link IllegalArgumentException}。
     */
    public synchronized void refresh(BatchWorkerConfigPOJO config) {
        int newQueueCapacity = config.getQueueCapacity() != null ? config.getQueueCapacity() : this.queueCapacity;
        int newBatchSize = config.getBatchSize() != null ? config.getBatchSize() : this.batchSize;
        long newMaxWaitMs = config.getMaxWaitMs() != null ? config.getMaxWaitMs() : this.maxWaitMs;
        long newOfferTimeoutMs = config.getOfferTimeoutMs() != null ? config.getOfferTimeoutMs() : this.offerTimeoutMs;
        int newConsumers = config.getConsumers() != null ? config.getConsumers() : this.consumers;

        // 统一校验（与 build() 共用同一套规则），保证攒批等运行时约束仍然成立
        validateParams(newQueueCapacity, newBatchSize, newMaxWaitMs, newOfferTimeoutMs, newConsumers);

        this.queueCapacity = newQueueCapacity;
        this.batchSize = newBatchSize;
        this.maxWaitMs = newMaxWaitMs;
        this.offerTimeoutMs = newOfferTimeoutMs;
        this.consumers = newConsumers;

        queue.setCapacity(newQueueCapacity);
        adjustConsumers(newConsumers);
        log.info("[{}] config refreshed, consumers={}, queueCapacity={}, batchSize={}, maxWaitMs={}, offerTimeoutMs={}",
                name, consumers, queueCapacity, batchSize, maxWaitMs, offerTimeoutMs);
    }

    /** 启动一条消费线程并登记其停止标志 */
    private void startConsumerThread(int index) {
        AtomicBoolean stopFlag = new AtomicBoolean(false);
        Thread thread = new Thread(() -> consumeLoop(stopFlag), "batch-processor-" + name + "-" + index);
        thread.setDaemon(true);
        thread.start();
        consumerThreads.add(thread);
        consumerStopFlags.add(stopFlag);
    }

    /**
     * 调整消费线程数：增则新建线程；减则置停止标志，等待其处理完手头批次自然退出。
     */
    private void adjustConsumers(int target) {
        int current = consumerThreads.size();
        if (target > current) {
            for (int i = current; i < target; i++) {
                startConsumerThread(i);
            }
            log.info("[{}] consumers increased: {} -> {}", name, current, target);
        } else if (target < current) {
            // 从尾部开始回收多余线程；消费线程最迟 WAKEUP_INTERVAL_MS 感知停止标志并退出
            for (int i = current; i > target; i--) {
                int idx = consumerThreads.size() - 1;
                consumerStopFlags.remove(idx).set(true);
                Thread thread = consumerThreads.remove(idx);
                try {
                    thread.join(BatchWorkerConstant.SHUTDOWN_WAIT_MS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    log.warn("[{}] interrupted while waiting for consumer to stop", name);
                }
                if (thread.isAlive()) {
                    log.warn("[{}] consumer still alive after {}ms, forcing interrupt",
                            name, BatchWorkerConstant.SHUTDOWN_WAIT_MS);
                    thread.interrupt();
                }
            }
            log.info("[{}] consumers decreased: {} -> {}", name, current, target);
        }
    }

    public boolean submit(T data) {
        // 已关闭则直接拒绝：避免「返回 true 但数据入队后无人消费」的静默丢失
        if (!running) {
            log.warn("[{}] submit rejected, worker not running", name);
            return false;
        }
        // 类型校验（fail fast）：错误类型的数据在业务线程就拒绝，
        // 否则会混入队列，直到消费端 flush 时整批抛 ClassCastException
        // 注：isInstance(null) 为 false，null 数据也会被拒绝
        if (!type.isInstance(data)) {
            log.error("[{}] submit rejected, type mismatch: expected={}, got={}",
                    name, type.getName(), data == null ? "null" : data.getClass().getName());
            return false;
        }
        try {
            boolean ok = queue.offer(data, offerTimeoutMs, TimeUnit.MILLISECONDS);
            if (!ok) {
                log.warn("[{}] queue full, offer timed out after {}ms", name, offerTimeoutMs);
            }
            return ok;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();   // 关键：恢复中断标志
            log.warn("[{}] submit interrupted", name);
            return false;
        }
    }

    private void consumeLoop(AtomicBoolean stopFlag) {
        List<T> batch = new ArrayList<>(batchSize);
        while (running && !stopFlag.get()) {
            try {
                // 用限时 poll 代替阻塞 take：running=false 后最迟 WAKEUP_INTERVAL_MS 感知到停止信号，
                // 不会被攒批窗口（maxWaitMs 可能很大）卡住，关闭响应与攒批时长彻底解耦
                T first = queue.poll(BatchWorkerConstant.WAKEUP_INTERVAL_MS, TimeUnit.MILLISECONDS);
                if (first == null) {
                    continue;
                }
                batch.add(first);

                long startTime = System.nanoTime();
                while (batch.size() < batchSize && running) {
                    // 用 nanoTime 计时：单调递增，不受系统校时（NTP/手动改时间）影响；
                    // currentTimeMillis 在时钟回拨时会把攒批窗口无限拉长，前跳时又会瞬间截断
                    long elapsed = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startTime);
                    if (elapsed >= maxWaitMs) {
                        break;
                    }
                    T next = queue.poll(Math.min(BatchWorkerConstant.WAKEUP_INTERVAL_MS, maxWaitMs - elapsed), TimeUnit.MILLISECONDS);
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
            if (failureHandler != null) {
                try {
                    failureHandler.accept(batch);
                    log.warn("[{}] failureHandler handled {} items", name, batch.size());
                } catch (Exception ex) {
                    log.error("[{}] failureHandler also failed, size={}", name, batch.size(), ex);
                }
            } else {
                log.error("[{}] no failureHandler, {} items may be lost", name, batch.size());
            }
        }
    }

    /**
     * 优雅关闭
     * <p>先等消费线程自然退出，超时则强制中断，最后分批刷盘队列中剩余数据</p>
     */
    public synchronized void shutdown() {
        running = false;

        if (consumerThreads.isEmpty()) {
            // 从未启动过，直接刷盘队列中剩余数据
            flushRemaining();
            log.info("[{}] worker stopped", name);
            return;
        }

        // 消费线程最迟 WAKEUP_INTERVAL_MS 内感知停止信号并自然退出，
        // 这里只需等它们把手头批次处理完（含 flushCallback 耗时），与攒批窗口 maxWaitMs 无关
        for (Thread consumerThread : consumerThreads) {
            try {
                consumerThread.join(BatchWorkerConstant.SHUTDOWN_WAIT_MS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                log.warn("[{}] shutdown interrupted while waiting for consumer", name);
            }
        }

        // 超时仍存活：说明卡在慢回调/异常中，强制中断兜底
        for (Thread consumerThread : consumerThreads) {
            if (consumerThread.isAlive()) {
                log.warn("[{}] consumer still alive after {}ms, forcing interrupt; " +
                        "flushCallback may still be running, ensure it is thread-safe", name, BatchWorkerConstant.SHUTDOWN_WAIT_MS);
                consumerThread.interrupt();
                // 中断后再短暂等待，确保消费线程真正退出，避免与下方刷盘并发执行回调
                try {
                    consumerThread.join(1000);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        }

        // 排空队列中剩余数据并分批刷盘
        flushRemaining();
        log.info("[{}] worker stopped", name);
    }

    /**
     * 排空队列中剩余数据，并按批次大小分批刷盘
     */
    private void flushRemaining() {
        List<T> remaining = new ArrayList<>();
        queue.drainTo(remaining);
        if (!remaining.isEmpty()) {
            log.info("[{}] flushing {} remaining items on shutdown", name, remaining.size());
            for (int i = 0; i < remaining.size(); i += batchSize) {
                int end = Math.min(i + batchSize, remaining.size());
                flushBatch(remaining.subList(i, end));
            }
        }
    }

    // ======================== 参数校验 ========================

    /**
     * 参数合法性校验，{@link Builder#build()} 与 {@link #refresh(BatchWorkerConfigPOJO)} 共用：
     * 非法参数抛 {@link IllegalArgumentException}。
     */
    private static void validateParams(int queueCapacity, int batchSize, long maxWaitMs, long offerTimeoutMs, int consumers) {
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
        if (offerTimeoutMs < 0) {
            throw new IllegalArgumentException("offerTimeoutMs must be >= 0, got " + offerTimeoutMs);
        }
        if (consumers <= 0) {
            throw new IllegalArgumentException("consumers must be > 0, got " + consumers);
        }
    }

    // ======================== Builder ========================

    /**
     * 创建 {@code BatchWorker} 构建器，{@code type} 与 {@code flushCallback} 为必填参数。
     *
     * <p>{@code type} 用于 submit 时的运行时类型校验；同时编译器可据此推断泛型，
     * 调用点无需显式写 {@code <T>}。
     *
     * <p>用法：
     * <pre>{@code
     * BatchWorker<MyData> worker = BatchWorker.builder(MyData.class, records -> insertBatch(records))
     *         .batchSize(100)
     *         .maxWaitMs(2000)
     *         .build();
     * }</pre>
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
        /** 队列容量，超过此值入队会阻塞直到超时 */
        private int queueCapacity = BatchWorkerConstant.DEFAULT_QUEUE_CAPACITY;
        /** 攒批条数，达到此数量立即触发 flush */
        private int batchSize = BatchWorkerConstant.DEFAULT_BATCH_SIZE;
        /** 最大等待毫秒，未攒满时最多等这么久再 flush（从收到第一条数据开始计时） */
        private long maxWaitMs = BatchWorkerConstant.DEFAULT_MAX_WAIT_MS;
        /** 入队超时毫秒，队列满时 {@code submit} 最多阻塞这么久，超时返回 false */
        private long offerTimeoutMs = BatchWorkerConstant.DEFAULT_OFFER_TIMEOUT_MS;
        /** 失败回调，flush 抛异常时调用，可为 null（此时只打 error 日志） */
        private Consumer<List<T>> failureHandler;
        /** 消费线程数：多条线程从同一队列竞争取数据、各自攒批各自 flush（分片并行，不保证顺序） */
        private int consumers = BatchWorkerConstant.DEFAULT_CONSUMERS;

        private Builder(Class<T> type, Consumer<List<T>> flushCallback) {
            this.type = Objects.requireNonNull(type, "type must not be null");
            this.flushCallback = Objects.requireNonNull(flushCallback, "flushCallback must not be null");
        }

        public Builder<T> queueCapacity(int queueCapacity) {
            this.queueCapacity = queueCapacity;
            return this;
        }

        public Builder<T> batchSize(int batchSize) {
            this.batchSize = batchSize;
            return this;
        }

        public Builder<T> maxWaitMs(long maxWaitMs) {
            this.maxWaitMs = maxWaitMs;
            return this;
        }

        public Builder<T> offerTimeoutMs(long offerTimeoutMs) {
            this.offerTimeoutMs = offerTimeoutMs;
            return this;
        }

        public Builder<T> failureHandler(Consumer<List<T>> failureHandler) {
            this.failureHandler = failureHandler;
            return this;
        }

        public Builder<T> consumers(int consumers) {
            this.consumers = consumers;
            return this;
        }

        /**
         * 构建 {@code BatchWorker} 实例，执行参数校验。
         */
        public BatchWorker<T> build() {
            validateParams(queueCapacity, batchSize, maxWaitMs, offerTimeoutMs, consumers);
            return new BatchWorker<>(this);
        }
    }
}