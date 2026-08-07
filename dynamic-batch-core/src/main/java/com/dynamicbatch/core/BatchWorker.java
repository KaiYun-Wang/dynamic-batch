package com.dynamicbatch.core;

import com.dynamicbatch.core.constants.BatchWorkerConstant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;


/**
 * 单个批处理 Worker
 */
public class BatchWorker<T> {
    private static final Logger log = LoggerFactory.getLogger(BatchWorker.class);

    private String name;
    /** 数据类型，submit 时做运行时校验（fail fast），避免错误类型混入队列 */
    private final Class<T> type;
    private final LinkedBlockingQueue<T> queue;
    private final int queueCapacity;
    private final int batchSize;
    private final long maxWaitMs;
    private final long offerTimeoutMs;
    private final Consumer<List<T>> flushCallback;
    private final Consumer<List<T>> failureHandler;

    private volatile boolean running;
    private Thread consumerThread;

    private BatchWorker(Builder<T> builder) {
        this.type = builder.type;
        this.queueCapacity = builder.queueCapacity;
        this.batchSize = builder.batchSize;
        this.maxWaitMs = builder.maxWaitMs;
        this.offerTimeoutMs = builder.offerTimeoutMs;
        this.flushCallback = builder.flushCallback;
        this.failureHandler = builder.failureHandler;
        this.queue = new LinkedBlockingQueue<>(queueCapacity);
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }

    public void start() {
        running = true;
        consumerThread = new Thread(this::consumeLoop, "batch-processor-" + name);
        consumerThread.setDaemon(true);
        consumerThread.start();
        log.info("[{}] worker started, queueCapacity={}, batchSize={}", name, queueCapacity, batchSize);
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

    private void consumeLoop() {
        List<T> batch = new ArrayList<>(batchSize);
        while (running) {
            try {
                // 用限时 poll 代替阻塞 take：running=false 后最迟 WAKEUP_INTERVAL_MS 感知到停止信号，
                // 不会被攒批窗口（maxWaitMs 可能很大）卡住，关闭响应与攒批时长彻底解耦
                T first = queue.poll(BatchWorkerConstant.WAKEUP_INTERVAL_MS, TimeUnit.MILLISECONDS);
                if (first == null) {
                    continue;
                }
                batch.add(first);

                long startTime = System.currentTimeMillis();
                while (batch.size() < batchSize && running) {
                    long elapsed = System.currentTimeMillis() - startTime;
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
    public void shutdown() {
        running = false;

        if (consumerThread == null) {
            // 从未启动过，直接刷盘队列中剩余数据
            flushRemaining();
            log.info("[{}] worker stopped", name);
            return;
        }

        // 消费线程最迟 WAKEUP_INTERVAL_MS 内感知停止信号并自然退出，
        // 这里只需等它把手头批次处理完（含 flushCallback 耗时），与攒批窗口 maxWaitMs 无关
        try {
            consumerThread.join(BatchWorkerConstant.SHUTDOWN_WAIT_MS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.warn("[{}] shutdown interrupted while waiting for consumer", name);
        }

        // 超时仍存活：说明卡在慢回调/异常中，强制中断兜底
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

        /**
         * 构建 {@code BatchWorker} 实例，执行参数校验。
         */
        public BatchWorker<T> build() {
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
            return new BatchWorker<>(this);
        }
    }
}