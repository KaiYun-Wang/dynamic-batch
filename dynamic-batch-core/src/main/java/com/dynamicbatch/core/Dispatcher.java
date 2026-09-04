package com.dynamicbatch.core;

import com.dynamicbatch.common.constants.BatchWorkerConstant;
import com.dynamicbatch.common.pojo.SpoolEntryPOJO;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiPredicate;

/**
 * Group 的分发线程：从 Spool poll 一条 → 投递到 Worker 队列，背压时短睡重试。
 *
 * <p>数据链路位置：{@code Spool(磁盘)} → <b>Dispatcher</b> → {@code Worker 内存队列} → flushCallback。
 * 由 {@link BatchWorkerGroup} 创建并编排启停。
 *
 * <p>不拥有路由逻辑（路由在 {@link #deliver} 的实现，通常为 Group 私有 {@code submitWorker} 的
 * {@code hash(routingKey) % partitions.size()}）。
 *
 * <p>背压语义：{@code deliver} 返回 false 为正常背压，短睡重试无上限、不丢消息；
 * 已 poll 的条目在 {@link #deliverUntilSuccess} 中不看 {@code running}，必须投递成功才离开
 * （关闭顺序为先 Dispatcher 后 Worker，关闭时 Worker 仍可接收投递）。
 *
 * <p>毒消息：poll 后反序列化/帧损坏抛异常 → 读位置已推进，打 error 日志后丢弃，继续循环。
 *
 * <p>中断临时约定：投递重试中 sleep 被 interrupt →
 * warn 含 entry 摘要后丢弃本条并结束线程；空轮 sleep 被 interrupt → 恢复中断标志并结束线程。
 *
 * <p>暂停检查在循环顶部（两条消息之间生效）：PAUSED 时不取数，本条投递成功才确认暂停。
 * 暂停/恢复为异步意图：置相位后立即返回，终态由线程自行确认，相位状态机是本类私有实现。
 */
class Dispatcher<T> {

    private static final Logger log = LoggerFactory.getLogger(Dispatcher.class);

    /** poll 时传给 Spool 的读锁获取超时（毫秒）；Chronicle 清理文件持锁冲突时抛 {@link TimeoutException}，按本次空轮处理 */
    private static final long POLL_LOCK_TIMEOUT_MS = 100L;

    /** Spool 空轮等待与 Worker 队列满时的背压重试睡眠间隔，与 Worker 轮询间隔同源，禁止忙等 */
    private static final long LOOP_SLEEP_MS = BatchWorkerConstant.WAKEUP_INTERVAL_MS;

    /**
     * 从 Spool 取一条落盘条目。
     * <p>自定义接口而非 {@code Supplier}：{@code Spool.poll} 抛受检 {@link TimeoutException}，
     * JDK 现成函数式接口签名装不下。
     */
    @FunctionalInterface
    interface Poller<T> {
        /**
         * @param lockTimeoutMs 获取 Spool 读锁的最长等待（毫秒）
         * @return 反序列化后的条目；队列为空返回 null
         * @throws TimeoutException 获取读锁超时（并发读与文件清理冲突），非致命
         */
        SpoolEntryPOJO<T> poll(long lockTimeoutMs) throws TimeoutException;
    }

    /** 取数函数，生产接线为 {@code spool::poll} */
    private final Poller<T> poller;

    /**
     * 投递函数：将 routingKey + payload 投入 Worker 分区队列，
     * 生产接线为 Group 的 {@code submitWorker}。
     * 返回 true = 入队成功；false = Worker 队列满（正常背压，触发重试）。
     */
    private final BiPredicate<String, T> deliver;

    /** 分发线程运行标志，{@code false} 时外层 {@link #dispatchLoop} 退出 */
    private volatile boolean running;

    /** 调度器暂停状态机相位：两段式——协调方写意图（*_PENDING），线程在循环顶部确认翻终态（PAUSED/RUNNING） */
    public enum PausePhase {
        /** 已运行 */
        RUNNING,
        /** 待暂停 */
        PAUSE_PENDING,
        /** 已暂停 */
        PAUSED,
        /** 待运行 */
        RUN_PENDING
    }

    /** 热更新暂停相位：协调方置 {@code *_PENDING}，分发线程在循环顶部以 CAS 切换到终态 */
    private final AtomicReference<PausePhase> pausePhase = new AtomicReference<>(PausePhase.RUNNING);

    /** 分发线程引用，{@link #stop} 时 join / interrupt */
    private Thread thread;

    /** 逻辑名称（不含线程名前缀），用于日志与线程命名 {@code batch-processor-{name}} */
    private String name;

    /**
     * @param poller  从 Spool 取条的函数（测试用 lambda，生产为 {@code spool::poll}）
     * @param deliver 投到 Worker 的函数（测试用 lambda，生产为 {@code this::submitWorker}）
     */
    Dispatcher(Poller<T> poller, BiPredicate<String, T> deliver) {
        this.poller = Objects.requireNonNull(poller, "poller must not be null");
        this.deliver = Objects.requireNonNull(deliver, "deliver must not be null");
    }

    /**
     * 设置逻辑名称，须在 {@link #start} 前调用，
     * 传 {@code "{groupKey}-dispatcher"}。
     */
    void setName(String name) {
        this.name = name;
    }

    // ======================== 暂停/恢复 ========================

    /** 读当前相位（volatile 读，直接反映线程与协调方共同的最新状态） */
    PausePhase getPhase() {
        return pausePhase.get();
    }

    /** 置「待暂停」意图（幂等）：意图生效后循环顶部即停止取数，终态由线程在两条消息之间确认 */
    void requestPause() {
        pausePhase.set(PausePhase.PAUSE_PENDING);
    }

    /** 置「待运行」意图；已 RUNNING 不写 */
    void requestRun() {
        if (pausePhase.get() != PausePhase.RUNNING) {
            pausePhase.set(PausePhase.RUN_PENDING);
        }
    }

    /**
     * 启动分发线程（daemon）。重复调用安全，已在运行时打 warn 并直接返回。
     */
    synchronized void start() {
        if (running) {
            log.warn("[{}] dispatcher already started", name);
            return;
        }
        running = true;
        thread = new Thread(this::dispatchLoop, "batch-processor-" + name);
        thread.setDaemon(true);
        thread.start();
        log.info("[{}] dispatcher started", name);
    }

    /**
     * 优雅关闭：置 {@code running=false} → join 消费线程 → 超时则 interrupt 再短等。
     * 重复调用安全（thread 已清空时直接返回）。
     */
    synchronized void stop() {
        running = false;
        if (thread == null) {
            return;
        }
        try {
            thread.join(BatchWorkerConstant.SHUTDOWN_WAIT_MS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.warn("[{}] dispatcher stop interrupted while waiting", name);
        }
        if (thread.isAlive()) {
            log.warn("[{}] dispatcher still alive after {}ms, forcing interrupt",
                    name, BatchWorkerConstant.SHUTDOWN_WAIT_MS);
            thread.interrupt();
            try {
                thread.join(1000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        thread = null;
        log.info("[{}] dispatcher stopped", name);
    }

    /**
     * 分发主循环（在独立线程中运行）：
     * poll 一条 → 非空则投递成功为止 → 空则短睡 → 重复。
     * <p>异常分支：锁超时 continue；反序列化失败 error 后 continue；空轮 sleep。
     */
    private void dispatchLoop() {
        while (running) {
            // 暂停检查：两条消息之间生效；PAUSED 时不取数，恢复后从读位置续读
            PausePhase phase = pausePhase.get();
            if (phase == PausePhase.PAUSE_PENDING || phase == PausePhase.PAUSED) {
                // CAS 切换：期望不符说明相位已变（如协调方超时回滚），下轮重读重判
                pausePhase.compareAndSet(PausePhase.PAUSE_PENDING, PausePhase.PAUSED);
                try {
                    Thread.sleep(LOOP_SLEEP_MS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                }
                continue;
            }
            if (phase == PausePhase.RUN_PENDING
                    && !pausePhase.compareAndSet(PausePhase.RUN_PENDING, PausePhase.RUNNING)) {
                continue;   // 切换失败（相位已变）：重读，勿直接取数
            }

            SpoolEntryPOJO<T> entry;
            try {
                entry = poller.poll(POLL_LOCK_TIMEOUT_MS);
            } catch (TimeoutException e) {
                continue;
            } catch (Exception e) {
                log.error("[{}] poll failed, dropping corrupted entry", name, e);
                continue;
            }

            if (entry == null) {
                try {
                    Thread.sleep(LOOP_SLEEP_MS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                }
                continue;
            }

            deliverUntilSuccess(entry);
        }
    }

    /**
     * 内层投递循环：已 poll 的条目必须投成功才返回，不看 {@code running}。
     * <p>{@code deliver} 返回 false 时短睡重试（背压）；sleep 被 interrupt 时 warn 丢本条并置 {@code running=false} 结束线程。
     */
    private void deliverUntilSuccess(SpoolEntryPOJO<T> entry) {
        while (true) {
            if (deliver.test(entry.getRoutingKey(), entry.getPayload())) {
                return;
            }
            try {
                Thread.sleep(LOOP_SLEEP_MS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                log.warn("[{}] deliver interrupted, dropping entry on forced shutdown: {}", name, entry);
                running = false;
                return;
            }
        }
    }
}
