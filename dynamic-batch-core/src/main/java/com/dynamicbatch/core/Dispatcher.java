package com.dynamicbatch.core;

import com.dynamicbatch.common.constants.BatchWorkerConstant;
import com.dynamicbatch.core.pojo.BatchWorkerGroupConfigPOJO;
import com.dynamicbatch.common.pojo.EnvelopePOJO;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Predicate;

/**
 * Group 的分发线程：从 Spool 取数 → 整封阻塞投递给 Worker 分区队列 → 循环。
 * 数据链路 {@code Spool(磁盘)} → <b>Dispatcher</b> → Worker 队列 → flushCallback；
 * 由 {@link BatchWorkerGroup} 创建编排，不拥有路由逻辑（路由在 {@link #deliver} 实现）。
 *
 * <p>取数侧机制（细节见对应方法）：限流——漏桶匀速、限速值热更下轮生效（{@link #pollWithRateLimit}）；
 * 空转等待——无数据挂起等写线程落盘信号，兜底周期到点强制真读（{@link #dispatchLoop}）；
 * 暂停——异步相位、线程自行确认终态（{@link PausePhase}）。
 * 投递被放弃（强制关闭中断 / worker 已关闭）是唯一中途结束线程的路径，毒消息丢弃后循环继续。
 */
class Dispatcher<T> {

    private static final Logger log = LoggerFactory.getLogger(Dispatcher.class);

    /** poll 时传给 Spool 的读锁获取超时（毫秒）；Chronicle 清理文件持锁冲突时抛 {@link TimeoutException}，按本次空轮处理 */
    private static final long POLL_LOCK_TIMEOUT_MS = 100L;

    /** Spool 空轮与暂停相位的循环睡眠间隔，与 Worker 轮询间隔同源，禁止忙等 */
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
        EnvelopePOJO<T> poll(long lockTimeoutMs) throws TimeoutException;
    }

    /**
     * 等待新数据信号（生产接线为 {@code spool::awaitData}，写线程落盘后释放）。
     * 挂起至多 {@code timeoutMs}；true = 有新落盘应立即真读，false = 超时无信号（走兜底真读）。
     */
    @FunctionalInterface
    interface DataAwaiter {
        boolean awaitData(long timeoutMs);
    }

    /** 取数函数，生产接线为 {@code spool::poll} */
    private final Poller<T> poller;

    /**
     * 投递函数：将信封整体（不拆包）投入 Worker 分区队列（阻塞 put），
     * 生产接线为 Group 的 {@code submitWorker}。
     * 返回 true = 本条已终结（入队成功或毒丸丢弃）；false = 本条未投递
     * （worker 已关闭 / 强制关闭中断），调用方应结束线程。
     */
    private final Predicate<EnvelopePOJO<T>> deliver;

    /** 组共享配置（必存），限速值每轮读取；{@code rateLimitPerSecond} 为 null = 未配置不限流 */
    private final BatchWorkerGroupConfigPOJO config;

    /** 新数据等待器：true = 有新落盘应立即真读（生产接线 {@code spool::awaitData}）；null = 无信号源，空轮睡满 timeout 后兜底真读 */
    private final DataAwaiter dataAwaiter;

    /** 上次真取数时刻（nanoTime 相对值），null = 尚未取数（首条不限）；限流间隔与空转兜底共用，仅分发线程访问 */
    private Long lastPollNanos;

    /** 分发线程运行标志，{@code false} 时外层 {@link #dispatchLoop} 退出 */
    private volatile boolean running;

    /** 调度器暂停状态机相位：两段式——协调方写意图（*_PENDING），线程在循环顶部确认终态（PAUSED/RUNNING） */
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
     * @param deliver 阻塞投到 Worker 的函数（测试用 lambda，生产为 {@code this::submitWorker}）
     */
    Dispatcher(Poller<T> poller, Predicate<EnvelopePOJO<T>> deliver) {
        this(poller, deliver, new BatchWorkerGroupConfigPOJO());
    }

    /**
     * @param config 组共享配置（必填，限速值每轮读取，支持热更）；
     *               {@code rateLimitPerSecond} 为 null = 未配置不限流
     */
    Dispatcher(Poller<T> poller, Predicate<EnvelopePOJO<T>> deliver, BatchWorkerGroupConfigPOJO config) {
        this(poller, deliver, config, null);
    }

    /**
     * @param dataAwaiter 新数据等待器（生产接线为 {@code spool::awaitData}）；
     *                    null = 无信号源（测试假 poller），空轮退化为睡满 timeout 后兜底真读
     */
    Dispatcher(Poller<T> poller, Predicate<EnvelopePOJO<T>> deliver, BatchWorkerGroupConfigPOJO config,
               DataAwaiter dataAwaiter) {
        this.poller = Objects.requireNonNull(poller, "poller must not be null");
        this.deliver = Objects.requireNonNull(deliver, "deliver must not be null");
        this.config = Objects.requireNonNull(config, "config must not be null");
        this.dataAwaiter = dataAwaiter;
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
     * 分发主循环（独立线程运行）：无数据时挂起等待写线程落盘信号（微秒级唤醒），
     * 信号到达或兜底周期到点才真读；读到数据满速连读至空，读空回到挂起。
     * <p>异常分支：锁超时 / 反序列化失败均 continue 不退出；等待被中断则恢复标志退出。
     */
    private void dispatchLoop() {
        boolean empty = false;  // 初始按可能有数据处理：首轮立即真读覆盖重启积压；此后上轮读空才挂起等铃
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

            // 空转挂起：等写线程落盘信号（数据到达微秒级唤醒），无信号且未到兜底周期不空读。
            // 兜底防御：信号机制不确定哪里会出问题，每 LOOP_SLEEP_MS 无视信号强制真读，失灵时最坏退化回固定 100ms 轮询
            if (empty && !awaitSignal(LOOP_SLEEP_MS) && !realPollDue()) {
                continue;
            }

            EnvelopePOJO<T> entry;
            try {
                entry = pollWithRateLimit();
            } catch (TimeoutException e) {
                continue;
            } catch (Exception e) {
                log.error("[{}] poll failed, dropping corrupted entry", name, e);
                continue;
            }

            empty = (entry == null);   // 读到数据满速连读至空，读空回到挂起等信号
            if (entry != null) {
                deliverEntry(entry);
            }
        }
    }

    // ======================== 空转等待（信号唤醒 + 兜底防御） ========================

    /** 等待新数据信号：无信号源（{@code dataAwaiter} 为 null，测试假 poller）时退化为睡满 timeoutMs */
    private boolean awaitSignal(long timeoutMs) {
        return dataAwaiter != null ? dataAwaiter.awaitData(timeoutMs) : sleepQuietly(timeoutMs);
    }

    /** 兜底防御：距上次真取数满 {@code LOOP_SLEEP_MS} 则无视信号强制真读，信号失灵时最坏退化回固定 100ms 轮询 */
    private boolean realPollDue() {
        return lastPollNanos == null
                || System.nanoTime() - lastPollNanos >= LOOP_SLEEP_MS * 1_000_000L;
    }

    /**
     * 可中断的空轮睡眠（仅无信号源退化路径使用）：被中断（强制关闭）→ 恢复中断标志并返回 false。
     */
    private boolean sleepQuietly(long millis) {
        try {
            Thread.sleep(millis);
            return true;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
        }
    }

    /**
     * 带限流的取数：配置限速时先睡足与上次取数的间隔（空闲超间隔则不睡）再 poll，
     * 取数前记录本次时刻闭环；未配置限速（null）或首条不限，直接 poll。
     * 限流等待被强制关闭中断 → 恢复中断标志后返回 null，空轮 sleep 随即抛中断结束线程。
     *
     * @return 反序列化后的条目；队列为空或限流等待被中断返回 null
     * @throws TimeoutException 获取读锁超时（并发读与文件清理冲突），非致命
     */
    private EnvelopePOJO<T> pollWithRateLimit() throws TimeoutException {
        Integer rate = config.getRateLimitPerSecond();
        if (rate != null) {
            long intervalNanos = 1_000_000_000L / rate;
            if (lastPollNanos != null) {
                long wait = intervalNanos - (System.nanoTime() - lastPollNanos);
                if (wait > 0) {
                    try {
                        Thread.sleep(wait / 1_000_000, (int) (wait % 1_000_000));
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        return null;
                    }
                }
            }
        }
        // 无条件记录本次真取数时刻：限流间隔与空转兜底（realPollDue）共用同一时间基准
        lastPollNanos = System.nanoTime();
        return poller.poll(POLL_LOCK_TIMEOUT_MS);
    }

    /**
     * 投递一条已 poll 的信封（整封直达，不拆包）：{@code deliver} 返回 false
     * （本条未投递：强制关闭中断 / worker 已关闭）时 warn 含 envelope 摘要并置
     * {@code running=false} 结束线程。
     */
    private void deliverEntry(EnvelopePOJO<T> entry) {
        if (!deliver.test(entry)) {
            log.warn("[{}] deliver abandoned, dropping entry on shutdown: {}", name, entry);
            running = false;
        }
    }
}
