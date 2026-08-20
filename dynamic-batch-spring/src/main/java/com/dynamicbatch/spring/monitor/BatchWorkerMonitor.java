package com.dynamicbatch.spring.monitor;

import com.dynamicbatch.common.vo.BatchWorkerInfoVO;
import com.dynamicbatch.core.BatchProcessor;
import com.dynamicbatch.core.notifier.manager.NotifyManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 * 队列积压定时检查：按配置周期遍历所有 Worker 的队列水位，利用率超阈值时发积压告警。
 *
 * <p>流程：从 {@link BatchProcessor} 取全部 Worker 运行时信息 → 逐个计算队列利用率
 * （元素数量 / 容量）→ 达到阈值调 {@link NotifyManager#tryNoticeQueueBlockedAsync} 异步告警。
 * 检查周期决定积压发现延迟（最长一个周期）；重复提醒频率可由静默期另行限制
 * （不配静默期则持续积压时每周期告警一条），两者解耦。
 * 设计参考 dromara dynamic-tp 的 DtpMonitor（自带调度线程，不依赖 Spring 定时）。
 */
public class BatchWorkerMonitor {

    private static final Logger log = LoggerFactory.getLogger(BatchWorkerMonitor.class);

    /** 未配置时默认检查周期秒 */
    public static final int DEFAULT_INTERVAL_SECONDS = 60;
    /** 未配置时默认队列利用率阈值（百分比） */
    public static final int DEFAULT_QUEUE_BLOCKED_THRESHOLD = 70;

    private final BatchProcessor batchProcessor;
    private final int intervalSeconds;
    private final int threshold;

    private final ScheduledExecutorService scheduler;
    private ScheduledFuture<?> future;

    public BatchWorkerMonitor(BatchProcessor batchProcessor, int intervalSeconds, int threshold) {
        this.batchProcessor = batchProcessor;
        this.intervalSeconds = intervalSeconds;
        this.threshold = threshold;
        this.scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "batch-worker-monitor");
            t.setDaemon(true);
            return t;
        });
    }

    /** 启动定时检查；上次检查结束后再等 intervalSeconds（fixed delay，避免检查卡住导致任务堆积） */
    public void start() {
        future = scheduler.scheduleWithFixedDelay(this::run, 0, intervalSeconds, TimeUnit.SECONDS);
        log.info("queue monitor started, intervalSeconds={}, threshold={}%", intervalSeconds, threshold);
    }

    private void run() {
        try {
            List<BatchWorkerInfoVO> infos = batchProcessor.listWorkerInfos();
            for (BatchWorkerInfoVO info : infos) {
                checkWorker(info);
            }
        } catch (Exception e) {
            log.error("queue monitor run error", e);
        }
    }

    private void checkWorker(BatchWorkerInfoVO info) {
        // 关闭中的 worker 正在排空队列，不告警
        if (!info.isRunning()) {
            return;
        }
        int queueSize = info.getQueueSize();
        if (queueSize <= 0) {
            return;
        }
        int queueCapacity = info.getConfig().getQueueCapacity();
        double utilization = queueSize * 100.0 / queueCapacity;
        if (utilization >= threshold) {
            log.warn("[{}] queue utilization {}% >= threshold {}%, queueSize={}, capacity={}",
                    info.getKey(), String.format("%.1f", utilization), threshold, queueSize, queueCapacity);
            NotifyManager.getInstance().tryNoticeQueueBlockedAsync(
                    info.getKey(), queueSize, queueCapacity, threshold, info.getActiveConsumers());
        }
    }

    /** 停止定时检查并关闭调度线程（Bean destroyMethod 调用） */
    public void shutdown() {
        if (future != null) {
            future.cancel(false);
        }
        scheduler.shutdownNow();
    }
}
