package com.dynamicbatch.example.controller;

import com.dynamicbatch.common.vo.BatchWorkerInfoVO;
import com.dynamicbatch.core.BatchProcessor;
import com.dynamicbatch.core.BatchWorker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/**
 * 队列积压告警自测入口：注册一个「flush 回调 sleep 卡死」的 worker 制造队列积压，
 * 配合 application.yml 的 queue_blocked 通知项（interval-seconds: 5, threshold: 80）
 * 观察每 5 秒一次的积压告警，并验证 worker 优雅关闭的强制中断流程。
 *
 * <pre>
 * curl -X POST "http://localhost:8080/block-demo/register"         # 注册（默认：容量 100、批 10、flush 睡 8s）
 * curl -X POST "http://localhost:8080/block-demo/submit?count=200" # 灌数据，队列稳定在 ~90/100
 * curl.exe "http://localhost:8080/block-demo/status"                  # 查队列水位（对应告警消息里的数字）
 * </pre>
 *
 * <p>积压来源是「小 batchSize + flush 卡住」而非攒批窗口：poll 有货立即返回，
 * 若 batchSize 设大，消费线程会把队列瞬间全抓进攒批，队列反而清空（不会告警）。
 * 告警发送结果见应用日志（钉钉 webhook 未填时每 5 秒一条发送失败日志，同样可见节奏）。
 * 关停流程（消费线程 5s 超时强制中断 → 剩余数据逐批刷出）直接关闭程序观察。
 */
@RestController
@RequestMapping("/block-demo")
public class QueueBlockedDemoController {

    private static final Logger log = LoggerFactory.getLogger(QueueBlockedDemoController.class);

    public static final String KEY = "queue_blocked_demo";

    private final BatchProcessor batchProcessor;

    public QueueBlockedDemoController(BatchProcessor batchProcessor) {
        this.batchProcessor = batchProcessor;
    }

    /**
     * 注册积压演示 worker（重复注册会先优雅关停旧 worker）。
     *
     * @param flushDelayMs  flush 回调 sleep 时长（模拟下游卡死），默认 8000ms；
     *                      建议 > 5000（SHUTDOWN_WAIT_MS），关停时才能看到强制中断路径
     * @param queueCapacity 队列容量，默认 100
     * @param batchSize     攒批条数，默认 10（小批量：消费线程抓满即 flush 并卡住，队列保持积压）
     */
    @PostMapping("/register")
    public String register(@RequestParam(defaultValue = "8000") long flushDelayMs,
                           @RequestParam(defaultValue = "100") int queueCapacity,
                           @RequestParam(defaultValue = "10") int batchSize) {
        BatchWorker<String> worker = BatchWorker.builder(String.class, batch -> {
            log.info("[{}] flush start, size={}, sleep {}ms", KEY, batch.size(), flushDelayMs);
            try {
                Thread.sleep(flushDelayMs);   // 模拟下游卡死：消费线程阻塞，队列持续积压
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();  // 关闭程序时 5s 超时强制中断会走到这里
            }
        })
                .queueCapacity(queueCapacity)
                .batchSize(batchSize)
                .maxWaitMs(60000)       // 攒批窗口拉长：本演示 batchSize 小，通常攒满即 flush
                .offerTimeoutMs(10)     // 队列满时 submit 最多阻塞 10ms，失败走 offer_failed 告警
                .build();
        batchProcessor.register(KEY, worker);
        double utilization = (queueCapacity - batchSize) * 100.0 / queueCapacity;
        return "registered: key=" + KEY + ", queueCapacity=" + queueCapacity + ", batchSize=" + batchSize
                + ", flushDelayMs=" + flushDelayMs + "（队列约保持 " + (queueCapacity - batchSize) + "/" + queueCapacity
                + " = " + String.format("%.0f", utilization) + "%，超过 yml 阈值 80 后每 5 秒告警一次）";
    }

    /** 向演示 worker 灌数据；队列满的提交会在 offerTimeoutMs(10ms) 后失败并触发 offer_failed 告警 */
    @PostMapping("/submit")
    public String submit(@RequestParam(defaultValue = "200") int count) {
        BatchWorkerInfoVO info = batchProcessor.getWorkerInfo(KEY);
        if (info == null) {
            return "worker not found: " + KEY + "（先调 POST /block-demo/register）";
        }
        int ok = 0;
        int failed = 0;
        for (int i = 0; i < count; i++) {
            if (batchProcessor.submit(KEY, "data-" + i)) {
                ok++;
            } else {
                failed++;
            }
        }
        info = batchProcessor.getWorkerInfo(KEY);
        return "submit done: ok=" + ok + ", failed=" + failed + ", queue=" + info.getQueueSize()
                + "/" + info.getConfig().getQueueCapacity();
    }

    /** 查询演示 worker 的队列水位（与告警消息里的数字对应） */
    @GetMapping("/status")
    public String status() {
        BatchWorkerInfoVO info = batchProcessor.getWorkerInfo(KEY);
        if (info == null) {
            return "worker not registered: " + KEY + "（先调 POST /block-demo/register）";
        }
        double utilization = info.getQueueSize() * 100.0 / info.getConfig().getQueueCapacity();
        return "key=" + info.getKey() + ", running=" + info.isRunning()
                + ", queue=" + info.getQueueSize() + "/" + info.getConfig().getQueueCapacity()
                + "（利用率 " + String.format("%.1f%%", utilization) + "）"
                + ", consumers=" + info.getActiveConsumers();
    }
}
