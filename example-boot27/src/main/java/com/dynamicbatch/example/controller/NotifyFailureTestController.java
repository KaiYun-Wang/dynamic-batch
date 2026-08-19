package com.dynamicbatch.example.controller;

import com.dynamicbatch.core.BatchProcessor;
import com.dynamicbatch.core.BatchWorker;
import com.dynamicbatch.example.config.BatchProcessorConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 通知失败场景自测入口：触发各类失败，验证钉钉告警。
 *
 * <p>填好 application.yml 中的 dynamic-batch.notify.platforms 后启动：
 * <pre>
 * curl -X POST "http://localhost:8080/fail-test/offer-full"           # 入队失败：队列满超时
 * curl -X POST "http://localhost:8080/fail-test/offer-type-mismatch"  # 入队失败：类型不匹配
 * curl -X POST "http://localhost:8080/fail-test/offer-stopped"        # 入队失败：worker 已关闭
 * curl -X POST "http://localhost:8080/fail-test/flush-error"          # 批次失败：failureHandler 接管
 * curl -X POST "http://localhost:8080/fail-test/flush-error-loss"     # 批次失败：数据丢失风险
 * </pre>
 * 告警发送结果见应用日志（钉钉接口响应码）。
 */
@RestController
@RequestMapping("/fail-test")
public class NotifyFailureTestController {

    private static final Logger log = LoggerFactory.getLogger(NotifyFailureTestController.class);

    /** 队列满场景使用的 processor worker：demo_item_insert 容量 100、单消费线程、入队超时 100ms */
    private static final String FULL_WORKER = BatchProcessorConfiguration.DEMO_INSERT;

    private final BatchProcessor batchProcessor;

    /** 慢消费 worker（容量 10、flush 睡 2s），用于稳定触发队列满超时，仅自测用 */
    private final BatchWorker<String> slowConsumerWorker;
    /** flush 回调必失败的 worker（有 failureHandler 接管），仅自测用，不注册进 processor */
    private final BatchWorker<String> flushFailWorker;
    /** flush 回调必失败的 worker（无 failureHandler，数据丢失风险），仅自测用 */
    private final BatchWorker<String> flushFailLossWorker;
    /** 构造后未 start 的 worker（running=false），仅自测用 */
    private final BatchWorker<String> stoppedWorker;

    public NotifyFailureTestController(BatchProcessor batchProcessor) {
        this.batchProcessor = batchProcessor;
        this.slowConsumerWorker = buildSlowConsumerWorker();
        this.flushFailWorker = buildFlushFailWorker("test_flush_error", true);
        this.flushFailLossWorker = buildFlushFailWorker("test_flush_error_loss", false);
        this.stoppedWorker = BatchWorker.builder(String.class, batch -> { }).build();
        this.stoppedWorker.setName("test_offer_stopped");
    }

    /** 构造一个消费极慢的 worker 并启动：flush 睡 2s，队列容量 10，入队超时 10ms，保证提交必满 */
    private static BatchWorker<String> buildSlowConsumerWorker() {
        BatchWorker<String> worker = BatchWorker.builder(String.class, batch -> {
            try {
                Thread.sleep(2000); // 模拟慢落库：消费线程卡住，队列持续打满
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        })
                .queueCapacity(10)
                .batchSize(5)
                .maxWaitMs(100)
                .offerTimeoutMs(10)
                .build();
        worker.setName("test_slow_consumer");
        worker.start();
        return worker;
    }

    /** 构造一个 flush 回调必失败的 worker 并启动；withHandler=true 时挂 failureHandler */
    private static BatchWorker<String> buildFlushFailWorker(String name, boolean withHandler) {
        BatchWorker.Builder<String> builder = BatchWorker.builder(String.class, batch -> {
            throw new RuntimeException("模拟 flush 失败，批次大小=" + batch.size());
        })
                .batchSize(2)
                .maxWaitMs(300);
        if (withHandler) {
            builder.failureHandler(failed -> log.warn("[{}] failureHandler 接管 {} 条", name, failed.size()));
        }
        BatchWorker<String> worker = builder.build();
        worker.setName(name);
        worker.start();
        return worker;
    }

    /**
     * 入队失败：队列满超时。
     * 8 线程并发向容量 10、flush 睡 2s 的慢消费 worker 灌 200 条：
     * 消费线程卡在 flush 里，队列瞬间打满，submit 阻塞 10ms 超时返回 false。
     */
    @PostMapping("/offer-full")
    public String offerFull() throws InterruptedException {
        int threads = 8;
        int perThread = 25;
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch startLatch = new CountDownLatch(1);
        AtomicInteger ok = new AtomicInteger();
        AtomicInteger failed = new AtomicInteger();
        for (int i = 0; i < threads; i++) {
            pool.execute(() -> {
                try {
                    startLatch.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                }
                for (int j = 0; j < perThread; j++) {
                    boolean success = slowConsumerWorker.submit("data-" + j);
                    if (success) {
                        ok.incrementAndGet();
                    } else {
                        failed.incrementAndGet();
                    }
                }
            });
        }
        startLatch.countDown();
        pool.shutdown();
        pool.awaitTermination(30, TimeUnit.SECONDS);
        return "并发提交完成：成功=" + ok.get() + "，入队超时失败=" + failed.get()
                + "（失败已触发钉钉告警，当前队列剩余=" + slowConsumerWorker.getQueueSize() + "）";
    }

    /** 入队失败：类型不匹配（向 DemoItem worker 提交 String） */
    @PostMapping("/offer-type-mismatch")
    public String offerTypeMismatch() {
        boolean ok = batchProcessor.submit(FULL_WORKER, "不是 DemoItem 类型");
        return "提交返回=" + ok + "（应为 false，已触发类型不匹配告警）";
    }

    /** 入队失败：worker 已关闭（自持 worker 未 start） */
    @PostMapping("/offer-stopped")
    public String offerStopped() {
        boolean ok = stoppedWorker.submit("data");
        return "提交返回=" + ok + "（应为 false，已触发 worker 已关闭告警）";
    }

    /** 批次执行失败：flush 抛异常，failureHandler 接管（消息应显示无丢失风险） */
    @PostMapping("/flush-error")
    public String flushError() throws InterruptedException {
        flushFailWorker.submit("a");
        flushFailWorker.submit("b"); // 攒满 2 条立即 flush → 抛异常 → failureHandler 接管
        Thread.sleep(1500);          // 等异步通知发出
        return "已触发 flush 失败（failureHandler 接管），查看钉钉/日志";
    }

    /** 批次执行失败：flush 抛异常且无 failureHandler（消息应显示数据丢失风险） */
    @PostMapping("/flush-error-loss")
    public String flushErrorLoss() throws InterruptedException {
        flushFailLossWorker.submit("a");
        flushFailLossWorker.submit("b"); // 攒满 2 条立即 flush → 抛异常 → 无兜底
        Thread.sleep(1500);
        return "已触发 flush 失败（无 failureHandler，数据丢失风险），查看钉钉/日志";
    }
}
