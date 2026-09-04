package com.dynamicbatch.example.controller;

import com.dynamicbatch.core.BatchProcessor;
import com.dynamicbatch.core.BatchWorker;
import com.dynamicbatch.core.BatchWorkerGroup;
import com.dynamicbatch.core.pojo.SpoolConfigPOJO;
import com.dynamicbatch.example.config.BatchProcessorConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import javax.annotation.PostConstruct;
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
 * curl -X POST "http://localhost:8080/fail-test/offer-full"           # 队列满背压：仅 debug 日志，不上报告警（告警收敛后）
 * curl -X POST "http://localhost:8080/fail-test/offer-type-mismatch"  # 入队失败：类型不匹配
 * curl -X POST "http://localhost:8080/fail-test/offer-stopped"        # 入队失败：worker 已关闭
 * curl -X POST "http://localhost:8080/fail-test/offer-spool-full"     # 入队失败：spool 磁盘预算满
 * curl -X POST "http://localhost:8080/fail-test/flush-error"          # 批次失败：failureHandler 接管
 * curl -X POST "http://localhost:8080/fail-test/flush-error-loss"     # 批次失败：数据丢失风险
 * </pre>
 * 告警发送结果见应用日志（钉钉接口响应码）。
 */
@RestController
@RequestMapping("/fail-test")
public class NotifyFailureTestController {

    private static final Logger log = LoggerFactory.getLogger(NotifyFailureTestController.class);

    /** 队列满场景使用的 processor worker：demo_item_insert 容量 100、单消费线程 */
    private static final String FULL_WORKER = BatchProcessorConfiguration.DEMO_INSERT;

    /** Spool 预算打满演示组：maxSizeBytes 16MB 低于 Chronicle 初始块（~80MB），首块落盘即超预算，后续 submit 全部被拒 */
    private static final String SPOOL_FULL_GROUP = "spool_full_demo";

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

    /** 注册 Spool 预算打满演示组：flush 空操作，仅用于触发 offer_failed 告警 */
    @PostConstruct
    public void registerSpoolFullDemoGroup() {
        try {
            batchProcessor.registerGroup(SPOOL_FULL_GROUP,
                    BatchWorkerGroup.builder(String.class,
                                    SpoolConfigPOJO.builder("spool-demo-full")
                                            .maxSizeBytes(16L * 1024 * 1024)
                                            .build(),
                                    batch -> { })
                            .build());
            log.info("spool_full_demo 组已注册, maxSizeBytes=16MB");
        } catch (IllegalStateException e) {
            log.warn("spool_full_demo 组已存在, 跳过注册: {}", e.getMessage());
        }
    }

    /** 构造一个消费极慢的 worker 并启动：flush 睡 2s，队列容量 10，保证提交必满 */
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
     * 队列满背压演示：队列满是链路中间的正常背压（数据还在磁盘缓冲），不上报告警，仅 debug 日志。
     * 8 线程并发向容量 10、flush 睡 2s 的慢消费 worker 灌 200 条：
     * 消费线程卡在 flush 里，队列瞬间打满，submit 返回 false。
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
        return "并发提交完成：成功=" + ok.get() + "，队列满背压拒绝=" + failed.get()
                + "（队列满是正常背压不上报告警，仅 debug 日志；当前队列剩余=" + slowConsumerWorker.getQueueSize() + "）";
    }

    /** 入队失败：类型不匹配（向 DemoItem worker 组提交 String） */
    @PostMapping("/offer-type-mismatch")
    public String offerTypeMismatch() {
        boolean ok = batchProcessor.submit(FULL_WORKER, "type-mismatch", "不是 DemoItem 类型");
        return "提交返回=" + ok + "（应为 false，已触发类型不匹配告警）";
    }

    /** 入队失败：worker 已关闭（自持 worker 未 start） */
    @PostMapping("/offer-stopped")
    public String offerStopped() {
        boolean ok = stoppedWorker.submit("data");
        return "提交返回=" + ok + "（应为 false，已触发 worker 已关闭告警）";
    }

    /**
     * 入队失败：spool 磁盘预算满。数据被挡在门外（业务方拿到 false，未落盘），属会丢数据的断点，告警。
     * 演示组 maxSizeBytes=16MB 低于 Chronicle 初始块（~80MB），首块落盘即超预算，此后 submit 全部被拒。
     */
    @PostMapping("/offer-spool-full")
    public String offerSpoolFull() {
        int total = 100;
        int accepted = 0;
        for (int i = 0; i < total; i++) {
            if (batchProcessor.submit(SPOOL_FULL_GROUP, "k-" + i, "data-" + i)) {
                accepted++;
            }
        }
        return "提交 " + total + " 条：成功=" + accepted + "，被拒=" + (total - accepted)
                + "（被拒触发 offer_failed 告警：spool 拒绝（磁盘预算满或暂存队列满超时）；offer_failed 静默期 30 秒，30 秒内只发一条）";
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
