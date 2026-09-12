package com.dynamicbatch.example;

import ch.qos.logback.classic.Level;
import com.dynamicbatch.common.util.JsonUtil;
import com.dynamicbatch.core.BatchProcessor;
import com.dynamicbatch.core.BatchWorkerGroup;
import com.dynamicbatch.core.pojo.SpoolConfigPOJO;
import com.dynamicbatch.core.pojo.StatsConfigPOJO;
import com.dynamicbatch.core.stats.StatsListeners;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 统计采集演示（纯 main，不启 Spring）：匀速提交 → 每 30s 采集一次 → 快照 JSON 打日志。
 *
 * <p>每份快照包含区间增量（submitCount / callbackCount / bucketCounts）、累计两组字段
 * 与算好的 submitQps / callbackQps / tp；Ctrl+C 触发 shutdown hook 排空落盘后退出。
 */
public class StatsDemo {

    private static final Logger log = LoggerFactory.getLogger(StatsDemo.class);

    private static final String GROUP = "stats_demo";
    /** 生产速率：每 5ms 提交一条 = 200 条/秒 */
    private static final long SUBMIT_INTERVAL_MS = 5L;

    public static void main(String[] args) throws InterruptedException {
        // 纯 main 不走 Spring，application.yml 的 logging.level 不生效，直接调 root 到 INFO
        ((ch.qos.logback.classic.Logger) LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME)).setLevel(Level.INFO);

        BatchProcessor processor = new BatchProcessor();

        // 快照出口：全局注册一次，服务所有组——把每份采集快照打成 JSON 打日志
        StatsListeners.register(snapshot ->
                log.info("stats snapshot: {}", JsonUtil.toJson(snapshot)));

        processor.registerGroup(GROUP,
                BatchWorkerGroup.builder(String.class,
                                SpoolConfigPOJO.builder("spool-stats-demo").build(),
                                batch -> { })   // 回调啥也不干
                        .partitionCount(2)
                        .queueCapacity(1024)
                        .batchSize(100)
                        .maxWaitMs(1000)
                        .statsConfig(StatsConfigPOJO.builder()
                                .collectIntervalMillis(30_000L)
                                .percentiles(0.5, 0.9, 0.95, 0.99,0.999)
                                .rtBuckets(1, 2, 5, 10, 20, 50, 100, 200, 500, 1000, 2000)
                                .build())
                        .build());

        // 匀速生产者：固定速率提交，routingKey 取模路由到不同分区
        AtomicLong seq = new AtomicLong();
        ScheduledExecutorService producer = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "stats-demo-producer");
            t.setDaemon(true);
            return t;
        });
        producer.scheduleAtFixedRate(() -> {
            long i = seq.getAndIncrement();
            processor.submit(GROUP, "key-" + (i % 8), "payload-" + i);
        }, 0, SUBMIT_INTERVAL_MS, TimeUnit.MILLISECONDS);
        log.info("producer started, {} msg/s, ctrl+c to stop", 1000 / SUBMIT_INTERVAL_MS);

        Runtime.getRuntime().addShutdownHook(new Thread(processor::shutdown, "stats-demo-shutdown"));
        Thread.currentThread().join();   // 阻塞主线程，生产与采集持续运行
    }
}
