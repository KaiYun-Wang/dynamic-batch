package com.dynamicbatch.example.controller;

import com.dynamicbatch.core.BatchProcessor;
import com.dynamicbatch.core.BatchWorkerGroup;
import com.dynamicbatch.core.pojo.SpoolConfigPOJO;
import com.dynamicbatch.spool.DiskUsage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import javax.annotation.PostConstruct;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.TimeoutException;

/**
 * 分区演示入口：启动时注册一个 3 分区的 Worker 组。
 *
 * <pre>
 * # 提交单条
 * curl "http://localhost:8080/test-batch/submit?n=42"
 *
 * # 批量投递：data 为 "1".."count"，routingKey 同 data（哈希散到各分区并行消费）
 * curl "http://localhost:8080/test-batch/batch-submit?count=1000"
 *
 * # 分区数运行时调整（三步按序调用，暂停/恢复为异步意图）：3 分区扩到 5 或缩到 2
 * curl "http://localhost:8080/test-batch/pause"
 * curl "http://localhost:8080/test-batch/resize?newSize=5&timeoutMs=30000"
 * curl "http://localhost:8080/test-batch/resume"
 *
 * # 容量告警演示：backlog_demo 组预算 400MB（一个预扩容文件 ~80MB），
 * # flush 回调 sleep 5s 拖慢消费使积压保持；灌一次后每次巡检（5s）都会告警
 * curl "http://localhost:8080/test-batch/backlog/submit"
 * curl "http://localhost:8080/test-batch/backlog/usage"
 *
 * # 限流演示：rate_limit_demo 组限速 5 条/秒，投 20 条约 4 秒匀速消化（看 flush 日志节奏）
 * curl "http://localhost:8080/test-batch/rate-limit/submit?count=20"
 * </pre>
 */
@RestController
@RequestMapping("/test-batch")
public class BatchTestController {

    private static final Logger log = LoggerFactory.getLogger(BatchTestController.class);

    private static final String GROUP_KEY = "partition_demo";

    /** 容量告警演示组：预算 40MB（小于一个预扩容文件 ~80MB，落盘即超阈值），1 秒滚动 */
    private static final String BACKLOG_GROUP_KEY = "backlog_demo";
    private static final long BACKLOG_MAX_SIZE_BYTES = 400L * 1024 * 1024;

    /** 限流演示组：限速 5 条/秒，投递 count 条后可观察分发线程匀速取数消费 */
    private static final String RATE_LIMIT_GROUP_KEY = "rate_limit_demo";
    private static final int RATE_LIMIT_PER_SECOND = 5;

    private final BatchProcessor batchProcessor;

    public BatchTestController(BatchProcessor batchProcessor) {
        this.batchProcessor = batchProcessor;
    }

    @PostConstruct
    public void registerDemoGroup() {
        try {
            batchProcessor.registerGroup(GROUP_KEY,
                    BatchWorkerGroup.builder(String.class,
                            SpoolConfigPOJO.builder("spool-demo-partition").build(),
                            batch -> log.info("flushed {} items: {}", batch.size(), batch))
                            .queueCapacity(200)
                            .batchSize(5)
                            .maxWaitMs(2000)
                            .partitionCount(3)
                            .build());
            log.info("test-batch 组已注册, group={}, partitions=3", GROUP_KEY);
        } catch (IllegalStateException e) {
            log.warn("test-batch 组已存在, 跳过注册: {}", e.getMessage());
        }
        try {
            batchProcessor.registerGroup(BACKLOG_GROUP_KEY,
                    BatchWorkerGroup.builder(String.class,
                            SpoolConfigPOJO.builder("spool-demo-backlog")
                                    .maxSizeBytes(BACKLOG_MAX_SIZE_BYTES)
                                    .rollCycleMillis(1000)
                                    .cleanupIntervalMs(10000)
                                    .build(),
                            batch -> {
                                // 拖慢消费：worker 队列堆满后调度器阻塞投递、停止从 Spool 取数，磁盘积压得以保持
                                try {
                                    Thread.sleep(5000);
                                } catch (InterruptedException e) {
                                    Thread.currentThread().interrupt();
                                }
                                log.info("backlog flushed {} items", batch.size());
                            })
                            .build());
            log.info("容量告警演示组已注册, group={}, maxSizeBytes=40MB", BACKLOG_GROUP_KEY);
        } catch (IllegalStateException e) {
            log.warn("容量告警演示组已存在, 跳过注册: {}", e.getMessage());
        }
        try {
            batchProcessor.registerGroup(RATE_LIMIT_GROUP_KEY,
                    BatchWorkerGroup.builder(String.class,
                            SpoolConfigPOJO.builder("spool-demo-rate-limit").build(),
                            batch -> log.info("rate-limit flushed {} items: {}", batch.size(), batch))
                            .batchSize(5)
                            .maxWaitMs(1000)
                            .rateLimit(RATE_LIMIT_PER_SECOND)
                            .build());
            log.info("限流演示组已注册, group={}, rateLimitPerSecond={}", RATE_LIMIT_GROUP_KEY, RATE_LIMIT_PER_SECOND);
        } catch (IllegalStateException e) {
            log.warn("限流演示组已存在, 跳过注册: {}", e.getMessage());
        }
    }

    @GetMapping("/submit")
    public String submit(@RequestParam int n) {
        String data = String.valueOf(n);
        boolean ok = batchProcessor.submit(GROUP_KEY, data, data);
        return (ok ? "ok" : "fail") + ": " + data;
    }

    /**
     * 批量投递演示：投递 count 条，data 为 "1".."count"，routingKey 同 data。
     * submit 仅写 Spool（磁盘缓冲）即返回，消费由分发线程异步进行；
     * count 上限 10 万，防止误操作打爆磁盘预算。
     */
    @GetMapping("/batch-submit")
    public Map<String, Object> batchSubmit(@RequestParam int count) {
        if (count <= 0 || count > 100_000) {
            throw new IllegalArgumentException("count must be in (0, 100000], got " + count);
        }
        long start = System.currentTimeMillis();
        int accepted = 0;
        for (int i = 1; i <= count; i++) {
            String data = String.valueOf(i);
            if (batchProcessor.submit(GROUP_KEY, data, data)) {
                accepted++;
            }
        }
        long costMs = System.currentTimeMillis() - start;
        log.info("batch submit done: total={}, accepted={}, costMs={}", count, accepted, costMs);

        Map<String, Object> result = new LinkedHashMap<>();
        result.put("total", count);
        result.put("accepted", accepted);
        result.put("rejected", count - accepted);
        result.put("costMs", costMs);
        return result;
    }

    /**
     * 暂停调度器：置暂停意图后立即返回，分发线程停止取数；
     * 分区 Worker 自然消化手头队列，submit 照常落盘。
     */
    @GetMapping("/pause")
    public Map<String, Object> pause() {
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("groupKey", GROUP_KEY);
        try {
            batchProcessor.pauseDispatcher(GROUP_KEY);
            result.put("ok", true);
        } catch (RuntimeException e) {
            result.put("ok", false);
            result.put("error", e.getMessage());
        }
        return result;
    }

    /**
     * 改变分区数：阻塞等待全员排空后原子替换。前置要求调度器已暂停（先调 /pause）。
     */
    @GetMapping("/resize")
    public Map<String, Object> resize(@RequestParam int newSize,
                                      @RequestParam(defaultValue = "30000") long timeoutMs) {
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("groupKey", GROUP_KEY);
        result.put("newSize", newSize);
        try {
            batchProcessor.resizePartitions(GROUP_KEY, newSize, timeoutMs);
            result.put("ok", true);
        } catch (TimeoutException e) {
            result.put("ok", false);
            result.put("error", "timeout: " + e.getMessage());
        } catch (RuntimeException e) {
            result.put("ok", false);
            result.put("error", e.getMessage());
        }
        return result;
    }

    /**
     * 恢复调度器：置运行意图后立即返回，自动消化暂停期间的磁盘积压。
     */
    @GetMapping("/resume")
    public Map<String, Object> resume() {
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("groupKey", GROUP_KEY);
        try {
            batchProcessor.resumeDispatcher(GROUP_KEY);
            result.put("ok", true);
        } catch (RuntimeException e) {
            result.put("ok", false);
            result.put("error", e.getMessage());
        }
        return result;
    }

    /**
     * 限流演示：往 rate_limit_demo 组投递 count 条（限速 5 条/秒）。
     * submit 仅写 Spool 即返回，分发线程按限速匀速取数消费，
     * count=20 约需 4 秒消化完，看 flush 日志节奏。
     */
    @GetMapping("/rate-limit/submit")
    public Map<String, Object> rateLimitSubmit(@RequestParam int count) {
        if (count <= 0 || count > 100_000) {
            throw new IllegalArgumentException("count must be in (0, 100000], got " + count);
        }
        long start = System.currentTimeMillis();
        int accepted = 0;
        for (int i = 1; i <= count; i++) {
            String data = String.valueOf(i);
            if (batchProcessor.submit(RATE_LIMIT_GROUP_KEY, data, data)) {
                accepted++;
            }
        }
        long costMs = System.currentTimeMillis() - start;
        log.info("rate limit submit done: total={}, accepted={}, costMs={}", count, accepted, costMs);

        Map<String, Object> result = new LinkedHashMap<>();
        result.put("groupKey", RATE_LIMIT_GROUP_KEY);
        result.put("rateLimitPerSecond", RATE_LIMIT_PER_SECOND);
        result.put("total", count);
        result.put("accepted", accepted);
        result.put("costMs", costMs);
        return result;
    }

    /**
     * 灌积压数据（容量告警演示）：往 backlog_demo 组投递 1 条。预算 40MB，一个预扩容
     * 文件 ~80MB 即超阈值，灌一次后每次巡检（5s）都会触发 SPOOL_CAPACITY 告警；
     * flush 回调 sleep 5s 拖慢消费使积压保持。预算满后后续 submit 被拒并触发 OFFER_FAILED 告警，
     * 测试完删除 spool-demo-backlog 目录。
     */
    @GetMapping("/backlog/submit")
    public Map<String, Object> backlogSubmit() {
        String data = "backlog-" + System.nanoTime();
        boolean ok = batchProcessor.submit(BACKLOG_GROUP_KEY, data, data);

        Map<String, Object> result = new LinkedHashMap<>();
        result.put("groupKey", BACKLOG_GROUP_KEY);
        result.put("data", data);
        result.put("accepted", ok);
        return result;
    }

    /**
     * 查看积压测试组当前磁盘占用（即时分桶统计，与容量告警同口径）。
     */
    @GetMapping("/backlog/usage")
    public Map<String, Object> backlogUsage() {
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("groupKey", BACKLOG_GROUP_KEY);
        DiskUsage usage = batchProcessor.getSpoolUsage(BACKLOG_GROUP_KEY);
        if (usage == null) {
            result.put("ok", false);
            result.put("error", "组不存在或未启动");
            return result;
        }
        result.put("totalBytes", usage.getTotalBytes());
        result.put("maxSizeBytes", usage.getMaxSizeBytes());
        result.put("percent", usage.getMaxSizeBytes() == Long.MAX_VALUE ? 0
                : (int) Math.min(100, usage.getTotalBytes() * 100 / usage.getMaxSizeBytes()));
        result.put("consumedBytes", usage.getConsumedBytes());
        result.put("pendingBytes", usage.getPendingBytes());
        return result;
    }
}
