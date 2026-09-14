package com.dynamicbatch.example.boot35.controller;

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
 * Boot 3.3.5 冒烟：与 example-boot27 的 BatchTestController 对齐（jakarta + 独立 spool 目录）。
 *
 * <pre>
 * curl "http://localhost:8083/test-batch/submit?n=42"
 * curl "http://localhost:8083/test-batch/batch-submit?count=1000"
 * curl "http://localhost:8083/test-batch/pause"
 * curl "http://localhost:8083/test-batch/resize?newSize=5&timeoutMs=30000"
 * curl "http://localhost:8083/test-batch/resume"
 * curl "http://localhost:8083/test-batch/backlog/submit"
 * curl "http://localhost:8083/test-batch/backlog/usage"
 * curl "http://localhost:8083/test-batch/rate-limit/submit?count=20"
 * </pre>
 */
@RestController
@RequestMapping("/test-batch")
public class BatchTestController {

    private static final Logger log = LoggerFactory.getLogger(BatchTestController.class);

    private static final String GROUP_KEY = "partition_demo";
    private static final String BACKLOG_GROUP_KEY = "backlog_demo";
    private static final long BACKLOG_MAX_SIZE_BYTES = 400L * 1024 * 1024;
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
                            SpoolConfigPOJO.builder("spool-boot35-partition").build(),
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
                            SpoolConfigPOJO.builder("spool-boot35-backlog")
                                    .maxSizeBytes(BACKLOG_MAX_SIZE_BYTES)
                                    .rollCycleMillis(1000)
                                    .cleanupIntervalMs(10000)
                                    .build(),
                            batch -> {
                                try {
                                    Thread.sleep(5000);
                                } catch (InterruptedException e) {
                                    Thread.currentThread().interrupt();
                                }
                                log.info("backlog flushed {} items", batch.size());
                            })
                            .build());
            log.info("容量告警演示组已注册, group={}, maxSizeBytes=400MB", BACKLOG_GROUP_KEY);
        } catch (IllegalStateException e) {
            log.warn("容量告警演示组已存在, 跳过注册: {}", e.getMessage());
        }
        try {
            batchProcessor.registerGroup(RATE_LIMIT_GROUP_KEY,
                    BatchWorkerGroup.builder(String.class,
                            SpoolConfigPOJO.builder("spool-boot35-rate-limit").build(),
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
