package com.dynamicbatch.example.controller;

import com.dynamicbatch.core.BatchProcessor;
import com.dynamicbatch.core.BatchWorkerGroup;
import com.dynamicbatch.core.pojo.SpoolConfigPOJO;
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
 * # 分区数热更新：3 分区扩到 5 或缩到 2
 * curl "http://localhost:8080/test-batch/resize?newSize=5&timeoutMs=30000"
 * </pre>
 */
@RestController
@RequestMapping("/test-batch")
public class BatchTestController {

    private static final Logger log = LoggerFactory.getLogger(BatchTestController.class);

    private static final String GROUP_KEY = "partition_demo";

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
                            .offerTimeoutMs(200)
                            .partitionCount(3)
                            .build());
            log.info("test-batch 组已注册, group={}, partitions=3", GROUP_KEY);
        } catch (IllegalStateException e) {
            log.warn("test-batch 组已存在, 跳过注册: {}", e.getMessage());
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
     * 分区数热更新演示。
     */
    @GetMapping("/resize")
    public Map<String, Object> resize(@RequestParam int newSize,
                                      @RequestParam(defaultValue = "30000") long timeoutMs) {
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("groupKey", GROUP_KEY);
        result.put("newSize", newSize);
        try {
            batchProcessor.resizeGroup(GROUP_KEY, newSize, timeoutMs);
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
}
