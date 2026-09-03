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
 * # 暂停消费：分发线程停止取数、各分区排空后待命；之后 submit 照常返回 ok（数据落盘不消费），
 * # 可观察「只进不出」的削峰形态；resume 后积压全部消化
 * curl "http://localhost:8080/test-batch/pause?timeoutMs=5000"
 *
 * # 恢复消费
 * curl "http://localhost:8080/test-batch/resume?timeoutMs=5000"
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
     * 暂停消费演示：分发线程停止从 Spool 取数，各分区排空手头批次与队列残留后待命。
     * 暂停期间 /submit、/batch-submit 照常返回 ok（数据落盘不消费）；恢复后自动消化积压。
     * 超时失败已自动回滚（整组保持消费），可加大 timeoutMs 重试。
     *
     * @deprecated 临时演示接口，热更新（resize）落地后随 pauseGroup 一并删除。
     */
    @Deprecated
    @GetMapping("/pause")
    public String pause(@RequestParam(defaultValue = "5000") long timeoutMs) {
        try {
            batchProcessor.pauseGroup(GROUP_KEY, timeoutMs);
            return "ok: " + GROUP_KEY + " paused (submit still writes to spool)";
        } catch (TimeoutException e) {
            return "fail: pause timeout, rolled back: " + e.getMessage();
        }
    }

    /**
     * 恢复消费演示：各分区恢复运行后分发线程从磁盘读位置续读，排空暂停期间积压。
     * 幂等可重试。
     *
     * @deprecated 临时演示接口，热更新（resize）落地后随 resumeGroup 一并删除。
     */
    @Deprecated
    @GetMapping("/resume")
    public String resume(@RequestParam(defaultValue = "5000") long timeoutMs) {
        try {
            batchProcessor.resumeGroup(GROUP_KEY, timeoutMs);
            return "ok: " + GROUP_KEY + " resumed";
        } catch (TimeoutException e) {
            return "fail: resume timeout, retry allowed: " + e.getMessage();
        }
    }
}