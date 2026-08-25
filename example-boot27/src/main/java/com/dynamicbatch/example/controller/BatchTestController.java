package com.dynamicbatch.example.controller;

import com.dynamicbatch.core.BatchProcessor;
import com.dynamicbatch.core.BatchWorkerGroup;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import javax.annotation.PostConstruct;

/**
 * 分区演示入口：启动时注册一个 3 分区的 Worker 组。
 *
 * <pre>
 * # 提交单条
 * curl "http://localhost:8080/test-batch/submit?n=42"
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
                    BatchWorkerGroup.builder(String.class, batch -> {
                        String first = batch.get(0);
                        String last = batch.get(batch.size() - 1);
                        log.info("flushed size={}, range=[{}..{}], thread={}",
                                batch.size(), first, last,
                                Thread.currentThread().getName());
                    })
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
}