package com.dynamicbatch.example.config;

import com.dynamicbatch.core.BatchProcessor;
import com.dynamicbatch.core.BatchWorker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.springframework.context.annotation.Configuration;

import javax.annotation.PostConstruct;

/**
 * 对齐 yikekong 的 BatchWriterConfiguration：
 * 注入 BatchProcessor，在 @PostConstruct 里 register。
 */
@Configuration
public class BatchProcessorConfiguration {

    private static final Logger log = LoggerFactory.getLogger(BatchProcessorConfiguration.class);

    public static final String DEMO_INSERT = "demo_item_insert";
    public static final String DEMO_UPDATE = "demo_item_update";

    private final BatchProcessor batchProcessor;

    public BatchProcessorConfiguration(BatchProcessor batchProcessor) {
        this.batchProcessor = batchProcessor;
    }

    @PostConstruct
    public void registerWriters() {
        batchProcessor.register(DEMO_INSERT,
                BatchWorker.builder(DemoItem.class, batch -> log.info("demo insert flush ok, size={}, first={}", batch.size(), batch.get(0)))
                        .queueCapacity(100)
                        .batchSize(5)
                        .maxWaitMs(2000)
                        .offerTimeoutMs(100)
                        .failureHandler(failed -> log.error("demo insert flush failed, size={}", failed.size()))
                        .build()
        );
        batchProcessor.register(DEMO_UPDATE,
                BatchWorker.builder(DemoItem.class, batch -> log.info("demo update flush ok, size={}, first={}", batch.size(), batch.get(0)))
                        .queueCapacity(256)
                        .batchSize(20)
                        .maxWaitMs(500)
                        .offerTimeoutMs(50)
                        .consumers(2)
                        .failureHandler(failed -> log.error("demo update flush failed, size={}", failed.size()))
                        .build()
        );
        log.info("批处理器注册完成: {}, {}", DEMO_INSERT, DEMO_UPDATE);
    }

    /** 示例数据类型 */
    public static class DemoItem {
        private final String id;

        public DemoItem(String id) {
            this.id = id;
        }

        public String getId() {
            return id;
        }

        @Override
        public String toString() {
            return "DemoItem{id='" + id + "'}";
        }
    }
}
