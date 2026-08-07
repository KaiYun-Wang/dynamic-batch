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

    public static final String DEMO_INSERT = "DemoItem:insert";

    private final BatchProcessor batchProcessor;

    public BatchProcessorConfiguration(BatchProcessor batchProcessor) {
        this.batchProcessor = batchProcessor;
    }

    @PostConstruct
    public void registerWriters() {
        batchProcessor.register(DEMO_INSERT, DemoItem.class, new BatchWorker<>(
                100,   // queueCapacity
                5,     // batchSize
                2000,  // maxWaitMs
                100,   // offerTimeoutMs
                batch -> log.info("demo flush ok, size={}, first={}", batch.size(), batch.get(0)),
                failed -> log.error("demo flush failed, size={}", failed.size())
        ));
        log.info("批处理器注册完成: {}", DEMO_INSERT);
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
