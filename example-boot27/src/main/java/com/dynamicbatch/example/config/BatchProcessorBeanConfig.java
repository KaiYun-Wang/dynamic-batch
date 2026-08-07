package com.dynamicbatch.example.config;

import com.dynamicbatch.core.BatchProcessor;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * 手动提供 BatchProcessor Bean（现阶段不用 starter）。
 */
@Configuration
public class BatchProcessorBeanConfig {

    @Bean(destroyMethod = "shutdown")
    public BatchProcessor batchProcessor() {
        return new BatchProcessor();
    }
}
