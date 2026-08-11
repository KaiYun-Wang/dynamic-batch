package com.dynamicbatch.spring.autoconfigure;

import com.dynamicbatch.core.BatchProcessor;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * 自动装配：注册 BatchProcessor 单例 Bean。
 *
 * <p>destroyMethod="shutdown"：容器关闭时回调 BatchProcessor.shutdown() 刷掉剩余数据。
 * <p>@ConditionalOnMissingBean：用户自定义 BatchProcessor Bean 时，本配置自动让位。
 */
@Configuration(proxyBeanMethods = false)
public class BatchProcessorAutoConfiguration {

    @Bean(destroyMethod = "shutdown")
    @ConditionalOnMissingBean(BatchProcessor.class)
    public BatchProcessor batchProcessor() {
        return new BatchProcessor();
    }
}
