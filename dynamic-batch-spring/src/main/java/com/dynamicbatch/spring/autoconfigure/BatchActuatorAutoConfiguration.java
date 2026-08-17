package com.dynamicbatch.spring.autoconfigure;

import com.dynamicbatch.core.BatchProcessor;
import com.dynamicbatch.spring.actuator.BatchEndpoint;
import org.springframework.boot.actuate.autoconfigure.endpoint.condition.ConditionalOnAvailableEndpoint;
import org.springframework.boot.autoconfigure.AutoConfigureAfter;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Actuator 端点自动装配：classpath 存在 Actuator 且用户暴露端点时注册 {@link BatchEndpoint}。
 */
@Configuration(proxyBeanMethods = false)
@AutoConfigureAfter(BatchProcessorAutoConfiguration.class)
@ConditionalOnClass(name = "org.springframework.boot.actuate.endpoint.annotation.Endpoint")
@ConditionalOnBean(BatchProcessor.class)
@ConditionalOnProperty(prefix = "dynamic-batch.actuator", name = "enabled", havingValue = "true", matchIfMissing = true)
public class BatchActuatorAutoConfiguration {

    @Bean
    @ConditionalOnMissingBean
    @ConditionalOnAvailableEndpoint
    public BatchEndpoint batchEndpoint(BatchProcessor batchProcessor) {
        return new BatchEndpoint(batchProcessor);
    }
}
