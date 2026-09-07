package com.dynamicbatch.spring.autoconfigure;

import com.dynamicbatch.core.BatchProcessor;
import com.dynamicbatch.spring.actuator.BatchEndpoint;
import org.springframework.boot.actuate.autoconfigure.endpoint.condition.ConditionalOnAvailableEndpoint;
import org.springframework.boot.autoconfigure.AutoConfigureAfter;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.autoconfigure.condition.ConditionalOnWebApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Actuator 端点自动装配：servlet web 应用且 classpath 存在 actuator 时注册 {@link BatchEndpoint}。
 *
 * <p>依赖 {@link BatchProcessor} Bean；端点可见性还受 exposure 配置与
 * {@code dynamic-batch.actuator.enabled} 控制。条件不满足时整体让位，不加载端点类。
 */
@Configuration(proxyBeanMethods = false)
@AutoConfigureAfter(BatchProcessorAutoConfiguration.class)
@ConditionalOnClass(name = "org.springframework.boot.actuate.endpoint.web.annotation.RestControllerEndpoint")
@ConditionalOnWebApplication(type = ConditionalOnWebApplication.Type.SERVLET)
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
