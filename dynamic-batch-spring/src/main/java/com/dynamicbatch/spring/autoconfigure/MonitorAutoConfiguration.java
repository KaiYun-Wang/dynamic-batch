package com.dynamicbatch.spring.autoconfigure;

import com.dynamicbatch.core.BatchProcessor;
import com.dynamicbatch.spring.monitor.SpoolCapacityMonitor;
import com.dynamicbatch.spring.properties.NotifyProperties;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * 容量巡检自动装配：注册 {@link SpoolCapacityMonitor} Bean。
 *
 * <p>巡检是否启动由 notify-items 决定：未登记/未启用时 Bean 空转不起线程。
 * 无 {@link BatchProcessor} Bean 时本配置让位。
 */
@Configuration(proxyBeanMethods = false)
@EnableConfigurationProperties(NotifyProperties.class)
public class MonitorAutoConfiguration {

    @Bean(destroyMethod = "stop")
    @ConditionalOnMissingBean
    @ConditionalOnBean(BatchProcessor.class)
    public SpoolCapacityMonitor spoolCapacityMonitor(BatchProcessor batchProcessor,
                                                     NotifyProperties notifyProperties) {
        return new SpoolCapacityMonitor(batchProcessor, notifyProperties);
    }
}
