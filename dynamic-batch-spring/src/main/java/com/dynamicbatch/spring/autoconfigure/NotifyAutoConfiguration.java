package com.dynamicbatch.spring.autoconfigure;

import com.dynamicbatch.spring.notify.NotifyService;
import com.dynamicbatch.spring.properties.NotifyProperties;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * 通知自动装配：启用 {@link NotifyProperties} yml 绑定，注册 {@link NotifyService} Bean。
 *
 * <p>用户只需在 yml 配置 {@code dynamic-batch.notify.platforms}，即可通过
 * NotifyService 发送通知（当前内置渠道：钉钉）。
 */
@Configuration(proxyBeanMethods = false)
@EnableConfigurationProperties(NotifyProperties.class)
public class NotifyAutoConfiguration {

    @Bean
    @ConditionalOnMissingBean
    public NotifyService notifyService(NotifyProperties properties) {
        return new NotifyService(properties);
    }
}
