package com.dynamicbatch.spring.autoconfigure;

import com.dynamicbatch.core.notifier.manager.NotifyManager;
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
 * NotifyService 发送通知（当前内置渠道：钉钉、企业微信）；启动时把平台配置
 * 注入核心 {@link NotifyManager}，配置热更新（BatchProcessor.refresh）会自动通知。
 */
@Configuration(proxyBeanMethods = false)
@EnableConfigurationProperties(NotifyProperties.class)
public class NotifyAutoConfiguration {

    @Bean
    @ConditionalOnMissingBean
    public NotifyService notifyService(NotifyProperties properties) {
        NotifyManager.getInstance().init(properties.getPlatforms());
        return new NotifyService(properties);
    }
}
