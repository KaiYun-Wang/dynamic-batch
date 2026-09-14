package com.dynamicbatch.spring.autoconfigure;

import com.dynamicbatch.common.util.AppInstance;
import com.dynamicbatch.core.notifier.manager.NotifyManager;
import com.dynamicbatch.spring.notify.NotifyService;
import com.dynamicbatch.spring.properties.NotifyProperties;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;

/**
 * 通知自动装配：启用 {@link NotifyProperties} yml 绑定，注册 {@link NotifyService} Bean。
 *
 * <p>用户只需在 yml 配置 {@code dynamic-batch.notify.platforms}，即可通过
 * NotifyService 发送通知（内置：钉钉、企业微信；邮件需额外引入
 * {@code spring-boot-starter-mail}，由 {@link NotifyEmailAutoConfiguration} 注册）；启动时把平台配置
 * 与告警规则（静默期）注入核心 {@link NotifyManager}，内置告警
 * （offer_failed / flush_failed）触发即发。同时从 {@link Environment} 读取实例信息
 * （应用名、端口）初始化 {@link AppInstance}，确保分布式场景下通知内容可区分实例。
 */
@Configuration(proxyBeanMethods = false)
@EnableConfigurationProperties(NotifyProperties.class)
public class NotifyAutoConfiguration {

    @Bean
    @ConditionalOnMissingBean
    public NotifyService notifyService(NotifyProperties properties, Environment environment) {
        AppInstance.init(
                environment.getProperty("spring.application.name"),
                parsePort(environment.getProperty("server.port"))
        );
        NotifyManager.getInstance().init(properties.getPlatforms());
        NotifyManager.getInstance().initItems(properties.getNotifyItems());
        return new NotifyService(properties);
    }

    private static int parsePort(String portStr) {
        if (portStr == null || portStr.isEmpty()) {
            return 0;
        }
        try {
            return Integer.parseInt(portStr);
        } catch (NumberFormatException e) {
            return 0;
        }
    }
}
