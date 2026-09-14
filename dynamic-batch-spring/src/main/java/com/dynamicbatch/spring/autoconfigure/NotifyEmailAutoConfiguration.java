package com.dynamicbatch.spring.autoconfigure;

import com.dynamicbatch.core.notifier.channel.EmailNotifier;
import com.dynamicbatch.core.notifier.channel.NotifierRegistry;
import org.springframework.boot.autoconfigure.AutoConfigureBefore;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * 邮件渠道自动装配（对齐 dynamic-tp {@code NotifyEmailAutoConfiguration}）。
 *
 * <p>仅当 classpath 存在 {@code javax.mail.Session}（通常即引入了
 * {@code spring-boot-starter-mail}）时生效；未引入 mail 时本配置类不会加载，无任何日志。
 */
@Configuration(proxyBeanMethods = false)
@ConditionalOnClass(name = "javax.mail.Session")
@AutoConfigureBefore(NotifyAutoConfiguration.class)
public class NotifyEmailAutoConfiguration {

    @Bean
    @ConditionalOnMissingBean
    public EmailNotifier emailNotifier() {
        EmailNotifier notifier = new EmailNotifier();
        NotifierRegistry.getInstance().register(notifier);
        return notifier;
    }
}
