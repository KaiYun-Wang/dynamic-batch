package com.dynamicbatch.core.notifier.channel;

import com.dynamicbatch.common.pojo.NotifyPlatformPOJO;

/**
 * 通知渠道接口。
 *
 * <p>实现类代表一个通知渠道（钉钉、企业微信、邮件……），通过 {@link #platform()}
 * 声明自己的平台名，由 {@link NotifierRegistry} 统一注册与分发。
 * 新增渠道：内置可直接改注册表；带额外依赖的渠道对齐 dynamic-tp，
 * 放独立扩展 / 用 {@code @ConditionalOnClass} 装配，勿写进 core 的 SPI。
 * 设计参考 dromara dynamic-tp 的 common/notifier 模块。
 */
public interface Notifier {

    /**
     * 平台名（小写），如 ding；作为注册表 key。
     *
     * @return 平台名
     */
    String platform();

    /**
     * 发送一条消息。
     *
     * @param platform 渠道配置（webhook、密钥、收件人等）
     * @param content  消息内容
     */
    void send(NotifyPlatformPOJO platform, String content);

    /**
     * 检查当前 classpath 是否满足本实现运行所需的依赖。
     *
     * <p>用于 SPI 加载时过滤不可用的实现。无额外依赖的渠道返回 true 即可。
     *
     * @return true 可用；false 跳过本实现
     */
    default boolean supports() {
        return true;
    }
}
