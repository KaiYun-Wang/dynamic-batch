package com.dynamicbatch.core.notifier.channel;

import com.dynamicbatch.common.pojo.NotifyPlatformPOJO;

/**
 * 通知渠道接口。
 *
 * <p>实现类代表一个通知渠道（钉钉、企业微信、邮件……），通过 {@link #platform()}
 * 声明自己的平台名，由 {@link NotifierRegistry} 统一注册与分发。
 * 新增渠道只需实现本接口并注册，无需改动核心逻辑。
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
}
