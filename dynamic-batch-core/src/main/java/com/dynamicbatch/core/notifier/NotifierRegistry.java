package com.dynamicbatch.core.notifier;

import com.dynamicbatch.common.pojo.NotifyPlatformPOJO;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 通知渠道注册表。
 *
 * <p>单例；按平台名维护渠道实例，发送时根据 {@link NotifyPlatformPOJO#getPlatform()}
 * 找到对应渠道并委托。构造时注册内置渠道（钉钉、企业微信），外部渠道通过
 * {@link #register} 追加，无需改动本类。
 * 设计参考 dromara dynamic-tp 的 core/handler/NotifierHandler。
 */
public class NotifierRegistry {

    private static final Logger log = LoggerFactory.getLogger(NotifierRegistry.class);

    private final Map<String, Notifier> notifiers = new ConcurrentHashMap<>();

    private NotifierRegistry() {
        register(new DingNotifier());
        register(new WechatNotifier());
    }

    public static NotifierRegistry getInstance() {
        return Holder.INSTANCE;
    }

    /**
     * 注册渠道；同平台名重复注册会覆盖旧实例。
     *
     * @param notifier 渠道实现
     */
    public void register(Notifier notifier) {
        String platform = notifier.platform().toLowerCase();
        Notifier previous = notifiers.put(platform, notifier);
        if (previous != null) {
            log.warn("replacing notifier: platform={}", platform);
        }
    }

    /**
     * 发送消息。
     *
     * @param platform 渠道配置（platform 字段指定走哪个渠道）
     * @param content  消息内容
     * @return true 已交给渠道发送；false 平台未注册
     */
    public boolean send(NotifyPlatformPOJO platform, String content) {
        Notifier notifier = notifiers.get(platform.getPlatform().toLowerCase());
        if (notifier == null) {
            log.error("notifier not found: platform={}", platform.getPlatform());
            return false;
        }
        notifier.send(platform, content);
        return true;
    }

    private static class Holder {
        private static final NotifierRegistry INSTANCE = new NotifierRegistry();
    }
}
