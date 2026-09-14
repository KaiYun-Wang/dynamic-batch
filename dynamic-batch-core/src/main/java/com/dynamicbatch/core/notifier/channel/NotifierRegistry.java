package com.dynamicbatch.core.notifier.channel;

import com.dynamicbatch.common.pojo.NotifyPlatformPOJO;
import com.dynamicbatch.common.util.ExtensionServiceLoader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 通知渠道注册表。
 *
 * <p>对齐 dynamic-tp {@code NotifierHandler}：构造时注册内置渠道（钉钉、企业微信），
 * 再通过 SPI 加载<strong>外部扩展包</strong>里的渠道。邮件不在 core SPI 里，
 * 由 Spring {@code NotifyEmailAutoConfiguration} 在存在 mail 依赖时注册。
 */
public class NotifierRegistry {

    private static final Logger log = LoggerFactory.getLogger(NotifierRegistry.class);

    private final Map<String, Notifier> notifiers = new ConcurrentHashMap<>();

    private NotifierRegistry() {
        register(new DingNotifier());
        register(new WechatNotifier());
        loadSpiNotifiers();
    }

    /** SPI：仅加载独立扩展 jar 声明的渠道（core 自身不往 META-INF/services 写邮件）。 */
    private void loadSpiNotifiers() {
        List<Notifier> loaded = ExtensionServiceLoader.get(Notifier.class);
        if (loaded == null || loaded.isEmpty()) {
            return;
        }
        for (Notifier n : loaded) {
            if (n.supports()) {
                register(n);
                log.info("SPI notifier registered: {}", n.getClass().getSimpleName());
            }
        }
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
