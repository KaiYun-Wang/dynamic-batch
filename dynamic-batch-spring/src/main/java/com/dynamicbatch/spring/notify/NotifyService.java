package com.dynamicbatch.spring.notify;

import com.dynamicbatch.common.pojo.NotifyPlatformPOJO;
import com.dynamicbatch.core.notifier.channel.NotifierRegistry;
import com.dynamicbatch.spring.properties.NotifyProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * 通知发送服务：yml 配置的渠道 → NotifierRegistry 分发。
 *
 * <p>{@link #send(String, String)} 按平台名找到 yml 中配置的
 * {@link NotifyPlatformPOJO}（webhook/secret/receivers），委托给核心
 * {@link NotifierRegistry} 发送；平台未配置或渠道未注册时返回 false 并记日志，
 * 不影响业务主流程（发送失败由 AbstractNotifier 兜底只记日志）。
 */
public class NotifyService {

    private static final Logger log = LoggerFactory.getLogger(NotifyService.class);

    private final List<NotifyPlatformPOJO> platforms;

    public NotifyService(NotifyProperties properties) {
        this.platforms = properties.getPlatforms();
    }

    /**
     * 按平台名发送通知。
     *
     * @param platform 平台名（如 ding），须与 yml 中配置的 platform 字段一致
     * @param content  消息内容
     * @return true 已交给渠道发送；false 平台未配置或渠道未注册
     */
    public boolean send(String platform, String content) {
        NotifyPlatformPOJO pojo = platforms.stream()
                .filter(p -> platform.equalsIgnoreCase(p.getPlatform()))
                .findFirst()
                .orElse(null);
        if (pojo == null) {
            log.error("notify platform not configured: platform={}, configured={}", platform, platforms);
            return false;
        }
        return NotifierRegistry.getInstance().send(pojo, content);
    }
}
