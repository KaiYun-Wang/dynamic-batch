package com.dynamicbatch.core.notifier;

import com.dynamicbatch.common.pojo.NotifyPlatformPOJO;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Notifier 模板基类。
 *
 * <p>约定发送失败的兜底行为：捕获一切异常只记日志、不向外抛出，
 * 保证通知失败不影响攒批主流程。子类只需实现 {@link #doSend}。
 * 设计参考 dromara dynamic-tp 的 common/notifier 模块。
 */
public abstract class AbstractNotifier implements Notifier {

    private static final Logger log = LoggerFactory.getLogger(AbstractNotifier.class);

    @Override
    public final void send(NotifyPlatformPOJO platform, String content) {
        try {
            doSend(platform, content);
        } catch (Exception e) {
            log.error("notify send failed, platform={}", platform(), e);
        }
    }

    /**
     * 真正的发送逻辑，由子类实现。
     *
     * @param platform 渠道配置
     * @param content  消息内容
     * @throws Exception 发送失败时抛出，由 {@link #send} 统一兜底
     */
    protected abstract void doSend(NotifyPlatformPOJO platform, String content) throws Exception;
}
