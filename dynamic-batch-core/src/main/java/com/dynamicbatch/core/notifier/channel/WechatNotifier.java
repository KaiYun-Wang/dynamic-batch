package com.dynamicbatch.core.notifier.channel;

import cn.hutool.http.HttpRequest;
import com.dynamicbatch.common.enums.NotifyPlatformEnum;
import com.dynamicbatch.common.pojo.NotifyPlatformPOJO;
import com.dynamicbatch.common.util.JsonUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * 企业微信群机器人渠道。
 *
 * <p>发送 markdown 消息；webhook 未携带 {@code key} 时用
 * {@link NotifyPlatformPOJO#getUrlKey()} 补参。无 webhook 时回落到官方默认地址再拼 key。
 * 设计参考 dromara dynamic-tp 的 {@code WechatNotifier}。
 */
public class WechatNotifier extends AbstractNotifier {

    private static final Logger log = LoggerFactory.getLogger(WechatNotifier.class);

    /** 企业微信群机器人默认 webhook */
    private static final String DEFAULT_WEBHOOK = "https://qyapi.weixin.qq.com/cgi-bin/webhook/send";

    private static final String KEY_PARAM = "key";

    @Override
    public String platform() {
        return NotifyPlatformEnum.WECHAT.name().toLowerCase();
    }

    @Override
    protected void doSend(NotifyPlatformPOJO platform, String content) throws Exception {
        String url = buildUrl(platform);
        String body = buildBody(content);
        String resp = HttpRequest.post(url)
                .setConnectionTimeout(platform.getTimeout())
                .setReadTimeout(platform.getTimeout())
                .body(body)
                .execute()
                .body();
        log.debug("wechat notify sent, url={}, response={}", url, resp);
    }

    /**
     * 拼接目标 URL：webhook 优先；缺 key 时用 urlKey 追加。
     */
    protected String buildUrl(NotifyPlatformPOJO platform) {
        String webhook = isBlank(platform.getWebhook()) ? DEFAULT_WEBHOOK : platform.getWebhook();
        if (isBlank(platform.getUrlKey()) || webhook.contains(KEY_PARAM + "=")) {
            return webhook;
        }
        char sep = webhook.indexOf('?') >= 0 ? '&' : '?';
        return webhook + sep + KEY_PARAM + '=' + platform.getUrlKey();
    }

    /**
     * 构建企微 markdown 消息体（字段为 content，无 title）。
     */
    protected String buildBody(String content) {
        // LinkedHashMap 保持插入序，保证任意 JsonParser 序列化输出的键序一致
        Map<String, Object> req = new LinkedHashMap<>();
        req.put("msgtype", "markdown");
        Map<String, Object> markdown = new LinkedHashMap<>();
        markdown.put("content", content);
        req.put("markdown", markdown);
        return JsonUtil.toJson(req);
    }

    private static boolean isBlank(String s) {
        return s == null || s.trim().isEmpty();
    }
}
