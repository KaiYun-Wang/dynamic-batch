package com.dynamicbatch.core.notifier.channel;

import cn.hutool.http.HttpRequest;
import com.dynamicbatch.common.enums.NotifyPlatformEnum;
import com.dynamicbatch.common.pojo.NotifyPlatformPOJO;
import com.dynamicbatch.common.util.JsonUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * 钉钉机器人渠道。
 *
 * <p>发送 markdown 消息，可选 @人；webhook 未携带 access_token 时用
 * {@link NotifyPlatformPOJO#getUrlKey()} 补参；配置 secret 时自动加签
 * （timestamp + sign，HmacSHA256 后 Base64 URL 安全编码）。
 * 设计参考 dromara dynamic-tp 的 common/notifier 模块。
 */
public class DingNotifier extends AbstractNotifier {

    private static final Logger log = LoggerFactory.getLogger(DingNotifier.class);

    private static final String NOTICE_TITLE = "攒批通知";

    private static final String ACCESS_TOKEN_PARAM = "access_token";
    private static final String TIMESTAMP_PARAM = "timestamp";
    private static final String SIGN_PARAM = "sign";

    @Override
    public String platform() {
        return NotifyPlatformEnum.DING.name().toLowerCase();
    }

    @Override
    protected void doSend(NotifyPlatformPOJO platform, String content) throws Exception {
        String url = buildUrl(platform);
        String body = buildBody(platform, content);
        String resp = HttpRequest.post(url)
                .setConnectionTimeout(platform.getTimeout())
                .setReadTimeout(platform.getTimeout())
                .body(body)
                .execute()
                .body();
        log.info("ding notify sent, url={}, response={}", url, resp);
    }

    /**
     * 拼接目标 URL：追加 access_token（可选）与签名参数（可选）。
     */
    protected String buildUrl(NotifyPlatformPOJO platform) {
        String webhook = platform.getWebhook();
        StringBuilder url = new StringBuilder(webhook);
        char sep = webhook.indexOf('?') >= 0 ? '&' : '?';
        if (isNotBlank(platform.getUrlKey()) && !webhook.contains(ACCESS_TOKEN_PARAM)) {
            url.append(sep).append(ACCESS_TOKEN_PARAM).append('=').append(platform.getUrlKey());
            sep = '&';
        }
        if (isNotBlank(platform.getSecret())) {
            long timestamp = System.currentTimeMillis();
            url.append(sep).append(TIMESTAMP_PARAM).append('=').append(timestamp)
                    .append('&').append(SIGN_PARAM).append('=').append(sign(platform.getSecret(), timestamp));
        }
        return url.toString();
    }

    /**
     * 构建钉钉 markdown 消息体。
     */
    protected String buildBody(NotifyPlatformPOJO platform, String content) {
        String receivers = platform.getReceivers();
        boolean atAll = isBlank(receivers) || "all".equalsIgnoreCase(receivers.trim());

        // LinkedHashMap 保持插入序，保证任意 JsonParser 序列化输出的键序一致
        Map<String, Object> req = new LinkedHashMap<>();
        req.put("msgtype", "markdown");

        // 钉钉 markdown 协议：title 必须在 markdown 对象内部，放顶层会报 400402 title 缺失
        Map<String, Object> markdown = new LinkedHashMap<>();
        markdown.put("title", NOTICE_TITLE);
        markdown.put("text", content);
        req.put("markdown", markdown);

        Map<String, Object> at = new LinkedHashMap<>();
        at.put("isAtAll", atAll);
        if (atAll) {
            at.put("atMobiles", new String[0]);
        } else {
            at.put("atMobiles", receivers.split(","));
        }
        req.put("at", at);

        return JsonUtil.toJson(req);
    }

    /** 钉钉加签：timestamp + "\n" + secret 做 HmacSHA256，标准 Base64 后 URLEncode */
    private static String sign(String secret, long timestamp) {
        try {
            String stringToSign = timestamp + "\n" + secret;
            Mac mac = Mac.getInstance("HmacSHA256");
            mac.init(new SecretKeySpec(secret.getBytes(StandardCharsets.UTF_8), "HmacSHA256"));
            byte[] digest = mac.doFinal(stringToSign.getBytes(StandardCharsets.UTF_8));
            return URLEncoder.encode(Base64.getEncoder().encodeToString(digest), StandardCharsets.UTF_8.name());
        } catch (Exception e) {
            throw new IllegalStateException("ding sign failed", e);
        }
    }

    private static boolean isBlank(String s) {
        return s == null || s.trim().isEmpty();
    }

    private static boolean isNotBlank(String s) {
        return !isBlank(s);
    }
}
