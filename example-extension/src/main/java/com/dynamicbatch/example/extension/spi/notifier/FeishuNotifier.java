package com.dynamicbatch.example.extension.spi.notifier;

import cn.hutool.http.HttpRequest;
import com.dynamicbatch.common.pojo.NotifyPlatformPOJO;
import com.dynamicbatch.common.util.JsonUtil;
import com.dynamicbatch.core.notifier.channel.AbstractNotifier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * 飞书群机器人渠道（SPI：管「怎么发」）。
 *
 * <p>与 {@code NoticeTemplate}（管「发什么」）分工：模板产出纯文本正文，本类包装成飞书
 * {@code post} 富文本（标题 + 正文），并可选加签。平台名 {@code feishu}。
 */
public class FeishuNotifier extends AbstractNotifier {

    private static final Logger log = LoggerFactory.getLogger(FeishuNotifier.class);

    public static final String PLATFORM = "feishu";

    private static final String DEFAULT_TITLE = "dynamic-batch 告警";

    @Override
    public String platform() {
        return PLATFORM;
    }

    @Override
    protected void doSend(NotifyPlatformPOJO platform, String content) throws Exception {
        String url = platform.getWebhook();
        if (url == null || url.trim().isEmpty()) {
            throw new IllegalArgumentException("feishu webhook must not be blank");
        }
        String body = buildBody(platform, content);
        String resp = HttpRequest.post(url)
                .setConnectionTimeout(platform.getTimeout())
                .setReadTimeout(platform.getTimeout())
                .body(body)
                .execute()
                .body();
        log.info("feishu notify sent, response={}", resp);
    }

    /**
     * 飞书 post 消息体（比 text 更适合多行告警）；配置了 secret 时附加 timestamp / sign。
     */
    protected String buildBody(NotifyPlatformPOJO platform, String content) {
        Map<String, Object> req = new LinkedHashMap<>();
        if (isNotBlank(platform.getSecret())) {
            long timestampSec = System.currentTimeMillis() / 1000L;
            req.put("timestamp", String.valueOf(timestampSec));
            req.put("sign", sign(platform.getSecret(), timestampSec));
        }
        req.put("msg_type", "post");

        String title = isNotBlank(platform.getTitle()) ? platform.getTitle() : DEFAULT_TITLE;
        // post.zh_cn.content 是「行 → 元素」二维数组；整段正文放一个 text 元素即可
        Map<String, Object> textEl = new LinkedHashMap<>();
        textEl.put("tag", "text");
        textEl.put("text", content == null ? "" : content);

        List<List<Map<String, Object>>> lines = Collections.singletonList(
                Collections.singletonList(textEl));

        Map<String, Object> zhCn = new LinkedHashMap<>();
        zhCn.put("title", title);
        zhCn.put("content", lines);

        Map<String, Object> post = new LinkedHashMap<>();
        post.put("zh_cn", zhCn);

        Map<String, Object> contentNode = new LinkedHashMap<>();
        contentNode.put("post", post);
        req.put("content", contentNode);

        return JsonUtil.toJson(req);
    }

    /** 飞书加签：timestamp + "\n" + secret 作 key，对空串做 HmacSHA256 再 Base64（timestamp 为秒） */
    private static String sign(String secret, long timestampSec) {
        try {
            String stringToSign = timestampSec + "\n" + secret;
            Mac mac = Mac.getInstance("HmacSHA256");
            mac.init(new SecretKeySpec(stringToSign.getBytes(StandardCharsets.UTF_8), "HmacSHA256"));
            byte[] digest = mac.doFinal(new byte[]{});
            return Base64.getEncoder().encodeToString(digest);
        } catch (Exception e) {
            throw new IllegalStateException("feishu sign failed", e);
        }
    }

    private static boolean isNotBlank(String s) {
        return s != null && !s.trim().isEmpty();
    }
}
