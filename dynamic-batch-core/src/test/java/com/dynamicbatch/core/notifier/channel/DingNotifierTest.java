package com.dynamicbatch.core.notifier.channel;

import com.dynamicbatch.common.pojo.NotifyPlatformPOJO;
import com.dynamicbatch.common.util.JsonUtil;
import com.sun.net.httpserver.HttpServer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class DingNotifierTest {

    // ========== 真实钉钉机器人集成测试 ==========
//    @Test
//    public void testRealDingTalkSend() {
//        NotifyPlatformPOJO platform = new NotifyPlatformPOJO();
//        platform.setWebhook(""); //填自己机器人的webhook和secret
//        platform.setSecret("");
//        platform.setReceivers("all");
//        platform.setTimeout(5000);
//        notifier.send(platform, "## Dynamic-Batch 测试消息\n\n这是一条来自\n\n- DingNotifierTest 的\n\n**集成测试**消息");
//    }

    private final DingNotifier notifier = new DingNotifier();


    private HttpServer server;
    private String webhookUrl;
    private volatile String receivedBody;

    @Before
    public void startServer() throws IOException {
        // 本地假钉钉服务：只收 POST、记录 body、回 200，用来验证完整发送链路
        server = HttpServer.create(new InetSocketAddress(0), 0);
        server.createContext("/robot/send", exchange -> {
            receivedBody = new BufferedReader(
                    new InputStreamReader(exchange.getRequestBody(), StandardCharsets.UTF_8)).readLine();
            byte[] resp = "{\"errcode\":0}".getBytes(StandardCharsets.UTF_8);
            exchange.sendResponseHeaders(200, resp.length);
            exchange.getResponseBody().write(resp);
            exchange.close();
        });
        server.start();
        webhookUrl = "http://127.0.0.1:" + server.getAddress().getPort() + "/robot/send";
    }

    @After
    public void stopServer() {
        server.stop(0);
    }

    @Test
    public void buildUrlWithoutSecretKeepsWebhook() {
        NotifyPlatformPOJO platform = new NotifyPlatformPOJO();
        platform.setWebhook("https://oapi.dingtalk.com/robot/send?access_token=token123");
        assertEquals(platform.getWebhook(), notifier.buildUrl(platform));
    }

    @Test
    public void buildUrlAppendsTokenWhenUrlKeyConfigured() {
        NotifyPlatformPOJO platform = new NotifyPlatformPOJO();
        platform.setWebhook("https://oapi.dingtalk.com/robot/send");
        platform.setUrlKey("token123");
        String url = notifier.buildUrl(platform);
        assertTrue(url.contains("access_token=token123"));
    }

    @Test
    public void buildUrlAppendsSignWhenSecretConfigured() {
        NotifyPlatformPOJO platform = new NotifyPlatformPOJO();
        platform.setWebhook("https://oapi.dingtalk.com/robot/send");
        platform.setSecret("SECabc");
        String url = notifier.buildUrl(platform);
        assertTrue(url.contains("timestamp="));
        assertTrue(url.contains("sign="));
    }

    @Test
    public void buildBodyIsMarkdownWithMobiles() {
        NotifyPlatformPOJO platform = new NotifyPlatformPOJO();
        platform.setReceivers("13800000000,13900000000");
        String body = notifier.buildBody(platform, "hello \"quoted\" \n newline");
        assertTrue(body.contains("\"msgtype\":\"markdown\""));
        assertTrue(body.contains("\"atMobiles\":[\"13800000000\",\"13900000000\"]"));
        assertTrue(body.contains("\"isAtAll\":false"));
        assertTrue(body.contains("hello \\\"quoted\\\""));
    }

    @Test
    public void buildBodyPutsTitleInsideMarkdown() {
        // 钉钉协议：title 必须在 markdown 对象内部；放顶层会被拒收（errcode 400402 title 缺失）
        NotifyPlatformPOJO platform = new NotifyPlatformPOJO();
        String body = notifier.buildBody(platform, "msg");
        Map<?, ?> req = JsonUtil.fromJson(body, Map.class);
        Map<?, ?> markdown = (Map<?, ?>) req.get("markdown");
        assertEquals("攒批通知", markdown.get("title"));
        assertEquals("msg", markdown.get("text"));
        assertNull(req.get("title"));
    }

    @Test
    public void buildBodyAtAllByDefault() {
        NotifyPlatformPOJO platform = new NotifyPlatformPOJO();
        String body = notifier.buildBody(platform, "msg");
        assertTrue(body.contains("\"isAtAll\":true"));
        assertTrue(body.contains("\"atMobiles\":[]"));
    }

    @Test
    public void sendPostsMarkdownBodyToWebhook() {
        NotifyPlatformPOJO platform = new NotifyPlatformPOJO();
        platform.setWebhook(webhookUrl);
        notifier.send(platform, "test content");
        assertNotNull(receivedBody);
        assertTrue(receivedBody.contains("\"text\":\"test content\""));
    }

    @Test
    public void sendFailureIsSwallowed() {
        // 连不上的地址（本机 1 端口），验证 AbstractNotifier 兜底不抛异常
        NotifyPlatformPOJO platform = new NotifyPlatformPOJO();
        platform.setWebhook("http://127.0.0.1:1/not-exist");
        platform.setTimeout(1000);
        notifier.send(platform, "x");
    }
}
