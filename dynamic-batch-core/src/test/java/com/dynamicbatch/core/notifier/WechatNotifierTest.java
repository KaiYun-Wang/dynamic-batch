package com.dynamicbatch.core.notifier;

import com.dynamicbatch.common.pojo.NotifyPlatformPOJO;
import com.sun.net.httpserver.HttpServer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class WechatNotifierTest {

    private final WechatNotifier notifier = new WechatNotifier();

    private HttpServer server;
    private String webhookUrl;
    private volatile String receivedBody;

    @Before
    public void startServer() throws IOException {
        server = HttpServer.create(new InetSocketAddress(0), 0);
        server.createContext("/cgi-bin/webhook/send", exchange -> {
            receivedBody = new BufferedReader(
                    new InputStreamReader(exchange.getRequestBody(), StandardCharsets.UTF_8)).readLine();
            byte[] resp = "{\"errcode\":0}".getBytes(StandardCharsets.UTF_8);
            exchange.sendResponseHeaders(200, resp.length);
            exchange.getResponseBody().write(resp);
            exchange.close();
        });
        server.start();
        webhookUrl = "http://127.0.0.1:" + server.getAddress().getPort() + "/cgi-bin/webhook/send";
    }

    @After
    public void stopServer() {
        server.stop(0);
    }

    @Test
    public void platformIsWechat() {
        assertEquals("wechat", notifier.platform());
    }

    @Test
    public void buildUrlKeepsWebhookWhenKeyAlreadyPresent() {
        NotifyPlatformPOJO platform = new NotifyPlatformPOJO();
        platform.setWebhook("https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key=abc");
        platform.setUrlKey("ignored");
        assertEquals(platform.getWebhook(), notifier.buildUrl(platform));
    }

    @Test
    public void buildUrlAppendsKeyFromUrlKey() {
        NotifyPlatformPOJO platform = new NotifyPlatformPOJO();
        platform.setWebhook("https://qyapi.weixin.qq.com/cgi-bin/webhook/send");
        platform.setUrlKey("abc123");
        assertEquals(
                "https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key=abc123",
                notifier.buildUrl(platform));
    }

    @Test
    public void buildUrlFallsBackToDefaultWebhookWhenBlank() {
        NotifyPlatformPOJO platform = new NotifyPlatformPOJO();
        platform.setUrlKey("abc123");
        assertEquals(
                "https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key=abc123",
                notifier.buildUrl(platform));
    }

    @Test
    public void buildBodyIsMarkdownWithContent() {
        String body = notifier.buildBody("hello \"quoted\" \n newline");
        assertTrue(body.contains("\"msgtype\":\"markdown\""));
        assertTrue(body.contains("\"content\":"));
        assertTrue(body.contains("hello \\\"quoted\\\""));
    }

    @Test
    public void sendPostsMarkdownBodyToWebhook() {
        NotifyPlatformPOJO platform = new NotifyPlatformPOJO();
        platform.setWebhook(webhookUrl);
        notifier.send(platform, "test content");
        assertNotNull(receivedBody);
        assertTrue(receivedBody.contains("\"content\":\"test content\""));
    }

    @Test
    public void sendFailureIsSwallowed() {
        NotifyPlatformPOJO platform = new NotifyPlatformPOJO();
        platform.setWebhook("http://127.0.0.1:1/not-exist");
        platform.setTimeout(1000);
        notifier.send(platform, "x");
    }
}
