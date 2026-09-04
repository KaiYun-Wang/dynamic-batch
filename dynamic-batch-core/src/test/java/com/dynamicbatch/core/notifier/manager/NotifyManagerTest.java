package com.dynamicbatch.core.notifier.manager;

import com.dynamicbatch.common.pojo.NotifyPlatformPOJO;
import com.dynamicbatch.core.notifier.channel.Notifier;
import com.dynamicbatch.core.notifier.channel.NotifierRegistry;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class NotifyManagerTest {

    private FakeNotifier fake;

    @Before
    public void setUp() {
        fake = new FakeNotifier();
        NotifierRegistry.getInstance().register(fake);
        NotifyManager.getInstance().init(Collections.singletonList(platform("fake")));
    }

    @After
    public void tearDown() {
        NotifyManager.getInstance().init(Collections.emptyList());
    }

    @Test
    public void offerFailedNoticeContainsReasonAndQueueSize() {
        NotifyManager.getInstance().tryNoticeOfferFailedAsync("wky", "组未启动或已关闭", 7);

        String content = fake.awaitContent(2000);
        assertNotNull(content);
        assertTrue(content.contains("wky"));
        assertTrue(content.contains("组未启动或已关闭"));
        assertTrue(content.contains("当前队列: 7"));
    }

    @Test
    public void noPlatformsSkipsSend() {
        NotifyManager.getInstance().init(Collections.emptyList());
        NotifyManager.getInstance().tryNoticeOfferFailedAsync("k", "worker 已关闭", 0);

        assertNull(fake.awaitContent(1000));
    }

    private static NotifyPlatformPOJO platform(String name) {
        NotifyPlatformPOJO p = new NotifyPlatformPOJO();
        p.setPlatform(name);
        return p;
    }

    /** 测试用假渠道，记录发送次数与最近内容（发送为异步，用轮询等待） */
    private static class FakeNotifier implements Notifier {

        private volatile int sentCount;
        private volatile String lastContent;

        @Override
        public String platform() {
            return "fake";
        }

        @Override
        public void send(NotifyPlatformPOJO platform, String content) {
            sentCount++;
            lastContent = content;
        }

        private String awaitContent(long timeoutMs) {
            long deadline = System.currentTimeMillis() + timeoutMs;
            while (System.currentTimeMillis() < deadline) {
                if (sentCount > 0) {
                    return lastContent;
                }
                try {
                    Thread.sleep(50);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return null;
                }
            }
            return null;
        }
    }
}
