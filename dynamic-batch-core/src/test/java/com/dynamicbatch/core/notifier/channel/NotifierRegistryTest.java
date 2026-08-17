package com.dynamicbatch.core.notifier.channel;

import com.dynamicbatch.common.pojo.NotifyPlatformPOJO;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class NotifierRegistryTest {

    @Test
    public void sendDelegatesToRegisteredNotifier() {
        FakeNotifier fake = new FakeNotifier();
        NotifierRegistry.getInstance().register(fake);

        NotifyPlatformPOJO platform = new NotifyPlatformPOJO();
        platform.setPlatform("fake");
        assertTrue(NotifierRegistry.getInstance().send(platform, "hello"));
        assertEquals("hello", fake.lastContent);
        assertEquals(platform, fake.lastPlatform);
    }

    @Test
    public void sendReturnsFalseForUnknownPlatform() {
        NotifyPlatformPOJO platform = new NotifyPlatformPOJO();
        platform.setPlatform("unknown");
        assertFalse(NotifierRegistry.getInstance().send(platform, "hello"));
    }

    @Test
    public void registerReplacesSamePlatform() {
        FakeNotifier first = new FakeNotifier();
        FakeNotifier second = new FakeNotifier();
        NotifierRegistry registry = NotifierRegistry.getInstance();
        registry.register(first);
        registry.register(second);

        NotifyPlatformPOJO platform = new NotifyPlatformPOJO();
        platform.setPlatform("fake");
        registry.send(platform, "x");
        assertNull(first.lastContent);
        assertEquals("x", second.lastContent);
    }

    /** 测试用假渠道，记录最近一次调用 */
    private static class FakeNotifier implements Notifier {

        private NotifyPlatformPOJO lastPlatform;
        private String lastContent;

        @Override
        public String platform() {
            return "fake";
        }

        @Override
        public void send(NotifyPlatformPOJO platform, String content) {
            this.lastPlatform = platform;
            this.lastContent = content;
        }
    }
}
