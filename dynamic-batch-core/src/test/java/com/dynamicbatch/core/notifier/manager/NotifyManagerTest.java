package com.dynamicbatch.core.notifier.manager;

import com.dynamicbatch.common.pojo.NotifyItemPOJO;
import com.dynamicbatch.common.pojo.NotifyPlatformPOJO;
import com.dynamicbatch.core.notifier.channel.Notifier;
import com.dynamicbatch.core.notifier.channel.NotifierRegistry;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;

import static org.junit.Assert.assertFalse;
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
        // 所有告警显式才开：登记且 enabled=true 才投递，测试用例按需登记
        NotifyManager.getInstance().initItems(Arrays.asList(
                item("offer_failed", true),
                item("spool_capacity", true)));
    }

    @After
    public void tearDown() {
        NotifyManager.getInstance().initItems(Collections.emptyList());
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

    @Test
    public void disabledTypeSkipsSend() {
        // 显式才开：登记但 enabled=false 不投递
        NotifyManager.getInstance().initItems(Collections.singletonList(item("offer_failed", false)));
        assertFalse(NotifyManager.getInstance().isEnabled(com.dynamicbatch.common.enums.NotifyTypeEnum.OFFER_FAILED));

        NotifyManager.getInstance().tryNoticeOfferFailedAsync("k", "组未启动", 0);
        assertNull("enabled=false 的类型不应投递", fake.awaitContent(1000));
    }

    @Test
    public void unregisteredTypeSkipsSend() {
        // 只登记了 offer_failed：flush_failed 未登记不发
        NotifyManager.getInstance().tryNoticeFlushFailedAsync("k", 3, "boom", true);
        assertNull("未登记的类型不应投递", fake.awaitContent(1000));
    }

    @Test
    public void spoolCapacityNoticeContainsBudgetAndBreakdown() {
        assertTrue(NotifyManager.getInstance().isEnabled(com.dynamicbatch.common.enums.NotifyTypeEnum.SPOOL_CAPACITY));

        NotifyManager.getInstance().tryNoticeSpoolCapacityAsync("demo_group", 700L, 1000L, 70, 200L, 500L);

        String content = fake.awaitContent(2000);
        assertNotNull(content);
        assertTrue(content.contains("demo_group"));
        assertTrue(content.contains("70%"));
        assertTrue(content.contains("500 B"));   // 还未读
        assertTrue(content.contains("200 B"));   // 已读完未删除
    }

    private static NotifyItemPOJO item(String type, boolean enabled) {
        NotifyItemPOJO item = new NotifyItemPOJO();
        item.setType(type);
        item.setEnabled(enabled);
        return item;
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
