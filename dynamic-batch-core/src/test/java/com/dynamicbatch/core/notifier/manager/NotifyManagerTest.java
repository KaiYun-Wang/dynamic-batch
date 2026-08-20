package com.dynamicbatch.core.notifier.manager;

import com.dynamicbatch.common.pojo.BatchWorkerConfigPOJO;
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
    public void changeNoticeContainsKeyAndChangedFields() {
        BatchWorkerConfigPOJO oldConfig = fullConfig(1000, 100, 2000L, 500L, 1);
        BatchWorkerConfigPOJO newConfig = new BatchWorkerConfigPOJO();
        newConfig.setQueueCapacity(2000);
        newConfig.setBatchSize(200);

        NotifyManager.getInstance().tryNoticeChangeAsync("DeviceDTO:insert", oldConfig, newConfig);

        String content = fake.awaitContent(2000);
        assertNotNull(content);
        assertTrue(content.contains("DeviceDTO:insert"));
        assertTrue(content.contains("队列容量"));
        assertTrue(content.contains("1000 → 2000"));
        assertTrue(content.contains("攒批条数"));
        assertTrue(content.contains("100 → 200"));
    }

    @Test
    public void sameValuesSkipsSend() {
        BatchWorkerConfigPOJO oldConfig = fullConfig(1000, 100, 2000L, 500L, 1);
        BatchWorkerConfigPOJO newConfig = new BatchWorkerConfigPOJO();
        // 与旧值相同：不应发送，避免相同配置反复推送刷屏
        newConfig.setQueueCapacity(1000);
        newConfig.setBatchSize(100);

        NotifyManager.getInstance().tryNoticeChangeAsync("k", oldConfig, newConfig);

        assertNull(fake.awaitContent(1000));
    }

    @Test
    public void nullFieldsAreIgnoredInDiff() {
        BatchWorkerConfigPOJO oldConfig = fullConfig(1000, 100, 2000L, 500L, 1);
        // 全 null = 无更新意图，不应发送
        NotifyManager.getInstance().tryNoticeChangeAsync("k", oldConfig, new BatchWorkerConfigPOJO());

        assertNull(fake.awaitContent(1000));
    }

    @Test
    public void noPlatformsSkipsSend() {
        NotifyManager.getInstance().init(Collections.emptyList());
        BatchWorkerConfigPOJO oldConfig = fullConfig(1000, 100, 2000L, 500L, 1);
        BatchWorkerConfigPOJO newConfig = new BatchWorkerConfigPOJO();
        newConfig.setQueueCapacity(2000);

        NotifyManager.getInstance().tryNoticeChangeAsync("k", oldConfig, newConfig);

        assertNull(fake.awaitContent(1000));
    }

    @Test
    public void queueBlockedNoticeContainsQueueWaterLevel() {
        NotifyManager.getInstance().tryNoticeQueueBlockedAsync("wky", 80, 100, 80, 2);

        String content = fake.awaitContent(2000);
        assertNotNull(content);
        assertTrue(content.contains("wky"));
        assertTrue(content.contains("80/100"));
        assertTrue(content.contains("80.0%"));
        assertTrue(content.contains("阈值: 80%"));
        assertTrue(content.contains("消费线程: 2"));
    }

    private static NotifyPlatformPOJO platform(String name) {
        NotifyPlatformPOJO p = new NotifyPlatformPOJO();
        p.setPlatform(name);
        return p;
    }

    private static BatchWorkerConfigPOJO fullConfig(int queueCapacity, int batchSize,
                                                    long maxWaitMs, long offerTimeoutMs, int consumers) {
        BatchWorkerConfigPOJO config = new BatchWorkerConfigPOJO();
        config.setQueueCapacity(queueCapacity);
        config.setBatchSize(batchSize);
        config.setMaxWaitMs(maxWaitMs);
        config.setOfferTimeoutMs(offerTimeoutMs);
        config.setConsumers(consumers);
        return config;
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
