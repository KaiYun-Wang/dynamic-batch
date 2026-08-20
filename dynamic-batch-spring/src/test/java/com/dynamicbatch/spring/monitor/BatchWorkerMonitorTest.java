package com.dynamicbatch.spring.monitor;

import com.dynamicbatch.common.pojo.NotifyPlatformPOJO;
import com.dynamicbatch.core.BatchProcessor;
import com.dynamicbatch.core.BatchWorker;
import com.dynamicbatch.core.notifier.channel.Notifier;
import com.dynamicbatch.core.notifier.channel.NotifierRegistry;
import com.dynamicbatch.core.notifier.manager.NotifyManager;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * 队列积压定时检查测试：直接起 monitor（周期 1s）验证阈值判断与告警投递。
 *
 * <p>注意：消费线程的 poll 在有元素时立即返回，会把可用元素瞬间抓进攒批，队列反而
 * 被清空（batchSize=容量时尤其明显）。因此超阈值用例用「小 batchSize + flush 门闩」
 * 让消费线程抓满一小批后阻塞，队列保持高水位，monitor 才能稳定看到积压。
 */
public class BatchWorkerMonitorTest {

    private BatchProcessor processor;
    private FakeNotifier fake;

    @Before
    public void setUp() {
        fake = new FakeNotifier();
        NotifierRegistry.getInstance().register(fake);
        NotifyManager.getInstance().init(Collections.singletonList(platform("fake")));
        processor = new BatchProcessor();
    }

    @After
    public void tearDown() {
        processor.shutdown();
        NotifyManager.getInstance().init(Collections.emptyList());
    }

    @Test
    public void utilizationBelowThresholdSkipsAlarm() {
        // 队列 100/1000 = 10% < 阈值 90：不应告警（消费线程把元素抓进攒批后，队列水位只会更低）
        registerIdleWorker("wky", 1000);
        for (int i = 0; i < 100; i++) {
            processor.submit("wky", "v" + i);
        }
        BatchWorkerMonitor monitor = new BatchWorkerMonitor(processor, 1, 90);
        try {
            monitor.start();
            assertNull(fake.awaitContent(3000));
        } finally {
            monitor.shutdown();
        }
    }

    @Test
    public void utilizationAboveThresholdSendsAlarm() {
        CountDownLatch flushGate = new CountDownLatch(1);
        // 小 batchSize：消费线程抓满 10 条后阻塞在 flush 门闩上，队列稳定在 790/1000 ≈ 79% ≥ 70
        registerGatedWorker("wky", 1000, 10, flushGate);
        for (int i = 0; i < 800; i++) {
            processor.submit("wky", "v" + i);
        }
        BatchWorkerMonitor monitor = new BatchWorkerMonitor(processor, 1, 70);
        try {
            monitor.start();
            String content = fake.awaitContent(5000);
            assertNotNull(content);
            assertTrue(content.contains("wky"));
            assertTrue(content.contains("阈值: 70%"));
        } finally {
            monitor.shutdown();
            flushGate.countDown();   // 放行消费线程，让后续 flush 立即返回，快速收尾
        }
    }

    /** 空闲 worker：flush 无操作，消费线程把元素全抓进攒批后长时间等攒批（batchSize=容量） */
    private void registerIdleWorker(String key, int queueCapacity) {
        processor.register(key, BatchWorker.builder(String.class, batch -> { })
                .queueCapacity(queueCapacity)
                .batchSize(queueCapacity)
                .maxWaitMs(60000)
                .build());
    }

    /** 门闩 worker：flush 阻塞在门闩上，模拟"消费卡住"，保持队列高水位 */
    private void registerGatedWorker(String key, int queueCapacity, int batchSize, CountDownLatch flushGate) {
        processor.register(key, BatchWorker.builder(String.class, batch -> {
                    try {
                        flushGate.await(5, TimeUnit.SECONDS);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                })
                .queueCapacity(queueCapacity)
                .batchSize(batchSize)
                .maxWaitMs(60000)
                .build());
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
