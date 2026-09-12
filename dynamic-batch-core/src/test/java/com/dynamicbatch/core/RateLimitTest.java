package com.dynamicbatch.core;

import com.dynamicbatch.core.pojo.BatchWorkerGroupConfigPOJO;
import com.dynamicbatch.common.pojo.EnvelopePOJO;
import com.dynamicbatch.core.pojo.SpoolConfigPOJO;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * 组级限流测试：Dispatcher 取数节奏（假 poller 驱动，无 Spool 文件 IO）、
 * 未配置不限流、热更生效、Builder 与 resize 校验、Group 全链路接线。
 */
public class RateLimitTest {

    /** 限速值（条/秒）：10 条用，间隔 50ms */
    private static final int RATE_SLOW = 20;
    /** 限速值（条/秒）：热更后用，间隔 2ms */
    private static final int RATE_FAST = 500;

    @Rule
    public TemporaryFolder temp = new TemporaryFolder();

    /** 轮询等待谓词成立（终态断言，超时即红） */
    private static void awaitTrue(Supplier<Boolean> predicate, long timeoutMs, String message)
            throws Exception {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (!predicate.get()) {
            if (System.currentTimeMillis() > deadline) {
                fail(message);
            }
            Thread.sleep(20);
        }
    }

    /** 组共享配置载体：仅填充限速值（null = 未配置不限流） */
    private static BatchWorkerGroupConfigPOJO rateLimitConfig(Integer rateLimitPerSecond) {
        BatchWorkerGroupConfigPOJO config = new BatchWorkerGroupConfigPOJO();
        config.setRateLimitPerSecond(rateLimitPerSecond);
        return config;
    }

    /** 假 poller：按脚本逐条返回，取完返回 null */
    private static Dispatcher.Poller<String> scriptedPoller(int total) {
        List<EnvelopePOJO<String>> scripted = new ArrayList<>();
        for (int i = 0; i < total; i++) {
            scripted.add(new EnvelopePOJO<>("k", "v" + i));
        }
        AtomicInteger pollIndex = new AtomicInteger();
        return lockTimeoutMs -> {
            int index = pollIndex.getAndIncrement();
            return index < scripted.size() ? scripted.get(index) : null;
        };
    }

    /** 配置限速后取数节奏被压到限速值以内（sleep 粒度只会睡过头，下限断言成立） */
    @Test
    public void throttlesPollingToConfiguredRate() throws Exception {
        List<String> delivered = new ArrayList<>();
        Dispatcher<String> dispatcher = new Dispatcher<>(
                scriptedPoller(10),
                envelope -> {
                    delivered.add(envelope.getPayload());
                    return true;
                },
                rateLimitConfig(RATE_SLOW));
        dispatcher.setName("test-rate-limit");
        long start = System.currentTimeMillis();
        dispatcher.start();

        awaitTrue(() -> delivered.size() >= 10, 10_000, "10 条应在预算内投完");
        dispatcher.stop();

        long elapsed = System.currentTimeMillis() - start;
        // 10 条 @20/s：9 个 50ms 间隔 = 450ms 理论下限，留误差余量断言 400ms
        assertTrue("投递应被限速（实际耗时 " + elapsed + "ms）", elapsed >= 400);
    }

    /** 未配置限速（null）直接取数，无节流延迟 */
    @Test
    public void unlimitedWhenNotConfigured() throws Exception {
        List<String> delivered = new ArrayList<>();
        Dispatcher<String> dispatcher = new Dispatcher<>(
                scriptedPoller(100),
                envelope -> {
                    delivered.add(envelope.getPayload());
                    return true;
                },
                rateLimitConfig(null));
        dispatcher.setName("test-rate-unlimited");
        long start = System.currentTimeMillis();
        dispatcher.start();

        awaitTrue(() -> delivered.size() >= 100, 5_000, "不限流应迅速投完");
        dispatcher.stop();

        long elapsed = System.currentTimeMillis() - start;
        assertTrue("不限流不应有节流延迟（实际耗时 " + elapsed + "ms）", elapsed < 1000);
    }

    /** 运行中热更限速值，下一次取数起按新值节流 */
    @Test
    public void hotUpdateTakesEffectOnNextPoll() throws Exception {
        BatchWorkerGroupConfigPOJO config = rateLimitConfig(RATE_SLOW);
        List<String> delivered = new ArrayList<>();
        Dispatcher<String> dispatcher = new Dispatcher<>(
                scriptedPoller(25),
                envelope -> {
                    delivered.add(envelope.getPayload());
                    return true;
                },
                config);
        dispatcher.setName("test-rate-hotupdate");
        long start = System.currentTimeMillis();
        dispatcher.start();

        awaitTrue(() -> delivered.size() >= 5, 5_000, "前 5 条应投完");
        // 热更写方：直接改共享配置（与 resizeGroupConfig 同款 volatile 写）
        config.setRateLimitPerSecond(RATE_FAST);

        awaitTrue(() -> delivered.size() >= 25, 10_000, "剩余条目应投完");
        dispatcher.stop();

        long elapsed = System.currentTimeMillis() - start;
        // 25 条若全程按旧速率需 ~1.2s；前 5 条旧速率 ~200ms + 剩余 20 条新速率远快于旧值
        assertTrue("总耗时应显著低于全程旧速率（实际 " + elapsed + "ms）", elapsed < 1000);
        assertTrue("前 5 条应保留旧速率节流（实际 " + elapsed + "ms）", elapsed >= 200);
    }

    /** Builder 配置限速 <= 0 构建期拒绝（fail fast） */
    @Test
    public void buildRejectsNonPositiveRateLimit() {
        int[] invalid = {0, -1};
        for (int rate : invalid) {
            try {
                BatchWorkerGroup.builder(String.class,
                                SpoolConfigPOJO.builder("target/rate-limit-invalid-" + rate).build(),
                                batch -> { })
                        .rateLimit(rate)
                        .build();
                fail("expected IllegalArgumentException for rate " + rate);
            } catch (IllegalArgumentException expected) {
                // 限速必须 > 0
            }
        }
    }

    /** 热更限速 <= 0 拒绝且配置不变 */
    @Test
    public void resizeRejectsNonPositiveRateLimit() throws Exception {
        BatchProcessor processor = new BatchProcessor();
        try {
            BatchWorkerGroup<Integer> group = BatchWorkerGroup.builder(Integer.class,
                            SpoolConfigPOJO.builder(
                                    new File(temp.getRoot(), "rate-resize").getAbsolutePath()).build(),
                            batch -> { })
                    .rateLimit(100)
                    .build();
            processor.registerGroup("rate-resize", group);

            try {
                processor.resizeGroupConfig("rate-resize", null, null, 0);
                fail("expected IllegalArgumentException");
            } catch (IllegalArgumentException expected) {
                // 限速必须 > 0
            }
            assertEquals(Integer.valueOf(100), group.getRateLimitPerSecond());
        } finally {
            processor.shutdown();
        }
    }

    /** Group 全链路：限速组正常消化全部数据，热更接口透传后继续消化 */
    @Test(timeout = 30_000)
    public void groupEndToEndRateLimitAndHotUpdate() throws Exception {
        BatchProcessor processor = new BatchProcessor();
        List<Integer> flushed = new CopyOnWriteArrayList<>();
        try {
            BatchWorkerGroup<Integer> group = BatchWorkerGroup.builder(Integer.class,
                            SpoolConfigPOJO.builder(
                                    new File(temp.getRoot(), "rate-e2e").getAbsolutePath()).build(),
                            (List<Integer> batch) -> flushed.addAll(batch))
                    .partitionCount(1)
                    .batchSize(5)
                    .maxWaitMs(50)
                    .rateLimit(500)
                    .build();
            processor.registerGroup("rate-e2e", group);

            for (int i = 0; i < 10; i++) {
                assertTrue(processor.submit("rate-e2e", "k", i));
            }
            awaitTrue(() -> flushed.size() >= 10, 10_000, "限速组应消化全部数据");

            processor.resizeGroupConfig("rate-e2e", null, null, 1000);
            assertEquals(Integer.valueOf(1000), group.getRateLimitPerSecond());
            for (int i = 10; i < 15; i++) {
                assertTrue(processor.submit("rate-e2e", "k", i));
            }
            awaitTrue(() -> flushed.size() >= 15, 10_000, "热更后应继续消化");
        } finally {
            processor.shutdown();
        }
    }
}
