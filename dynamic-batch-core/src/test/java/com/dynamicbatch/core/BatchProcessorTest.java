package com.dynamicbatch.core;

import org.junit.After;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * BatchProcessor 组级 API 测试：注册 Worker 组、路由、分区有序、关闭刷盘。
 */
public class BatchProcessorTest {

    private BatchProcessor processor;

    @After
    public void tearDown() {
        if (processor != null) {
            processor.shutdown();
        }
    }

    @Test
    public void shouldFlushByBatchSize() throws Exception {
        processor = new BatchProcessor();
        CountDownLatch latch = new CountDownLatch(1);
        List<String> flushed = new CopyOnWriteArrayList<>();

        Consumer<List<String>> handler = batch -> {
            flushed.addAll(batch);
            latch.countDown();
        };
        // batchSize=3 凑满即 flush；maxWaitMs 只是攒批窗口，与关闭耗时无关
        processor.registerGroup("demo",
                BatchWorkerGroup.builder(String.class, handler)
                        .queueCapacity(100)
                        .batchSize(3)
                        .maxWaitMs(2000)
                        .offerTimeoutMs(1000)
                        .build());

        assertTrue(processor.submit("demo", "r1", "a"));
        assertTrue(processor.submit("demo", "r1", "b"));
        assertTrue(processor.submit("demo", "r1", "c"));
        assertTrue(latch.await(3, TimeUnit.SECONDS));
        assertEquals(3, flushed.size());
    }

    @Test
    public void shutdownShouldFlushRemaining() throws Exception {
        processor = new BatchProcessor();
        List<Integer> flushed = new CopyOnWriteArrayList<>();
        Consumer<List<Integer>> handler = flushed::addAll;

        // maxWaitMs 故意给大值（模拟长时间攒批场景）：验证关闭耗时与其无关，
        // 消费线程每 WAKEUP_INTERVAL_MS 醒来检查一次停止信号，剩余数据由 shutdown 直接刷盘
        processor.registerGroup("remain",
                BatchWorkerGroup.builder(Integer.class, handler)
                        .queueCapacity(100)
                        .batchSize(100)
                        .maxWaitMs(60_000)
                        .offerTimeoutMs(1000)
                        .build());

        assertTrue(processor.submit("remain", "r1", 1));
        assertTrue(processor.submit("remain", "r1", 2));
        processor.shutdown();
        processor = null;

        assertEquals(2, flushed.size());
    }

    @Test
    public void submitShouldRejectWrongType() throws Exception {
        processor = new BatchProcessor();
        processor.registerGroup("typed",
                BatchWorkerGroup.builder(String.class, batch -> {
                })
                        .batchSize(10)
                        .build());

        assertTrue(processor.submit("typed", "r1", "ok"));
        // 错误类型在 submit 处被拒（fail fast），不会混入队列
        assertFalse(processor.submit("typed", "r1", 123));
        processor.shutdown();
        processor = null;
    }

    @Test
    public void submitToUnknownGroupReturnsFalse() {
        processor = new BatchProcessor();
        // 组不存在：返回 false，不抛异常
        assertFalse(processor.submit("unknown", "r1", "data"));
    }

    @Test
    public void routingShouldPinSameKeyToSamePartition() throws Exception {
        processor = new BatchProcessor();
        Set<String> flushThreads = ConcurrentHashMap.newKeySet();
        List<Integer> flushed = new CopyOnWriteArrayList<>();

        // 4 分区：同一 routingKey 永远路由到同一分区，flush 只来自该分区的单条消费线程
        processor.registerGroup("route",
                BatchWorkerGroup.builder(Integer.class, batch -> {
                    flushThreads.add(Thread.currentThread().getName());
                    flushed.addAll(batch);
                })
                        .queueCapacity(2000)
                        .batchSize(500)
                        .maxWaitMs(50)
                        .offerTimeoutMs(1000)
                        .partitionCount(4)
                        .build());

        for (int i = 0; i < 1000; i++) {
            assertTrue(processor.submit("route", "same-key", i));
        }

        long deadline = System.currentTimeMillis() + 3000;
        while (flushed.size() < 1000 && System.currentTimeMillis() < deadline) {
            Thread.sleep(20);
        }
        processor.shutdown();
        processor = null;

        assertEquals(1000, flushed.size());
        // 同一 routingKey 的数据只在一个分区消费（batch-processor-route-{i} 中固定一个）
        assertEquals("same routing key should be pinned to exactly 1 partition", 1, flushThreads.size());
    }

    @Test
    public void partitionOrderingShouldBeStrict() throws Exception {
        processor = new BatchProcessor();
        List<String> flushed = new CopyOnWriteArrayList<>();

        // batchSize=1：每条数据独立 flush，出队顺序 = flush 顺序；
        // 分区单线程 + FIFO → 同 routingKey 的数据严格按提交顺序输出
        processor.registerGroup("order",
                BatchWorkerGroup.builder(String.class, flushed::addAll)
                        .queueCapacity(2000)
                        .batchSize(1)
                        .maxWaitMs(50)
                        .offerTimeoutMs(1000)
                        .partitionCount(4)
                        .build());

        // k1 / k2 交替提交，各自 50 条；数据内容编码为 "{key}-{seq}"
        for (int i = 0; i < 50; i++) {
            assertTrue(processor.submit("order", "k1", "k1-" + i));
            assertTrue(processor.submit("order", "k2", "k2-" + i));
        }

        long deadline = System.currentTimeMillis() + 3000;
        while (flushed.size() < 100 && System.currentTimeMillis() < deadline) {
            Thread.sleep(20);
        }
        processor.shutdown();
        processor = null;

        assertEquals(100, flushed.size());
        // 按 key 分组后，各自 seq 必须严格递增（分区内有序）
        assertStrictOrder(flushed, "k1");
        assertStrictOrder(flushed, "k2");
    }

    /** 从全局 flush 序列中提取指定 key 的项，断言 seq 严格递增 */
    private static void assertStrictOrder(List<String> flushed, String keyPrefix) {
        int lastSeq = -1;
        for (String item : flushed) {
            if (!item.startsWith(keyPrefix + "-")) {
                continue;
            }
            int seq = Integer.parseInt(item.substring(keyPrefix.length() + 1));
            assertTrue("partition order broken for " + keyPrefix + ": seq=" + seq + " after " + lastSeq,
                    seq > lastSeq);
            lastSeq = seq;
        }
    }

    @Test
    public void registerDuplicateGroupShouldThrow() {
        processor = new BatchProcessor();
        BatchWorkerGroup<String> first = BatchWorkerGroup.builder(String.class, batch -> {
        }).build();
        processor.registerGroup("same", first);

        // 同 key 重复注册：启动期配置错误，快速失败；旧组保持运行不受影响
        try {
            processor.registerGroup("same", BatchWorkerGroup.builder(String.class, batch -> {
            }).build());
            fail("expected IllegalStateException");
        } catch (IllegalStateException ex) {
            assertTrue(ex.getMessage().contains("already registered"));
        }
        assertTrue("old group should stay running", first.submit("r1", "x"));
    }

    @Test
    public void registerGroupShouldRejectInvalidGroupKey() {
        processor = new BatchProcessor();
        BatchWorkerGroup<String> group = BatchWorkerGroup.builder(String.class, batch -> {
        }).build();

        try {
            processor.registerGroup("DemoItem:insert", group);
            fail("expected IllegalArgumentException");
        } catch (IllegalArgumentException ex) {
            assertTrue(ex.getMessage().contains("invalid group key"));
        }

        try {
            processor.registerGroup("", group);
            fail("expected IllegalArgumentException");
        } catch (IllegalArgumentException ex) {
            assertTrue(ex.getMessage().contains("must not be blank"));
        }

        processor.registerGroup("demo_item-insert_1", group);
    }

    @Test(expected = IllegalArgumentException.class)
    public void partitionCountShouldRejectInvalid() {
        // 分区数必须 > 0
        BatchWorkerGroup.builder(String.class, batch -> {
        }).partitionCount(0).build();
    }

    @Test
    public void nullRoutingKeyFallsBackToPartitionZero() throws Exception {
        processor = new BatchProcessor();
        Set<String> flushThreads = ConcurrentHashMap.newKeySet();
        AtomicInteger flushed = new AtomicInteger();

        processor.registerGroup("null-key",
                BatchWorkerGroup.builder(Integer.class, batch -> {
                    flushThreads.add(Thread.currentThread().getName());
                    flushed.addAndGet(batch.size());
                })
                        .queueCapacity(100)
                        .batchSize(10)
                        .maxWaitMs(50)
                        .partitionCount(3)
                        .build());

        for (int i = 0; i < 30; i++) {
            assertTrue(processor.submit("null-key", null, i));
        }
        // 等全部消费完再关闭：shutdown 的剩余刷盘在调用线程执行，会把 main 混入 flush 线程集合
        waitUntil(() -> flushed.get() >= 30, 3000);
        processor.shutdown();
        processor = null;

        assertEquals(30, flushed.get());
        // null routingKey 固定路由到分区 0，flush 只来自分区 0 的消费线程
        assertEquals("null routing key should pin to partition 0", 1, flushThreads.size());
        assertTrue(flushThreads.iterator().next().endsWith("-0"));
    }

    private static void waitUntil(BooleanSupplier condition, long timeoutMs) throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (!condition.getAsBoolean() && System.currentTimeMillis() < deadline) {
            Thread.sleep(20);
        }
    }
}
