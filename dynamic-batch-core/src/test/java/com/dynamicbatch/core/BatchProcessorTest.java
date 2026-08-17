package com.dynamicbatch.core;

import org.junit.After;
import org.junit.Test;

import com.dynamicbatch.core.BatchWorker;
import com.dynamicbatch.common.pojo.BatchWorkerConfigPOJO;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

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
        processor.register("demo",
                BatchWorker.builder(String.class, handler)
                        .queueCapacity(100)
                        .batchSize(3)
                        .maxWaitMs(2000)
                        .offerTimeoutMs(1000)
                        .build());

        assertTrue(processor.submit("demo", "a"));
        assertTrue(processor.submit("demo", "b"));
        assertTrue(processor.submit("demo", "c"));
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
        processor.register("remain",
                BatchWorker.builder(Integer.class, handler)
                        .queueCapacity(100)
                        .batchSize(100)
                        .maxWaitMs(60_000)
                        .offerTimeoutMs(1000)
                        .build());

        assertTrue(processor.submit("remain", 1));
        assertTrue(processor.submit("remain", 2));
        processor.shutdown();
        processor = null;

        assertEquals(2, flushed.size());
    }

    @Test
    public void shardingShouldConsumeWithMultipleThreads() throws Exception {
        processor = new BatchProcessor();
        Set<String> flushThreads = ConcurrentHashMap.newKeySet();
        List<Integer> flushed = new CopyOnWriteArrayList<>();

        // 分片并行消费：batchSize 故意大于总提交数，靠 maxWaitMs 触发 flush；
        // 验证两条消费线程都在实际工作（flush 回调来自不同线程），不丢数据
        processor.register("shard",
                BatchWorker.builder(Integer.class, batch -> {
                    flushThreads.add(Thread.currentThread().getName());
                    flushed.addAll(batch);
                })
                        .queueCapacity(2000)
                        .batchSize(1000)
                        .maxWaitMs(50)
                        .offerTimeoutMs(1000)
                        .consumers(2)
                        .build());

        for (int i = 0; i < 1000; i++) {
            assertTrue(processor.submit("shard", i));
        }

        // 轮询等待消费完成且两条线程都 flush 过（分片不保证顺序，只断言总数与消费线程数）
        long deadline = System.currentTimeMillis() + 3000;
        while ((flushed.size() < 1000 || flushThreads.size() < 2) && System.currentTimeMillis() < deadline) {
            Thread.sleep(20);
        }
        processor.shutdown();
        processor = null;

        assertEquals(1000, flushed.size());
        // 分片生效：flush 回调应来自 2 条不同消费线程（batch-processor-shard-0 / -1）
        assertTrue("flush should happen on multiple consumer threads", flushThreads.size() >= 2);
    }

    @Test
    public void submitShouldRejectWrongType() throws Exception {
        processor = new BatchProcessor();
        processor.register("typed",
                BatchWorker.builder(String.class, batch -> {
                })
                        .batchSize(10)
                        .build());

        assertTrue(processor.submit("typed", "ok"));
        // 错误类型在 submit 处被拒（fail fast），不会混入队列
        assertTrue(!processor.submit("typed", 123));
        processor.shutdown();
        processor = null;
    }

    @Test
    public void refreshShouldApplyBatchSizeImmediately() throws Exception {
        processor = new BatchProcessor();
        List<List<Integer>> batches = new CopyOnWriteArrayList<>();
        processor.register("bs",
                BatchWorker.builder(Integer.class, batch -> batches.add(new ArrayList<>(batch)))
                        .queueCapacity(100)
                        .batchSize(2)
                        .maxWaitMs(2000)
                        .offerTimeoutMs(1000)
                        .build());

        // 初始 batchSize=2：凑满 2 条即 flush
        assertTrue(processor.submit("bs", 1));
        assertTrue(processor.submit("bs", 2));
        waitUntil(() -> !batches.isEmpty(), 3000);
        assertEquals(2, batches.get(0).size());

        // 热更新 batchSize=1：无需重启，下一批即按新值攒批
        BatchWorkerConfigPOJO config = new BatchWorkerConfigPOJO();
        config.setBatchSize(1);
        assertTrue(processor.refresh("bs", config));

        assertTrue(processor.submit("bs", 3));
        waitUntil(() -> batches.size() >= 2, 3000);
        assertEquals(1, batches.get(1).size());
    }

    @Test
    public void refreshShouldApplyMaxWaitMsImmediately() throws Exception {
        processor = new BatchProcessor();
        CountDownLatch latch = new CountDownLatch(1);
        processor.register("mw",
                BatchWorker.builder(Integer.class, batch -> latch.countDown())
                        .queueCapacity(100)
                        .batchSize(100)
                        .maxWaitMs(5000)
                        .offerTimeoutMs(1000)
                        .build());

        // 提交 1 条进入攒批窗口后，立刻热更新 maxWaitMs 5000 -> 200：
        // 攒批循环读 volatile 字段即时生效，旧值需等 5s，新值 200ms 内即 flush
        assertTrue(processor.submit("mw", 1));
        BatchWorkerConfigPOJO config = new BatchWorkerConfigPOJO();
        config.setMaxWaitMs(200L);
        assertTrue(processor.refresh("mw", config));

        assertTrue("flush should happen with refreshed maxWaitMs", latch.await(2, TimeUnit.SECONDS));
    }

    @Test
    public void refreshShouldApplyOfferTimeoutImmediately() throws Exception {
        processor = new BatchProcessor();
        // 慢 flush 回调（sleep）：flush 期间消费线程不取数据，队列保持满，为 submit 阻塞提供稳定窗口
        Consumer<List<Integer>> slowHandler = batch -> {
            try {
                Thread.sleep(2000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        };
        processor.register("ot",
                BatchWorker.builder(Integer.class, slowHandler)
                        .queueCapacity(2)
                        .batchSize(2)
                        .maxWaitMs(1000)
                        .offerTimeoutMs(100)
                        .build());

        // 快速灌入 6 条：2 条入队，消费线程取走攒批后进入 2s 慢 flush，期间队列满，其余按旧值 100ms 快速失败
        for (int i = 0; i < 6; i++) {
            processor.submit("ot", i);
        }

        // 热更新 offerTimeoutMs 100 -> 3000：下一次遇到满队列的 submit 将按新值阻塞
        BatchWorkerConfigPOJO config = new BatchWorkerConfigPOJO();
        config.setOfferTimeoutMs(3000L);
        assertTrue(processor.refresh("ot", config));

        // 队列仍满：本次 submit 阻塞直到消费线程 flush 完成腾出空位（约 1~2s），远大于旧值 100ms
        long start = System.currentTimeMillis();
        processor.submit("ot", 100);
        long elapsed = System.currentTimeMillis() - start;
        assertTrue("submit should block with refreshed offerTimeout, elapsed=" + elapsed + "ms",
                elapsed >= 1000 && elapsed <= 3500);
    }

    @Test
    public void refreshShouldApplyQueueCapacity() throws Exception {
        processor = new BatchProcessor();
        // 慢 flush 回调（sleep）：flush 期间消费线程不取数据，队列保持满，容量限制可被稳定观测
        Consumer<List<Integer>> slowHandler = batch -> {
            try {
                Thread.sleep(500);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        };
        processor.register("cap",
                BatchWorker.builder(Integer.class, slowHandler)
                        .queueCapacity(10)
                        .batchSize(10)
                        .maxWaitMs(1000)
                        .offerTimeoutMs(30)
                        .build());

        // 初始容量 10：灌入 60 条，容量限制住成功数（消费者攒批慢，队列长期满）
        int firstSuccess = 0;
        for (int i = 0; i < 60; i++) {
            if (processor.submit("cap", i)) {
                firstSuccess++;
            }
        }
        assertTrue("initial capacity should bound enqueue, success=" + firstSuccess, firstSuccess < 60);

        // 热更新容量 10 -> 60：本轮 20 条全部入队
        BatchWorkerConfigPOJO config = new BatchWorkerConfigPOJO();
        config.setQueueCapacity(60);
        assertTrue(processor.refresh("cap", config));
        int secondSuccess = 0;
        for (int i = 0; i < 20; i++) {
            if (processor.submit("cap", i)) {
                secondSuccess++;
            }
        }
        assertEquals(20, secondSuccess);

        // 热更新容量 60 -> 5（同时 batchSize=5 满足 batchSize <= queueCapacity 校验）：重新受容量限制
        config.setQueueCapacity(5);
        config.setBatchSize(5);
        assertTrue(processor.refresh("cap", config));
        int thirdSuccess = 0;
        for (int i = 0; i < 20; i++) {
            if (processor.submit("cap", i)) {
                thirdSuccess++;
            }
        }
        assertTrue("shrunk capacity should bound enqueue, success=" + thirdSuccess, thirdSuccess < 20);

        // 调大 batchSize 加快 tearDown 的剩余数据刷盘（残留 < 60 时 1 批即可），避免多批 × 500ms
        config.setQueueCapacity(60);
        config.setBatchSize(60);
        processor.refresh("cap", config);
    }

    @Test
    public void refreshShouldAddAndRemoveConsumers() throws Exception {
        processor = new BatchProcessor();
        Set<String> flushThreads = ConcurrentHashMap.newKeySet();
        Map<String, AtomicInteger> flushCounts = new ConcurrentHashMap<>();
        List<Integer> flushed = new CopyOnWriteArrayList<>();
        Consumer<List<Integer>> handler = batch -> {
            String threadName = Thread.currentThread().getName();
            flushThreads.add(threadName);
            flushCounts.computeIfAbsent(threadName, k -> new AtomicInteger()).incrementAndGet();
            flushed.addAll(batch);
        };

        processor.register("con",
                BatchWorker.builder(Integer.class, handler)
                        .queueCapacity(2000)
                        .batchSize(1000)
                        .maxWaitMs(50)
                        .offerTimeoutMs(1000)
                        .consumers(1)
                        .build());

        // 初始 1 线程：灌入 100 条，flush 只来自 1 个线程
        for (int i = 0; i < 100; i++) {
            processor.submit("con", i);
        }
        waitUntil(() -> flushed.size() >= 100, 3000);
        assertEquals(1, flushThreads.size());

        // 热更新 consumers 1 -> 2：新增线程参与消费
        BatchWorkerConfigPOJO config = new BatchWorkerConfigPOJO();
        config.setConsumers(2);
        assertTrue(processor.refresh("con", config));
        for (int i = 0; i < 100; i++) {
            processor.submit("con", i);
        }
        waitUntil(() -> flushed.size() >= 200 && flushThreads.size() >= 2, 3000);
        assertEquals(2, flushThreads.size());

        // 热更新 consumers 2 -> 1：refresh 同步等待多余线程退出后，新 flush 只来自 1 个线程
        config.setConsumers(1);
        assertTrue(processor.refresh("con", config));
        Map<String, Integer> base = snapshot(flushCounts);
        for (int i = 0; i < 200; i++) {
            processor.submit("con", i);
        }
        waitUntil(() -> flushed.size() >= 400, 3000);
        Set<String> active = new HashSet<>();
        flushCounts.forEach((name, count) -> {
            if (count.get() > base.getOrDefault(name, 0)) {
                active.add(name);
            }
        });
        assertEquals("after shrink, flush should come from exactly 1 consumer thread", 1, active.size());
    }

    @Test(expected = IllegalArgumentException.class)
    public void refreshShouldRejectInvalidConfig() throws Exception {
        processor = new BatchProcessor();
        processor.register("bad",
                BatchWorker.builder(Integer.class, batch -> {
                })
                        .queueCapacity(100)
                        .build());

        // batchSize(999) > queueCapacity(100)：与 build() 相同的校验，非法组合直接拒绝
        BatchWorkerConfigPOJO config = new BatchWorkerConfigPOJO();
        config.setBatchSize(999);
        config.setQueueCapacity(100);
        processor.refresh("bad", config);
    }

    @Test
    public void registerShouldRejectInvalidWorkerKey() {
        processor = new BatchProcessor();
        BatchWorker<String> worker = BatchWorker.builder(String.class, batch -> {
        }).build();

        try {
            processor.register("DemoItem:insert", worker);
            fail("expected IllegalArgumentException");
        } catch (IllegalArgumentException ex) {
            assertTrue(ex.getMessage().contains("invalid worker key"));
        }

        try {
            processor.register("", worker);
            fail("expected IllegalArgumentException");
        } catch (IllegalArgumentException ex) {
            assertTrue(ex.getMessage().contains("must not be blank"));
        }

        processor.register("demo_item-insert_1", worker);
    }

    private static void waitUntil(BooleanSupplier condition, long timeoutMs) throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (!condition.getAsBoolean() && System.currentTimeMillis() < deadline) {
            Thread.sleep(20);
        }
    }

    private static Map<String, Integer> snapshot(Map<String, AtomicInteger> counts) {
        Map<String, Integer> copy = new HashMap<>();
        counts.forEach((name, count) -> copy.put(name, count.get()));
        return copy;
    }
}
