package com.dynamicbatch.core;

import org.junit.After;
import org.junit.Test;

import com.dynamicbatch.core.BatchWorker;

import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

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
}
