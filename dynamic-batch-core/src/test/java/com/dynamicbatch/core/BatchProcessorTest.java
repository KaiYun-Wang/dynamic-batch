package com.dynamicbatch.core;

import org.junit.After;
import org.junit.Test;

import com.dynamicbatch.core.BatchWorker;

import java.util.List;
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
