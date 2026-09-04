package com.dynamicbatch.core;

import com.dynamicbatch.core.pojo.SpoolConfigPOJO;
import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Consumer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * 阻塞投递端到端集成测试：队列满是下游 flush 慢的正常背压，Dispatcher 阻塞在 put 上
 * 等空位，背压期间数据零丢失；强制关闭时唯一允许的丢失点是被中断的在途投递（至多一条），
 * 队列内数据由 flushRemaining 兜底。
 */
public class GroupBlockingDeliveryTest {

    @Rule
    public TemporaryFolder temp = new TemporaryFolder();

    private BatchProcessor processor;

    @After
    public void tearDown() {
        if (processor != null) {
            processor.shutdown();
        }
    }

    /**
     * 背压与强制关闭对账：容量 5 + flush 慢（150ms/条），灌入后 Dispatcher 阻塞在 put 上；
     * 强制关闭时被中断的在途投递至多丢弃一条，队列内数据由 flushRemaining 兜底，
     * 磁盘上未消费的数据不删除——同目录重建组续读后总账一条不丢。
     */
    @Test(timeout = 90_000)
    public void backpressureBlocksDispatcherAndShutdownReconciles() throws Exception {
        File dir = new File(temp.getRoot(), "blocking-e2e");
        List<Integer> flushedFirst = new CopyOnWriteArrayList<>();
        Consumer<List<Integer>> slowFlush = batch -> {
            try {
                Thread.sleep(150);   // 模拟慢落库：消费速率 ~6 条/秒，内存队列很快打满
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            flushedFirst.addAll(batch);
        };
        BatchProcessor first = new BatchProcessor();
        first.registerGroup("blocking-e2e",
                BatchWorkerGroup.builder(Integer.class,
                                SpoolConfigPOJO.builder(dir.getAbsolutePath()).build(), slowFlush)
                        .partitionCount(1)
                        .queueCapacity(5)
                        .batchSize(1)
                        .maxWaitMs(10)
                        .build());

        int accepted = 0;
        for (int i = 0; i < 30; i++) {
            if (first.submit("blocking-e2e", "k-" + (i % 2), i)) {
                accepted++;
            }
        }
        assertEquals("submit 全部落盘（内存队列满由投递侧背压消化）", 30, accepted);

        // 让 Worker 消化一部分（~3 秒 ≈ 20 条），Dispatcher 持续投递直至阻塞在队列满的 put 上
        Thread.sleep(3_000);

        first.shutdown();   // 强制关闭：interrupt 打断阻塞 put，丢至多一条在途条目

        // 磁盘上未消费的数据不删除：同目录重建组续读（flush 不再放慢），剩余数据全部消化
        List<Integer> flushedSecond = new CopyOnWriteArrayList<>();
        BatchProcessor second = new BatchProcessor();
        try {
            second.registerGroup("blocking-e2e",
                    BatchWorkerGroup.builder(Integer.class,
                                    SpoolConfigPOJO.builder(dir.getAbsolutePath()).build(),
                                    flushedSecond::addAll)
                            .partitionCount(1)
                            .batchSize(10)
                            .maxWaitMs(50)
                            .build());
            long deadline = System.currentTimeMillis() + 30_000;
            while (flushedFirst.size() + flushedSecond.size() < accepted - 1
                    && System.currentTimeMillis() < deadline) {
                Thread.sleep(50);
            }
        } finally {
            second.shutdown();
        }

        int total = flushedFirst.size() + flushedSecond.size();
        assertTrue("总账不丢，唯一允许的丢失点是被中断的在途投递（至多一条）："
                        + "第一轮=" + flushedFirst.size() + "，续读=" + flushedSecond.size()
                        + "，accepted=" + accepted,
                total >= accepted - 1 && total <= accepted);
    }

    /**
     * 优雅关闭对照：队列不满、Dispatcher 未阻塞时关闭，全量对账一条不丢
     * （阻塞化不得引入正常路径的数据丢失）。
     */
    @Test(timeout = 30_000)
    public void gracefulShutdownNoDataLoss() throws Exception {
        processor = new BatchProcessor();
        List<Integer> flushed = new CopyOnWriteArrayList<>();
        BatchWorkerGroup<Integer> group = BatchWorkerGroup.builder(Integer.class,
                        SpoolConfigPOJO.builder(
                                new File(temp.getRoot(), "blocking-graceful").getAbsolutePath()).build(),
                        flushed::addAll)
                .partitionCount(1)
                .batchSize(5)
                .maxWaitMs(50)
                .build();
        processor.registerGroup("blocking-graceful", group);

        int accepted = 0;
        for (int i = 0; i < 100; i++) {
            if (processor.submit("blocking-graceful", "k-" + (i % 4), i)) {
                accepted++;
            }
        }
        long deadline = System.currentTimeMillis() + 30_000;
        while (flushed.size() < accepted && System.currentTimeMillis() < deadline) {
            Thread.sleep(50);
        }
        assertEquals("全部消化后再关闭", accepted, flushed.size());

        processor.shutdown();
        processor = null;
        assertEquals("优雅关闭不丢数据", accepted, flushed.size());
    }
}
