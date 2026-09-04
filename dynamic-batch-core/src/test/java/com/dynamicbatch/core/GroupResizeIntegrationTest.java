package com.dynamicbatch.core;

import com.dynamicbatch.core.pojo.SpoolConfigPOJO;
import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * 分区数热更新端到端集成测试：三步编排（暂停调度器 → 改变分区数 → 恢复调度器），每步各自原子。
 */
public class GroupResizeIntegrationTest {

    @Rule
    public TemporaryFolder temp = new TemporaryFolder();

    private BatchProcessor processor;

    private static SpoolConfigPOJO spoolConfig(File dir) {
        return SpoolConfigPOJO.builder(dir.getAbsolutePath()).build();
    }

    private static void awaitFlushed(int expected, List<Integer> flushed, long timeoutMs)
            throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (flushed.size() < expected && System.currentTimeMillis() < deadline) {
            Thread.sleep(50);
        }
    }

    /** 轮询等待谓词成立（终态断言，超时即红） */
    private static void awaitTrue(Supplier<Boolean> predicate, long timeoutMs, String message)
            throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (!predicate.get()) {
            if (System.currentTimeMillis() > deadline) {
                fail(message);
            }
            Thread.sleep(20);
        }
    }

    /** 运行时调整编排（finally 恢复，避免失败把组留在暂停态） */
    private void runHeatUpdate(String key, int newSize) throws Exception {
        processor.pauseDispatcher(key);
        awaitTrue(() -> processor.getDispatcherPhase(key) == Dispatcher.PausePhase.PAUSED,
                5_000, "暂停应在调度器确认后生效");
        try {
            processor.resizePartitions(key, newSize, 10_000);
        } finally {
            processor.resumeDispatcher(key);
        }
    }

    @After
    public void tearDown() {
        if (processor != null) {
            processor.shutdown();
        }
    }

    /** 扩容 1→4：同一 routingKey 跨热更新边界仍严格有序，全量对账 */
    @Test(timeout = 90_000)
    public void expandPartitionsPreservesOrderPerKey() throws Exception {
        processor = new BatchProcessor();
        List<Integer> flushed = new CopyOnWriteArrayList<>();
        BatchWorkerGroup<Integer> group = BatchWorkerGroup.builder(Integer.class,
                        spoolConfig(new File(temp.getRoot(), "resize-expand")),
                        flushed::addAll)
                .partitionCount(1)
                .batchSize(10)
                .maxWaitMs(50)
                .build();
        processor.registerGroup("resize-expand", group);

        for (int i = 0; i < 100; i++) {
            processor.submit("resize-expand", "same-key", i);
        }
        awaitFlushed(50, flushed, 10_000);

        runHeatUpdate("resize-expand", 4);
        assertEquals(4, group.getPartitionCount());

        for (int i = 100; i < 200; i++) {
            processor.submit("resize-expand", "same-key", i);
        }
        awaitFlushed(200, flushed, 30_000);

        List<Integer> expected = new ArrayList<>();
        for (int i = 0; i < 200; i++) {
            expected.add(i);
        }
        assertEquals(expected, new ArrayList<>(flushed));
    }

    /** 缩容 4→2：存量与热更新期间的数据全部消化，一条不丢 */
    @Test(timeout = 90_000)
    public void shrinkPartitionsNoDataLoss() throws Exception {
        processor = new BatchProcessor();
        List<Integer> flushed = new CopyOnWriteArrayList<>();
        BatchWorkerGroup<Integer> group = BatchWorkerGroup.builder(Integer.class,
                        spoolConfig(new File(temp.getRoot(), "resize-shrink")),
                        flushed::addAll)
                .partitionCount(4)
                .batchSize(1)
                .maxWaitMs(50)
                .build();
        processor.registerGroup("resize-shrink", group);

        int accepted = 0;
        for (int p = 0; p < 4; p++) {
            for (int i = 0; i < 50; i++) {
                if (processor.submit("resize-shrink", "p" + p, p * 100 + i)) {
                    accepted++;
                }
            }
        }

        runHeatUpdate("resize-shrink", 2);
        assertEquals(2, group.getPartitionCount());

        awaitFlushed(accepted, flushed, 30_000);
        assertEquals(accepted, flushed.size());
    }

    /**
     * 排空超时：Worker 手头批次卡在慢回调，预算耗尽抛 TimeoutException；
     * 分区数不变（失败发生在变更前，即无效果），恢复后组继续消化完存量（自愈）。
     */
    @Test(timeout = 90_000)
    public void resizePartitionsTimeoutKeepsTopology() throws Exception {
        processor = new BatchProcessor();
        CountDownLatch firstFlushStarted = new CountDownLatch(1);
        CountDownLatch releaseFlush = new CountDownLatch(1);
        List<Integer> flushed = new CopyOnWriteArrayList<>();
        BatchWorkerGroup<Integer> group = BatchWorkerGroup.builder(Integer.class,
                        spoolConfig(new File(temp.getRoot(), "resize-timeout")),
                        batch -> {
                            flushed.addAll(batch);
                            if (firstFlushStarted.getCount() > 0) {
                                firstFlushStarted.countDown();
                                try {
                                    releaseFlush.await(10, TimeUnit.SECONDS);
                                } catch (InterruptedException e) {
                                    Thread.currentThread().interrupt();
                                }
                            }
                        })
                .partitionCount(1)
                .batchSize(50)
                .maxWaitMs(100)
                .build();
        processor.registerGroup("resize-timeout", group);

        for (int i = 0; i < 100; i++) {
            processor.submit("resize-timeout", "k", i);
        }
        assertTrue("首批 flush 应已开始并卡在回调内", firstFlushStarted.await(5, TimeUnit.SECONDS));

        processor.pauseDispatcher("resize-timeout");
        awaitTrue(() -> processor.getDispatcherPhase("resize-timeout") == Dispatcher.PausePhase.PAUSED,
                5_000, "暂停应在调度器确认后生效");
        try {
            processor.resizePartitions("resize-timeout", 4, 500);
            fail("expected TimeoutException");
        } catch (TimeoutException expected) {
            // 排空等待超时：未动分区
        }
        assertEquals("失败即无效果，分区数不变", 1, group.getPartitionCount());
        releaseFlush.countDown();

        processor.resumeDispatcher("resize-timeout");
        awaitFlushed(100, flushed, 30_000);
        assertEquals(100, flushed.size());
    }

    /** 前置检查：调度器未暂停时改变分区数直接拒绝 */
    @Test
    public void resizePartitionsRequiresPausedDispatcher() throws Exception {
        processor = new BatchProcessor();
        BatchWorkerGroup<Integer> group = BatchWorkerGroup.builder(Integer.class,
                        spoolConfig(new File(temp.getRoot(), "resize-require-pause")),
                        batch -> { })
                .partitionCount(2)
                .build();
        processor.registerGroup("resize-require-pause", group);
        try {
            processor.resizePartitions("resize-require-pause", 4, 1_000);
            fail("expected IllegalStateException");
        } catch (IllegalStateException expected) {
            // 未暂停，fail fast
        }
    }

    /** 幂等：同尺寸变更直接返回（须先暂停） */
    @Test(timeout = 30_000)
    public void resizeSameSizeIsNoOp() throws Exception {
        processor = new BatchProcessor();
        BatchWorkerGroup<Integer> group = BatchWorkerGroup.builder(Integer.class,
                        spoolConfig(new File(temp.getRoot(), "resize-noop")),
                        batch -> { })
                .partitionCount(3)
                .build();
        processor.registerGroup("resize-noop", group);
        processor.pauseDispatcher("resize-noop");
        awaitTrue(() -> processor.getDispatcherPhase("resize-noop") == Dispatcher.PausePhase.PAUSED,
                5_000, "暂停应在调度器确认后生效");
        try {
            processor.resizePartitions("resize-noop", 3, 5_000);
        } finally {
            processor.resumeDispatcher("resize-noop");
        }
        assertEquals(3, group.getPartitionCount());
    }

    @Test
    public void resizeUnknownKeyThrows() throws Exception {
        processor = new BatchProcessor();
        try {
            processor.resizePartitions("nope", 2, 1_000);
            fail("expected IllegalArgumentException");
        } catch (IllegalArgumentException expected) {
            // fail fast
        }
    }

    @Test
    public void resizeInvalidSizeThrows() throws Exception {
        processor = new BatchProcessor();
        BatchWorkerGroup<Integer> group = BatchWorkerGroup.builder(Integer.class,
                        spoolConfig(new File(temp.getRoot(), "resize-invalid")),
                        batch -> { })
                .partitionCount(2)
                .build();
        processor.registerGroup("resize-invalid", group);
        try {
            processor.resizePartitions("resize-invalid", 0, 1_000);
            fail("expected IllegalArgumentException");
        } catch (IllegalArgumentException expected) {
            // fail fast
        }
    }

    @Test
    public void resizeAfterShutdownRejected() throws Exception {
        BatchWorkerGroup<Integer> group = BatchWorkerGroup.builder(Integer.class,
                        spoolConfig(new File(temp.getRoot(), "resize-shutdown")),
                        batch -> { })
                .partitionCount(2)
                .build();
        processor = new BatchProcessor();
        processor.registerGroup("resize-shutdown", group);
        group.shutdown();
        try {
            group.resizePartitions(4, 1_000);
            fail("expected IllegalStateException");
        } catch (IllegalStateException expected) {
            // shuttingDown 已置位
        }
    }

    /** 并发 submit 不断流时执行三步热更新：全量对账不丢 */
    @Test(timeout = 90_000)
    public void heatUpdateConcurrentSubmit() throws Exception {
        processor = new BatchProcessor();
        List<Integer> flushed = new CopyOnWriteArrayList<>();
        BatchWorkerGroup<Integer> group = BatchWorkerGroup.builder(Integer.class,
                        spoolConfig(new File(temp.getRoot(), "resize-concurrent")),
                        flushed::addAll)
                .partitionCount(2)
                .batchSize(20)
                .maxWaitMs(50)
                .build();
        processor.registerGroup("resize-concurrent", group);

        AtomicBoolean submitting = new AtomicBoolean(true);
        int[] accepted = {0};
        Thread submitter = new Thread(() -> {
            for (int i = 0; i < 5_000 && submitting.get(); i++) {
                if (processor.submit("resize-concurrent", "k-" + (i % 8), i)) {
                    synchronized (accepted) {
                        accepted[0]++;
                    }
                }
            }
        });
        submitter.start();

        runHeatUpdate("resize-concurrent", 4);
        submitting.set(false);
        submitter.join(10_000);

        awaitFlushed(accepted[0], flushed, 30_000);
        assertEquals(accepted[0], flushed.size());
    }
}
