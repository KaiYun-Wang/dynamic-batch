package com.dynamicbatch.core;

import com.dynamicbatch.core.pojo.SpoolConfigPOJO;
import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

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

    @After
    public void tearDown() {
        if (processor != null) {
            processor.shutdown();
        }
    }

    @Test
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

        processor.resizeGroup("resize-expand", 4, 30_000);
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

    @Test
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

        processor.resizeGroup("resize-shrink", 2, 30_000);
        assertEquals(2, group.getPartitionCount());

        awaitFlushed(accepted, flushed, 30_000);
        assertEquals(accepted, flushed.size());
    }

    @Test(timeout = 20_000)
    public void resizeTimeoutRollsBackExpand() throws Exception {
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
        assertTrue(firstFlushStarted.await(5, TimeUnit.SECONDS));

        try {
            processor.resizeGroup("resize-timeout", 4, 500);
            fail("expected TimeoutException");
        } catch (TimeoutException expected) {
            // 扩容应回滚
        }
        assertEquals(1, group.getPartitionCount());
        releaseFlush.countDown();

        awaitFlushed(100, flushed, 15_000);
        assertEquals(100, flushed.size());
    }

    @Test(timeout = 30_000)
    public void resizeFailureRollsBackShrink() throws Exception {
        processor = new BatchProcessor();
        Thread testThread = Thread.currentThread();
        CountDownLatch drainStarted = new CountDownLatch(1);
        List<Integer> flushed = new CopyOnWriteArrayList<>();
        BatchWorkerGroup<Integer> group = BatchWorkerGroup.builder(Integer.class,
                        SpoolConfigPOJO.builder(new File(temp.getRoot(), "resize-shrink-rollback").getAbsolutePath())
                                .flushIntervalMs(20)
                                .build(),
                        batch -> {
                            flushed.addAll(batch);
                            // pauseAll 的排空（drain）在 resize 调用线程同步执行：
                            // 回调运行在调用线程即说明已进入 drain 窗口（缩容摘除前）
                            if (Thread.currentThread() == testThread) {
                                drainStarted.countDown();
                            }
                            // 攒批节奏放缓，拉长 drain 窗口，给 kill 线程留足时间
                            try {
                                Thread.sleep(100);
                            } catch (InterruptedException e) {
                                Thread.currentThread().interrupt();
                            }
                        })
                .partitionCount(4)
                .batchSize(10)
                .maxWaitMs(100)
                .build();
        processor.registerGroup("resize-shrink-rollback", group);

        // 先提交一批并等到有产出：确认 spool→dispatcher→worker 链路已通，
        // 之后的提交才会及时进入 Worker 队列（否则 spool 落盘延迟会让 dispatcher 先暂停）
        int accepted = 0;
        for (int i = 0; i < 100; i++) {
            if (processor.submit("resize-shrink-rollback", "k" + (i % 4), i)) {
                accepted++;
            }
        }
        awaitFlushed(20, flushed, 10_000);

        // resize 前锁定幸存分区 worker-0 的消费线程引用（缩容只摘尾部，0 号必幸存）
        Field partitionsField = BatchWorkerGroup.class.getDeclaredField("partitions");
        partitionsField.setAccessible(true);
        @SuppressWarnings("unchecked")
        List<BatchWorker<Integer>> partitions = (List<BatchWorker<Integer>>) partitionsField.get(group);
        Field consumerThreadField = BatchWorker.class.getDeclaredField("consumerThread");
        consumerThreadField.setAccessible(true);
        Thread survivor0 = (Thread) consumerThreadField.get(partitions.get(0));

        // drain 开始后杀死幸存分区 worker-0 的消费线程（此时整组已 PAUSED，线程在暂停睡眠中）：
        // 线程死后 phase 永远停在 PAUSED，resumeAll 等它 RUNNING 必然超时，
        // 制造「暂停成功、缩容已摘除、恢复失败」的失败场景
        Thread killer = new Thread(() -> {
            try {
                if (!drainStarted.await(10, TimeUnit.SECONDS)) {
                    return;
                }
                survivor0.interrupt();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });
        killer.start();

        // 再提交一批制造队列积压（drain 窗口），随后立刻缩容
        for (int i = 100; i < 300; i++) {
            if (processor.submit("resize-shrink-rollback", "k" + (i % 4), i)) {
                accepted++;
            }
        }
        try {
            processor.resizeGroup("resize-shrink-rollback", 2, 6_000);
            fail("expected TimeoutException");
        } catch (TimeoutException expected) {
            // 缩容应回滚
        }
        killer.join(1_000);

        // 分区数回滚到缩容前
        assertEquals(4, group.getPartitionCount());
    }

    @Test(timeout = 20_000)
    public void resizeConcurrentSubmit() throws Exception {
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

        processor.resizeGroup("resize-concurrent", 4, 30_000);
        submitting.set(false);
        submitter.join(10_000);

        awaitFlushed(accepted[0], flushed, 30_000);
        assertEquals(accepted[0], flushed.size());
    }

    @Test
    public void resizeSameSizeIsNoOp() throws Exception {
        processor = new BatchProcessor();
        BatchWorkerGroup<Integer> group = BatchWorkerGroup.builder(Integer.class,
                        spoolConfig(new File(temp.getRoot(), "resize-noop")),
                        batch -> { })
                .partitionCount(3)
                .build();
        processor.registerGroup("resize-noop", group);
        processor.resizeGroup("resize-noop", 3, 5_000);
        assertEquals(3, group.getPartitionCount());
    }

    @Test
    public void resizeUnknownKeyThrows() throws TimeoutException {
        processor = new BatchProcessor();
        try {
            processor.resizeGroup("nope", 2, 1_000);
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
            processor.resizeGroup("resize-invalid", 0, 1_000);
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
            group.resize(4, 1_000);
            fail("expected IllegalStateException");
        } catch (IllegalStateException expected) {
            // shuttingDown 已置位
        }
    }
}
