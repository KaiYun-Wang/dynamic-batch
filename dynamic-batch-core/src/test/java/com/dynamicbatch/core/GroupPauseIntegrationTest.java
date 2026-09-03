package com.dynamicbatch.core;

import com.dynamicbatch.core.pojo.SpoolConfigPOJO;
import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * 暂停/恢复端到端集成测试：submit → Spool(磁盘) → Dispatcher → Worker → flushCallback 全链路。
 */
public class GroupPauseIntegrationTest {

    @Rule
    public TemporaryFolder temp = new TemporaryFolder();

    private BatchProcessor processor;
    private BatchWorkerGroup<Integer> group;

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
    public void pauseResumeEndToEnd() throws Exception {
        processor = new BatchProcessor();
        List<Integer> flushed = new CopyOnWriteArrayList<>();
        group = BatchWorkerGroup.builder(Integer.class,
                        spoolConfig(new File(temp.getRoot(), "pause-e2e")),
                        flushed::addAll)
                .batchSize(50)
                .maxWaitMs(100)
                .build();
        processor.registerGroup("pause-e2e", group);

        int accepted = 0;
        for (int i = 0; i < 1000; i++) {
            if (processor.submit("pause-e2e", "k-" + (i % 4), i)) {
                accepted++;
            }
        }

        group.pauseAll(5_000);
        int frozen = flushed.size();
        Thread.sleep(300);
        assertEquals("暂停后 flush 应冻结", frozen, flushed.size());

        assertTrue("暂停期间 submit 应照常落盘", processor.submit("pause-e2e", "k-x", -1));
        int expectedTotal = accepted + 1;

        group.resumeAll(5_000);
        awaitFlushed(expectedTotal, flushed, 30_000);
        processor.shutdown();
        processor = null;

        assertEquals("恢复后应对账一条不丢（含暂停期间 submit 的数据）", expectedTotal, flushed.size());
    }

    @Test(timeout = 20_000)
    public void pauseAllTimeoutRollsBack() throws Exception {
        processor = new BatchProcessor();
        CountDownLatch firstFlushStarted = new CountDownLatch(1);
        CountDownLatch releaseFlush = new CountDownLatch(1);
        List<Integer> flushed = new CopyOnWriteArrayList<>();
        group = BatchWorkerGroup.builder(Integer.class,
                        spoolConfig(new File(temp.getRoot(), "pause-timeout")),
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
                .batchSize(50)
                .maxWaitMs(100)
                .build();
        processor.registerGroup("pause-timeout", group);

        int accepted = 0;
        for (int i = 0; i < 100; i++) {
            if (processor.submit("pause-timeout", "k", i)) {
                accepted++;
            }
        }
        assertTrue("首批 flush 应已开始并卡在回调内", firstFlushStarted.await(5, TimeUnit.SECONDS));

        try {
            group.pauseAll(500);
            fail("expected TimeoutException");
        } catch (TimeoutException expected) {
            // 已自动回滚
        }
        releaseFlush.countDown();

        group.resumeAll(5_000);
        awaitFlushed(accepted, flushed, 10_000);
        processor.shutdown();
        processor = null;

        assertEquals("回滚后链路应完好，全部数据消费完", accepted, flushed.size());
    }

    @Test
    public void pauseAllOnUnstartedGroupThrows() {
        BatchWorkerGroup<String> unstarted = BatchWorkerGroup.builder(String.class,
                        SpoolConfigPOJO.builder("unused-dir").build(),
                        batch -> { })
                .build();
        try {
            unstarted.pauseAll(1_000);
            fail("expected IllegalStateException");
        } catch (IllegalStateException expected) {
            // 未 start 的组零资源，fail fast
        } catch (TimeoutException e) {
            fail("unexpected TimeoutException");
        }
    }
}
