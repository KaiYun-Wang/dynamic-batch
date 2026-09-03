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
 *
 * <p>覆盖：暂停冻结消费（submit 照常落盘）→ 恢复对账一条不丢；超时自动回滚后链路完好；
 * 未启动/未知组的 fail fast。全部使用 TemporaryFolder 隔离目录（同 JVM 同目录 = 目录锁冲突）。
 * 走 {@link BatchProcessor} 公共入口（pauseGroup/resumeGroup），同时覆盖转发链路。
 */
@SuppressWarnings("deprecation")   // pauseGroup/resumeGroup 已标废弃：本测试验证的正是这两个临时入口，resize 落地后随删除一并翻新
public class GroupPauseIntegrationTest {

    @Rule
    public TemporaryFolder temp = new TemporaryFolder();

    private BatchProcessor processor;

    /** 组级测试辅助：每组独占 Spool 目录（同 JVM 同目录 = 目录锁冲突 fail fast） */
    private static SpoolConfigPOJO spoolConfig(File dir) {
        return SpoolConfigPOJO.builder(dir.getAbsolutePath()).build();
    }

    /** 等待 flush 总数到达期望值（Dispatcher 搬运 + Worker 消费的异步同步点） */
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

    /**
     * 暂停/恢复端到端：不等消费即暂停 → flush 冻结（搬运停、队列已排空）→
     * 暂停期间 submit 照常成功（落盘不消费）→ 恢复后全部对账（一条不丢）。
     */
    @Test
    public void pauseResumeEndToEnd() throws Exception {
        processor = new BatchProcessor();
        List<Integer> flushed = new CopyOnWriteArrayList<>();
        processor.registerGroup("pause-e2e",
                BatchWorkerGroup.builder(Integer.class,
                                spoolConfig(new File(temp.getRoot(), "pause-e2e")),
                                flushed::addAll)
                        .batchSize(50)
                        .maxWaitMs(100)
                        .build());

        int accepted = 0;
        for (int i = 0; i < 1000; i++) {
            if (processor.submit("pause-e2e", "k-" + (i % 4), i)) {
                accepted++;
            }
        }

        // 不等消费直接暂停:pauseGroup 内部等全部确认 + 排空队列残留
        processor.pauseGroup("pause-e2e", 5_000);
        int frozen = flushed.size();
        Thread.sleep(300);
        assertEquals("暂停后 flush 应冻结", frozen, flushed.size());

        // 暂停期间 submit 照常成功，数据落盘不消费
        assertTrue("暂停期间 submit 应照常落盘", processor.submit("pause-e2e", "k-x", -1));
        int expectedTotal = accepted + 1;

        processor.resumeGroup("pause-e2e", 5_000);
        awaitFlushed(expectedTotal, flushed, 30_000);
        processor.shutdown();
        processor = null;

        assertEquals("恢复后应对账一条不丢（含暂停期间 submit 的数据）", expectedTotal, flushed.size());
    }

    /**
     * 暂停超时自动回滚：flush 回调卡死使 Worker 无法确认暂停 → pauseGroup 超时抛出
     * （已自动回滚）→ 放行回调后恢复消费，数据链路完好。
     */
    @Test(timeout = 20_000)
    public void pauseAllTimeoutRollsBack() throws Exception {
        processor = new BatchProcessor();
        CountDownLatch firstFlushStarted = new CountDownLatch(1);
        CountDownLatch releaseFlush = new CountDownLatch(1);
        List<Integer> flushed = new CopyOnWriteArrayList<>();
        processor.registerGroup("pause-timeout",
                BatchWorkerGroup.builder(Integer.class,
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
                        .build());

        int accepted = 0;
        for (int i = 0; i < 100; i++) {
            if (processor.submit("pause-timeout", "k", i)) {
                accepted++;
            }
        }
        // 先等首批 flush 卡在回调内：此刻 Worker 阻塞在 flush 中无法确认暂停，pauseGroup 必超时
        assertTrue("首批 flush 应已开始并卡在回调内", firstFlushStarted.await(5, TimeUnit.SECONDS));

        try {
            processor.pauseGroup("pause-timeout", 500);
            fail("expected TimeoutException");
        } catch (TimeoutException expected) {
            // 已自动回滚，整组保持消费
        }
        releaseFlush.countDown();

        processor.resumeGroup("pause-timeout", 5_000);
        awaitFlushed(accepted, flushed, 10_000);
        processor.shutdown();
        processor = null;

        assertEquals("回滚后链路应完好，全部数据消费完", accepted, flushed.size());
    }

    /** 未知组 fail fast（IllegalArgumentException），而非静默失败掩盖打错 key */
    @Test
    public void pauseGroupOnUnknownKeyThrows() {
        processor = new BatchProcessor();
        try {
            processor.pauseGroup("no-such-group", 1_000);
            fail("expected IllegalArgumentException");
        } catch (IllegalArgumentException expected) {
            // 组不存在抛 IAE，显式暴露误用
        } catch (TimeoutException e) {
            fail("unexpected TimeoutException");
        }
    }

    /** 未启动的组直接调组级入口 fail fast（IllegalStateException）；build 期零磁盘资源 */
    @Test
    public void pauseAllOnUnstartedGroupThrows() {
        BatchWorkerGroup<String> group = BatchWorkerGroup.builder(String.class,
                        SpoolConfigPOJO.builder("unused-dir").build(),
                        batch -> { })
                .build();
        try {
            group.pauseAll(1_000);
            fail("expected IllegalStateException");
        } catch (IllegalStateException expected) {
            // 未 start 的组零资源，fail fast 暴露误用
        } catch (TimeoutException e) {
            fail("unexpected TimeoutException");
        }
    }
}
