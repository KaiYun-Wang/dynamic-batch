package com.dynamicbatch.core;

import com.dynamicbatch.common.pojo.EnvelopePOJO;
import com.dynamicbatch.core.pojo.SpoolConfigPOJO;
import com.dynamicbatch.core.pojo.StatsConfigPOJO;
import com.dynamicbatch.core.stats.CumulativeStats;
import com.dynamicbatch.core.stats.StatsSnapshot;
import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BooleanSupplier;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * 组级统计端到端：submit → Spool → Dispatcher → Worker flush（含 shutdown 排空）
 * 全径计数对账，以及组快照中 live 统计（现场差分）的可见性与空态约定。
 */
public class GroupStatsIntegrationTest {

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

    /** 等待布尔条件成立（超时返回 false） */
    private static boolean awaitTrue(BooleanSupplier condition, long timeoutMs)
            throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (!condition.getAsBoolean()) {
            if (System.currentTimeMillis() > deadline) {
                return false;
            }
            Thread.sleep(50);
        }
        return true;
    }

    /** 桶计数总和（累计口径：桶累计与回调累计同源，关闭后无并发写严格相等） */
    private static long bucketSum(StatsSnapshot stats) {
        long sum = 0;
        for (long count : stats.getBucketTotals()) {
            sum += count;
        }
        return sum;
    }

    @After
    public void tearDown() {
        if (processor != null) {
            processor.shutdown();
        }
    }

    /**
     * 全径对账：全部条目消费完成后，提交计数 == append 成功数，
     * 回调计数 == flush 总数，桶总和 == 回调计数。
     */
    @Test
    public void countersReconcileAcrossFullPipeline() throws Exception {
        processor = new BatchProcessor();
        List<Integer> flushed = new CopyOnWriteArrayList<>();
        BatchWorkerGroup<Integer> group = BatchWorkerGroup.builder(Integer.class,
                        spoolConfig(new File(temp.getRoot(), "stats-e2e")),
                        flushed::addAll)
                .queueCapacity(1024)
                .batchSize(100)
                .maxWaitMs(100)
                .build();
        processor.registerGroup("stats-e2e", group);

        int accepted = 0;
        for (int i = 0; i < 1000; i++) {
            if (processor.submit("stats-e2e", "k-" + (i % 4), i)) {
                accepted++;
            }
        }
        awaitFlushed(accepted, flushed, 30_000);
        processor.shutdown();
        processor = null;

        StatsSnapshot stats = group.snapshot().getStats();
        assertNotNull(stats);
        assertEquals("提交计数应与 append 成功数一致", accepted, stats.getSubmitTotal());
        assertEquals("回调计数应与 flush 总数一致（一条不丢）", accepted, stats.getCallbackTotal());
        assertEquals("桶总和应与回调计数对账", stats.getCallbackTotal(), bucketSum(stats));
    }

    /**
     * 成败同径：flush 回调抛异常的批次同样计入回调计数与 RT 桶（flushBatch 内部吞 Exception，
     * 成功失败都执行 finally 记录）。
     */
    @Test
    public void failureCallbackCountsSameAsSuccess() throws Exception {
        processor = new BatchProcessor();
        AtomicBoolean flushInvoked = new AtomicBoolean(false);
        BatchWorkerGroup<String> group = BatchWorkerGroup.builder(String.class,
                        spoolConfig(new File(temp.getRoot(), "stats-fail")),
                        batch -> {
                            flushInvoked.set(true);
                            throw new RuntimeException("模拟回调失败");
                        })
                .queueCapacity(64)
                .batchSize(10)
                .maxWaitMs(50)
                .failureHandler(batch -> { })
                .build();
        processor.registerGroup("stats-fail", group);

        for (int i = 0; i < 3; i++) {
            assertTrue(processor.submit("stats-fail", "k", "v-" + i));
        }
        assertTrue("flush 应已被调用", awaitTrue(flushInvoked::get, 10_000));
        assertTrue("失败回调也应计入回调计数（成败同径）",
                awaitTrue(() -> group.snapshot().getStats().getCallbackTotal() == 3, 10_000));

        StatsSnapshot stats = group.snapshot().getStats();
        assertEquals(3L, stats.getSubmitTotal());
        assertEquals(3L, stats.getCallbackTotal());
        assertEquals("失败批次也应落 RT 桶", stats.getCallbackTotal(), bucketSum(stats));
    }

    /**
     * shutdown 排空路径对账：不等消费直接关闭（排空 + 中断兜底），
     * 回调计数不超过提交计数，桶总和与回调计数保持一致。
     */
    @Test
    public void shutdownDrainCountsReconcile() throws Exception {
        processor = new BatchProcessor();
        List<Integer> flushed = new CopyOnWriteArrayList<>();
        BatchWorkerGroup<Integer> group = BatchWorkerGroup.builder(Integer.class,
                        spoolConfig(new File(temp.getRoot(), "stats-drain")),
                        flushed::addAll)
                .queueCapacity(1024)
                .batchSize(500)
                .maxWaitMs(60_000)
                .build();
        processor.registerGroup("stats-drain", group);

        int accepted = 0;
        for (int i = 0; i < 500; i++) {
            if (processor.submit("stats-drain", "k", i)) {
                accepted++;
            }
        }
        // 不等消费，直接 shutdown：排空/中断路径后对账
        processor.shutdown();
        processor = null;

        StatsSnapshot stats = group.snapshot().getStats();
        assertEquals("提交计数应与 append 成功数一致", accepted, stats.getSubmitTotal());
        assertTrue("回调计数不应超过提交计数", stats.getCallbackTotal() <= stats.getSubmitTotal());
        assertEquals("桶总和应与回调计数对账（成败同径）", stats.getCallbackTotal(), bucketSum(stats));
    }

    /**
     * 组快照统计可见性：未启动组 stats 为 null（采集未建，与 dispatcherPhase 的 null 约定一致），
     * 启动消费后 live 差分立即可见（不必等采集 tick），关闭后累计读数仍可查。
     */
    @Test
    public void snapshotExposesLiveStats() throws Exception {
        processor = new BatchProcessor();
        List<Integer> flushed = new CopyOnWriteArrayList<>();
        BatchWorkerGroup<Integer> group = BatchWorkerGroup.builder(Integer.class,
                        spoolConfig(new File(temp.getRoot(), "stats-snap")),
                        flushed::addAll)
                .batchSize(10)
                .maxWaitMs(50)
                .build();

        // 未启动：采集任务未建，stats 为 null
        assertNull("未启动组 stats 应为 null", group.snapshot().getStats());

        processor.registerGroup("stats-snap", group);
        assertTrue(processor.submit("stats-snap", "k", 1));
        awaitFlushed(1, flushed, 10_000);
        // 运行中：live 现场差分立即可见，不等采集 tick
        assertTrue("消费完成后 live 统计立即可见",
                awaitTrue(() -> group.snapshot().getStats() != null
                        && group.snapshot().getStats().getCallbackTotal() == 1, 10_000));
        StatsSnapshot live = group.snapshot().getStats();
        assertEquals("live 差分的桶数应 = 默认边界数 + 1",
                CumulativeStats.defaultBoundaries().length + 1, live.getBucketCounts().length);
        assertTrue("区间应有效（start <= end）",
                live.getIntervalStartMillis() <= live.getIntervalEndMillis());

        processor.shutdown();
        processor = null;

        // 关闭后：采集已 cancel，累计读数仍可查
        StatsSnapshot after = group.snapshot().getStats();
        assertNotNull(after);
        assertEquals(1L, after.getSubmitTotal());
        assertEquals(1L, after.getCallbackTotal());
    }

    /**
     * enabled=false：采集不建、endpoint stats 为 null；热路径裸数据照常累计不受管辖。
     */
    @Test
    public void disabledCollectKeepsHotPathCounting() throws Exception {
        processor = new BatchProcessor();
        List<Integer> flushed = new CopyOnWriteArrayList<>();
        BatchWorkerGroup<Integer> group = BatchWorkerGroup.builder(Integer.class,
                        spoolConfig(new File(temp.getRoot(), "stats-off")),
                        flushed::addAll)
                .batchSize(10)
                .maxWaitMs(50)
                .statsConfig(StatsConfigPOJO.builder().enabled(false).build())
                .build();
        processor.registerGroup("stats-off", group);

        assertTrue(processor.submit("stats-off", "k", 1));
        awaitFlushed(1, flushed, 10_000);
        processor.shutdown();
        processor = null;

        assertNull("采集关闭时 endpoint stats 应为 null", group.snapshot().getStats());
    }
}
