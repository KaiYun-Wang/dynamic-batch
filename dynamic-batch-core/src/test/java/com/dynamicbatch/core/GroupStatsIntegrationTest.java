package com.dynamicbatch.core;

import com.dynamicbatch.common.pojo.EnvelopePOJO;
import com.dynamicbatch.core.pojo.SpoolConfigPOJO;
import com.dynamicbatch.core.stats.CumulativeStats;
import com.dynamicbatch.core.vo.CumulativeStatsVO;
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
import static org.junit.Assert.assertTrue;

/**
 * 组级累计统计端到端：submit → Spool → Dispatcher → Worker flush（含 shutdown 排空）
 * 全径计数对账，以及组快照中的累计值可见性。
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

    /** 桶计数总和 */
    private static long bucketSum(CumulativeStatsVO stats) {
        long sum = 0;
        for (long count : stats.getRtBucketCounts()) {
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

        CumulativeStatsVO stats = group.snapshot().getStats();
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

        CumulativeStatsVO stats = group.snapshot().getStats();
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

        CumulativeStatsVO stats = group.snapshot().getStats();
        assertEquals("提交计数应与 append 成功数一致", accepted, stats.getSubmitTotal());
        assertTrue("回调计数不应超过提交计数", stats.getCallbackTotal() <= stats.getSubmitTotal());
        assertEquals("桶总和应与回调计数对账（成败同径）", stats.getCallbackTotal(), bucketSum(stats));
    }

    /**
     * 组快照暴露累计统计：未启动组为全 0（计数器构造期常驻，桶数 = 默认边界数 + 1），
     * 启动并消费后计数增长——endpoint 数据源可直接读数。
     */
    @Test
    public void snapshotExposesCumulativeStats() throws Exception {
        processor = new BatchProcessor();
        List<Integer> flushed = new CopyOnWriteArrayList<>();
        BatchWorkerGroup<Integer> group = BatchWorkerGroup.builder(Integer.class,
                        spoolConfig(new File(temp.getRoot(), "stats-snap")),
                        flushed::addAll)
                .batchSize(10)
                .maxWaitMs(50)
                .build();

        // 未注册：计数器已存在且为全 0
        CumulativeStatsVO before = group.snapshot().getStats();
        assertNotNull(before);
        assertEquals(0L, before.getSubmitTotal());
        assertEquals("桶数应 = 默认边界数 + 1",
                CumulativeStats.defaultBoundaries().length + 1, before.getRtBucketCounts().length);

        processor.registerGroup("stats-snap", group);
        assertTrue(processor.submit("stats-snap", "k", 1));
        awaitFlushed(1, flushed, 10_000);
        processor.shutdown();
        processor = null;

        CumulativeStatsVO after = group.snapshot().getStats();
        assertEquals(1L, after.getSubmitTotal());
        assertEquals(1L, after.getCallbackTotal());
    }
}
