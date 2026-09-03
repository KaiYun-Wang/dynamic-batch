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
import static org.junit.Assert.fail;

/**
 * Spool 新链路集成测试：submit → Spool(磁盘) → Dispatcher → Worker → flushCallback 端到端。
 *
 * <p>与 {@link BatchProcessorTest} 的区别：这里走完整链路对账（削峰基本盘），
 * 并覆盖强关存活与 Spool 目录独占约束；全部使用 TemporaryFolder 隔离目录。
 */
public class GroupSpoolIntegrationTest {

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
     * 削峰链路基本盘：1 万条 submit 全部经磁盘缓冲后消费完成，append 成功数 == flush 总数。
     */
    @Test
    public void endToEndTenThousandEntries() throws Exception {
        processor = new BatchProcessor();
        List<Integer> flushed = new CopyOnWriteArrayList<>();

        processor.registerGroup("e2e",
                BatchWorkerGroup.builder(Integer.class,
                        spoolConfig(new File(temp.getRoot(), "e2e-10k")),
                        flushed::addAll)
                        .queueCapacity(1024)
                        .batchSize(100)
                        .maxWaitMs(100)
                        .offerTimeoutMs(1000)
                        .build());

        int total = 10_000;
        int accepted = 0;
        for (int i = 0; i < total; i++) {
            if (processor.submit("e2e", "k-" + (i % 8), i)) {
                accepted++;
            }
        }
        assertTrue("append success count should be positive, got " + accepted, accepted > 0);

        awaitFlushed(accepted, flushed, 30_000);
        processor.shutdown();
        processor = null;

        assertEquals("append 成功数应与 flush 总数对账（一条不丢）", accepted, flushed.size());
    }

    /**
     * 积压未消化时强关不挂死：append 500 条立即 shutdown，验证强关路径正常完成
     * （未消费数据留存磁盘属预期；精确对账由 endToEndTenThousandEntries 承担）。
     */
    @Test
    public void shutdownWithPendingEntriesSurvives() throws Exception {
        processor = new BatchProcessor();
        List<Integer> flushed = new CopyOnWriteArrayList<>();

        processor.registerGroup("pending",
                BatchWorkerGroup.builder(Integer.class,
                        spoolConfig(new File(temp.getRoot(), "pending")),
                        flushed::addAll)
                        .queueCapacity(1024)
                        .batchSize(500)
                        .maxWaitMs(60_000)
                        .build());

        int accepted = 0;
        for (int i = 0; i < 500; i++) {
            if (processor.submit("pending", "k", i)) {
                accepted++;
            }
        }

        long start = System.currentTimeMillis();
        processor.shutdown();
        long elapsed = System.currentTimeMillis() - start;
        processor = null;

        assertTrue("shutdown 应在有限时间内完成（不挂死），actual=" + elapsed + "ms", elapsed < 15_000);
        assertTrue("flushed 不应超过 accepted", flushed.size() <= accepted);
    }

    /**
     * Spool 目录独占约束：同目录第二个组 register 时构建 Spool fail fast（目录锁冲突），
     * 且失败组已从注册表清理——同 key 重新 register 新目录组应成功（无僵尸组）。
     */
    @Test
    public void sameDirSecondRegisterFailsFast() throws Exception {
        processor = new BatchProcessor();
        File shared = new File(temp.getRoot(), "shared-dir");
        processor.registerGroup("first",
                BatchWorkerGroup.builder(String.class, spoolConfig(shared), batch -> { })
                        .build());

        try {
            processor.registerGroup("second",
                    BatchWorkerGroup.builder(String.class, spoolConfig(shared), batch -> { })
                            .build());
            fail("expected exception for duplicate spool dir");
        } catch (RuntimeException expected) {
            // Spool 目录锁冲突（OverlappingFileLockException → IllegalStateException），启动期 fail fast
        }

        // start 失败的组已被 Processor 从注册表移除：同 key 重新 register（新目录）应成功
        File another = new File(temp.getRoot(), "another-dir");
        processor.registerGroup("second",
                BatchWorkerGroup.builder(String.class, spoolConfig(another), batch -> { })
                        .build());
    }
}
