package com.dynamicbatch.core;

import com.dynamicbatch.core.pojo.SpoolConfigPOJO;
import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Supplier;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * 调度器暂停/恢复端到端集成测试：submit → Spool(磁盘) → Dispatcher → Worker → flushCallback 全链路。
 * 语义：暂停只冻结搬运，Worker 继续消化手头批次至队列清空，暂停期间 submit 照常落盘。
 */
public class GroupPauseIntegrationTest {

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

    /** 等待 flush 计数进入稳定态（连续 500ms 无增长），返回稳定值 */
    private static int awaitStable(List<Integer> flushed, long timeoutMs) throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMs;
        int last = flushed.size();
        long lastChange = System.currentTimeMillis();
        while (System.currentTimeMillis() < deadline) {
            Thread.sleep(100);
            int now = flushed.size();
            if (now != last) {
                last = now;
                lastChange = System.currentTimeMillis();
            } else if (System.currentTimeMillis() - lastChange >= 500) {
                return last;
            }
        }
        return last;
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

    @After
    public void tearDown() {
        if (processor != null) {
            processor.shutdown();
        }
    }

    /**
     * 端到端：暂停后 Worker 清空手头队列、磁盘积压不再被搬运；暂停期间 submit 照常落盘；
     * 恢复后自动消化积压，全量对账一条不丢。
     */
    @Test(timeout = 90_000)
    public void pauseResumeEndToEnd() throws Exception {
        processor = new BatchProcessor();
        List<Integer> flushed = new CopyOnWriteArrayList<>();
        BatchWorkerGroup<Integer> group = BatchWorkerGroup.builder(Integer.class,
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

        processor.pauseDispatcher("pause-e2e");
        awaitTrue(() -> processor.getDispatcherPhase("pause-e2e") == Dispatcher.PausePhase.PAUSED,
                5_000, "暂停应在调度器确认后生效");
        // 调度器已停止取数：Worker 清空手头队列后 flush 计数进入稳定态，磁盘积压不再被搬运
        int stable = awaitStable(flushed, 30_000);
        Thread.sleep(500);
        assertEquals("暂停期间磁盘积压不应被搬运", stable, flushed.size());

        assertTrue("暂停期间 submit 应照常落盘", processor.submit("pause-e2e", "k-x", -1));
        int expectedTotal = accepted + 1;

        processor.resumeDispatcher("pause-e2e");
        awaitFlushed(expectedTotal, flushed, 30_000);
        processor.shutdown();
        processor = null;

        assertEquals("恢复后应对账一条不丢（含暂停期间 submit 的数据）", expectedTotal, flushed.size());
    }

    /** 幂等：重复置暂停意图、重复置运行意图均无害，终态正确 */
    @Test(timeout = 30_000)
    public void pauseAndResumeIdempotent() throws Exception {
        processor = new BatchProcessor();
        BatchWorkerGroup<Integer> group = BatchWorkerGroup.builder(Integer.class,
                        spoolConfig(new File(temp.getRoot(), "pause-idempotent")),
                        batch -> { })
                .build();
        processor.registerGroup("pause-idempotent", group);

        processor.pauseDispatcher("pause-idempotent");
        processor.pauseDispatcher("pause-idempotent");   // 重复置意图无害
        awaitTrue(() -> processor.getDispatcherPhase("pause-idempotent") == Dispatcher.PausePhase.PAUSED,
                5_000, "应确认暂停");
        processor.resumeDispatcher("pause-idempotent");
        processor.resumeDispatcher("pause-idempotent");  // 重复置意图无害
        awaitTrue(() -> processor.getDispatcherPhase("pause-idempotent") == Dispatcher.PausePhase.RUNNING,
                5_000, "应确认恢复");
    }

    /** 未注册的组 key：fail fast */
    @Test
    public void pauseDispatcherOnUnregisteredKeyThrows() {
        processor = new BatchProcessor();
        try {
            processor.pauseDispatcher("nope");
            fail("expected IllegalArgumentException");
        } catch (IllegalArgumentException expected) {
            // fail fast
        }
    }

    /** 未启动的组：零资源 fail fast */
    @Test
    public void pauseDispatcherOnUnstartedGroupThrows() {
        BatchWorkerGroup<String> unstarted = BatchWorkerGroup.builder(String.class,
                        SpoolConfigPOJO.builder("unused-dir").build(),
                        batch -> { })
                .build();
        try {
            unstarted.pauseDispatcher();
            fail("expected IllegalStateException");
        } catch (IllegalStateException expected) {
            // 未 start 的组零资源，fail fast
        }
    }
}
