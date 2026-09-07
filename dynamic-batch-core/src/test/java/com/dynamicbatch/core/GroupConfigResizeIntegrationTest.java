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
import static org.junit.Assert.fail;

/**
 * 攒批参数热更新（resizeGroupConfig）集成测试：null 透传、整体校验零写入、新参数生效、前置拒绝。
 */
public class GroupConfigResizeIntegrationTest {

    @Rule
    public TemporaryFolder temp = new TemporaryFolder();

    private BatchProcessor processor;

    private static SpoolConfigPOJO spoolConfig(File dir) {
        return SpoolConfigPOJO.builder(dir.getAbsolutePath()).build();
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

    /** 改 batchSize 后，新批次按新值攒批 */
    @Test(timeout = 30_000)
    public void resizedBatchSizeAppliesToNextBatches() throws Exception {
        processor = new BatchProcessor();
        List<Integer> flushed = new CopyOnWriteArrayList<>();
        List<Integer> batchSizes = new CopyOnWriteArrayList<>();
        processor.registerGroup("config-batchsize",
                BatchWorkerGroup.builder(Integer.class,
                                spoolConfig(new File(temp.getRoot(), "config-batchsize")),
                                batch -> {
                                    batchSizes.add(batch.size());
                                    flushed.addAll(batch);
                                })
                        .partitionCount(1)
                        .batchSize(2)
                        .maxWaitMs(50_000)
                        .build());

        for (int i = 0; i < 4; i++) {
            processor.submit("config-batchsize", "k", i);
        }
        awaitTrue(() -> flushed.size() >= 4, 10_000, "首批应按原 batchSize=2 flush");
        assertEquals(2, batchSizes.size());
        assertEquals(2, batchSizes.get(0).intValue());
        assertEquals(2, batchSizes.get(1).intValue());

        processor.resizeGroupConfig("config-batchsize", 4, null, null);
        for (int i = 4; i < 8; i++) {
            processor.submit("config-batchsize", "k", i);
        }
        awaitTrue(() -> flushed.size() >= 8, 10_000, "resize 后应按新 batchSize=4 攒批");
        assertEquals(4, batchSizes.get(batchSizes.size() - 1).intValue());
        assertEquals(8, flushed.size());
    }

    /** 改小 maxWaitMs 后，不满批也按新窗口 flush */
    @Test(timeout = 30_000)
    public void resizedMaxWaitMsFlushesEarly() throws Exception {
        processor = new BatchProcessor();
        List<Integer> batchSizes = new CopyOnWriteArrayList<>();
        processor.registerGroup("config-maxwait",
                BatchWorkerGroup.builder(Integer.class,
                                spoolConfig(new File(temp.getRoot(), "config-maxwait")),
                                batch -> batchSizes.add(batch.size()))
                        .partitionCount(1)
                        .batchSize(100)
                        .maxWaitMs(60_000)
                        .build());

        for (int i = 0; i < 3; i++) {
            processor.submit("config-maxwait", "k", i);
        }
        Thread.sleep(400);
        assertEquals("长窗口内不应 flush", 0, batchSizes.size());

        processor.resizeGroupConfig("config-maxwait", null, 200L, null);
        awaitTrue(() -> !batchSizes.isEmpty(), 5_000, "改小 maxWaitMs 后应按新窗口 flush");
        assertEquals(3, batchSizes.get(0).intValue());
    }

    /** null = 不改：单改一个参数，另一个保持原值 */
    @Test
    public void nullParamsKeepCurrentConfig() throws Exception {
        processor = new BatchProcessor();
        BatchWorkerGroup<Integer> group = registerEmptyGroup("config-null", 5, 100);

        processor.resizeGroupConfig("config-null", null, null, null);
        assertEquals(5, group.getBatchSize());
        assertEquals(100, group.getMaxWaitMs());

        processor.resizeGroupConfig("config-null", 7, null, null);
        assertEquals(7, group.getBatchSize());
        assertEquals(100, group.getMaxWaitMs());

        processor.resizeGroupConfig("config-null", null, 250L, null);
        assertEquals(7, group.getBatchSize());
        assertEquals(250, group.getMaxWaitMs());
    }

    /** 整体校验：任一参数非法则配置不变（同调用的合法参数也不写入） */
    @Test
    public void invalidParamsRejectedWithoutPartialWrite() throws Exception {
        processor = new BatchProcessor();
        BatchWorkerGroup<Integer> group = registerEmptyGroup("config-invalid", 5, 100);

        int[] badBatchSizes = {-1, 0, group.getQueueCapacity() + 1};
        for (int bs : badBatchSizes) {
            try {
                processor.resizeGroupConfig("config-invalid", bs, 500L, null);
                fail("expected IllegalArgumentException, batchSize=" + bs);
            } catch (IllegalArgumentException expected) {
                // fail fast
            }
            assertEquals("非法 batchSize 不得写入", 5, group.getBatchSize());
            assertEquals("同调用的合法 maxWaitMs 也不得写入", 100, group.getMaxWaitMs());
        }

        try {
            processor.resizeGroupConfig("config-invalid", null, -1L, null);
            fail("expected IllegalArgumentException");
        } catch (IllegalArgumentException expected) {
            // fail fast
        }
        assertEquals(5, group.getBatchSize());
        assertEquals(100, group.getMaxWaitMs());
    }

    /** 未启动组拒绝调参（与 pause / resize 行为一致） */
    @Test
    public void notStartedGroupRejected() throws Exception {
        BatchWorkerGroup<Integer> group = BatchWorkerGroup.builder(Integer.class,
                        spoolConfig(new File(temp.getRoot(), "config-notstarted")),
                        batch -> { })
                .batchSize(5)
                .build();
        try {
            group.resizeGroupConfig(2, 100L, null);
            fail("expected IllegalStateException");
        } catch (IllegalStateException expected) {
            // 组未启动
        }
    }

    /** 组不存在 fail fast */
    @Test
    public void unknownGroupKeyThrows() throws Exception {
        processor = new BatchProcessor();
        try {
            processor.resizeGroupConfig("nope", 1, 1L, null);
            fail("expected IllegalArgumentException");
        } catch (IllegalArgumentException expected) {
            // fail fast
        }
    }

    private BatchWorkerGroup<Integer> registerEmptyGroup(String key, int batchSize, long maxWaitMs) {
        BatchWorkerGroup<Integer> group = BatchWorkerGroup.builder(Integer.class,
                        spoolConfig(new File(temp.getRoot(), key)),
                        batch -> { })
                .batchSize(batchSize)
                .maxWaitMs(maxWaitMs)
                .build();
        processor.registerGroup(key, group);
        return group;
    }
}
