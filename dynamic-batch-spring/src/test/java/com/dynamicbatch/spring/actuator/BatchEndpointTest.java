package com.dynamicbatch.spring.actuator;

import com.dynamicbatch.core.BatchProcessor;
import com.dynamicbatch.core.BatchWorkerGroup;
import com.dynamicbatch.core.pojo.SpoolConfigPOJO;
import com.dynamicbatch.core.vo.GroupSnapshotVO;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.util.List;
import java.util.function.BooleanSupplier;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * 运维端点处理方法测试：直接实例化端点调用处理方法，验证统一返回码与数据，
 * 覆盖查询、参数热更、暂停/恢复、分区数变更及异常映射（400 / 404 / 409 / 504）。
 */
public class BatchEndpointTest {

    @Rule
    public TemporaryFolder temp = new TemporaryFolder();

    private static final String KEY = "endpoint-demo";
    /** 排空超时用例组：flush 回调阻塞 3 秒，制造排空等待超时 */
    private static final String SLOW_KEY = "endpoint-slow";

    private BatchProcessor processor;
    private BatchEndpoint endpoint;

    @Before
    public void setUp() throws Exception {
        processor = new BatchProcessor();
        processor.registerGroup(KEY,
                BatchWorkerGroup.builder(Integer.class,
                                spoolConfig(KEY),
                                batch -> { })
                        .partitionCount(2)
                        .batchSize(5)
                        .maxWaitMs(200)
                        .build());
        processor.registerGroup(SLOW_KEY,
                BatchWorkerGroup.builder(Integer.class,
                                spoolConfig(SLOW_KEY),
                                batch -> {
                                    try {
                                        Thread.sleep(3000);
                                    } catch (InterruptedException e) {
                                        Thread.currentThread().interrupt();
                                    }
                                })
                        .partitionCount(1)
                        .build());
        endpoint = new BatchEndpoint(processor);
    }

    @After
    public void tearDown() {
        processor.shutdown();
    }

    private SpoolConfigPOJO spoolConfig(String name) {
        return SpoolConfigPOJO.builder(new File(temp.getRoot(), name).getAbsolutePath()).build();
    }

    /** 轮询等待谓词成立（终态断言，超时即红） */
    private static void awaitTrue(BooleanSupplier predicate, long timeoutMs, String message)
            throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (!predicate.getAsBoolean()) {
            if (System.currentTimeMillis() > deadline) {
                throw new AssertionError(message);
            }
            Thread.sleep(20);
        }
    }

    /** 列表与单组查询：code=200，快照内容正确 */
    @Test(timeout = 30_000)
    public void listAndGetReturnSnapshots() {
        ApiResult list = endpoint.listGroups();
        assertEquals(200, list.getCode());
        List<?> groups = (List<?>) list.getData();
        assertEquals(2, groups.size());

        ApiResult single = endpoint.getGroup(KEY);
        assertEquals(200, single.getCode());
        GroupSnapshotVO snapshot = (GroupSnapshotVO) single.getData();
        assertEquals(KEY, snapshot.getGroupKey());
        assertTrue(snapshot.isRunning());
        assertEquals("RUNNING", snapshot.getDispatcherPhase());
        // 默认 statsConfig（采集开）：运行组 live 统计随快照带出
        assertNotNull("运行组 stats 应随快照带出", snapshot.getStats());
    }

    /** 单组查询未知 key 返回 404 */
    @Test
    public void getUnknownGroupReturns404() {
        assertEquals(404, endpoint.getGroup("nope").getCode());
    }

    /** 参数热更成功：data 快照反映新值 */
    @Test(timeout = 30_000)
    public void resizeGroupConfigApplies() {
        ApiResult result = endpoint.resizeGroupConfig(KEY, 3, 500L, null);
        assertEquals(200, result.getCode());
        GroupSnapshotVO snapshot = (GroupSnapshotVO) result.getData();
        assertEquals(3, snapshot.getBatchSize().intValue());
        assertEquals(500, snapshot.getMaxWaitMs().longValue());
    }

    /** 限流热更：合法值 200 且快照带出新值，<= 0 映射 400 */
    @Test(timeout = 30_000)
    public void resizeGroupConfigRateLimit() {
        ApiResult result = endpoint.resizeGroupConfig(KEY, null, null, 200);
        assertEquals(200, result.getCode());
        GroupSnapshotVO snapshot = (GroupSnapshotVO) result.getData();
        assertEquals(200, snapshot.getRateLimitPerSecond().intValue());

        assertEquals(400, endpoint.resizeGroupConfig(KEY, null, null, 0).getCode());
        assertEquals(400, endpoint.resizeGroupConfig(KEY, null, null, -1).getCode());
    }

    /** 非法参数（batchSize=0）映射 400 */
    @Test(timeout = 30_000)
    public void resizeGroupConfigInvalidReturns400() {
        assertEquals(400, endpoint.resizeGroupConfig(KEY, 0, null, null).getCode());
    }

    /** 未知 key 的运维操作映射 400（组不存在 fail fast） */
    @Test(timeout = 30_000)
    public void opsOnUnknownGroupReturn400() {
        assertEquals(400, endpoint.pause("nope").getCode());
        assertEquals(400, endpoint.resume("nope").getCode());
        assertEquals(400, endpoint.resizeGroupConfig("nope", 3, null, null).getCode());
    }

    /** 暂停/恢复：返回置位后的相位，终态经快照查询确认 */
    @Test(timeout = 30_000)
    public void pauseAndResumeReturnIntentPhase() throws Exception {
        ApiResult paused = endpoint.pause(KEY);
        assertEquals(200, paused.getCode());
        String pausedPhase = (String) paused.getData();
        assertTrue("置位后相位应为 PAUSE_PENDING 或已确认 PAUSED，实际 " + pausedPhase,
                "PAUSE_PENDING".equals(pausedPhase) || "PAUSED".equals(pausedPhase));
        awaitTrue(() -> "PAUSED".equals(processor.getGroupSnapshot(KEY).getDispatcherPhase()),
                5_000, "调度器应确认暂停");

        ApiResult resumed = endpoint.resume(KEY);
        assertEquals(200, resumed.getCode());
        String resumedPhase = (String) resumed.getData();
        assertTrue("置位后相位应为 RUN_PENDING 或已确认 RUNNING，实际 " + resumedPhase,
                "RUN_PENDING".equals(resumedPhase) || "RUNNING".equals(resumedPhase));
    }

    /** 调度器未暂停直接 resize 映射 409 */
    @Test(timeout = 30_000)
    public void resizeWithoutPauseReturns409() {
        assertEquals(409, endpoint.resizePartitions(KEY, 3, 1000L).getCode());
    }

    /** 排空等待超时映射 504：flush 回调阻塞，500ms 预算内无法排空 */
    @Test(timeout = 30_000)
    public void resizeTimeoutReturns504() throws Exception {
        for (int i = 0; i < 2; i++) {
            assertTrue(processor.submit(SLOW_KEY, "k" + i, i));
        }
        // 等首批进入阻塞的 flush 回调，Worker 非 idle，排空轮询无法收敛
        Thread.sleep(500);
        assertEquals(200, endpoint.pause(SLOW_KEY).getCode());
        awaitTrue(() -> "PAUSED".equals(processor.getGroupSnapshot(SLOW_KEY).getDispatcherPhase()),
                5_000, "调度器应确认暂停");

        assertEquals(504, endpoint.resizePartitions(SLOW_KEY, 2, 500L).getCode());
        // 超时后分区未动、组保持暂停态
        assertEquals(1, processor.getGroupSnapshot(SLOW_KEY).getPartitionCount());
        assertEquals("PAUSED", processor.getGroupSnapshot(SLOW_KEY).getDispatcherPhase());
    }

    /** 排空成功后 resize 生效：data 快照反映新分区数 */
    @Test(timeout = 30_000)
    public void resizeAfterPauseApplies() throws Exception {
        assertEquals(200, endpoint.pause(KEY).getCode());
        awaitTrue(() -> "PAUSED".equals(processor.getGroupSnapshot(KEY).getDispatcherPhase()),
                5_000, "调度器应确认暂停");

        ApiResult result = endpoint.resizePartitions(KEY, 4, 10_000L);
        assertEquals(200, result.getCode());
        GroupSnapshotVO snapshot = (GroupSnapshotVO) result.getData();
        assertEquals(4, snapshot.getPartitionCount());
        assertEquals(4, snapshot.getQueueSizes().size());
    }
}
