package com.dynamicbatch.core;

import com.dynamicbatch.core.pojo.SpoolConfigPOJO;
import com.dynamicbatch.core.vo.GroupSnapshotVO;
import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * 组运行快照查询测试：运行中组字段完整性、未启动组降级、未知 key 返回 null、全组列表。
 */
public class GroupSnapshotTest {

    @Rule
    public TemporaryFolder temp = new TemporaryFolder();

    private BatchProcessor processor;

    private static SpoolConfigPOJO spoolConfig(File dir) {
        return SpoolConfigPOJO.builder(dir.getAbsolutePath()).build();
    }

    @After
    public void tearDown() {
        if (processor != null) {
            processor.shutdown();
        }
    }

    /** 运行中组：配置、分区、相位与磁盘字段完整 */
    @Test(timeout = 30_000)
    public void runningGroupSnapshotPopulated() throws Exception {
        processor = new BatchProcessor();
        processor.registerGroup("snapshot-running",
                BatchWorkerGroup.builder(Integer.class,
                                spoolConfig(new File(temp.getRoot(), "snapshot-running")),
                                batch -> { })
                        .partitionCount(2)
                        .batchSize(5)
                        .maxWaitMs(200)
                        .queueCapacity(64)
                        .build());

        GroupSnapshotVO snapshot = processor.getGroupSnapshot("snapshot-running");
        assertNotNull(snapshot);
        assertEquals("snapshot-running", snapshot.getGroupKey());
        assertTrue(snapshot.isRunning());
        assertEquals(2, snapshot.getPartitionCount());
        assertEquals(2, snapshot.getQueueSizes().size());
        assertEquals(5, snapshot.getBatchSize().intValue());
        assertEquals(200, snapshot.getMaxWaitMs().longValue());
        assertEquals(64, snapshot.getQueueCapacity().intValue());
        assertEquals("RUNNING", snapshot.getDispatcherPhase());
        assertNotNull(snapshot.getSpoolUsage());
        assertNotNull(snapshot.getStagingSize());
    }

    /** 未启动组：相位与磁盘字段为 null，配置与分区水位保留（未启动队列恒空） */
    @Test
    public void notStartedGroupSnapshotDegraded() throws Exception {
        BatchWorkerGroup<Integer> group = BatchWorkerGroup.builder(Integer.class,
                        spoolConfig(new File(temp.getRoot(), "snapshot-notstarted")),
                        batch -> { })
                .partitionCount(3)
                .batchSize(5)
                .build();

        GroupSnapshotVO snapshot = group.snapshot();
        assertFalse(snapshot.isRunning());
        assertEquals(3, snapshot.getPartitionCount());
        assertEquals(3, snapshot.getQueueSizes().size());
        assertEquals(0, snapshot.getQueueSizes().stream().mapToInt(Integer::intValue).sum());
        assertNull(snapshot.getDispatcherPhase());
        assertNull(snapshot.getStagingSize());
        assertNull(snapshot.getSpoolUsage());
        assertNull("未启动组采集未建，stats 应为 null", snapshot.getStats());
    }

    /** 组不存在返回 null */
    @Test
    public void unknownGroupReturnsNull() {
        processor = new BatchProcessor();
        assertNull(processor.getGroupSnapshot("nope"));
    }

    /** 全组快照列表覆盖全部注册组，且不可变 */
    @Test(timeout = 30_000)
    public void listCoversAllRegisteredGroups() throws Exception {
        processor = new BatchProcessor();
        processor.registerGroup("snapshot-a",
                BatchWorkerGroup.builder(Integer.class,
                                spoolConfig(new File(temp.getRoot(), "snapshot-a")),
                                batch -> { })
                        .partitionCount(1).build());
        processor.registerGroup("snapshot-b",
                BatchWorkerGroup.builder(Integer.class,
                                spoolConfig(new File(temp.getRoot(), "snapshot-b")),
                                batch -> { })
                        .partitionCount(1).build());

        List<GroupSnapshotVO> snapshots = processor.listGroupSnapshots();
        assertEquals(2, snapshots.size());
        for (GroupSnapshotVO snapshot : snapshots) {
            assertTrue(snapshot.getGroupKey().equals("snapshot-a")
                    || snapshot.getGroupKey().equals("snapshot-b"));
            assertTrue(snapshot.isRunning());
        }
        try {
            snapshots.add(snapshots.get(0));
            fail("expected UnsupportedOperationException");
        } catch (UnsupportedOperationException expected) {
            // 不可变列表
        }
    }
}
