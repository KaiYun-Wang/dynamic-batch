package com.dynamicbatch.core;

import com.dynamicbatch.core.pojo.SpoolConfigPOJO;
import com.dynamicbatch.spool.DiskUsage;
import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.Serializable;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * {@link BatchProcessor} Spool 占用窄查询测试：组级查询 / 全量遍历、
 * 未注册组不可见、关闭后返回 null（查询类 API 不抛异常契约）。
 */
public class BatchProcessorSpoolUsageTest {

    @Rule
    public TemporaryFolder tempDir = new TemporaryFolder();

    private final BatchProcessor processor = new BatchProcessor();

    @After
    public void tearDown() {
        processor.shutdown();
    }

    @Test
    public void usageReturnedAfterRegister() throws Exception {
        register("g1", "spool-g1");
        Thread.sleep(300); // 等 Spool 初始化产物落稳

        DiskUsage usage = processor.getSpoolUsage("g1");
        assertNotNull("已注册且启动的组应能查到占用", usage);
        assertEquals("预算透传", Long.MAX_VALUE, usage.getMaxSizeBytes());
    }

    @Test
    public void unknownGroupReturnsNull() {
        assertNull("组不存在返回 null，不抛异常", processor.getSpoolUsage("nope"));
    }

    @Test
    public void builtButUnregisteredGroupIsInvisible() {
        // build 未 register：组不在注册表，查询 API 天然看不见，不构成查询边界
        BatchWorkerGroup.builder(DemoItem.class, spoolConfig("spool-ghost"), batch -> { }).build();
        assertTrue(processor.listSpoolUsages().isEmpty());
    }

    @Test
    public void listSpoolUsagesContainsAllRegisteredGroups() {
        register("g1", "spool-g1");
        register("g2", "spool-g2");

        Map<String, DiskUsage> usages = processor.listSpoolUsages();
        assertEquals(2, usages.size());
        assertTrue(usages.containsKey("g1"));
        assertTrue(usages.containsKey("g2"));
    }

    @Test
    public void usageNullAfterShutdown() {
        register("g1", "spool-g1");
        assertNotNull(processor.getSpoolUsage("g1"));

        processor.shutdown();
        assertNull("关闭后注册表已清空，查询返回 null", processor.getSpoolUsage("g1"));
        assertTrue(processor.listSpoolUsages().isEmpty());
    }

    private void register(String key, String spoolDirName) {
        processor.registerGroup(key, BatchWorkerGroup.builder(DemoItem.class,
                        spoolConfig(spoolDirName),
                        batch -> { })
                .build());
    }

    private SpoolConfigPOJO spoolConfig(String dirName) {
        return SpoolConfigPOJO.builder(tempDir.getRoot().toPath().resolve(dirName).toString()).build();
    }

    /** 示例载荷：默认 JDK 序列化要求可序列化 */
    public static class DemoItem implements Serializable {
        private static final long serialVersionUID = 1L;
    }
}
