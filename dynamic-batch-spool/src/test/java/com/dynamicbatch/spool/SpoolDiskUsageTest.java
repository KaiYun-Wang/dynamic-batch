package com.dynamicbatch.spool;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.nio.charset.StandardCharsets;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * {@link Spool#diskUsage()} 分桶统计测试：覆盖未读 / 已读未删除拆分、
 * 跨文件推进、预算透传与关闭语义。
 */
public class SpoolDiskUsageTest {

    @Rule
    public TemporaryFolder tempDir = new TemporaryFolder();

    private Spool<String> spool;

    /** 最简单的字符串序列化器 */
    private static final Serializer<String> STRING_SERIALIZER = new Serializer<String>() {
        @Override
        public byte[] serialize(String data) {
            return data.getBytes(StandardCharsets.UTF_8);
        }

        @Override
        public String deserialize(byte[] bytes, Class<String> type) {
            return new String(bytes, StandardCharsets.UTF_8);
        }
    };

    @Before
    public void setUp() {
        spool = null;
    }

    @After
    public void tearDown() {
        if (spool != null) {
            spool.close();
            spool = null;
        }
    }

    /** 未 poll 过（tailer 从未推进，currentFile 为 null）：全部数据文件计入未读侧 */
    @Test
    public void unpolledDataAllCountedAsPending() throws Exception {
        spool = Spool.builder(String.class, tempDir.getRoot().toPath(), STRING_SERIALIZER)
                .flushIntervalMs(100)
                .build();
        assertTrue(spool.append("hello"));
        Thread.sleep(500); // 等写线程落盘

        DiskUsage usage = spool.diskUsage();
        assertNotNull(usage);
        assertTrue("落盘后总占用应大于 0", usage.getTotalBytes() > 0);
        assertEquals("未 poll 时不应有已读完文件", 0, usage.getConsumedBytes());
        assertEquals("只统计 .cq4 数据文件，未 poll 时全部在未读侧",
                usage.getTotalBytes(), usage.getPendingBytes());
    }

    /**
     * 跨文件推进拆分：1 秒滚动写两批，poll 完后早于 tailer 所在文件的全部计入已读未删除。
     * 清理间隔拉到 60s，避免测试期间已读文件被删导致 consumed 归零。
     */
    @Test
    public void polledAcrossFilesSplitsConsumedAndPending() throws Exception {
        spool = Spool.builder(String.class, tempDir.getRoot().toPath(), STRING_SERIALIZER)
                .rollCycleMillis(1000)
                .cleanupIntervalMs(60_000)
                .flushIntervalMs(100)
                .build();
        assertTrue(spool.append("first-1"));
        assertTrue(spool.append("first-2"));
        Thread.sleep(1500); // 跨过一个滚动周期，第二批进入新文件
        assertTrue(spool.append("second-1"));
        Thread.sleep(500);  // 等落盘

        // poll 完全部数据，tailer 推进到最后一个文件
        while (spool.poll(200) != null) {
            // drain
        }

        DiskUsage usage = spool.diskUsage();
        assertNotNull(usage);
        assertTrue("跨文件后应存在已读完未删除的旧文件", usage.getConsumedBytes() > 0);
        assertTrue("tailer 所在的最后文件整体算未读", usage.getPendingBytes() > 0);
        assertEquals("只统计 .cq4 数据文件，总量应等于两桶之和",
                usage.getConsumedBytes() + usage.getPendingBytes(), usage.getTotalBytes());
    }

    /** 构建期磁盘预算透传到统计结果，占比计算直接可用 */
    @Test
    public void maxSizeBytesCarriedInUsage() throws Exception {
        spool = Spool.builder(String.class, tempDir.getRoot().toPath(), STRING_SERIALIZER)
                .maxSizeBytes(123456)
                .build();

        DiskUsage usage = spool.diskUsage();
        assertNotNull(usage);
        assertEquals(123456, usage.getMaxSizeBytes());
    }

    /** 未配置预算 = Long.MAX_VALUE（不限），调用方按占比 0 处理、永不触发 */
    @Test
    public void unconfiguredBudgetIsLongMaxValue() {
        spool = Spool.builder(String.class, tempDir.getRoot().toPath(), STRING_SERIALIZER)
                .build();

        DiskUsage usage = spool.diskUsage();
        assertNotNull(usage);
        assertEquals(Long.MAX_VALUE, usage.getMaxSizeBytes());
    }

    /** 已关闭的 Spool 查询返回 null，不抛异常（查询类 API 契约） */
    @Test
    public void closedSpoolReturnsNull() {
        spool = Spool.builder(String.class, tempDir.getRoot().toPath(), STRING_SERIALIZER)
                .build();
        spool.close();

        assertNull("关闭后 diskUsage 应返回 null", spool.diskUsage());
        spool = null; // tearDown 不重复 close
    }
}
