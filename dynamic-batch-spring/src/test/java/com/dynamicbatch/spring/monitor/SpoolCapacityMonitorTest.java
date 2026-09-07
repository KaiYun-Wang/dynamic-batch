package com.dynamicbatch.spring.monitor;

import com.dynamicbatch.common.pojo.NotifyItemPOJO;
import com.dynamicbatch.common.pojo.NotifyPlatformPOJO;
import com.dynamicbatch.core.BatchProcessor;
import com.dynamicbatch.core.BatchWorkerGroup;
import com.dynamicbatch.core.notifier.channel.Notifier;
import com.dynamicbatch.core.notifier.channel.NotifierRegistry;
import com.dynamicbatch.core.notifier.manager.NotifyManager;
import com.dynamicbatch.core.pojo.SpoolConfigPOJO;
import com.dynamicbatch.spring.properties.NotifyProperties;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.Serializable;
import java.util.Collections;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * {@link SpoolCapacityMonitor} 测试：显式才开的启停语义、
 * 阈值触发投递、未配置预算永不触发。巡检触发用包级 checkAllGroups 直调，不等调度周期。
 */
public class SpoolCapacityMonitorTest {

    @Rule
    public TemporaryFolder tempDir = new TemporaryFolder();

    private final BatchProcessor processor = new BatchProcessor();
    private final NotifyProperties properties = new NotifyProperties();
    private SpoolCapacityMonitor monitor;
    private FakeNotifier fake;

    @Before
    public void setUp() {
        fake = new FakeNotifier();
        NotifierRegistry.getInstance().register(fake);
        NotifyManager.getInstance().init(Collections.singletonList(platform("fake")));
        NotifyManager.getInstance().initItems(Collections.singletonList(item("spool_capacity", true)));
    }

    @After
    public void tearDown() {
        if (monitor != null) {
            monitor.stop();
            monitor = null;
        }
        processor.shutdown();
        NotifyManager.getInstance().initItems(Collections.emptyList());
        NotifyManager.getInstance().init(Collections.emptyList());
    }

    @Test
    public void disabledWhenItemMissingOrNotEnabled() {
        properties.setNotifyItems(Collections.emptyList());
        monitor = new SpoolCapacityMonitor(processor, properties);
        assertFalse("未登记 spool_capacity 项不应启动巡检", monitor.isRunning());

        NotifyItemPOJO disabled = item("spool_capacity", false);
        properties.setNotifyItems(Collections.singletonList(disabled));
        monitor.stop();
        monitor = new SpoolCapacityMonitor(processor, properties);
        assertFalse("enabled=false 不应启动巡检", monitor.isRunning());

        monitor.stop(); // 未启动时 stop 不应抛异常
    }

    @Test
    public void runningWhenItemRegisteredAndEnabled() {
        properties.setNotifyItems(Collections.singletonList(item("spool_capacity", true)));
        monitor = new SpoolCapacityMonitor(processor, properties);
        assertTrue(monitor.isRunning());
    }

    @Test
    public void checkTriggersAlarmWhenOverThreshold() throws Exception {
        // 预算 8MB：统计只认 .cq4 数据文件（构造期无数据文件可正常入队），
        // 落盘文件预扩 ~80MB 后远超预算，占比必超阈值
        register("over", spoolConfig("spool-over", 8L * 1024 * 1024));
        assertTrue(processor.submit("over", "k1", new DemoItem()));
        Thread.sleep(500); // 等落盘

        properties.setNotifyItems(Collections.singletonList(item("spool_capacity", true)));
        monitor = new SpoolCapacityMonitor(processor, properties);
        monitor.checkAllGroups();

        String content = fake.awaitContent(2000);
        assertNotNull("超阈值应触发告警", content);
        assertTrue(content.contains("over"));
    }

    @Test
    public void checkSkipsWhenBudgetUnlimited() throws Exception {
        // 未配置预算 = Long.MAX_VALUE：占比恒 0，永不触发（不做特判，自然不报）
        register("nolimit", spoolConfig("spool-nolimit", null));
        assertTrue(processor.submit("nolimit", "k1", new DemoItem()));
        Thread.sleep(500);

        properties.setNotifyItems(Collections.singletonList(item("spool_capacity", true)));
        monitor = new SpoolCapacityMonitor(processor, properties);
        monitor.checkAllGroups();

        assertNull("预算不限时不应告警", fake.awaitContent(1000));
    }

    private void register(String key, SpoolConfigPOJO spoolConfig) {
        processor.registerGroup(key, BatchWorkerGroup.builder(DemoItem.class, spoolConfig, batch -> { }).build());
    }

    /** maxSizeBytes 为 null 时不设预算（Spool 默认 Long.MAX_VALUE = 不限） */
    private static SpoolConfigPOJO spoolConfig(String dirName, Long maxSizeBytes) {
        String dir = java.nio.file.Paths.get(System.getProperty("java.io.tmpdir"),
                "spool-cap-test-" + dirName + "-" + System.nanoTime()).toString();
        SpoolConfigPOJO.Builder builder = SpoolConfigPOJO.builder(dir);
        if (maxSizeBytes != null) {
            builder.maxSizeBytes(maxSizeBytes);
        }
        return builder.build();
    }

    private static NotifyItemPOJO item(String type, boolean enabled) {
        NotifyItemPOJO item = new NotifyItemPOJO();
        item.setType(type);
        item.setEnabled(enabled);
        return item;
    }

    private static NotifyPlatformPOJO platform(String name) {
        NotifyPlatformPOJO p = new NotifyPlatformPOJO();
        p.setPlatform(name);
        return p;
    }

    /** 示例载荷：默认 JDK 序列化要求可序列化 */
    public static class DemoItem implements Serializable {
        private static final long serialVersionUID = 1L;
    }

    /** 测试用假渠道，记录发送次数与最近内容（发送为异步，用轮询等待） */
    private static class FakeNotifier implements Notifier {

        private volatile int sentCount;
        private volatile String lastContent;

        @Override
        public String platform() {
            return "fake";
        }

        @Override
        public void send(NotifyPlatformPOJO platform, String content) {
            sentCount++;
            lastContent = content;
        }

        private String awaitContent(long timeoutMs) {
            long deadline = System.currentTimeMillis() + timeoutMs;
            while (System.currentTimeMillis() < deadline) {
                if (sentCount > 0) {
                    return lastContent;
                }
                try {
                    Thread.sleep(50);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return null;
                }
            }
            return null;
        }
    }
}
