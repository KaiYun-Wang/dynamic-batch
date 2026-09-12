package com.dynamicbatch.spool;

import java.io.File;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * 新数据信号（{@link Spool#awaitData}）单测：
 * 落盘按铃立即返回、写侧边沿触发（突发落盘仅一个未消费信号）、重启（重建实例）无信号但积压照常可读。
 */
public class SpoolAwaitDataTest {

    @Rule
    public TemporaryFolder temp = new TemporaryFolder();

    /** 轮询等待信号出现（writer 异步落盘，release 有延迟） */
    private static void awaitSignal(Spool<String> spool, long timeoutMs, String message) throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (!spool.awaitData(20)) {
            if (System.currentTimeMillis() > deadline) {
                fail(message);
            }
        }
    }

    /** 无写入超时假 → 落盘按铃真 → 多条落盘连续消费直至落盘完毕转安静（边沿语义） */
    @Test(timeout = 30_000)
    public void signalOnWriteAndDrain() throws Exception {
        File dir = temp.newFolder("signal-basic");
        try (Spool<String> spool = Spool.builder(String.class, dir.getAbsolutePath()).build()) {
            assertFalse("无写入：应超时返回假", spool.awaitData(200));

            assertTrue(spool.append("a"));
            assertTrue("落盘后信号应立即可用", spool.awaitData(5000));

            // 多条落盘：写侧边沿触发——无未消费信号才按铃，消费后 writer 落盘剩余条目会补按；
            // append 异步落盘，持续消费直至落盘完毕转安静
            for (int i = 0; i < 3; i++) {
                assertTrue(spool.append("x" + i));
            }
            awaitSignal(spool, 5000, "多条落盘仍应唤醒");
            while (spool.awaitData(300)) {
                // 持续消费 release 信号，直至 writer 落盘完毕、无新信号
            }
            assertFalse("落盘完毕后应转安静", spool.awaitData(300));
        }
    }

    /** 写侧边沿触发：突发多条落盘且未消费时仅一个未消费信号（许可恒 ≤1，杜绝长期积压下许可无限增长溢出） */
    @Test(timeout = 30_000)
    public void edgeTriggeredSinglePermitForBurst() throws Exception {
        File dir = temp.newFolder("signal-edge");
        try (Spool<String> spool = Spool.builder(String.class, dir.getAbsolutePath()).build()) {
            for (int i = 0; i < 5; i++) {
                assertTrue(spool.append("burst-" + i));
            }
            // 等待写线程取空暂存队列；单条落盘微秒级，追加静置确保 writeOne 循环全部结束
            long deadline = System.currentTimeMillis() + 5000;
            while (spool.stagingSize() > 0) {
                assertTrue("暂存队列未被消费", System.currentTimeMillis() <= deadline);
                Thread.sleep(10);
            }
            Thread.sleep(300);
    
            assertTrue("突发落盘应有信号", spool.awaitData(200));
            assertFalse("写侧边沿触发：仅一个未消费信号，不应再次唤醒", spool.awaitData(200));
        }
    }

    /**
     * 重启积压场景：信号不跨实例——新实例无信号（awaitData 超时假），
     * 但积压照常可读（取数侧兜底真读），不丢数据。
     */
    @Test(timeout = 30_000)
    public void restartBacklogReadableWithoutSignal() throws Exception {
        File dir = temp.newFolder("signal-restart");

        // 上个"进程"：写入 3 条后关闭（writer 排空落盘）
        try (Spool<String> spool = Spool.builder(String.class, dir.getAbsolutePath()).build()) {
            for (int i = 0; i < 3; i++) {
                assertTrue(spool.append("old-" + i));
            }
            awaitSignal(spool, 5000, "写入应落盘按铃");
        }

        // 新"进程"：无信号（计数归零），但积压照常可读
        try (Spool<String> spool = Spool.builder(String.class, dir.getAbsolutePath()).build()) {
            assertFalse("信号不跨实例：应超时假", spool.awaitData(300));

            assertNotNull(spool.poll(1000));
            assertNotNull(spool.poll(1000));
            assertNotNull(spool.poll(1000));
        }
    }
}
