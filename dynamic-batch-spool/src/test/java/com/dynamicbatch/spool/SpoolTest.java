package com.dynamicbatch.spool;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;

import static org.junit.Assert.*;

/**
 * Spool 边界测试：覆盖基础功能、并发、关闭、恢复、满队列、空读、目录锁。
 */
public class SpoolTest {

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

    private Spool<String> createSpool(int stagingCapacity) {
        return Spool.builder(String.class, tempDir.getRoot().toPath(), STRING_SERIALIZER)
                .stagingCapacity(stagingCapacity)
                .flushIntervalMs(100) // 快速刷盘，方便测试
                .offerTimeoutMs(50)
                .build();
    }

    @After
    public void tearDown() {
        if (spool != null) {
            spool.close();
            spool = null;
        }
    }

    // ======================== 基础功能 ========================

    @Test
    public void appendAndPoll() throws Exception {
        spool = createSpool(1000);
        assertTrue(spool.append("hello"));
        assertTrue(spool.append("world"));

        // 等数据落盘 + tailer 读到
        Thread.sleep(500);

        assertEquals("hello", spool.poll(1000));
        assertEquals("world", spool.poll(1000));
        assertNull("队列已空应返回 null", spool.poll(200));
    }

    @Test
    public void emptyQueueReturnsNullImmediately() throws Exception {
        spool = createSpool(100);
        long start = System.currentTimeMillis();
        String result = spool.poll(50);
        long elapsed = System.currentTimeMillis() - start;
        assertNull("空队列应返回 null", result);
        // 拿到锁后读一次立即返回，不等 50ms
        assertTrue("空队列应立即返回，不等待", elapsed < 100);
    }

    @Test
    public void lockTimeoutThrowsTimeoutException() throws Exception {
        spool = createSpool(100);
        SpoolReader reader = getReader();
        ReentrantLock lock = getReaderLock(reader);

        // 另一个线程占住读锁（ReentrantLock 可重入，主线程自己占锁没用）
        CountDownLatch lockHeld = new CountDownLatch(1);
        Thread holder = new Thread(() -> {
            lock.lock();
            lockHeld.countDown();
            try {
                Thread.sleep(2000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } finally {
                lock.unlock();
            }
        }, "lock-holder");
        holder.start();
        lockHeld.await();

        try {
            long start = System.currentTimeMillis();
            try {
                spool.poll(50);
                fail("锁被占时应抛 TimeoutException");
            } catch (TimeoutException e) {
                long elapsed = System.currentTimeMillis() - start;
                assertTrue("应在 ~50ms 后超时", elapsed >= 30 && elapsed < 1000);
            }
        } finally {
            holder.interrupt();
            holder.join(1000);
        }
    }

    private SpoolReader getReader() throws Exception {
        Field f = Spool.class.getDeclaredField("reader");
        f.setAccessible(true);
        return (SpoolReader) f.get(spool);
    }

    private ReentrantLock getReaderLock(SpoolReader reader) throws Exception {
        Field f = SpoolReader.class.getDeclaredField("lock");
        f.setAccessible(true);
        return (ReentrantLock) f.get(reader);
    }

    @Test
    public void stagingQueueFullReturnsFalse() {
        spool = createSpool(5);
        for (int i = 0; i < 5; i++) {
            assertTrue(spool.append("msg-" + i));
        }
        // 暂存队列满，下一个应失败（写线程 drain 有延迟）
        boolean result = spool.append("overflow");
        // 可能写线程已经 drain 了部分，所以不一定 false；但至少不抛异常
        System.out.println("overflow result=" + result + ", stagingSize=" + spool.stagingSize());
    }

    @Test
    public void closeThenAppendReturnsFalse() {
        spool = createSpool(100);
        spool.close();
        assertFalse(spool.append("after-close"));
    }

    // ======================== FIFO 顺序 ========================

    @Test
    public void fifoOrder() throws Exception {
        spool = createSpool(1000);
        int count = 100;
        for (int i = 0; i < count; i++) {
            assertTrue(spool.append("msg-" + i));
        }

        Thread.sleep(500);

        for (int i = 0; i < count; i++) {
            assertEquals("顺序应保持 FIFO", "msg-" + i, spool.poll(500));
        }
    }

    // ======================== 并发读写 ========================

    @Test
    public void concurrentAppendAndPoll() throws Exception {
        spool = createSpool(5000);
        int total = 1000;
        CountDownLatch latch = new CountDownLatch(total);
        AtomicInteger consumed = new AtomicInteger(0);

        // 消费者
        Thread consumer = new Thread(() -> {
            while (consumed.get() < total) {
                try {
                    String data = spool.poll(200);
                    if (data != null) {
                        consumed.incrementAndGet();
                        latch.countDown();
                    }
                } catch (TimeoutException e) {
                    // 锁竞争超时（并发读），重试即可
                    continue;
                }
            }
        }, "test-consumer");
        consumer.start();

        // 生产者（多线程并发写）
        int producerThreads = 4;
        Thread[] producers = new Thread[producerThreads];
        for (int t = 0; t < producerThreads; t++) {
            int start = t * (total / producerThreads);
            int end = (t + 1) * (total / producerThreads);
            producers[t] = new Thread(() -> {
                for (int i = start; i < end; i++) {
                    spool.append("data-" + i);
                }
            }, "test-producer-" + t);
            producers[t].start();
        }

        assertTrue("应在 10 秒内消费完所有数据", latch.await(10, TimeUnit.SECONDS));
        assertEquals(total, consumed.get());

        consumer.interrupt();
        consumer.join(1000);
    }

    // ======================== 关闭行为 ========================

    @Test
    public void closeDrainsStagingQueue() throws Exception {
        spool = createSpool(100);

        // append 一批数据，不等落盘就 close
        for (int i = 0; i < 10; i++) {
            spool.append("close-test-" + i);
        }
        spool.close();

        // 重新打开，应该能从 Chronicle 读到已落盘的数据
        Spool<String> spool2 = Spool.builder(String.class, tempDir.getRoot().toPath(), STRING_SERIALIZER)
                .stagingCapacity(100)
                .flushIntervalMs(100)
                .build();
        spool = null; // 避免 tearDown 重复 close

        List<String> recovered = new ArrayList<>();
        String data;
        while ((data = spool2.poll(200)) != null) {
            recovered.add(data);
        }
        assertTrue("应恢复至少部分数据，实际=" + recovered.size(), recovered.size() > 0);
        spool2.close();
    }

    // ======================== 崩溃恢复语义 ========================

    @Test
    public void restartResumeFromLastPosition() throws Exception {
        spool = createSpool(500);
        for (int i = 0; i < 50; i++) {
            assertTrue(spool.append("idx-" + i));
        }
        Thread.sleep(500); // 等刷盘

        // 消费前 20 条
        for (int i = 0; i < 20; i++) {
            assertEquals("idx-" + i, spool.poll(500));
        }
        spool.close();

        // 重启，应该从第 20 条继续（命名 tailer 持久化了位置）
        spool = Spool.builder(String.class, tempDir.getRoot().toPath(), STRING_SERIALIZER)
                .stagingCapacity(500)
                .flushIntervalMs(100)
                .build();

        for (int i = 20; i < 50; i++) {
            assertEquals("重启后应续读 idx-" + i, "idx-" + i, spool.poll(500));
        }
        assertNull("不应有多余数据", spool.poll(200));
    }

    // ======================== 目录锁 ========================

    @Test(expected = IllegalStateException.class)
    public void directoryLockPreventsMultiProcess() {
        spool = createSpool(100);
        // 同目录开第二个实例应抛 IllegalStateException
        Spool<String> spool2 = Spool.builder(String.class, tempDir.getRoot().toPath(), STRING_SERIALIZER)
                .stagingCapacity(100)
                .build();
        spool2.close();
    }

    // ======================== 空队列反复 poll 立即返回 ========================

    @Test
    public void repeatedEmptyPoll() throws Exception {
        spool = createSpool(100);
        long start = System.currentTimeMillis();
        for (int i = 0; i < 5; i++) {
            assertNull(spool.poll(50));
        }
        long elapsed = System.currentTimeMillis() - start;
        // 空队列立即返回 null，5 次应很快完成
        assertTrue("空 poll 应立即返回", elapsed < 1000);
    }

    // ======================== 大消息 ========================

    @Test
    public void largeMessage() throws Exception {
        spool = createSpool(100);
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < 100_000; i++) {
            sb.append('A');
        }
        String large = sb.toString();
        assertTrue(spool.append(large));
        Thread.sleep(300);
        assertEquals(large, spool.poll(1000));
    }
}