package com.dynamicbatch.core;

import com.dynamicbatch.common.pojo.SpoolEntryPOJO;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Dispatcher 单测：假 poller / 假 deliver 驱动，无 Spool 文件 IO。
 */
public class DispatcherTest {

    @Test
    public void deliversEachEntryOnce() throws Exception {
        List<SpoolEntryPOJO<String>> scripted = new ArrayList<>();
        scripted.add(new SpoolEntryPOJO<>("k1", "v1"));
        scripted.add(new SpoolEntryPOJO<>("k2", "v2"));
        scripted.add(new SpoolEntryPOJO<>("k3", "v3"));
        AtomicInteger pollIndex = new AtomicInteger();
        List<String> deliveredKeys = new ArrayList<>();

        Dispatcher<String> dispatcher = new Dispatcher<>(
                lockTimeoutMs -> {
                    int index = pollIndex.getAndIncrement();
                    if (index < scripted.size()) {
                        return scripted.get(index);
                    }
                    return null;
                },
                (routingKey, payload) -> {
                    deliveredKeys.add(routingKey);
                    return true;
                });
        dispatcher.setName("test-deliver-once");
        dispatcher.start();

        Thread.sleep(800);
        dispatcher.stop();

        assertEquals(3, deliveredKeys.size());
        assertEquals("k1", deliveredKeys.get(0));
        assertEquals("k2", deliveredKeys.get(1));
        assertEquals("k3", deliveredKeys.get(2));
    }

    /**
     * deliver 返回 false（投递被放弃：worker 已关闭 / 阻塞投递被中断）时，线程应随即退出。
     * 门闩保证断言时线程已到达 poll 点：start 与查找间无同步，线程本就验完即退。
     */
    @Test
    public void deliverAbandonedExitsThread() throws Exception {
        SpoolEntryPOJO<String> entry = new SpoolEntryPOJO<>("stuck-key", "stuck-value");
        CountDownLatch pollStarted = new CountDownLatch(1);
        AtomicBoolean release = new AtomicBoolean();

        Dispatcher<String> dispatcher = new Dispatcher<>(
                lockTimeoutMs -> {
                    pollStarted.countDown();
                    // 等主线程完成“线程活着”断言后再返回条目，避免线程提前跑完消失
                    while (!release.get()) {
                        try {
                            Thread.sleep(10);
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                            break;   // 强制关闭中断：返回后 deliver false 退出，语义不变
                        }
                    }
                    return entry;
                },
                (routingKey, payload) -> false);

        dispatcher.setName("test-abandon");
        dispatcher.start();
        assertTrue("线程应到达 poll 点（此时必然活着）", pollStarted.await(2, TimeUnit.SECONDS));

        Thread dispatcherThread = findDispatcherThread("test-abandon");
        assertTrue(dispatcherThread != null && dispatcherThread.isAlive());
        release.set(true);

        dispatcherThread.join(2000);
        assertFalse("投递被放弃后线程应结束", dispatcherThread.isAlive());
        dispatcher.stop();
    }

    @Test
    public void pollEmptyDoesNotBusyWait() throws Exception {
        AtomicInteger pollCount = new AtomicInteger();
        Dispatcher<String> dispatcher = new Dispatcher<>(
                lockTimeoutMs -> {
                    pollCount.incrementAndGet();
                    return null;
                },
                (routingKey, payload) -> true);

        dispatcher.setName("test-empty-poll");
        dispatcher.start();
        Thread.sleep(600);
        dispatcher.stop();

        assertTrue("应持续轮询而非卡死", pollCount.get() >= 3);
        assertTrue("禁止忙等空转", pollCount.get() <= 8);
    }

    @Test
    public void lockTimeoutContinuesLoop() throws Exception {
        AtomicInteger pollCount = new AtomicInteger();
        SpoolEntryPOJO<String> entry = new SpoolEntryPOJO<>("after-timeout", "v");
        List<String> delivered = new ArrayList<>();

        Dispatcher<String> dispatcher = new Dispatcher<>(
                lockTimeoutMs -> {
                    if (pollCount.incrementAndGet() == 1) {
                        throw new TimeoutException("lock busy");
                    }
                    if (pollCount.get() == 2) {
                        return entry;
                    }
                    return null;
                },
                (routingKey, payload) -> {
                    delivered.add(routingKey);
                    return true;
                });

        dispatcher.setName("test-lock-timeout");
        dispatcher.start();
        Thread.sleep(500);
        dispatcher.stop();

        assertEquals(1, delivered.size());
        assertEquals("after-timeout", delivered.get(0));
        assertTrue(pollCount.get() >= 2);
    }

    @Test
    public void corruptedEntryDroppedAndLoopSurvives() throws Exception {
        AtomicInteger pollCount = new AtomicInteger();
        SpoolEntryPOJO<String> entry = new SpoolEntryPOJO<>("after-corrupt", "v");
        List<String> delivered = new ArrayList<>();

        Dispatcher<String> dispatcher = new Dispatcher<>(
                lockTimeoutMs -> {
                    if (pollCount.incrementAndGet() == 1) {
                        throw new RuntimeException("deserialize failed");
                    }
                    if (pollCount.get() == 2) {
                        return entry;
                    }
                    return null;
                },
                (routingKey, payload) -> {
                    delivered.add(routingKey);
                    return true;
                });

        dispatcher.setName("test-poison");
        dispatcher.start();
        Thread.sleep(500);
        dispatcher.stop();

        assertEquals(1, delivered.size());
        assertEquals("after-corrupt", delivered.get(0));
    }

    /**
     * 阻塞投递被中断（强制关闭）：deliver 丢本条返回 false，线程结束。
     */
    @Test
    public void interruptDuringBlockingDeliverExitsThread() throws Exception {
        SpoolEntryPOJO<String> entry = new SpoolEntryPOJO<>("stuck-key", "stuck-value");
        AtomicBoolean polled = new AtomicBoolean();
        CountDownLatch deliverStarted = new CountDownLatch(1);

        Dispatcher<String> dispatcher = new Dispatcher<>(
                lockTimeoutMs -> polled.compareAndSet(false, true) ? entry : null,
                (routingKey, payload) -> {
                    deliverStarted.countDown();
                    try {
                        new CountDownLatch(1).await();   // 模拟阻塞 put（永不放行）
                        return true;
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        return false;   // 阻塞投递被中断：丢本条，返回 false
                    }
                });

        dispatcher.setName("test-interrupt-put");
        dispatcher.start();
        assertTrue(deliverStarted.await(2, TimeUnit.SECONDS));

        Thread dispatcherThread = findDispatcherThread("test-interrupt-put");
        assertTrue(dispatcherThread != null && dispatcherThread.isAlive());
        dispatcherThread.interrupt();

        dispatcherThread.join(2000);
        assertFalse(dispatcherThread.isAlive());
        dispatcher.stop();
    }

    @Test
    public void stopIsIdempotent() throws Exception {
        Dispatcher<String> dispatcher = new Dispatcher<>(
                lockTimeoutMs -> null,
                (routingKey, payload) -> true);
        dispatcher.setName("test-stop-idempotent");
        dispatcher.start();
        dispatcher.stop();

        Thread dispatcherThread = findDispatcherThread("test-stop-idempotent");
        if (dispatcherThread != null) {
            assertFalse(dispatcherThread.isAlive());
        }

        dispatcher.stop();
    }

    private static Thread findDispatcherThread(String name) {
        for (Thread thread : Thread.getAllStackTraces().keySet()) {
            if (("batch-processor-" + name).equals(thread.getName())) {
                return thread;
            }
        }
        return null;
    }
}
