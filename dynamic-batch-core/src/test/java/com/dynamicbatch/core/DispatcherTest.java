package com.dynamicbatch.core;

import com.dynamicbatch.common.pojo.SpoolEntryPOJO;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
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

    @Test
    public void queueFullRetriesUntilSuccess() throws Exception {
        AtomicInteger deliverAttempts = new AtomicInteger();
        SpoolEntryPOJO<String> entry = new SpoolEntryPOJO<>("retry-key", "retry-value");
        AtomicBoolean polled = new AtomicBoolean();

        Dispatcher<String> dispatcher = new Dispatcher<>(
                lockTimeoutMs -> polled.compareAndSet(false, true) ? entry : null,
                (routingKey, payload) -> deliverAttempts.incrementAndGet() >= 4);

        dispatcher.setName("test-retry");
        dispatcher.start();
        Thread.sleep(500);
        dispatcher.stop();

        assertTrue("应重试直到成功", deliverAttempts.get() >= 4);
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

    @Test
    public void interruptDuringRetryDropsEntryAndExits() throws Exception {
        SpoolEntryPOJO<String> entry = new SpoolEntryPOJO<>("stuck-key", "stuck-value");
        AtomicBoolean polled = new AtomicBoolean();

        Dispatcher<String> dispatcher = new Dispatcher<>(
                lockTimeoutMs -> polled.compareAndSet(false, true) ? entry : null,
                (routingKey, payload) -> false);

        dispatcher.setName("test-interrupt");
        dispatcher.start();
        Thread.sleep(300);

        Thread dispatcherThread = findDispatcherThread("test-interrupt");
        assertTrue(dispatcherThread != null && dispatcherThread.isAlive());
        dispatcherThread.interrupt();

        dispatcherThread.join(1000);
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
