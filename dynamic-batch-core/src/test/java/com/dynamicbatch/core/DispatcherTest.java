package com.dynamicbatch.core;

import com.dynamicbatch.common.pojo.EnvelopePOJO;
import com.dynamicbatch.core.pojo.BatchWorkerGroupConfigPOJO;
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
        List<EnvelopePOJO<String>> scripted = new ArrayList<>();
        scripted.add(new EnvelopePOJO<>("k1", "v1"));
        scripted.add(new EnvelopePOJO<>("k2", "v2"));
        scripted.add(new EnvelopePOJO<>("k3", "v3"));
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
                envelope -> {
                    deliveredKeys.add(envelope.getRoutingKey());
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
        EnvelopePOJO<String> entry = new EnvelopePOJO<>("stuck-key", "stuck-value");
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
                envelope -> false);

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
                envelope -> true,
                new BatchWorkerGroupConfigPOJO(),
                ms -> {   // 无信号：睡满 timeout（模拟真挂起），仅兜底周期强制真读
                    try {
                        Thread.sleep(ms);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                    return false;
                });

        dispatcher.setName("test-empty-poll");
        dispatcher.start();
        Thread.sleep(600);
        dispatcher.stop();

        assertTrue("应持续轮询而非卡死", pollCount.get() >= 3);
        assertTrue("禁止忙等空转", pollCount.get() <= 8);
    }

    /**
     * 空转挂起中信号到达：写端置“有新数据”（模拟落盘 release）→ 挂起立即唤醒真读取数。
     */
    @Test
    public void signalWakePollsImmediately() throws Exception {
        AtomicBoolean hasNewData = new AtomicBoolean();
        AtomicInteger pollCount = new AtomicInteger();
        CountDownLatch delivered = new CountDownLatch(1);
        EnvelopePOJO<String> entry = new EnvelopePOJO<>("signal-key", "signal-value");

        Dispatcher<String> dispatcher = new Dispatcher<>(
                lockTimeoutMs -> {
                    // 首轮空（进入空转挂起）；其后信号说有货才真的有，否则兜底轮也空读
                    return pollCount.incrementAndGet() == 1 || !hasNewData.get()
                            ? null
                            : entry;
                },
                envelope -> {
                    delivered.countDown();
                    return true;
                },
                new BatchWorkerGroupConfigPOJO(),
                ms -> hasNewData.get());
        dispatcher.setName("test-signal-wake");
        dispatcher.start();

        Thread.sleep(200);   // 进入空转挂起节奏（兜底周期 100ms）
        long start = System.nanoTime();
        hasNewData.set(true);   // 模拟写端落盘按铃
        assertTrue("信号到达后应取到数据", delivered.await(1, TimeUnit.SECONDS));
        long costMs = (System.nanoTime() - start) / 1_000_000;
        assertTrue("信号唤醒应立即取数（实际 " + costMs + "ms）", costMs < 300);
        dispatcher.stop();
    }

    @Test
    public void lockTimeoutContinuesLoop() throws Exception {
        AtomicInteger pollCount = new AtomicInteger();
        EnvelopePOJO<String> entry = new EnvelopePOJO<>("after-timeout", "v");
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
                envelope -> {
                    delivered.add(envelope.getRoutingKey());
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
        EnvelopePOJO<String> entry = new EnvelopePOJO<>("after-corrupt", "v");
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
                envelope -> {
                    delivered.add(envelope.getRoutingKey());
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
        EnvelopePOJO<String> entry = new EnvelopePOJO<>("stuck-key", "stuck-value");
        AtomicBoolean polled = new AtomicBoolean();
        CountDownLatch deliverStarted = new CountDownLatch(1);

        Dispatcher<String> dispatcher = new Dispatcher<>(
                lockTimeoutMs -> polled.compareAndSet(false, true) ? entry : null,
                envelope -> {
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
                envelope -> true);
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
