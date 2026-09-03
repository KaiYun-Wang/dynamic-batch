package com.dynamicbatch.core;

import com.dynamicbatch.common.enums.PausePhase;
import com.dynamicbatch.common.pojo.SpoolEntryPOJO;
import org.junit.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Dispatcher 暂停单测：假 poller / 假 deliver 驱动，无文件 IO。
 * 覆盖：暂停确认、PAUSED 冻结取数、恢复续投、超时回滚竞态。
 */
public class DispatcherPauseTest {

    /** 短轮询等待相位到达期望值（终态断言，超时即红） */
    private static void awaitPhase(Supplier<PausePhase> phase, PausePhase expected, long timeoutMs)
            throws Exception {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (phase.get() != expected) {
            if (System.currentTimeMillis() > deadline) {
                fail("phase not reach " + expected + ", current=" + phase.get());
            }
            Thread.sleep(20);
        }
    }

    /**
     * 暂停不打断在途投递：deliver 阻塞中置暂停，本条投完才确认；确认后不再取数。
     */
    @Test
    public void pausesBetweenMessages() throws Exception {
        CountDownLatch deliverStarted = new CountDownLatch(1);
        CountDownLatch releaseDeliver = new CountDownLatch(1);
        AtomicInteger delivered = new AtomicInteger();
        AtomicInteger pollCount = new AtomicInteger();

        Dispatcher<String> dispatcher = new Dispatcher<>(
                lockTimeoutMs -> {
                    if (pollCount.incrementAndGet() == 1) {
                        return new SpoolEntryPOJO<>("k1", "v1");
                    }
                    return null;
                },
                (routingKey, payload) -> {
                    if (delivered.incrementAndGet() == 1) {
                        deliverStarted.countDown();
                        try {
                            releaseDeliver.await(5, TimeUnit.SECONDS);
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                            return false;
                        }
                    }
                    return true;
                });
        dispatcher.setName("test-pause-between");
        dispatcher.start();

        assertTrue("deliver 应已进入在途投递", deliverStarted.await(2, TimeUnit.SECONDS));
        dispatcher.requestPause();          // 协调方在投递中置暂停：不打断本条
        releaseDeliver.countDown();         // 放行：本条投完 → 回循环顶部才 ack

        awaitPhase(dispatcher::getPausePhase, PausePhase.PAUSED, 2000);
        Thread.sleep(200);
        assertEquals("暂停不打断本条，且确认后不取下一条", 1, delivered.get());
        assertEquals("PAUSED 后不再取数", 1, pollCount.get());
        dispatcher.stop();
    }

    /** PAUSED 冻结取数：暂停后取数计数不再增长 */
    @Test
    public void pausedStopsPolling() throws Exception {
        AtomicInteger pollCount = new AtomicInteger();
        Dispatcher<String> dispatcher = new Dispatcher<>(
                lockTimeoutMs -> {
                    pollCount.incrementAndGet();
                    return null;
                },
                (routingKey, payload) -> true);
        dispatcher.setName("test-pause-poll");
        dispatcher.start();

        dispatcher.requestPause();
        awaitPhase(dispatcher::getPausePhase, PausePhase.PAUSED, 2000);

        int frozen = pollCount.get();
        Thread.sleep(300);
        assertEquals("PAUSED 后 poll 计数应冻结", frozen, pollCount.get());
        dispatcher.stop();
    }

    /** 恢复：切换回 RUNNING 后继续取数投递 */
    @Test
    public void resumeRestoresDelivery() throws Exception {
        AtomicInteger delivered = new AtomicInteger();
        Dispatcher<String> dispatcher = new Dispatcher<>(
                lockTimeoutMs -> new SpoolEntryPOJO<>("k", "v" + System.nanoTime()),
                (routingKey, payload) -> {
                    delivered.incrementAndGet();
                    return true;
                });
        dispatcher.setName("test-resume");
        dispatcher.start();

        dispatcher.requestPause();
        awaitPhase(dispatcher::getPausePhase, PausePhase.PAUSED, 2000);
        int frozen = delivered.get();
        Thread.sleep(200);
        assertEquals("暂停期间不投递", frozen, delivered.get());

        dispatcher.requestRun();
        awaitPhase(dispatcher::getPausePhase, PausePhase.RUNNING, 2000);
        int afterResume = delivered.get();
        Thread.sleep(200);
        assertTrue("恢复后应继续投递", delivered.get() > afterResume);
        dispatcher.stop();
    }

    /**
     * 竞态回归：模拟「协调方超时回滚」与「线程切换相位」并发——放行在途投递的同时立刻回滚，
     * 20 轮高频压；断言线程不会假死、终态恢复运行且继续投递。
     */
    @Test(timeout = 15_000)
    public void rollbackRaceRecovers() throws Exception {
        for (int round = 0; round < 20; round++) {
            final String key = "k" + round;
            CountDownLatch deliverStarted = new CountDownLatch(1);
            CountDownLatch releaseDeliver = new CountDownLatch(1);
            AtomicInteger delivered = new AtomicInteger();

            Dispatcher<String> dispatcher = new Dispatcher<>(
                    lockTimeoutMs -> new SpoolEntryPOJO<>(key, "v"),
                    (routingKey, payload) -> {
                        if (delivered.incrementAndGet() == 1) {
                            deliverStarted.countDown();
                            try {
                                releaseDeliver.await(5, TimeUnit.SECONDS);
                            } catch (InterruptedException e) {
                                Thread.currentThread().interrupt();
                                return false;
                            }
                        }
                        return true;
                    });
            dispatcher.setName("test-race-" + round);
            dispatcher.start();

            assertTrue("round " + round + " deliver 应已进入在途投递", deliverStarted.await(2, TimeUnit.SECONDS));
            dispatcher.requestPause();          // 请暂停（线程在内层投递，尚未读牌）
            releaseDeliver.countDown();         // 本条投完，线程即将回顶部读牌
            dispatcher.requestRun();            // 紧接着回滚：请恢复（与读牌-CAS 真实竞争）

            awaitPhase(dispatcher::getPausePhase, PausePhase.RUNNING, 2000);   // 终态断言：假死实现在此超时转红
            assertTrue("round " + round + " 回滚后应继续投递（未假死）", delivered.get() >= 1);
            dispatcher.stop();
        }
    }
}
