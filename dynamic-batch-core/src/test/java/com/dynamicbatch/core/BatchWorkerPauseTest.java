package com.dynamicbatch.core;

import com.dynamicbatch.common.enums.PausePhase;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * BatchWorker 暂停单测：独立构造，flushCallback 脚本化。
 * 覆盖：批次边界确认（批不截断）、暂停后 drain、drain 超时留队列、超时回滚竞态。
 */
public class BatchWorkerPauseTest {

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

    /** 等待谓词成立（终态断言，超时即红） */
    private static void awaitTrue(Supplier<Boolean> predicate, long timeoutMs, String message)
            throws Exception {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (!predicate.get()) {
            if (System.currentTimeMillis() > deadline) {
                fail(message);
            }
            Thread.sleep(20);
        }
    }

    /**
     * 批次边界语义：攒批中置暂停，必须等本批完整 flush 才确认——批不截断、不丢。
     */
    @Test
    public void ackAtBatchBoundary() throws Exception {
        List<List<String>> batches = new CopyOnWriteArrayList<>();
        BatchWorker<String> worker = BatchWorker.builder(String.class,
                        batch -> batches.add(new ArrayList<>(batch)))
                .batchSize(3).maxWaitMs(400).queueCapacity(64).build();
        worker.setName("pause-boundary");
        worker.start();

        worker.submit("a");
        Thread.sleep(100);   // 确保 a 已被消费线程取走进入攒批
        worker.submit("b");
        Thread.sleep(100);   // 确保 b 已攒入批内，此刻正在等第 3 条
        worker.requestPause();
        awaitPhase(worker::getPausePhase, PausePhase.PAUSED, 3000);

        assertEquals("应恰好 flush 一批", 1, batches.size());
        assertEquals("本批应完整未截断（2 条一批，而非 1+1）", java.util.Arrays.asList("a", "b"), batches.get(0));
        worker.shutdown();
    }

    /**
     * 暂停后 drain：消费完或 drain 掉两条路径合计不丢、队列排空；重复 drain 幂等。
     */
    @Test
    public void pausedDrainsQueue() throws Exception {
        List<String> flushed = new CopyOnWriteArrayList<>();
        BatchWorker<String> worker = BatchWorker.builder(String.class, flushed::addAll)
                .batchSize(1).queueCapacity(64).build();
        worker.setName("pause-drain");
        worker.start();

        for (int i = 0; i < 3; i++) {
            worker.submit("d" + i);   // 快速提交，Worker 逐条消费中
        }
        worker.requestPause();
        awaitPhase(worker::getPausePhase, PausePhase.PAUSED, 3000);

        worker.flushRemainingWithTimeout(2000);
        assertEquals("消费完或 drain 掉，合计应 3 条", 3, flushed.size());
        assertEquals("drain 后队列应排空", 0, worker.getQueueSize());

        int count = flushed.size();
        worker.flushRemainingWithTimeout(2000);   // 幂等：对空队列再排空不重复 flush
        assertEquals(count, flushed.size());
        worker.shutdown();
    }

    /**
     * drain 超时：逐批捞取语义——超时抛 TimeoutException 时未捞数据留在队列（零丢失），
     * 已刷前缀保持 FIFO；恢复后剩余条目全部被消费。
     */
    @Test(timeout = 10_000)
    public void drainTimeoutKeepsRemainingInQueue() throws Exception {
        CountDownLatch firstFlushStarted = new CountDownLatch(1);
        CountDownLatch releaseFlush = new CountDownLatch(1);
        AtomicInteger flushed = new AtomicInteger();
        BatchWorker<String> worker = BatchWorker.builder(String.class, batch -> {
            flushed.addAndGet(batch.size());
            if (firstFlushStarted.getCount() > 0) {
                firstFlushStarted.countDown();
                try {
                    releaseFlush.await(5, TimeUnit.SECONDS);   // 首批 flush 卡住，制造 drain 超时
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        }).batchSize(1).queueCapacity(64).build();
        worker.setName("pause-drain-timeout");
        worker.start();

        // 先暂停（队列空，ack 快），再向已暂停的 Worker 队列预置 4 条（此刻消费线程停在循环顶部不消费）
        worker.requestPause();
        awaitPhase(worker::getPausePhase, PausePhase.PAUSED, 3000);
        for (int i = 0; i < 4; i++) {
            worker.submit("t" + i);
        }
        assertEquals(4, worker.getQueueSize());

        AtomicReference<Throwable> drainError = new AtomicReference<>();
        Thread drainThread = new Thread(() -> {
            try {
                worker.flushRemainingWithTimeout(200);
            } catch (Throwable t) {
                drainError.set(t);
            }
        });
        drainThread.start();

        assertTrue("首批 flush 应已开始（drain 卡在其中）", firstFlushStarted.await(2, TimeUnit.SECONDS));
        Thread.sleep(300);              // flush 卡住超过 200ms 预算
        releaseFlush.countDown();       // 放行 flush：drain 在批边界发现超预算 → 抛
        drainThread.join(2000);

        assertTrue("drain 超时应抛 TimeoutException，actual=" + drainError.get(),
                drainError.get() instanceof TimeoutException);
        assertEquals("已刷前缀 1 条", 1, flushed.get());
        assertEquals("剩余 3 条应留在队列（零丢失）", 3, worker.getQueueSize());

        // 恢复消费：剩余条目按 FIFO 全部消化
        worker.requestRun();
        awaitPhase(worker::getPausePhase, PausePhase.RUNNING, 2000);
        awaitTrue(() -> flushed.get() == 4, 3000, "恢复后剩余 3 条应全部被消费");
        worker.shutdown();
    }

    /** 竞态回归：模拟「协调方超时回滚」与「线程切换相位」并发，20 轮高频压；断言不假死、能恢复消费 */
    @Test(timeout = 15_000)
    public void rollbackRaceRecovers() throws Exception {
        for (int round = 0; round < 20; round++) {
            List<String> flushed = new CopyOnWriteArrayList<>();
            CountDownLatch flushStarted = new CountDownLatch(1);
            CountDownLatch releaseFlush = new CountDownLatch(1);
            BatchWorker<String> worker = BatchWorker.builder(String.class, batch -> {
                flushed.addAll(batch);
                if (flushStarted.getCount() > 0) {
                    flushStarted.countDown();
                    try {
                        releaseFlush.await(5, TimeUnit.SECONDS);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                }
            }).batchSize(2).maxWaitMs(100).queueCapacity(64).build();
            worker.setName("race-" + round);
            worker.start();

            worker.submit("a");                                 // Worker 取走 a 攒批中
            Thread.sleep(50);                                   // 确保已进入攒批（避免空轮间隙提前确认的时序竞争）
            worker.requestPause();                              // 请暂停（线程在内层攒批，尚未读牌）
            assertTrue("round " + round + " 首批 flush 应已开始",
                    flushStarted.await(2, TimeUnit.SECONDS));
            releaseFlush.countDown();                           // 本批 flush 完成，线程即将回顶部读牌
            worker.requestRun();                                // 紧接着回滚：请恢复（与读牌-CAS 真实竞争）

            awaitPhase(worker::getPausePhase, PausePhase.RUNNING, 2000);   // 终态断言：假死实现在此超时转红
            worker.submit("b-" + round);
            awaitTrue(() -> !flushed.isEmpty(), 2000,
                    "round " + round + " 回滚后应继续消费（未假死）");
            worker.shutdown();
        }
    }
}
