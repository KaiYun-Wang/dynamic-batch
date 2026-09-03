package com.dynamicbatch.spool;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;

/**
 * Spool 性能压测（main 方法手动执行，不进 CI；类名不带 Test 后缀，避免被 surefire 扫描）。
 * <p>测试：纯写吞吐、纯读吞吐。</p>
 * <p>历史结论（供参考）：大消息 ops/s 下降主因是磁盘带宽而非文件扩容。</p>
 */
public class SpoolPerf {

    private static final Logger log = LoggerFactory.getLogger(SpoolPerf.class);

    private static final int RECORDS = 200_000;
    private static final int STAGING_CAPACITY = 50000;
    private static final int PRODUCER_THREADS = 4;

    private static final Serializer<byte[]> PASSTHROUGH = new Serializer<byte[]>() {
        @Override
        public byte[] serialize(byte[] data) {
            return data;
        }

        @Override
        public byte[] deserialize(byte[] bytes, Class<byte[]> type) {
            return bytes;
        }
    };

    public static void main(String[] args) throws Exception {
        Path dir = Files.createTempDirectory("spool-perf-");
        try {
            System.out.println("=== Spool 性能测试 ===");
            System.out.printf("记录数: %d, 暂存容量: %d, 生产者线程: %d%n",
                    RECORDS, STAGING_CAPACITY, PRODUCER_THREADS);

            // 小消息 256B
            byte[] smallPayload = new byte[256];
            for (int i = 0; i < smallPayload.length; i++) {
                smallPayload[i] = (byte) ('A' + (i % 26));
            }
            runTest(dir.resolve("small"), smallPayload, "小消息(256B)");

            // 中消息 4KB
            byte[] mediumPayload = new byte[4096];
            for (int i = 0; i < mediumPayload.length; i++) {
                mediumPayload[i] = (byte) ('A' + (i % 26));
            }
            runTest(dir.resolve("medium"), mediumPayload, "中消息(4KB)");

            // 大消息 64KB
            byte[] largePayload = new byte[65536];
            for (int i = 0; i < largePayload.length; i++) {
                largePayload[i] = (byte) ('A' + (i % 26));
            }
            runTest(dir.resolve("large"), largePayload, "大消息(64KB)");

            System.out.println("=== 性能测试完成 ===");
        } finally {
            cleanup(dir);
        }
    }

    /** 删除压测临时目录，避免十几 GB 数据残留占满系统盘 */
    private static void cleanup(Path dir) {
        try (Stream<Path> walk = Files.walk(dir)) {
            walk.sorted(Comparator.reverseOrder()).forEach(p -> {
                try {
                    Files.delete(p);
                } catch (Exception e) {
                    System.err.println("删除失败: " + p + " — " + e.getMessage());
                }
            });
            System.out.println("已清理压测临时目录: " + dir);
        } catch (Exception e) {
            System.err.println("清理压测临时目录失败: " + dir + " — " + e.getMessage());
        }
    }

    private static void runTest(Path dir, byte[] payload, String label) throws Exception {
        Spool<byte[]> spool = Spool.builder(byte[].class, dir, PASSTHROUGH)
                .stagingCapacity(STAGING_CAPACITY)
                .flushIntervalMs(1000)
                .offerTimeoutMs(100)
                .build();

        try {
            runStress(spool, payload, label);
        } finally {
            spool.close();
        }
        System.out.printf("[%s] 完成%n", label);
    }

    private static void runStress(Spool<byte[]> spool, byte[] payload, String label) throws Exception {
        // 纯写测试
        AtomicLong writeNanos = new AtomicLong();
        CountDownLatch writeLatch = new CountDownLatch(PRODUCER_THREADS);

        Thread[] producers = new Thread[PRODUCER_THREADS];
        for (int t = 0; t < PRODUCER_THREADS; t++) {
            int perThread = RECORDS / PRODUCER_THREADS;
            producers[t] = new Thread(() -> {
                long start = System.nanoTime();
                for (int i = 0; i < perThread; i++) {
                    while (!spool.append(payload)) {
                        Thread.yield();
                    }
                }
                writeNanos.addAndGet(System.nanoTime() - start);
                writeLatch.countDown();
            }, "writer-" + t);
            producers[t].start();
        }

        writeLatch.await();
        // 等写线程落盘完毕
        Thread.sleep(2000);

        long writeTimeMs = writeNanos.get() / PRODUCER_THREADS / 1_000_000;
        double writeOps = (double) RECORDS / writeTimeMs * 1000;
        System.out.printf("[%s] 纯写: %dms, %,.0f ops/s%n", label, writeTimeMs, writeOps);

        // 纯读测试
        AtomicLong readNanos = new AtomicLong();
        CountDownLatch readLatch = new CountDownLatch(1);

        int[] readCount = {0};
        Thread reader = new Thread(() -> {
            long start = System.nanoTime();
            int count = 0;
            try {
                while (count < RECORDS) {
                    byte[] data = spool.poll(500);
                    if (data != null) {
                        count++;
                    }
                }
            } catch (TimeoutException e) {
                throw new RuntimeException(e);
            }
            readNanos.addAndGet(System.nanoTime() - start);
            readCount[0] = count;
            readLatch.countDown();
        }, "reader");
        reader.start();

        readLatch.await();
        long readTimeMs = readNanos.get() / 1_000_000;
        double readOps = (double) readCount[0] / readTimeMs * 1000;
        System.out.printf("[%s] 纯读: %dms, %,.0f ops/s%n", label, readTimeMs, readOps);
    }
}
