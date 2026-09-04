package com.dynamicbatch.core;

import com.dynamicbatch.core.pojo.SpoolConfigPOJO;
import com.dynamicbatch.spool.Serializer;
import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * 毒丸丢弃测试：类型不匹配的条目 error 留痕后丢弃、继续下一条，不阻塞投递链路、不发运维告警。
 * 覆盖两层：直接向 Worker 提交错误类型（单测）；毒丸序列化器走全链路
 * （submit → Spool → Dispatcher → Worker），模拟反序列化类型错配。
 */
public class PoisonEntryTest {

    @Rule
    public TemporaryFolder temp = new TemporaryFolder();

    private BatchProcessor processor;

    private static void awaitFlushed(int expected, List<String> flushed, long timeoutMs)
            throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (flushed.size() < expected && System.currentTimeMillis() < deadline) {
            Thread.sleep(50);
        }
    }

    @After
    public void tearDown() {
        if (processor != null) {
            processor.shutdown();
        }
    }

    /**
     * Worker 层：错误类型提交返回 true（视为终结，丢弃不占队列），后续正常数据照常消费；
     * 若毒丸堵塞队列，后续数据永远到不了 flush，本用例超时转红。
     */
    @Test(timeout = 10_000)
    public void workerDropsPoisonAndContinues() throws Exception {
        List<String> flushed = new CopyOnWriteArrayList<>();
        BatchWorker<String> worker = BatchWorker.builder(String.class, flushed::addAll)
                .batchSize(2)
                .maxWaitMs(100)
                .build();
        worker.setName("test-poison-worker");
        worker.start();

        // raw type 绕过编译期泛型检查，模拟反序列化出的类型错配（序列化器 bug / 升级续读）
        @SuppressWarnings({"rawtypes", "unchecked"})
        boolean terminated = ((BatchWorker) worker).submit(Integer.valueOf(42));
        assertTrue("毒丸应视为已终结（丢弃），投递方可继续", terminated);

        assertTrue(worker.submit("a"));
        assertTrue(worker.submit("b"));
        awaitFlushed(2, flushed, 5_000);
        assertEquals("毒丸不应出现在 flush 结果", Arrays.asList("a", "b"), flushed);

        worker.shutdown();
    }

    /**
     * 链路层：毒丸序列化器落盘的数据读出来是错误类型，Worker 侧丢弃；同 routingKey 的
     * 后续健康数据照常有序消化，组不卡死、可继续服务。
     */
    @Test(timeout = 30_000)
    public void pipelineDropsPoisonFromSerializerAndContinues() throws Exception {
        processor = new BatchProcessor();
        List<String> flushed = new CopyOnWriteArrayList<>();
        BatchWorkerGroup<String> group = BatchWorkerGroup.builder(String.class,
                        SpoolConfigPOJO.builder(
                                        new File(temp.getRoot(), "poison-pipeline").getAbsolutePath())
                                .serializer(new PoisonSerializer())
                                .build(),
                        flushed::addAll)
                .partitionCount(1)
                .batchSize(2)
                .maxWaitMs(100)
                .build();
        processor.registerGroup("poison-pipeline", group);

        String key = "k1";
        assertTrue(processor.submit("poison-pipeline", key, "poison:abc"));
        assertTrue(processor.submit("poison-pipeline", key, "healthy:1"));
        assertTrue(processor.submit("poison-pipeline", key, "healthy:2"));

        // 毒丸被 Worker 丢弃，两条健康数据同分区 FIFO 消化
        awaitFlushed(2, flushed, 10_000);
        assertEquals(Arrays.asList("healthy:1", "healthy:2"), flushed);

        // 组未卡死：再投一条仍能消化
        assertTrue(processor.submit("poison-pipeline", key, "healthy:3"));
        awaitFlushed(3, flushed, 10_000);
        assertEquals(Arrays.asList("healthy:1", "healthy:2", "healthy:3"), flushed);
    }

    /**
     * 毒丸序列化器（raw type 实现）：payload 以 "poison:" 开头时反序列化返回 Integer——
     * 泛型擦除下堆上真实类型 Integer 谎报为 String，模拟有 bug 的自定义序列化器。
     */
    @SuppressWarnings({"rawtypes", "unchecked"})
    private static final class PoisonSerializer implements Serializer {
        @Override
        public byte[] serialize(Object data) {
            return ((String) data).getBytes(StandardCharsets.UTF_8);
        }

        @Override
        public Object deserialize(byte[] bytes, Class type) {
            String s = new String(bytes, StandardCharsets.UTF_8);
            if (s.startsWith("poison:")) {
                return Integer.valueOf(s.length());
            }
            return s;
        }
    }
}
