package com.dynamicbatch.example;

import com.dynamicbatch.core.BatchProcessor;
import com.dynamicbatch.example.config.BatchProcessorConfiguration;
import com.dynamicbatch.example.config.BatchProcessorConfiguration.DemoItem;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.util.concurrent.TimeUnit;

/**
 * 往已注册的 Worker 组里塞数据（类似业务 MQ 消费后 submit，带路由 key）。
 */
@SpringBootTest(classes = ExampleBoot27Application.class)
public class BatchProcessorDemoTest {

    private static final Logger log = LoggerFactory.getLogger(BatchProcessorDemoTest.class);

    @Autowired
    private BatchProcessor batchProcessor;

    @Test
    public void submitDemoData() throws Exception {
        for (int i = 0; i < 12; i++) {
            boolean ok = batchProcessor.submit(
                    BatchProcessorConfiguration.DEMO_INSERT,
                    "id-" + i,
                    new DemoItem("id-" + i));
            if (!ok) {
                log.warn("submit failed at {}", i);
            }
        }
        TimeUnit.SECONDS.sleep(3);
        log.info("submit demo done");
    }
}
