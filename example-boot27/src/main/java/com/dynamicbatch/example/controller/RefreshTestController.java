package com.dynamicbatch.example.controller;

import com.dynamicbatch.common.enums.BatchWorkerHotUpdateType;
import com.dynamicbatch.common.pojo.BatchWorkerConfigPOJO;
import com.dynamicbatch.core.BatchProcessor;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/**
 * 配置热更新自测入口：以 endpoint 通道触发 {@link BatchProcessor#refresh(String, BatchWorkerConfigPOJO, BatchWorkerHotUpdateType)}，
 * 验证变更通知链路。
 *
 * <p>本入口与 Actuator 端点一样声明 endpoint 通道：仅当 worker 构建时声明了
 * {@code BatchWorkerHotUpdateType.ENDPOINT} 才允许刷新（见 application.yml 中的注册示例）。
 *
 * <p>填好 application.yml 中的 dynamic-batch.notify.platforms 后（Windows PowerShell 单行执行，
 * 注意 PowerShell 不支持 \ 续行）：
 * <pre>
 * # 只改队列容量 100 -&gt; 200
 * curl.exe -X POST "http://localhost:8080/worker/refresh?key=demo_item_insert" -H "Content-Type: application/json" -d '{"queueCapacity": 200}'
 *
 * # 同时改攒批条数与最大等待毫秒
 * curl.exe -X POST "http://localhost:8080/worker/refresh?key=demo_item_insert" -H "Content-Type: application/json" -d '{"batchSize": 10, "maxWaitMs": 1000}'
 * </pre>
 * JSON 中未传的字段为 null（不更新）；若所有字段与生效值相同则 diff 为空，不会发通知。
 * 发送结果见机器人消息与应用日志。
 */
@RestController
@RequestMapping("/worker")
public class RefreshTestController {

    private final BatchProcessor batchProcessor;

    public RefreshTestController(BatchProcessor batchProcessor) {
        this.batchProcessor = batchProcessor;
    }

    @PostMapping("/refresh")
    public String refresh(@RequestParam(defaultValue = "demo_item_insert") String key,
                          @RequestBody BatchWorkerConfigPOJO config) {
        boolean ok = batchProcessor.refresh(key, config, BatchWorkerHotUpdateType.ENDPOINT);
        return ok ? "refresh 已提交，变更通知异步发送（结果见机器人/日志）"
                : "refresh 失败：worker 不存在或未声明 endpoint 热更新（见日志）";
    }
}