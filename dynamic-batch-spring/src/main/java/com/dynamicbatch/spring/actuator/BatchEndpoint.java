package com.dynamicbatch.spring.actuator;

import com.dynamicbatch.common.enums.BatchWorkerHotUpdateType;
import com.dynamicbatch.common.pojo.BatchWorkerConfigPOJO;
import com.dynamicbatch.common.vo.BatchWorkerInfoVO;
import com.dynamicbatch.core.BatchProcessor;
import org.springframework.boot.actuate.endpoint.annotation.Endpoint;
import org.springframework.boot.actuate.endpoint.annotation.ReadOperation;
import org.springframework.boot.actuate.endpoint.annotation.Selector;
import org.springframework.boot.actuate.endpoint.annotation.WriteOperation;
import org.springframework.lang.Nullable;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Actuator 端点：查询 Worker 状态、触发热更新。
 *
 * <p>依赖 {@code spring-boot-starter-actuator}，并在配置中暴露本端点：
 * <pre>
 * management:
 *   endpoints:
 *     web:
 *       exposure:
 *         include: dynamicbatch
 * </pre>
 *
 * <p>默认映射前缀为 {@code /actuator}（可通过 {@code management.endpoints.web.base-path} 调整）。
 */
@Endpoint(id = "dynamicbatch")
public class BatchEndpoint {

    private final BatchProcessor batchProcessor;

    public BatchEndpoint(BatchProcessor batchProcessor) {
        this.batchProcessor = batchProcessor;
    }

    /**
     * 列出全部已注册 Worker 的运行时信息。
     *
     * <p>{@code GET /actuator/dynamicbatch}
     *
     * @return Worker 信息列表；无 Worker 时返回空列表
     */
    @ReadOperation
    public List<BatchWorkerInfoVO> workers() {
        return batchProcessor.listWorkerInfos();
    }

    /**
     * 按 key 查询单个 Worker 的运行时信息。
     *
     * <p>{@code GET /actuator/dynamicbatch/{key}}
     *
     * @param key Worker 唯一标识（路径变量）
     * @return 该 Worker 的运行时信息
     * @throws IllegalArgumentException key 不存在时
     */
    @ReadOperation
    public BatchWorkerInfoVO worker(@Selector String key) {
        BatchWorkerInfoVO info = batchProcessor.getWorkerInfo(key);
        if (info == null) {
            throw new IllegalArgumentException("worker not found: " + key);
        }
        return info;
    }

    /**
     * 热更新指定 Worker 的配置；请求体中未传的字段为 {@code null}，表示不更新。
     *
     * <p>注意：Actuator {@code @WriteOperation} 按「方法参数名」从 JSON 取值，
     * 不能像 {@code @RequestBody} 那样直接用 POJO 接整个 body，因此这里展开为扁平参数。
     *
     * <p>{@code POST /actuator/dynamicbatch/{key}}
     * <pre>
     * Content-Type: application/json
     *
     * {
     *   "batchSize": 10,
     *   "queueCapacity": 200
     * }
     * </pre>
     *
     * @param key            Worker 唯一标识（路径变量）
     * @param queueCapacity  队列容量；未传则不更新
     * @param batchSize      攒批条数；未传则不更新
     * @param maxWaitMs      最大等待毫秒；未传则不更新
     * @param offerTimeoutMs 入队超时毫秒；未传则不更新
     * @param consumers      消费线程数；未传则不更新
     * @return 含 {@code key} / {@code success} / {@code message} / {@code config}（成功时）的结果
     */
    @WriteOperation
    public Map<String, Object> refresh(@Selector String key,
                                       @Nullable Integer queueCapacity,
                                       @Nullable Integer batchSize,
                                       @Nullable Long maxWaitMs,
                                       @Nullable Long offerTimeoutMs,
                                       @Nullable Integer consumers) {
        BatchWorkerConfigPOJO refreshConfig = new BatchWorkerConfigPOJO();
        refreshConfig.setQueueCapacity(queueCapacity);
        refreshConfig.setBatchSize(batchSize);
        refreshConfig.setMaxWaitMs(maxWaitMs);
        refreshConfig.setOfferTimeoutMs(offerTimeoutMs);
        refreshConfig.setConsumers(consumers);
        // 显式声明 endpoint 通道：worker 构建时未声明 BatchWorkerHotUpdateType.ENDPOINT 则拒绝，防止通道混用
        Map<String, Object> result = new LinkedHashMap<>(4);
        result.put("key", key);
        try {
            boolean ok = batchProcessor.refresh(key, refreshConfig, BatchWorkerHotUpdateType.ENDPOINT);
            if (ok) {
                result.put("success", true);
                result.put("message", "refresh submitted, change notification sent asynchronously");
                BatchWorkerInfoVO info = batchProcessor.getWorkerInfo(key);
                if (info != null) {
                    result.put("config", info.getConfig());
                }
            } else {
                // refresh 返回 false 仅表示 worker 不存在（其余失败由异常表达）
                result.put("success", false);
                result.put("message", "refresh failed: worker not found");
            }
        } catch (IllegalArgumentException ex) {
            // 通道类型不匹配或参数非法：由 worker 层校验抛出，透传可读信息
            result.put("success", false);
            result.put("message", "refresh failed: " + ex.getMessage());
        }
        return result;
    }
}
