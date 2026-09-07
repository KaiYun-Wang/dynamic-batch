package com.dynamicbatch.spring.actuator;

import com.dynamicbatch.core.BatchProcessor;
import com.dynamicbatch.core.vo.GroupSnapshotVO;
import org.springframework.boot.actuate.endpoint.web.annotation.RestControllerEndpoint;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;

import java.util.List;
import java.util.concurrent.TimeoutException;

/**
 * 运维端点：Worker 组快照查询与运行时变更（参数热更、调度器暂停/恢复、分区数变更）。
 *
 * <p>使用前提：应用引入 spring-boot-starter-web 与 spring-boot-starter-actuator，
 * 并在配置中暴露本端点（默认映射前缀 {@code /actuator}，可经
 * {@code management.endpoints.web.base-path} 调整）：
 * <pre>
 * management:
 *   endpoints:
 *     web:
 *       exposure:
 *         include: dynamicbatch
 * </pre>
 * 条件不满足时本 Bean 不装配，BatchProcessor 其余功能不受影响；
 * {@code dynamic-batch.actuator.enabled=false} 可显式关闭。
 *
 * <p>所有操作统一返回 {@link ApiResult}（code / message / data）：
 * <pre>
 * GET  /actuator/dynamicbatch               全部组快照
 * GET  /actuator/dynamicbatch/{key}         单组快照
 * POST /actuator/dynamicbatch/{key}         参数热更：?batchSize=100&maxWaitMs=300&rateLimitPerSecond=50，缺参 = 不改
 * POST /actuator/dynamicbatch/{key}/pause   暂停调度器（异步意图，置相位即返回，data 为置位后的相位名）
 * POST /actuator/dynamicbatch/{key}/resume  恢复调度器（异步意图，data 为置位后的相位名）
 * POST /actuator/dynamicbatch/{key}/resize  改变分区数：?newSize=4&timeoutMs=5000；前置要求调度器已暂停
 * </pre>
 */
@RestControllerEndpoint(id = "dynamicbatch")
public class BatchEndpoint {

    /** 排空等待预算（毫秒），请求体未配 timeoutMs 时生效 */
    private static final long DEFAULT_RESIZE_TIMEOUT_MS = 30_000L;

    private final BatchProcessor batchProcessor;

    public BatchEndpoint(BatchProcessor batchProcessor) {
        this.batchProcessor = batchProcessor;
    }

    /** 查询全部组快照 */
    @GetMapping
    public ApiResult listGroups() {
        return invoke(batchProcessor::listGroupSnapshots);
    }

    /** 查询单组快照；组不存在返回 404 */
    @GetMapping("/{key}")
    public ApiResult getGroup(@PathVariable String key) {
        GroupSnapshotVO snapshot = batchProcessor.getGroupSnapshot(key);
        if (snapshot == null) {
            return ApiResult.fail(404, "worker group not found: key=" + key);
        }
        return ApiResult.ok(snapshot);
    }

    /** 参数热更：batchSize / maxWaitMs / rateLimitPerSecond，缺参 = 不改；data 为热更后的组快照 */
    @PostMapping("/{key}")
    public ApiResult resizeGroupConfig(@PathVariable String key,
                                       @RequestParam(required = false) Integer batchSize,
                                       @RequestParam(required = false) Long maxWaitMs,
                                       @RequestParam(required = false) Integer rateLimitPerSecond) {
        return invoke(() -> {
            batchProcessor.resizeGroupConfig(key, batchSize, maxWaitMs, rateLimitPerSecond);
            return batchProcessor.getGroupSnapshot(key);
        });
    }

    /** 暂停调度器：置暂停意图即返回，data 为置位后的相位名（PAUSE_PENDING，终态 PAUSED 经快照查询） */
    @PostMapping("/{key}/pause")
    public ApiResult pause(@PathVariable String key) {
        return invoke(() -> {
            batchProcessor.pauseDispatcher(key);
            return phaseName(key);
        });
    }

    /** 恢复调度器：置运行意图即返回，data 为置位后的相位名（RUN_PENDING） */
    @PostMapping("/{key}/resume")
    public ApiResult resume(@PathVariable String key) {
        return invoke(() -> {
            batchProcessor.resumeDispatcher(key);
            return phaseName(key);
        });
    }

    /** 调度器相位名：Dispatcher 为包私有类型，对外统一以字符串输出 */
    private String phaseName(String key) {
        return String.valueOf(batchProcessor.getDispatcherPhase(key));
    }

    /**
     * 改变分区数：阻塞等待全员排空后原子替换，前置要求调度器已暂停（先调 pause）；
     * newSize 必填（缺失由 MVC 返回 400）；timeoutMs 缺省
     * {@value #DEFAULT_RESIZE_TIMEOUT_MS} 毫秒；data 为变更后的组快照
     */
    @PostMapping("/{key}/resize")
    public ApiResult resizePartitions(@PathVariable String key,
                                      @RequestParam Integer newSize,
                                      @RequestParam(required = false) Long timeoutMs) {
        long budget = timeoutMs != null ? timeoutMs : DEFAULT_RESIZE_TIMEOUT_MS;
        return invoke(() -> {
            batchProcessor.resizePartitions(key, newSize, budget);
            return batchProcessor.getGroupSnapshot(key);
        });
    }

    // ======================== 异常映射 ========================

    /**
     * 运维动作：允许抛出受检异常（{@code resizePartitions} 的 {@link TimeoutException}）
     */
    @FunctionalInterface
    private interface OpsAction {
        Object run() throws Exception;
    }

    /**
     * 执行运维动作并把异常映射为返回码：IllegalArgumentException → 400、
     * IllegalStateException → 409、TimeoutException → 504、其余 → 500。
     */
    private ApiResult invoke(OpsAction action) {
        try {
            return ApiResult.ok(action.run());
        } catch (IllegalArgumentException e) {
            return ApiResult.fail(400, e.getMessage());
        } catch (IllegalStateException e) {
            return ApiResult.fail(409, e.getMessage());
        } catch (TimeoutException e) {
            return ApiResult.fail(504, e.getMessage());
        } catch (Exception e) {
            return ApiResult.fail(500, e.toString());
        }
    }
}
