package com.dynamicbatch.core;

import com.dynamicbatch.common.enums.BatchWorkerHotUpdateType;
import com.dynamicbatch.common.pojo.BatchWorkerConfigPOJO;
import com.dynamicbatch.common.vo.BatchWorkerInfoVO;
import com.dynamicbatch.core.notifier.manager.NotifyManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 批处理门面
 *
 * <p>事务与幂等由 flush 回调自行保证；失败交给 failureHandler，不做自动重试。
 */
public class BatchProcessor {

    private static final Logger log = LoggerFactory.getLogger(BatchProcessor.class);

    /** Worker key 仅允许字母、数字、连字符、下划线。 */
    private static final String WORKER_KEY_PATTERN = "^[a-zA-Z0-9_-]+$";

    private final Map<String, BatchWorker<?>> workerMap = new ConcurrentHashMap<>();

    /**
     * 注册 Worker。
     *
     * @param key    唯一标识，仅允许字母、数字、{@code -}、{@code _}，如 {@code device_dto_insert}
     * @param worker 已构造好的 Worker（含队列参数与回调）
     * @throws IllegalArgumentException key 为空或不符合命名规则
     */
    public <T> void register(String key, BatchWorker<T> worker) {
        validateWorkerKey(key);
        worker.setName(key);
        BatchWorker<?> previous = workerMap.put(key, worker);
        if (previous != null) {
            log.warn("replacing worker: key={}", key);
            previous.shutdown();
        }
        worker.start();
        log.info("registered worker: key={}", key);
    }

    /**
     * 提交一条数据。
     *
     * @return true 入队成功；false 队列满且超时（或 key 不存在、worker 已关闭）
     */
    @SuppressWarnings("unchecked")
    public <T> boolean submit(String key, T data) {
        BatchWorker<T> worker = (BatchWorker<T>) workerMap.get(key);
        if (worker == null) {
            log.error("worker not found: key={}", key);
            return false;
        }
        return worker.submit(data);
    }

    /**
     * 指定热更新通道刷新 Worker 配置，{@code null} 字段不更新。
     *
     * <p>batchSize/maxWaitMs/offerTimeoutMs/queueCapacity/consumers 均可动态调整，
     * 校验失败抛 {@link IllegalArgumentException}。
     *
     * <p>所有刷新必须显式声明通道：{@code type} 须与本 Worker 声明类型一致；Worker 未声明通道
     * （getHotUpdateType() 为 null，即不支持热更新）时任何 type 都会被拒绝。校验由
     * {@link BatchWorker#refresh(BatchWorkerConfigPOJO, BatchWorkerHotUpdateType)} 统一执行，
     * 直接持有 Worker 引用调用也无法绕过；不匹配抛 {@link IllegalArgumentException}。
     * 外部通道（Actuator、配置中心）均通过本入口刷新。
     *
     * @param key    Worker 唯一标识
     * @param config 待更新配置，{@code null} 字段不更新
     * @param type   热更新通道，必填，须与 Worker 声明类型一致
     * @return true 更新成功；false key 不存在
     * @throws IllegalArgumentException type 为 null 或与 Worker 声明类型不匹配
     */
    public boolean refresh(String key, BatchWorkerConfigPOJO config, BatchWorkerHotUpdateType type) {
        BatchWorker<?> worker = workerMap.get(key);
        if (worker == null) {
            log.error("worker not found: key={}", key);
            return false;
        }
        // 先拍生效中配置快照，再更新；通知由 NotifyManager 异步处理，这里只投递数据。
        BatchWorkerConfigPOJO oldConfig = worker.configSnapshot();
        worker.refresh(config, type);
        NotifyManager.getInstance().tryNoticeChangeAsync(key, oldConfig, config);
        return true;
    }

    /** 已注册 Worker 的 key 集合（只读视图）。 */
    public Set<String> listWorkerKeys() {
        return Collections.unmodifiableSet(new TreeSet<>(workerMap.keySet()));
    }

    /**
     * 查询指定 Worker 的运行时信息。
     *
     * @return 不存在时返回 {@code null}
     */
    public BatchWorkerInfoVO getWorkerInfo(String key) {
        BatchWorker<?> worker = workerMap.get(key);
        if (worker == null) {
            return null;
        }
        BatchWorkerInfoVO info = new BatchWorkerInfoVO();
        info.setKey(key);
        info.setDataType(worker.getDataTypeName());
        info.setHotUpdateType(worker.getHotUpdateType());
        info.setRunning(worker.isRunning());
        info.setQueueSize(worker.getQueueSize());
        info.setActiveConsumers(worker.getActiveConsumerCount());
        info.setConfig(worker.configSnapshot());
        return info;
    }

    /** 查询所有 Worker 的运行时信息。 */
    public List<BatchWorkerInfoVO> listWorkerInfos() {
        List<BatchWorkerInfoVO> infos = new ArrayList<>(workerMap.size());
        for (String key : listWorkerKeys()) {
            BatchWorkerInfoVO info = getWorkerInfo(key);
            if (info != null) {
                infos.add(info);
            }
        }
        return infos;
    }

    /**
     * 校验 Worker key 命名：非空，且仅含字母、数字、{@code -}、{@code _}。
     *
     * @throws IllegalArgumentException 不符合规则时
     */
    public static void validateWorkerKey(String key) {
        if (key == null || key.isEmpty()) {
            throw new IllegalArgumentException("worker key must not be blank");
        }
        if (!key.matches(WORKER_KEY_PATTERN)) {
            throw new IllegalArgumentException(
                    "invalid worker key: " + key + ", only letters, digits, '-' and '_' are allowed");
        }
    }

    /** 关闭所有 Worker，并尽量刷掉剩余数据 */
    public void shutdown() {
        log.info("shutting down BatchProcessor, workers={}", workerMap.size());
        for (BatchWorker<?> worker : workerMap.values()) {
            worker.shutdown();
        }
        workerMap.clear();
        log.info("BatchProcessor shut down");
    }
}
