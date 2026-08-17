package com.dynamicbatch.core;

import com.dynamicbatch.common.pojo.BatchWorkerConfigPOJO;
import com.dynamicbatch.core.notifier.manager.NotifyManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 批处理门面
 *
 * <p>事务与幂等由 flush 回调自行保证；失败交给 failureHandler，不做自动重试。
 */
public class BatchProcessor {

    private static final Logger log = LoggerFactory.getLogger(BatchProcessor.class);

    private final Map<String, BatchWorker<?>> workerMap = new ConcurrentHashMap<>();

    /**
     * 注册 Worker。
     *
     * @param key    唯一标识，建议「类名:业务名」，如 DeviceDTO:insert
     * @param worker 已构造好的 Worker（含队列参数与回调）
     */
    public <T> void register(String key, BatchWorker<T> worker) {
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
     * 热更新指定 Worker 的配置，{@code null} 字段不更新。
     *
     * <p>batchSize/maxWaitMs/offerTimeoutMs/queueCapacity/consumers 均可动态调整，
     * 校验失败抛 {@link IllegalArgumentException}。
     *
     * @return true 更新成功；false key 不存在
     */
    public boolean refresh(String key, BatchWorkerConfigPOJO config) {
        BatchWorker<?> worker = workerMap.get(key);
        if (worker == null) {
            log.error("worker not found: key={}", key);
            return false;
        }
        // 先拍生效中配置快照，再更新；通知由 NotifyManager 异步处理，这里只投递数据
        BatchWorkerConfigPOJO oldConfig = worker.configSnapshot();
        worker.refresh(config);
        NotifyManager.getInstance().tryNoticeChangeAsync(key, oldConfig, config);
        return true;
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
