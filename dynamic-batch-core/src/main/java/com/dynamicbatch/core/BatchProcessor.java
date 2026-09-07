package com.dynamicbatch.core;

import com.dynamicbatch.spool.DiskUsage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeoutException;

/**
 * 批处理门面：统一管理 Worker 组（{@link BatchWorkerGroup}）。
 *
 * <p>业务方只与本门面和 Worker 组交互，不直接操作分区 Worker：
 * 注册以组为单位，提交带路由 key（组内路由到固定分区，分区内有序），
 * 关闭按组收口。事务与幂等由 flush 回调自行保证；失败交给
 * failureHandler，不做自动重试。
 *
 * <p>分区数运行时调整按「暂停调度器 → 改变分区数 → 恢复调度器」三步编排：
 * 暂停与恢复为异步意图（置相位即返回），改变分区数要求调度器已暂停。
 */
public class BatchProcessor {

    private static final Logger log = LoggerFactory.getLogger(BatchProcessor.class);

    /** Worker 组 key 仅允许字母、数字、连字符、下划线。 */
    private static final String GROUP_KEY_PATTERN = "^[a-zA-Z0-9_-]+$";

    private final Map<String, BatchWorkerGroup<?>> groupMap = new ConcurrentHashMap<>();

    /**
     * 注册 Worker 组。
     *
     * <p>同 key 重复注册视为配置错误，直接抛异常（启动期 fail fast），
     * 不静默替换；putIfAbsent 原子保证并发注册安全，无需加锁。
     *
     * <p>组启动（start）失败时先从注册表移除再原样上抛：避免「已注册但未启动」的
     * 僵尸组占住 key（否则捕获异常重试 register 会误报 already registered）。
     *
     * @param key   唯一标识，仅允许字母、数字、{@code -}、{@code _}，如 {@code device_dto_insert}
     * @param group 已构造好的 Worker 组（含分区数、队列参数与回调）
     * @throws IllegalArgumentException key 为空或不符合命名规则
     * @throws IllegalStateException    key 已被注册；或组启动失败（如 Spool 目录锁冲突）
     */
    public <T> void registerGroup(String key, BatchWorkerGroup<T> group) {
        validateGroupKey(key);
        BatchWorkerGroup<?> previous = groupMap.putIfAbsent(key, group);
        if (previous != null) {
            throw new IllegalStateException("worker group already registered: key=" + key);
        }
        group.setKey(key);
        try {
            group.start();
        } catch (RuntimeException e) {
            groupMap.remove(key);
            throw e;
        }
        log.info("registered worker group: key={}, partitions={}", key, group.getPartitionCount());
    }

    /**
     * 提交一条数据，按 routingKey 路由到组内固定分区。
     *
     * <p>同一 routingKey 的数据永远进入同一分区，分区内严格有序（FIFO + 单线程消费）。
     *
     * @param groupKey   组 key，不可为 null
     * @param routingKey 业务 key（路由依据），不可为 null
     * @return true 入队成功；false 队列满且超时（或组不存在、组已关闭）
     * @throws NullPointerException groupKey 或 routingKey 为 null
     */
    @SuppressWarnings("unchecked")
    public <T> boolean submit(String groupKey, String routingKey, T data) {
        Objects.requireNonNull(groupKey, "groupKey must not be null");
        Objects.requireNonNull(routingKey, "routingKey must not be null");
        BatchWorkerGroup<T> group = (BatchWorkerGroup<T>) groupMap.get(groupKey);
        if (group == null) {
            log.error("worker group not found: key={}", groupKey);
            return false;
        }
        return group.submit(routingKey, data);
    }

    /**
     * 暂停指定组的调度器：置暂停意图后立即返回，分发线程停止从磁盘缓冲取数；
     * 分区 Worker 自然消化手头批次至队列清空，submit 照常落盘（削峰语义保留），
     * 恢复后自动消化积压。改变分区数前须确认调度器已暂停。
     *
     * @param groupKey 组 key，不可为 null
     * @throws NullPointerException     groupKey 为 null
     * @throws IllegalArgumentException 组不存在
     * @throws IllegalStateException    组未启动
     */
    public void pauseDispatcher(String groupKey) {
        requireGroup(groupKey).pauseDispatcher();
    }

    /**
     * 改变指定组的分区数：阻塞等待全员排空后以全新分区组原子替换。
     * 前置要求调度器已暂停（未暂停抛 {@link IllegalStateException}）；
     * 失败时分区数不变、组保持暂停态，可直接重试。
     *
     * @param groupKey  组 key，不可为 null
     * @param newSize   目标分区数，&gt;= 1
     * @param timeoutMs 排空等待的超时预算（毫秒）
     * @throws NullPointerException     groupKey 为 null
     * @throws IllegalArgumentException 组不存在，或 newSize &lt; 1
     * @throws IllegalStateException    组未启动、正在 shutdown，或调度器未暂停
     * @throws TimeoutException         预算内未排空（未动分区，组保持暂停态）
     */
    public void resizePartitions(String groupKey, int newSize, long timeoutMs) throws TimeoutException {
        requireGroup(groupKey).resizePartitions(newSize, timeoutMs);
    }

    /**
     * 恢复指定组的调度器：置运行意图后立即返回，分发线程续投，自动消化磁盘积压。
     *
     * @param groupKey 组 key，不可为 null
     * @throws NullPointerException     groupKey 为 null
     * @throws IllegalArgumentException 组不存在
     * @throws IllegalStateException    组未启动
     */
    public void resumeDispatcher(String groupKey) {
        requireGroup(groupKey).resumeDispatcher();
    }

    /**
     * 运行时调整指定组的攒批参数：batchSize / maxWaitMs，null = 不改。
     * 合并后整体校验，任一非法则组配置不变；下一批自动生效。
     *
     * @param groupKey  组 key，不可为 null
     * @param batchSize 攒批条数，null = 不改
     * @param maxWaitMs 最大等待毫秒，null = 不改
     * @throws NullPointerException     groupKey 为 null
     * @throws IllegalArgumentException 组不存在，或参数非法
     * @throws IllegalStateException    组未启动
     */
    public void resizeGroupConfig(String groupKey, Integer batchSize, Long maxWaitMs) {
        requireGroup(groupKey).resizeGroupConfig(batchSize, maxWaitMs);
    }

    /**
     * 查询指定组调度器的运行状态：RUNNING（已运行）/ PAUSE_PENDING（待暂停）/
     * PAUSED（已暂停）/ RUN_PENDING（待运行）；组未启动返回 null。
     *
     * @param groupKey 组 key，不可为 null
     * @throws NullPointerException     groupKey 为 null
     * @throws IllegalArgumentException 组不存在
     */
    public Dispatcher.PausePhase getDispatcherPhase(String groupKey) {
        return requireGroup(groupKey).getDispatcherPhase();
    }

    // ======================== 磁盘占用查询 ========================

    /**
     * 查询指定组的 Spool 磁盘占用即时拆分（见 {@link DiskUsage}）。
     *
     * @param groupKey 组 key，不可为 null
     * @return 组不存在 / 未启动 / 关闭中 / 统计失败返回 null，不抛异常
     */
    public DiskUsage getSpoolUsage(String groupKey) {
        Objects.requireNonNull(groupKey, "groupKey must not be null");
        BatchWorkerGroup<?> group = groupMap.get(groupKey);
        return group == null ? null : group.getSpoolUsage();
    }

    /**
     * 查询全部组的 Spool 磁盘占用即时拆分（不可用组不出现在结果里）。
     *
     * @return 组 key → 占用拆分，不可变快照；无可用组时为空 map
     */
    public Map<String, DiskUsage> listSpoolUsages() {
        Map<String, DiskUsage> result = new LinkedHashMap<>();
        for (Map.Entry<String, BatchWorkerGroup<?>> entry : groupMap.entrySet()) {
            DiskUsage usage = entry.getValue().getSpoolUsage();
            if (usage != null) {
                result.put(entry.getKey(), usage);
            }
        }
        return result;
    }

    /** 按组 key 查找已注册组，不存在即抛（运维操作 fail fast） */
    private BatchWorkerGroup<?> requireGroup(String groupKey) {
        Objects.requireNonNull(groupKey, "groupKey must not be null");
        BatchWorkerGroup<?> group = groupMap.get(groupKey);
        if (group == null) {
            throw new IllegalArgumentException("worker group not found: key=" + groupKey);
        }
        return group;
    }

    /**
     * 校验 Worker 组 key 命名：非空，且仅含字母、数字、{@code -}、{@code _}。
     *
     * @throws IllegalArgumentException 不符合规则时
     */
    public static void validateGroupKey(String key) {
        if (key == null || key.isEmpty()) {
            throw new IllegalArgumentException("group key must not be blank");
        }
        if (!key.matches(GROUP_KEY_PATTERN)) {
            throw new IllegalArgumentException(
                    "invalid group key: " + key + ", only letters, digits, '-' and '_' are allowed");
        }
    }

    /** 关闭所有 Worker 组，并尽量刷掉剩余数据 */
    public void shutdown() {
        log.info("shutting down BatchProcessor, groups={}", groupMap.size());
        for (BatchWorkerGroup<?> group : groupMap.values()) {
            group.shutdown();
        }
        groupMap.clear();
        log.info("BatchProcessor shut down");
    }
}
