package com.dynamicbatch.core;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
     * 暂停指定 Worker 组：分发线程停止取数，各分区排空手头批次与队列残留后待命。
     * 暂停期间 submit 不受影响，数据照常落盘，恢复后自动消化积压；
     * 超时自动回滚（整组保持消费）后抛 {@link TimeoutException}，可加大预算重试。
     *
     * @param groupKey  组 key，不可为 null
     * @param timeoutMs 超时预算（毫秒，积压大时相应加大）
     * @throws NullPointerException     groupKey 为 null
     * @throws IllegalArgumentException 组不存在
     * @throws IllegalStateException    组未启动
     * @throws TimeoutException         预算内未完成（已自动回滚）
     * @deprecated 临时暴露，仅用于暂停/恢复的测试与 example 演示；
     *             热更新能力（resize）落地后将删除，请勿在正式业务中使用。
     */
    @Deprecated
    public void pauseGroup(String groupKey, long timeoutMs) throws TimeoutException {
        Objects.requireNonNull(groupKey, "groupKey must not be null");
        BatchWorkerGroup<?> group = groupMap.get(groupKey);
        if (group == null) {
            throw new IllegalArgumentException("worker group not found: key=" + groupKey);
        }
        group.pauseAll(timeoutMs);
    }

    /**
     * 恢复指定 Worker 组的消费，排空暂停期间积压。幂等可重试。
     *
     * @param groupKey  组 key，不可为 null
     * @param timeoutMs 超时预算（毫秒）
     * @throws NullPointerException     groupKey 为 null
     * @throws IllegalArgumentException 组不存在
     * @throws IllegalStateException    组未启动
     * @throws TimeoutException         预算内未全部恢复，可重试
     * @deprecated 临时暴露，仅用于暂停/恢复的测试与 example 演示；
     *             热更新能力（resize）落地后将删除，请勿在正式业务中使用。
     */
    @Deprecated
    public void resumeGroup(String groupKey, long timeoutMs) throws TimeoutException {
        Objects.requireNonNull(groupKey, "groupKey must not be null");
        BatchWorkerGroup<?> group = groupMap.get(groupKey);
        if (group == null) {
            throw new IllegalArgumentException("worker group not found: key=" + groupKey);
        }
        group.resumeAll(timeoutMs);
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
