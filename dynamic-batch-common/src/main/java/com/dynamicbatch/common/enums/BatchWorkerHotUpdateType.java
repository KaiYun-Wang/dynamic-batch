package com.dynamicbatch.common.enums;

/**
 * BatchWorker 热更新通道类型枚举（BatchWorker 专属）。
 *
 * <p>在构建 BatchWorker 时声明其允许的热更新通道，用于通道隔离，防止混用：
 * 例如配置中心仍推送旧值期间，被 endpoint 手动热更新覆盖，导致多实例配置不一致。
 * 类型在构建期锁定（{@code BatchWorker} 中为 final 字段），运行期不可变更。
 *
 * <p>「不支持热更新」不占枚举值：{@code BatchWorker.hotUpdateType} 为 {@code null}
 * 即表示不支持热更新（Builder 未调用 hotUpdateType() 时的默认状态）。枚举只含实际通道值，
 * 调用方在类型层面就不可能传入"无通道"值。
 *
 * <p>各通道的刷新入口在 {@code BatchProcessor.refresh(key, config, type)} 中校验
 * BatchWorker 声明类型与传入 type 是否一致，不匹配直接拒绝。
 *
 * <p>本枚举专为 BatchWorker 设计：未来其他组件若需热更新通道声明，
 * 请新建独立枚举，勿复用本枚举，避免语义耦合。
 */
public enum BatchWorkerHotUpdateType {

    /** endpoint 方式热更新：由 Actuator 端点等本进程运维入口手动触发 */
    ENDPOINT,

    /** 配置中心热更新：由配置中心（Nacos 等）推送触发，适用于多实例集群 */
    CONFIG_CENTER
}
