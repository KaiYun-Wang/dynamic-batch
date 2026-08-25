package com.dynamicbatch.core.notifier.context;

import com.dynamicbatch.common.pojo.BatchWorkerGroupConfigPOJO;

/**
 * 配置变更场景上下文：携带变更通知所需的旧值快照与本次传入配置。
 */
public class ChangeContext extends NotifyContext {

    private final BatchWorkerGroupConfigPOJO oldConfig;
    private final BatchWorkerGroupConfigPOJO newConfig;

    public ChangeContext(String key, BatchWorkerGroupConfigPOJO oldConfig, BatchWorkerGroupConfigPOJO newConfig) {
        super(key);
        this.oldConfig = oldConfig;
        this.newConfig = newConfig;
    }

    /** 变更前生效中的配置快照（全字段非 null） */
    public BatchWorkerGroupConfigPOJO getOldConfig() {
        return oldConfig;
    }

    /** 本次传入的配置（可能部分字段为 null） */
    public BatchWorkerGroupConfigPOJO getNewConfig() {
        return newConfig;
    }
}
