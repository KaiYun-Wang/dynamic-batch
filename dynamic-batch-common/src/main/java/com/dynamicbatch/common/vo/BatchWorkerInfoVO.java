package com.dynamicbatch.common.vo;

import com.dynamicbatch.common.enums.BatchWorkerHotUpdateType;
import com.dynamicbatch.common.pojo.BatchWorkerConfigPOJO;

/**
 * Worker 运行时信息快照，供监控、Actuator 查询等场景使用。
 */
public class BatchWorkerInfoVO {

    private String key;
    private String dataType;
    /** 热更新通道类型，运行期只读，构建时锁定；null 表示不支持热更新 */
    private BatchWorkerHotUpdateType hotUpdateType;
    private boolean running;
    private int queueSize;
    private int activeConsumers;
    private BatchWorkerConfigPOJO config;

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public String getDataType() {
        return dataType;
    }

    public void setDataType(String dataType) {
        this.dataType = dataType;
    }

    public BatchWorkerHotUpdateType getHotUpdateType() {
        return hotUpdateType;
    }

    public void setHotUpdateType(BatchWorkerHotUpdateType hotUpdateType) {
        this.hotUpdateType = hotUpdateType;
    }

    public boolean isRunning() {
        return running;
    }

    public void setRunning(boolean running) {
        this.running = running;
    }

    public int getQueueSize() {
        return queueSize;
    }

    public void setQueueSize(int queueSize) {
        this.queueSize = queueSize;
    }

    public int getActiveConsumers() {
        return activeConsumers;
    }

    public void setActiveConsumers(int activeConsumers) {
        this.activeConsumers = activeConsumers;
    }

    public BatchWorkerConfigPOJO getConfig() {
        return config;
    }

    public void setConfig(BatchWorkerConfigPOJO config) {
        this.config = config;
    }
}
