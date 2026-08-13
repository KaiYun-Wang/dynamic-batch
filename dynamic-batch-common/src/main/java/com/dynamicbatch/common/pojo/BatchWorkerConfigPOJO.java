package com.dynamicbatch.common.pojo;


/**
 * BatchWorker 热更新配置 POJO。
 *
 * <p>包装类型字段：{@code null} 表示"该项不更新"，只传需要变更的字段即可，
 * 兼容配置中心全量推送与局部增量调整两种场景。
 */
public class BatchWorkerConfigPOJO {

    /** 队列容量，变更时调用 queue.setCapacity 动态调整 */
    private Integer queueCapacity;
    /** 攒批条数，变更后下一批即生效 */
    private Integer batchSize;
    /** 最大等待毫秒，变更后攒批窗口即时生效 */
    private Long maxWaitMs;
    /** 入队超时毫秒，变更后新的 submit 调用生效 */
    private Long offerTimeoutMs;
    /** 消费线程数，变更后增删线程 */
    private Integer consumers;

    public Integer getQueueCapacity() {
        return queueCapacity;
    }

    public void setQueueCapacity(Integer queueCapacity) {
        this.queueCapacity = queueCapacity;
    }

    public Integer getBatchSize() {
        return batchSize;
    }

    public void setBatchSize(Integer batchSize) {
        this.batchSize = batchSize;
    }

    public Long getMaxWaitMs() {
        return maxWaitMs;
    }

    public void setMaxWaitMs(Long maxWaitMs) {
        this.maxWaitMs = maxWaitMs;
    }

    public Long getOfferTimeoutMs() {
        return offerTimeoutMs;
    }

    public void setOfferTimeoutMs(Long offerTimeoutMs) {
        this.offerTimeoutMs = offerTimeoutMs;
    }

    public Integer getConsumers() {
        return consumers;
    }

    public void setConsumers(Integer consumers) {
        this.consumers = consumers;
    }
}