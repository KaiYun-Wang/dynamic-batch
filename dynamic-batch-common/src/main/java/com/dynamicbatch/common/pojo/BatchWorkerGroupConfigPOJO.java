package com.dynamicbatch.common.pojo;


/**
 * Worker 组共享配置 POJO。
 *
 * <p>组内全部分区 Worker 共享同一实例：组与分区之间零拷贝传递配置，
 * 构建期由 {@code BatchWorkerGroup.builder} 链式填充后锁定（字段不更新）。
 *
 * <p>包装类型字段：{@code null} 表示"该项未设置"（builder 未链式调用时保持默认空），
 * 构建时统一校验并取默认值兜底。
 */
public class BatchWorkerGroupConfigPOJO {

    /** 队列容量，超过此值入队会阻塞直到超时 */
    private Integer queueCapacity;
    /** 攒批条数，达到此数量立即触发 flush */
    private Integer batchSize;
    /** 最大等待毫秒，未攒满时最多等这么久再 flush（从收到第一条数据开始计时） */
    private Long maxWaitMs;
    /** 入队超时毫秒，队列满时 {@code submit} 最多阻塞这么久，超时返回 false */
    private Long offerTimeoutMs;

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
}
