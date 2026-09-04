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

    /** 内存队列容量上限（队列满时由投递方背压处理，见 {@code BatchWorker#submit}） */
    private Integer queueCapacity;
    /** 攒批条数，达到此数量立即触发 flush；volatile：消费线程每轮读，调参线程写 */
    private volatile Integer batchSize;
    /** 最大等待毫秒，未攒满时最多等这么久再 flush（从收到第一条数据开始计时）；volatile 同 batchSize */
    private volatile Long maxWaitMs;

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
}
