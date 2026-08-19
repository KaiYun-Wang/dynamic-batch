package com.dynamicbatch.core.notifier.context;

/**
 * 批次执行失败场景上下文：携带 flush 回调抛异常时的现场信息。
 *
 * <p>由 {@code NotifyManager.tryNoticeFlushFailedAsync} 在 worker 刷盘失败分支
 * 构造并投递（failureHandler 处理完之后），模板据此渲染失败规模与数据丢失风险。
 */
public class FlushFailedContext extends NotifyContext {

    private final int failedSize;
    private final String errorMsg;
    private final boolean dataLossRisk;

    public FlushFailedContext(String key, int failedSize, String errorMsg, boolean dataLossRisk) {
        super(key);
        this.failedSize = failedSize;
        this.errorMsg = errorMsg;
        this.dataLossRisk = dataLossRisk;
    }

    /** 本次失败批次的元素数量（体现影响面） */
    public int getFailedSize() {
        return failedSize;
    }

    /** flush 回调抛出的异常信息 */
    public String getErrorMsg() {
        return errorMsg;
    }

    /** 数据丢失风险：failureHandler 未配置或也失败时为 true */
    public boolean isDataLossRisk() {
        return dataLossRisk;
    }
}
