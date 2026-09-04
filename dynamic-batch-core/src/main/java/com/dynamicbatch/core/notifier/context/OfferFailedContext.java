package com.dynamicbatch.core.notifier.context;

/**
 * 入队失败场景上下文：携带 submit 被拒绝时的原因与现场信息。
 *
 * <p>由 {@code NotifyManager.tryNoticeOfferFailedAsync} 在 worker 提交失败分支
 * 构造并投递，模板据此渲染"哪个 worker、为什么失败、当时队列水位"。
 */
public class OfferFailedContext extends NotifyContext {

    private final String reason;
    private final int queueSize;

    public OfferFailedContext(String key, String reason, int queueSize) {
        super(key);
        this.reason = reason;
        this.queueSize = queueSize;
    }

    /** 失败原因（组未启动 / 类型不匹配 / Spool 拒绝 / 写入异常等） */
    public String getReason() {
        return reason;
    }

    /** 失败时刻队列中的元素数量（体现积压程度） */
    public int getQueueSize() {
        return queueSize;
    }
}
