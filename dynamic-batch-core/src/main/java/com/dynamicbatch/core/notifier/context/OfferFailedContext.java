package com.dynamicbatch.core.notifier.context;

/**
 * 入队失败场景上下文：携带 submit 被拒绝时的原因与现场信息。
 *
 * <p>由 {@code NotifyManager.tryNoticeOfferFailedAsync} 在 worker 提交失败分支
 * 构造并投递，模板据此渲染"哪个 worker、为什么失败、当时队列水位"。
 */
public class OfferFailedContext extends NotifyContext {

    private final String reason;
    private final long offerTimeoutMs;
    private final int queueSize;

    public OfferFailedContext(String key, String reason, long offerTimeoutMs, int queueSize) {
        super(key);
        this.reason = reason;
        this.offerTimeoutMs = offerTimeoutMs;
        this.queueSize = queueSize;
    }

    /** 失败原因（worker 未运行 / 类型不匹配 / 队列满超时 / 被中断） */
    public String getReason() {
        return reason;
    }

    /** 入队超时毫秒（队列满场景下体现等待时长） */
    public long getOfferTimeoutMs() {
        return offerTimeoutMs;
    }

    /** 失败时刻队列中的元素数量（体现积压程度） */
    public int getQueueSize() {
        return queueSize;
    }
}
