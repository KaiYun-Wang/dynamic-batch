package com.dynamicbatch.core.notifier.context;

/**
 * 队列积压告警场景上下文：携带定时检查发现队列利用率超阈值时的现场信息。
 *
 * <p>由 {@code NotifyManager.tryNoticeQueueBlockedAsync} 在定时检查（spring 层 monitor）发现
 * 队列利用率超阈值后构造并投递，模板据此渲染"哪个 worker、队列水位、利用率、消费线程数"。
 */
public class QueueBlockedContext extends NotifyContext {

    /** 检查时刻队列中的元素数量（体现积压程度） */
    private final int queueSize;
    /** 队列容量 */
    private final int queueCapacity;
    /** 告警阈值（百分比），用于消息中展示配置值 */
    private final int threshold;
    /** 消费线程数（体现"积压但消费慢"的现场） */
    private final int consumers;

    public QueueBlockedContext(String key, int queueSize, int queueCapacity, int threshold, int consumers) {
        super(key);
        this.queueSize = queueSize;
        this.queueCapacity = queueCapacity;
        this.threshold = threshold;
        this.consumers = consumers;
    }

    public int getQueueSize() {
        return queueSize;
    }

    public int getQueueCapacity() {
        return queueCapacity;
    }

    public int getThreshold() {
        return threshold;
    }

    public int getConsumers() {
        return consumers;
    }
}
