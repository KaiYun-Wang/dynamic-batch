package com.dynamicbatch.common.constants;


/**
 * 批处理 Worker 参数默认值常量。
 */
public class BatchWorkerConstant {
    /** 轮询间隔：消费线程以此间隔醒来检查运行状态，关闭信号响应延迟不超过该值 */
    public static final long WAKEUP_INTERVAL_MS = 100L;
    /** 关闭时等待消费线程自然退出（处理完手头批次）的上限，与攒批窗口 maxWaitMs 无关 */
    public static final long SHUTDOWN_WAIT_MS = 5000L;
    /** 队列容量默认值 */
    public static final int DEFAULT_QUEUE_CAPACITY = 1024;
    /** 攒批条数默认值 */
    public static final int DEFAULT_BATCH_SIZE = 50;
    /** 最大等待毫秒默认值 */
    public static final long DEFAULT_MAX_WAIT_MS = 1000;
    /** 入队超时毫秒默认值 */
    public static final long DEFAULT_OFFER_TIMEOUT_MS = 100;
    /** 消费线程数默认值 */
    public static final int DEFAULT_CONSUMERS = 1;
}