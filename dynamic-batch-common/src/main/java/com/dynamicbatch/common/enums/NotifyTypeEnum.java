package com.dynamicbatch.common.enums;

/**
 * 通知类型枚举。
 *
 * <p>调用 {@code NotifyManager.tryNoticeAsync} 时传入，决定构建并发送哪种消息；
 * 每种类型对应一个 {@code NoticeTemplate} 实现。新增消息类型时扩展本枚举并
 * 注册对应模板即可，门面与渠道层无需改动。
 */
public enum NotifyTypeEnum {

    /** 配置变更通知：worker 热更新后，汇总实际变更的字段与新旧值 */
    CHANGE,

    /** 入队失败告警：submit 被拒绝（worker 未运行 / 类型不匹配 / 队列满超时 / 被中断） */
    OFFER_FAILED,

    /** 批次执行失败告警：flush 回调抛异常（含 failureHandler 是否兜住的信息） */
    FLUSH_FAILED,

    /** 队列积压告警：定时检查发现队列利用率超阈值（周期与阈值见 notify-items 的 interval-seconds / threshold） */
    QUEUE_BLOCKED
}
