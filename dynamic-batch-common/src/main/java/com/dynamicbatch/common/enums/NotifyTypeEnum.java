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
    CHANGE
}
