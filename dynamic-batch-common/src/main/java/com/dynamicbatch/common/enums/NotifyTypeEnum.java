package com.dynamicbatch.common.enums;

/**
 * 通知类型枚举。
 *
 * <p>每种类型对应一个 {@code NoticeTemplate} 实现，由 {@code NotifyManager}
 * 的分场景入口方法（如 {@code tryNoticeOfferFailedAsync}）绑定并投递；
 * 新增消息类型时扩展本枚举并注册对应模板即可，门面与渠道层无需改动。
 */
public enum NotifyTypeEnum {

    /** 入队失败告警：submit 入口被拒（组未启动 / 类型不匹配 / Spool 拒绝 / 写入异常），及 Worker 侧类型不匹配毒丸 */
    OFFER_FAILED,

    /** 批次执行失败告警：flush 回调抛异常（含 failureHandler 是否兜住的信息） */
    FLUSH_FAILED
}
