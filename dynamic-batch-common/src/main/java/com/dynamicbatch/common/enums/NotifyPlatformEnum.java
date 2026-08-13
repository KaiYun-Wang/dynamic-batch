package com.dynamicbatch.common.enums;

/**
 * 通知平台类型枚举。
 *
 * <p>定义平台类型的合法值集合（有穷枚举，类型安全）；
 * 渠道实现类通过 {@code NotifyPlatformEnum.DING.name().toLowerCase()} 声明身份，
 * {@code NotifyPlatformPOJO.platform} 字段仍用 String 承接配置文件传入的值。
 * 设计参考 dromara dynamic-tp 的 common/em/NotifyPlatformEnum。
 */
public enum NotifyPlatformEnum {

    /** 钉钉 */
    DING,

    /** 企业微信 */
    WECHAT,

    /** 邮件 */
    EMAIL,

    /** 短信 */
    SMS
}
