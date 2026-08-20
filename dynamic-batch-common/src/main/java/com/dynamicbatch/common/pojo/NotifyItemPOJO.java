package com.dynamicbatch.common.pojo;

/**
 * 通知项配置 POJO。
 *
 * <p>描述"一种通知类型"的告警规则：对应 yml
 * {@code dynamic-batch.notify.notify-items} 中每一条配置。
 * 当前只用到 {@code type} 和 {@code silencePeriod}，后续按需追加。
 */
public class NotifyItemPOJO {

    /** 通知类型，对应 NotifyTypeEnum 名称（大小写不敏感） */
    private String type;

    /** 静默期秒数，上次发送后 N 秒内不再发送同类通知；0 表示不限流 */
    private long silencePeriod = 0;

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public long getSilencePeriod() {
        return silencePeriod;
    }

    public void setSilencePeriod(long silencePeriod) {
        this.silencePeriod = silencePeriod;
    }
}