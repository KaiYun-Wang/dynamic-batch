package com.dynamicbatch.common.pojo;

/**
 * 通知项配置 POJO。
 *
 * <p>描述"一种通知类型"的告警规则：对应 yml
 * {@code dynamic-batch.notify.notify-items} 中每一条配置。
 * {@code silencePeriod} 对所有类型生效（事件型告警防刷屏）；{@code threshold} 与
 * {@code intervalSeconds} 仅检测型告警（queue_blocked）读取，其他类型配置了也不读取、不校验。
 */
public class NotifyItemPOJO {

    /** 通知类型，对应 NotifyTypeEnum 名称（大小写不敏感） */
    private String type;

    /** 静默期秒数，上次发送后 N 秒内不再发送同类通知；0 表示不限流 */
    private long silencePeriod = 0;

    /** 阈值（百分比），仅 queue_blocked 使用：队列利用率 ≥ 此值触发积压告警；未配置默认 70 */
    private Integer threshold;

    /** 定时检查周期秒，仅 queue_blocked 使用：定时任务按此周期遍历所有 worker 队列；未配置默认 60 */
    private Integer intervalSeconds;

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

    public Integer getThreshold() {
        return threshold;
    }

    public void setThreshold(Integer threshold) {
        this.threshold = threshold;
    }

    public Integer getIntervalSeconds() {
        return intervalSeconds;
    }

    public void setIntervalSeconds(Integer intervalSeconds) {
        this.intervalSeconds = intervalSeconds;
    }
}