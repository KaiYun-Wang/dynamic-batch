package com.dynamicbatch.common.pojo;

import java.util.UUID;

/**
 * 通知平台配置 POJO。
 *
 * <p>一个实例描述"一个通知渠道"的发送参数：{@code platform} 决定用哪个 Notifier。
 * 字段为超集模型，各渠道各取所需：webhook/urlKey/secret 是 HTTP 机器人渠道
 * （钉钉/企微/飞书）的发送参数；邮件/短信渠道只需填 receivers，其余字段可留空。
 * 设计参考 dromara dynamic-tp 的 common/entity/NotifyPlatform。
 */
public class NotifyPlatformPOJO {

    /** 平台配置唯一 id，供配置中心多实例引用；不填自动生成 */
    private String platformId = UUID.randomUUID().toString();

    /** 平台类型，如 ding / wechat / email，对应 Notifier.platform() */
    private String platform;

    /** webhook 地址（HTTP 机器人渠道用，可自带 access_token；邮件/短信渠道无需填写） */
    private String webhook;

    /** 访问令牌（HTTP 机器人渠道用）；webhook 未携带 access_token 时作为追加参数 */
    private String urlKey;

    /** 加签密钥（HTTP 机器人渠道用），配置后请求自动携带 timestamp + sign */
    private String secret;

    /** 收件人，逗号分隔（钉钉/企微为 @ 的手机号，邮箱渠道为收件地址；"all" 或空表示 @所有人） */
    private String receivers = "all";

    /** HTTP 请求超时毫秒 */
    private Integer timeout = 3000;

    public String getPlatformId() {
        return platformId;
    }

    public void setPlatformId(String platformId) {
        this.platformId = platformId;
    }

    public String getPlatform() {
        return platform;
    }

    public void setPlatform(String platform) {
        this.platform = platform;
    }

    public String getWebhook() {
        return webhook;
    }

    public void setWebhook(String webhook) {
        this.webhook = webhook;
    }

    public String getUrlKey() {
        return urlKey;
    }

    public void setUrlKey(String urlKey) {
        this.urlKey = urlKey;
    }

    public String getSecret() {
        return secret;
    }

    public void setSecret(String secret) {
        this.secret = secret;
    }

    public String getReceivers() {
        return receivers;
    }

    public void setReceivers(String receivers) {
        this.receivers = receivers;
    }

    public Integer getTimeout() {
        return timeout;
    }

    public void setTimeout(Integer timeout) {
        this.timeout = timeout;
    }
}