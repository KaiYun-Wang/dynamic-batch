package com.dynamicbatch.common.pojo;

import java.util.UUID;

/**
 * 通知平台配置 POJO。
 *
 * <p>一个实例描述"一个通知渠道"的发送参数：{@code platform} 决定用哪个 Notifier。
 * 字段为超集模型，各渠道各取所需：webhook/urlKey/secret 是 HTTP 机器人渠道
 * （钉钉/企微/飞书）的发送参数；email 渠道用 host/port/username/password 发 SMTP。
 * 设计参考 dromara dynamic-tp 的 common/entity/NotifyPlatform。
 */
public class NotifyPlatformPOJO {

    /** 平台配置唯一 id，供配置中心多实例引用；不填自动生成 */
    private String platformId = UUID.randomUUID().toString();

    /** 平台类型，如 ding / wechat / email，对应 Notifier.platform() */
    private String platform;

    /** webhook 地址（HTTP 机器人渠道用，可自带 access_token；邮件渠道无需填写） */
    private String webhook;

    /** 访问令牌（HTTP 机器人渠道用）；webhook 未携带 access_token 时作为追加参数 */
    private String urlKey;

    /** 加签密钥（HTTP 机器人渠道用），配置后请求自动携带 timestamp + sign */
    private String secret;

    /**
     * 收件人，逗号分隔。
     * 钉钉为 @ 的手机号；企微 markdown 需在正文写 {@code <@userid>}（本字段暂不参与拼装）；
     * 邮箱渠道为收件地址；"all" 或空表示 @所有人（钉钉）。
     */
    private String receivers = "all";

    /** HTTP 请求超时毫秒 */
    private Integer timeout = 3000;

    // ========== 邮箱渠道专用字段（platform=email 时使用）==========

    /** SMTP 服务器地址 */
    private String host;

    /** SMTP 端口 */
    private Integer port;

    /** 发件人邮箱（SMTP 登录账号） */
    private String username;

    /** SMTP 授权码 / 密码 */
    private String password;

    /** 邮件标题，默认"攒批通知" */
    private String title = "攒批通知";

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

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public Integer getPort() {
        return port;
    }

    public void setPort(Integer port) {
        this.port = port;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public String getTitle() {
        return title;
    }

    public void setTitle(String title) {
        this.title = title;
    }
}