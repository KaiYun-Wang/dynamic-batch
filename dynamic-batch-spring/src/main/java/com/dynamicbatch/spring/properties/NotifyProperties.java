package com.dynamicbatch.spring.properties;

import com.dynamicbatch.common.pojo.NotifyPlatformPOJO;
import org.springframework.boot.context.properties.ConfigurationProperties;

import java.util.ArrayList;
import java.util.List;

/**
 * 通知平台 yml 配置绑定。
 *
 * <p>对应配置前缀 {@code dynamic-batch.notify}，示例：
 * <pre>
 * dynamic-batch:
 *   notify:
 *     platforms:
 *       - platform: ding
 *         webhook: https://oapi.dingtalk.com/robot/send?access_token=xxx
 *         secret: xxx
 *         receivers: "all"
 * </pre>
 * 由 {@code NotifyAutoConfiguration} 通过 {@code @EnableConfigurationProperties} 启用，
 * 配置项绑定到 {@link NotifyPlatformPOJO} 超集模型，各渠道各取所需。
 */
@ConfigurationProperties(prefix = "dynamic-batch.notify")
public class NotifyProperties {

    /** 通知平台列表，platform 字段对应 Notifier.platform() */
    private List<NotifyPlatformPOJO> platforms = new ArrayList<>();

    public List<NotifyPlatformPOJO> getPlatforms() {
        return platforms;
    }

    public void setPlatforms(List<NotifyPlatformPOJO> platforms) {
        this.platforms = platforms;
    }
}
