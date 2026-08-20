package com.dynamicbatch.spring.autoconfigure;

import com.dynamicbatch.common.enums.NotifyTypeEnum;
import com.dynamicbatch.common.pojo.NotifyItemPOJO;
import com.dynamicbatch.core.BatchProcessor;
import com.dynamicbatch.spring.monitor.BatchWorkerMonitor;
import com.dynamicbatch.spring.properties.NotifyProperties;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * 队列积压定时检查自动装配：配置了 {@code queue_blocked} 通知项时注册并启动
 * {@link BatchWorkerMonitor}，移除该通知项即关闭定时检查。
 *
 * <p>检查周期（interval-seconds）与阈值（threshold）取自该通知项，未配置时用
 * {@link BatchWorkerMonitor} 的默认值；两项对其他通知类型无效，配置了也不读取、不校验。
 */
@Configuration(proxyBeanMethods = false)
@EnableConfigurationProperties(NotifyProperties.class)
@ConditionalOnBean(BatchProcessor.class)
public class MonitorAutoConfiguration {

    @Bean(destroyMethod = "shutdown")
    @ConditionalOnMissingBean
    public BatchWorkerMonitor batchWorkerMonitor(BatchProcessor batchProcessor, NotifyProperties properties) {
        NotifyItemPOJO item = properties.getNotifyItems().stream()
                .filter(x -> x.getType() != null && x.getType().equalsIgnoreCase(NotifyTypeEnum.QUEUE_BLOCKED.name()))
                .findFirst()
                .orElse(null);
        if (item == null) {
            // 未配置 queue_blocked 通知项：定时检查关闭，不注册 monitor
            return null;
        }
        int intervalSeconds = item.getIntervalSeconds() != null && item.getIntervalSeconds() > 0
                ? item.getIntervalSeconds() : BatchWorkerMonitor.DEFAULT_INTERVAL_SECONDS;
        int threshold = item.getThreshold() != null && item.getThreshold() > 0
                ? item.getThreshold() : BatchWorkerMonitor.DEFAULT_QUEUE_BLOCKED_THRESHOLD;
        BatchWorkerMonitor monitor = new BatchWorkerMonitor(batchProcessor, intervalSeconds, threshold);
        monitor.start();
        return monitor;
    }
}
