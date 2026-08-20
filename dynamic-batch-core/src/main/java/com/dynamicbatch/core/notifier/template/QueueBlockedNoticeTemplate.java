package com.dynamicbatch.core.notifier.template;

import com.dynamicbatch.common.enums.NotifyTypeEnum;
import com.dynamicbatch.common.util.AppInstance;
import com.dynamicbatch.core.notifier.context.QueueBlockedContext;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

/**
 * 队列积压告警模板。
 *
 * <p>渲染定时检查发现队列利用率超阈值时的现场信息：worker、队列水位、利用率、阈值、消费线程数。
 * 内容为 markdown 列表格式（钉钉/企微均支持），与 {@link OfferFailedNoticeTemplate} 风格一致。
 */
public class QueueBlockedNoticeTemplate implements NoticeTemplate<QueueBlockedContext> {

    private static final DateTimeFormatter TIME_FORMAT = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    @Override
    public NotifyTypeEnum type() {
        return NotifyTypeEnum.QUEUE_BLOCKED;
    }

    @Override
    public String build(QueueBlockedContext context) {
        double utilization = context.getQueueSize() * 100.0 / context.getQueueCapacity();
        StringBuilder content = new StringBuilder();
        content.append("## ⚠️ 队列积压\n\n");
        content.append("- worker: `").append(context.getKey()).append("`\n");
        content.append("- 实例: `").append(AppInstance.instanceLabel()).append("`\n");
        content.append("- 队列: ").append(context.getQueueSize()).append("/").append(context.getQueueCapacity())
                .append(" (").append(String.format("%.1f%%", utilization)).append(")\n");
        content.append("- 阈值: ").append(context.getThreshold()).append("%\n");
        content.append("- 消费线程: ").append(context.getConsumers()).append("\n");
        content.append("- 时间: ").append(LocalDateTime.now().format(TIME_FORMAT)).append("\n");
        return content.toString();
    }
}
