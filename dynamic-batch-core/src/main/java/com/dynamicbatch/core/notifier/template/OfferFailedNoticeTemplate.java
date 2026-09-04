package com.dynamicbatch.core.notifier.template;

import com.dynamicbatch.common.enums.NotifyTypeEnum;
import com.dynamicbatch.common.util.AppInstance;
import com.dynamicbatch.core.notifier.context.OfferFailedContext;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

/**
 * 入队失败告警模板。
 *
 * <p>渲染 submit 被拒绝时的现场信息：worker、失败原因、失败时刻队列水位。
 * 内容为 markdown 列表格式（钉钉/企微均支持），与 {@link FlushFailedNoticeTemplate} 风格一致。
 */
public class OfferFailedNoticeTemplate implements NoticeTemplate<OfferFailedContext> {

    private static final DateTimeFormatter TIME_FORMAT = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    @Override
    public NotifyTypeEnum type() {
        return NotifyTypeEnum.OFFER_FAILED;
    }

    @Override
    public String build(OfferFailedContext context) {
        StringBuilder content = new StringBuilder();
        content.append("## ⚠️ 入队失败\n\n");
        content.append("- worker: `").append(context.getKey()).append("`\n");
        content.append("- 实例: `").append(AppInstance.instanceLabel()).append("`\n");
        content.append("- 原因: ").append(context.getReason()).append("\n");
        content.append("- 当前队列: ").append(context.getQueueSize()).append("\n");
        content.append("- 时间: ").append(LocalDateTime.now().format(TIME_FORMAT)).append("\n");
        return content.toString();
    }
}
