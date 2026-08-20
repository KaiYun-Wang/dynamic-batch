package com.dynamicbatch.core.notifier.template;

import com.dynamicbatch.common.enums.NotifyTypeEnum;
import com.dynamicbatch.common.util.AppInstance;
import com.dynamicbatch.core.notifier.context.FlushFailedContext;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

/**
 * 批次执行失败告警模板。
 *
 * <p>渲染 flush 回调抛异常时的现场信息：worker、失败规模、异常信息、数据丢失风险。
 * 内容为 markdown 列表格式（钉钉/企微均支持），与 {@link ChangeNoticeTemplate} 风格一致。
 */
public class FlushFailedNoticeTemplate implements NoticeTemplate<FlushFailedContext> {

    private static final DateTimeFormatter TIME_FORMAT = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    @Override
    public NotifyTypeEnum type() {
        return NotifyTypeEnum.FLUSH_FAILED;
    }

    @Override
    public String build(FlushFailedContext context) {
        StringBuilder content = new StringBuilder();
        content.append("## ⚠️ 批次执行失败\n\n");
        content.append("- worker: `").append(context.getKey()).append("`\n");
        content.append("- 实例: `").append(AppInstance.instanceLabel()).append("`\n");
        content.append("- 失败条数: ").append(context.getFailedSize()).append("\n");
        content.append("- 异常: ").append(context.getErrorMsg()).append("\n");
        content.append("- 数据丢失风险: ")
                .append(context.isDataLossRisk() ? "failureHandler 未配置" : "failureHandler 已接管")
                .append("\n");
        content.append("- 时间: ").append(LocalDateTime.now().format(TIME_FORMAT)).append("\n");
        return content.toString();
    }
}
