package com.dynamicbatch.example.extension.spi.template;

import com.dynamicbatch.common.enums.NotifyTypeEnum;
import com.dynamicbatch.common.util.AppInstance;
import com.dynamicbatch.core.notifier.context.FlushFailedContext;
import com.dynamicbatch.core.notifier.template.NoticeTemplate;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

/**
 * 自定义「批次失败」消息模板：覆盖内置 {@code FlushFailedNoticeTemplate}。
 */
public class FeishuFlushFailedNoticeTemplate implements NoticeTemplate<FlushFailedContext> {

    private static final DateTimeFormatter TIME_FORMAT = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    @Override
    public NotifyTypeEnum type() {
        return NotifyTypeEnum.FLUSH_FAILED;
    }

    @Override
    public String build(FlushFailedContext context) {
        String risk = context.isDataLossRisk() ? "有丢失风险（无 failureHandler）" : "已由 failureHandler 接管";
        return ""
                + "【dynamic-batch · 飞书自定义模板】\n"
                + "──────── 批次失败 ────────\n"
                + "组名　　: " + context.getKey() + "\n"
                + "实例　　: " + AppInstance.instanceLabel() + "\n"
                + "失败条数: " + context.getFailedSize() + "\n"
                + "异常　　: " + context.getErrorMsg() + "\n"
                + "数据风险: " + risk + "\n"
                + "时间　　: " + LocalDateTime.now().format(TIME_FORMAT) + "\n"
                + "────────────────────────\n"
                + "（本条由 FeishuFlushFailedNoticeTemplate 渲染）";
    }
}
