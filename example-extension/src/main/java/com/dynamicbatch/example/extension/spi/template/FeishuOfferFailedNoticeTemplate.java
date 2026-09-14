package com.dynamicbatch.example.extension.spi.template;

import com.dynamicbatch.common.enums.NotifyTypeEnum;
import com.dynamicbatch.common.util.AppInstance;
import com.dynamicbatch.core.notifier.context.OfferFailedContext;
import com.dynamicbatch.core.notifier.template.NoticeTemplate;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

/**
 * 自定义「入队失败」消息模板：覆盖内置 {@code OfferFailedNoticeTemplate}。
 *
 * <p>内置模板是钉钉/企微 markdown（{@code ##} / {@code -} 列表）；飞书 text/post 对 markdown
 * 支持差，这里改成纯文本分块样式，证明「发什么」与「怎么发」是两层扩展。
 */
public class FeishuOfferFailedNoticeTemplate implements NoticeTemplate<OfferFailedContext> {

    private static final DateTimeFormatter TIME_FORMAT = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    @Override
    public NotifyTypeEnum type() {
        return NotifyTypeEnum.OFFER_FAILED;
    }

    @Override
    public String build(OfferFailedContext context) {
        return ""
                + "【dynamic-batch · 飞书自定义模板】\n"
                + "──────── 入队失败 ────────\n"
                + "组名　　: " + context.getKey() + "\n"
                + "实例　　: " + AppInstance.instanceLabel() + "\n"
                + "原因　　: " + context.getReason() + "\n"
                + "队列水位: " + context.getQueueSize() + "\n"
                + "时间　　: " + LocalDateTime.now().format(TIME_FORMAT) + "\n"
                + "────────────────────────\n"
                + "（本条由 FeishuOfferFailedNoticeTemplate 渲染，不是内置 markdown）";
    }
}
