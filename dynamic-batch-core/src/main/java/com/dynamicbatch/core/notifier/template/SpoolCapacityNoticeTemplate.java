package com.dynamicbatch.core.notifier.template;

import com.dynamicbatch.common.enums.NotifyTypeEnum;
import com.dynamicbatch.common.util.AppInstance;
import com.dynamicbatch.core.notifier.context.SpoolCapacityContext;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

/**
 * Spool 容量告警模板（检测型）。
 *
 * <p>渲染巡检时刻的磁盘占用现场：组、当前占用 / 预算与占比、
 * 已读完未删除 / 还未读拆分（分别指向清理滞后与消费跟不上）。
 * 内容为 markdown 列表格式（钉钉/企微均支持），与 {@link OfferFailedNoticeTemplate} 风格一致。
 */
public class SpoolCapacityNoticeTemplate implements NoticeTemplate<SpoolCapacityContext> {

    private static final DateTimeFormatter TIME_FORMAT = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    @Override
    public NotifyTypeEnum type() {
        return NotifyTypeEnum.SPOOL_CAPACITY;
    }

    @Override
    public String build(SpoolCapacityContext context) {
        StringBuilder content = new StringBuilder();
        content.append("## ⚠️ Spool 容量告警\n\n");
        content.append("- 组: `").append(context.getKey()).append("`\n");
        content.append("- 实例: `").append(AppInstance.instanceLabel()).append("`\n");
        content.append("- 占用: ").append(formatBytes(context.getCurrentBytes()))
                .append(" / ").append(formatBytes(context.getMaxBytes()))
                .append("（").append(context.getPercent()).append("%）\n");
        content.append("- 还未读: ").append(formatBytes(context.getPendingBytes()))
                .append("\n");
        content.append("- 已读完未删除: ").append(formatBytes(context.getConsumedBytes()))
                .append("\n");
        content.append("- 时间: ").append(LocalDateTime.now().format(TIME_FORMAT)).append("\n");
        return content.toString();
    }

    /** 字节数人类可读化：B / KB / MB / GB / TB，向上取整保留整数 */
    private static String formatBytes(long bytes) {
        if (bytes == Long.MAX_VALUE) {
            return "不限";
        }
        if (bytes < 1024) {
            return bytes + " B";
        }
        long value = bytes;
        String[] units = {"KB", "MB", "GB", "TB"};
        String unit = "B";
        for (String u : units) {
            // 1024 以下保持 B/KB 精度，再往上按 1024 进位
            if (value < 1024) {
                break;
            }
            value = (value + 1023) / 1024;   // 向上取整，告警只高不低
            unit = u;
        }
        return value + " " + unit;
    }
}
