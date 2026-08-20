package com.dynamicbatch.core.notifier.template;

import com.dynamicbatch.common.enums.NotifyTypeEnum;
import com.dynamicbatch.common.pojo.BatchWorkerConfigPOJO;
import com.dynamicbatch.common.util.AppInstance;
import com.dynamicbatch.core.notifier.context.ChangeContext;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * 配置变更通知模板。
 *
 * <p>对比新旧配置，只保留实际变化的字段（newConfig 非 null 字段表示本次意图更新，
 * 与 oldConfig 生效值不同才算变更）；无差异时返回 {@code null} 表示无需发送，
 * 避免相同配置反复推送刷屏。
 */
public class ChangeNoticeTemplate implements NoticeTemplate<ChangeContext> {

    /** 配置字段中文名，用于通知内容可读性；按 BatchWorkerConfigPOJO 声明顺序排列 */
    private static final Map<String, String> FIELD_NAMES = new LinkedHashMap<>();

    static {
        FIELD_NAMES.put("queueCapacity", "队列容量");
        FIELD_NAMES.put("batchSize", "攒批条数");
        FIELD_NAMES.put("maxWaitMs", "最大等待(ms)");
        FIELD_NAMES.put("offerTimeoutMs", "入队超时(ms)");
        FIELD_NAMES.put("consumers", "消费线程数");
    }

    @Override
    public NotifyTypeEnum type() {
        return NotifyTypeEnum.CHANGE;
    }

    @Override
    public String build(ChangeContext context) {
        String key = context.getKey();
        BatchWorkerConfigPOJO oldConfig = context.getOldConfig();
        BatchWorkerConfigPOJO newConfig = context.getNewConfig();
        List<String> changedFields = diffFields(oldConfig, newConfig);
        if (changedFields.isEmpty()) {
            return null;
        }
        return buildContent(key, oldConfig, newConfig, changedFields);
    }

    private static List<String> diffFields(BatchWorkerConfigPOJO oldConfig, BatchWorkerConfigPOJO newConfig) {
        List<String> changed = new ArrayList<>();
        if (newConfig.getQueueCapacity() != null
                && !Objects.equals(newConfig.getQueueCapacity(), oldConfig.getQueueCapacity())) {
            changed.add("queueCapacity");
        }
        if (newConfig.getBatchSize() != null
                && !Objects.equals(newConfig.getBatchSize(), oldConfig.getBatchSize())) {
            changed.add("batchSize");
        }
        if (newConfig.getMaxWaitMs() != null
                && !Objects.equals(newConfig.getMaxWaitMs(), oldConfig.getMaxWaitMs())) {
            changed.add("maxWaitMs");
        }
        if (newConfig.getOfferTimeoutMs() != null
                && !Objects.equals(newConfig.getOfferTimeoutMs(), oldConfig.getOfferTimeoutMs())) {
            changed.add("offerTimeoutMs");
        }
        if (newConfig.getConsumers() != null
                && !Objects.equals(newConfig.getConsumers(), oldConfig.getConsumers())) {
            changed.add("consumers");
        }
        return changed;
    }

    /**
     * 构建变更通知 markdown 内容（钉钉/企微均支持该语法）。
     *
     * <p>注意钉钉渲染规则：纯文本行之间的单个换行（软换行）会被渲染成空格，
     * 只有列表项之间的换行或空行分隔才生效，因此明细行统一用列表项
     * （无序列表为标准 markdown，钉钉/企微均原生支持；
     * 不用 dynamic-tp 的 \n\n 空行 + <font color=#hex> 方案，
     * 因为 <font> 十六进制色值是钉钉专有语法，企微只认 green/info/warning 等关键字）。
     */
    private static String buildContent(String key, BatchWorkerConfigPOJO oldConfig,
                                       BatchWorkerConfigPOJO newConfig, List<String> changedFields) {
        String fieldNames = changedFields.stream()
                .map(FIELD_NAMES::get)
                .collect(Collectors.joining(", "));
        StringBuilder content = new StringBuilder();
        content.append("## 攒批配置变更\n\n");
        content.append("- worker: `").append(key).append("`\n");
        content.append("- 实例: `").append(AppInstance.instanceLabel()).append("`\n");
        content.append("- 变更字段: ").append(fieldNames).append("\n\n");
        for (String field : changedFields) {
            content.append("- ").append(FIELD_NAMES.get(field))
                    .append(": ").append(getFieldValue(oldConfig, field))
                    .append(" → ").append(getFieldValue(newConfig, field))
                    .append("\n");
        }
        return content.toString();
    }

    private static Object getFieldValue(BatchWorkerConfigPOJO config, String field) {
        switch (field) {
            case "queueCapacity":
                return config.getQueueCapacity();
            case "batchSize":
                return config.getBatchSize();
            case "maxWaitMs":
                return config.getMaxWaitMs();
            case "offerTimeoutMs":
                return config.getOfferTimeoutMs();
            case "consumers":
                return config.getConsumers();
            default:
                return null;
        }
    }
}
