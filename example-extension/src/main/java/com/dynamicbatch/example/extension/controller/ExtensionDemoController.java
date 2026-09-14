package com.dynamicbatch.example.extension.controller;

import com.dynamicbatch.common.parser.json.JsonParser;
import com.dynamicbatch.common.util.ExtensionServiceLoader;
import com.dynamicbatch.common.util.JsonUtil;
import com.dynamicbatch.core.notifier.context.FlushFailedContext;
import com.dynamicbatch.core.notifier.context.OfferFailedContext;
import com.dynamicbatch.core.notifier.manager.NotifyManager;
import com.dynamicbatch.example.extension.spi.notifier.FeishuNotifier;
import com.dynamicbatch.example.extension.spi.template.FeishuFlushFailedNoticeTemplate;
import com.dynamicbatch.example.extension.spi.template.FeishuOfferFailedNoticeTemplate;
import com.dynamicbatch.spring.notify.NotifyService;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * 扩展示例 HTTP 入口。路径按扩展点分组，互不混用：
 * <ul>
 *   <li>{@code /extension/feishu*}、{@code /extension/notice*} — 通知（渠道 / 模板）</li>
 *   <li>{@code /extension/json} — JsonParser SPI</li>
 * </ul>
 * StatsListener 广播无 HTTP：启动后看控制台 {@code [stats-hook-A/B/C]} 即可。
 */
@RestController
@RequestMapping("/extension")
public class ExtensionDemoController {

    private final NotifyService notifyService;

    public ExtensionDemoController(NotifyService notifyService) {
        this.notifyService = notifyService;
    }

    // ======================== 1. 通知：渠道（Notifier SPI）========================

    /**
     * 只测「怎么发」：固定文案直发飞书，不经过 NoticeTemplate。
     * <pre>POST /extension/feishu?content=hello</pre>
     */
    @PostMapping("/feishu")
    public Map<String, Object> sendFeishu(
            @RequestParam(defaultValue = "dynamic-batch 飞书 SPI（直发，未走模板）") String content) {
        boolean ok = notifyService.send(FeishuNotifier.PLATFORM, content);
        Map<String, Object> resp = new LinkedHashMap<>();
        resp.put("platform", FeishuNotifier.PLATFORM);
        resp.put("sent", ok);
        resp.put("layer", "Notifier only（怎么发）");
        resp.put("hint", ok
                ? "已交给 FeishuNotifier；看飞书群。自定义模板请调 /extension/notice/offer-failed"
                : "失败：检查 yml webhook 与 SPI");
        return resp;
    }

    // ======================== 2. 通知：模板（NoticeTemplate）========================

    /**
     * 预览自定义模板正文（不发送）。
     * <pre>GET /extension/notice/preview</pre>
     */
    @GetMapping("/notice/preview")
    public Map<String, Object> previewTemplates() {
        String offer = new FeishuOfferFailedNoticeTemplate()
                .build(new OfferFailedContext("extension_demo", "演示：队列满超时", 128));
        String flush = new FeishuFlushFailedNoticeTemplate()
                .build(new FlushFailedContext("extension_demo", 20, "演示：flush 抛异常", true));
        Map<String, Object> resp = new LinkedHashMap<>();
        resp.put("offerFailed", offer);
        resp.put("flushFailed", flush);
        resp.put("note", "对比内置 ## markdown；这里是飞书友好的纯文本分块");
        return resp;
    }

    /**
     * 完整链路：自定义 NoticeTemplate → FeishuNotifier。
     * <pre>POST /extension/notice/offer-failed</pre>
     */
    @PostMapping("/notice/offer-failed")
    public Map<String, Object> triggerOfferFailed() {
        NotifyManager.getInstance().tryNoticeOfferFailedAsync(
                "extension_demo", "演示触发：自定义模板 + 飞书渠道", 99);
        Map<String, Object> resp = new LinkedHashMap<>();
        resp.put("triggered", true);
        resp.put("layers", new String[]{
                "NoticeTemplate = FeishuOfferFailedNoticeTemplate（发什么 / 样式）",
                "Notifier = FeishuNotifier post（怎么发）"
        });
        resp.put("hint", "异步发送；看飞书群「飞书自定义模板」。受 silence-period 限流。");
        return resp;
    }

    /**
     * <pre>POST /extension/notice/flush-failed</pre>
     */
    @PostMapping("/notice/flush-failed")
    public Map<String, Object> triggerFlushFailed() {
        NotifyManager.getInstance().tryNoticeFlushFailedAsync(
                "extension_demo", 7, "演示：自定义 FlushFailed 模板", true);
        Map<String, Object> resp = new LinkedHashMap<>();
        resp.put("triggered", true);
        resp.put("hint", "异步发送；看飞书群「批次失败」自定义样式。受 silence-period 限流。");
        return resp;
    }

    // ======================== 3. JsonParser SPI（与通知、统计无关）========================

    /**
     * 查看 JsonParser SPI 候选与 JsonUtil 实际选用结果。
     * <pre>GET /extension/json</pre>
     */
    @GetMapping("/json")
    public Map<String, Object> jsonProbe() {
        List<JsonParser> parsers = ExtensionServiceLoader.get(JsonParser.class);
        List<Map<String, Object>> candidates = new ArrayList<>();
        if (parsers != null) {
            for (JsonParser p : parsers) {
                Map<String, Object> row = new LinkedHashMap<>();
                row.put("class", p.getClass().getName());
                row.put("supports", p.supports());
                candidates.add(row);
            }
        }
        Map<String, Object> sample = new LinkedHashMap<>();
        sample.put("hello", "extension");
        sample.put("spi", true);

        Map<String, Object> resp = new LinkedHashMap<>();
        resp.put("spiCandidates", candidates);
        resp.put("jsonUtilSample", JsonUtil.toJson(sample));
        resp.put("note", "JsonUtil 取第一个 supports()==true 的 SPI；本模块 DemoGsonJsonParser 应排在前面");
        return resp;
    }

}

