package com.dynamicbatch.example.extension.config;

import com.dynamicbatch.common.util.ExtensionServiceLoader;
import com.dynamicbatch.common.util.JsonUtil;
import com.dynamicbatch.core.BatchProcessor;
import com.dynamicbatch.core.BatchWorkerGroup;
import com.dynamicbatch.core.notifier.channel.Notifier;
import com.dynamicbatch.core.notifier.channel.NotifierRegistry;
import com.dynamicbatch.core.notifier.manager.NotifyManager;
import com.dynamicbatch.core.pojo.SpoolConfigPOJO;
import com.dynamicbatch.core.pojo.StatsConfigPOJO;
import com.dynamicbatch.core.stats.StatsListeners;
import com.dynamicbatch.example.extension.spi.json.DemoGsonJsonParser;
import com.dynamicbatch.example.extension.spi.notifier.FeishuNotifier;
import com.dynamicbatch.example.extension.spi.template.FeishuFlushFailedNoticeTemplate;
import com.dynamicbatch.example.extension.spi.template.FeishuOfferFailedNoticeTemplate;
import com.dynamicbatch.example.extension.stats.JsonLogStatsListener;
import com.dynamicbatch.example.extension.stats.SummaryLogStatsListener;
import com.dynamicbatch.spring.notify.NotifyService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Configuration;

import javax.annotation.PostConstruct;
import java.io.Serializable;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 扩展示例启动装配。四块扩展彼此独立，按方法分开注册，不要当成同一件事：
 * <ol>
 *   <li>{@link #registerNoticeChannelProbe} — Notifier SPI（怎么发）</li>
 *   <li>{@link #registerNoticeTemplates} — NoticeTemplate（发什么 / 样式）</li>
 *   <li>{@link #registerJsonParserProbe} — JsonParser SPI（全局 JSON 引擎）</li>
 *   <li>{@link #registerStatsHooks} — StatsListener 多钩子广播（采集出口）</li>
 * </ol>
 */
@Configuration
public class ExtensionDemoConfiguration {

    private static final Logger log = LoggerFactory.getLogger(ExtensionDemoConfiguration.class);

    public static final String DEMO_GROUP = "extension_demo";

    private final BatchProcessor batchProcessor;
    /** 注入仅为保证排在 NotifyAutoConfiguration 之后（yml platforms / items 已 init） */
    @SuppressWarnings("unused")
    private final NotifyService notifyService;

    public ExtensionDemoConfiguration(BatchProcessor batchProcessor, NotifyService notifyService) {
        this.batchProcessor = batchProcessor;
        this.notifyService = notifyService;
    }

    @PostConstruct
    public void init() {
        // ---- 扩展点按块初始化（互不依赖）----
        registerNoticeChannelProbe();
        registerNoticeTemplates();
        registerJsonParserProbe();
        registerStatsHooks();

        // ---- 仅为驱动统计采集提供流量；flush 故意空，不是本 demo 重点 ----
        registerDemoGroup();
        startBackgroundTraffic();
    }

    // -------------------------------------------------------------------------
    // 扩展① 通知渠道 SPI：META-INF/services → FeishuNotifier
    // -------------------------------------------------------------------------

    /** 触发 NotifierRegistry 加载 SPI，并打印飞书渠道是否在候选列表里。 */
    private void registerNoticeChannelProbe() {
        NotifierRegistry.getInstance();
        List<Notifier> spiNotifiers = ExtensionServiceLoader.get(Notifier.class);
        log.info("[ext-notify-channel] SPI Notifier candidates: {}", classNames(spiNotifiers));
        log.info("[ext-notify-channel] expected platform={}", FeishuNotifier.PLATFORM);
    }

    // -------------------------------------------------------------------------
    // 扩展② 消息模板：覆盖内置 OFFER_FAILED / FLUSH_FAILED 正文样式
    // -------------------------------------------------------------------------

    /**
     * 同类型重复 {@code registerTemplate} 会替换内置模板。
     * 渠道只负责投递；正文长什么样由 NoticeTemplate 决定。
     */
    private void registerNoticeTemplates() {
        NotifyManager.getInstance().registerTemplate(new FeishuOfferFailedNoticeTemplate());
        NotifyManager.getInstance().registerTemplate(new FeishuFlushFailedNoticeTemplate());
        log.info("[ext-notify-template] overridden OFFER_FAILED + FLUSH_FAILED → Feishu plain-text");
    }

    // -------------------------------------------------------------------------
    // 扩展③ JsonParser SPI：与通知无关；JsonUtil 全局选用第一个 supports()==true
    // -------------------------------------------------------------------------

    /** 打一条样例 JSON，确认当前跑的是 DemoGsonJsonParser 而不是内置 Jackson。 */
    private void registerJsonParserProbe() {
        Map<String, Object> sample = new LinkedHashMap<>();
        sample.put("demo", "gson-spi");
        sample.put("engine", DemoGsonJsonParser.class.getSimpleName());
        log.info("[ext-json-spi] JsonUtil.toJson sample: {}", JsonUtil.toJson(sample));
        log.info("[ext-json-spi] probe HTTP: GET /extension/json");
    }

    // -------------------------------------------------------------------------
    // 扩展④ StatsListener 广播：多次 register，每次采集按顺序回调全部钩子
    // -------------------------------------------------------------------------

    /**
     * 与 JsonParser SPI 无关。采集线程每 tick 产出一份 {@code StatsSnapshot}，
     * 再广播给下面注册的全部 listener（A → B → C）。
     */
    private void registerStatsHooks() {
        StatsListeners.register(new JsonLogStatsListener());      // A：整份 JSON
        StatsListeners.register(new SummaryLogStatsListener());   // B：QPS 摘要
        StatsListeners.register(snapshot ->                       // C：再挂一个 lambda
                log.info("[stats-hook-C/lambda] tick group={}, intervalMs={}",
                        snapshot.getGroupKey(),
                        snapshot.getIntervalEndMillis() - snapshot.getIntervalStartMillis()));
        log.info("[ext-stats] registered hooks A+B+C; watch console every ~5s (not /actuator/metrics)");
    }

    // -------------------------------------------------------------------------
    // 演示用 Worker 组 + 后台流量（只为让统计钩子有数据可打）
    // -------------------------------------------------------------------------

    private void registerDemoGroup() {
        batchProcessor.registerGroup(DEMO_GROUP,
                BatchWorkerGroup.builder(DemoItem.class,
                                SpoolConfigPOJO.builder("spool-extension-demo").build(),
                                batch -> { /* 故意空：本 demo 不关注 flush */ })
                        .partitionCount(1)
                        .queueCapacity(256)
                        .batchSize(20)
                        .maxWaitMs(200)
                        .statsConfig(StatsConfigPOJO.builder()
                                .collectIntervalMillis(5_000L)
                                .percentiles(0.5, 0.9, 0.95, 0.99)
                                .build())
                        .build());
        log.info("[demo-traffic] registered group={}", DEMO_GROUP);
    }

    private void startBackgroundTraffic() {
        AtomicLong seq = new AtomicLong();
        Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "extension-demo-traffic");
            t.setDaemon(true);
            return t;
        }).scheduleAtFixedRate(() -> {
            long i = seq.getAndIncrement();
            batchProcessor.submit(DEMO_GROUP, "rk-" + (i % 4), new DemoItem("id-" + i));
        }, 0, 50, TimeUnit.MILLISECONDS);
        log.info("[demo-traffic] ~20 msg/s background submit started");
    }

    private static String classNames(List<?> list) {
        if (list == null || list.isEmpty()) {
            return "[]";
        }
        StringBuilder sb = new StringBuilder("[");
        for (int i = 0; i < list.size(); i++) {
            if (i > 0) {
                sb.append(", ");
            }
            sb.append(list.get(i).getClass().getName());
        }
        return sb.append(']').toString();
    }

    public static class DemoItem implements Serializable {
        private static final long serialVersionUID = 1L;
        private final String id;

        public DemoItem(String id) {
            this.id = id;
        }

        public String getId() {
            return id;
        }
    }
}
