package com.dynamicbatch.core.notifier.manager;

import com.dynamicbatch.common.enums.NotifyTypeEnum;
import com.dynamicbatch.common.pojo.BatchWorkerConfigPOJO;
import com.dynamicbatch.common.pojo.NotifyItemPOJO;
import com.dynamicbatch.common.pojo.NotifyPlatformPOJO;
import com.dynamicbatch.core.notifier.limiter.NotifyLimiter;
import com.dynamicbatch.core.notifier.channel.NotifierRegistry;
import com.dynamicbatch.core.notifier.context.ChangeContext;
import com.dynamicbatch.core.notifier.context.FlushFailedContext;
import com.dynamicbatch.core.notifier.context.NotifyContext;
import com.dynamicbatch.core.notifier.context.OfferFailedContext;
import com.dynamicbatch.core.notifier.template.ChangeNoticeTemplate;
import com.dynamicbatch.core.notifier.template.FlushFailedNoticeTemplate;
import com.dynamicbatch.core.notifier.template.NoticeTemplate;
import com.dynamicbatch.core.notifier.template.OfferFailedNoticeTemplate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * 通知门面：业务事件 → 按场景构造上下文 → 选模板构建内容 → 异步分发到渠道。
 *
 * <p>触发点按场景调用对应入口方法（如 {@link #tryNoticeChangeAsync}），一行投递纯数据；
 * 入口内部绑定通知类型并构造场景上下文（{@link ChangeContext} 等），内容构建由对应
 * {@link NoticeTemplate} 负责（模板返回 null 表示本次无需发送），平台分发由渠道层完成，
 * 触发点与门面都不感知渠道细节。
 * 平台配置由 spring 层启动时通过 {@link #init} 注入（yml 绑定）；未配置平台时通知
 * 直接跳过，因此纯 core（无 spring）使用不会产生任何通知行为。
 * 设计参考 dromara dynamic-tp 的 core/notifier/manager（NoticeManager/AlarmManager 按场景分方法）。
 */
public class NotifyManager {

    private static final Logger log = LoggerFactory.getLogger(NotifyManager.class);

    /** 单线程异步发送池：通知不阻塞业务线程；队列满丢弃最老任务（通知可丢失，不影响主流程） */
    private static final ExecutorService NOTIFY_EXECUTOR = new ThreadPoolExecutor(
            1, 1, 0L, TimeUnit.MILLISECONDS,
            new LinkedBlockingQueue<>(100),
            r -> {
                Thread t = new Thread(r, "batch-notify");
                t.setDaemon(true);
                return t;
            },
            new ThreadPoolExecutor.DiscardOldestPolicy());

    /** 通知类型 → 内容模板注册表；构造时注册内置模板，外部模板通过 {@link #registerTemplate} 追加 */
    private final Map<NotifyTypeEnum, NoticeTemplate<?>> templates = new ConcurrentHashMap<>();

    /** 平台配置（yml 绑定），init 时注入；空列表 = 通知功能关闭 */
    private volatile List<NotifyPlatformPOJO> platforms = Collections.emptyList();

    private NotifyManager() {
        registerTemplate(new ChangeNoticeTemplate());
        registerTemplate(new OfferFailedNoticeTemplate());
        registerTemplate(new FlushFailedNoticeTemplate());
    }

    public static NotifyManager getInstance() {
        return Holder.INSTANCE;
    }

    /**
     * 注入平台配置（spring 层启动时调用，绑定 yml 的 dynamic-batch.notify.platforms）。
     *
     * @param platforms 平台配置列表，可为空（通知功能关闭）
     */
    public void init(List<NotifyPlatformPOJO> platforms) {
        this.platforms = platforms == null ? Collections.emptyList() : platforms;
        NotifierRegistry.getInstance(); // 触发 SPI 加载
        log.info("notify manager initialized, platforms={}", this.platforms);
    }

    /**
     * 注入通知项配置（spring 层启动时调用，绑定 yml 的 dynamic-batch.notify.notify-items）。
     *
     * <p>每个通知项配置了静默期等告警行为参数，初始化后传给 {@link NotifyLimiter}。
     * 不调此方法或传空列表时全部不限流（向后兼容）。
     *
     * @param items 通知项配置列表，可为空
     */
    public void initItems(List<NotifyItemPOJO> items) {
        NotifyLimiter.init(items);
        log.info("notify items initialized, items={}", items);
    }

    /**
     * 注册消息模板；同类型重复注册会覆盖旧模板。
     *
     * @param template 模板实现
     */
    public void registerTemplate(NoticeTemplate<?> template) {
        NoticeTemplate<?> previous = templates.put(template.type(), template);
        if (previous != null) {
            log.warn("replacing template: type={}", template.type());
        }
    }

    /**
     * 配置变更通知：worker 热更新完成后投递，异步执行立即返回。
     *
     * <p>入口内部绑定 {@link NotifyTypeEnum#CHANGE} 并构造 {@link ChangeContext}；
     * 新旧配置无实际差异（相同配置反复推送）时模板返回 null，不发送。
     *
     * @param key       worker 唯一标识
     * @param oldConfig 变更前生效中的配置快照（全字段非 null）
     * @param newConfig 本次传入的配置（可能部分字段为 null）
     */
    public void tryNoticeChangeAsync(String key, BatchWorkerConfigPOJO oldConfig, BatchWorkerConfigPOJO newConfig) {
        NOTIFY_EXECUTOR.execute(() -> doTryNotice(NotifyTypeEnum.CHANGE, new ChangeContext(key, oldConfig, newConfig)));
    }

    /**
     * 入队失败告警：worker 提交失败（未运行 / 类型不匹配 / 队列满超时 / 中断）时投递，异步执行立即返回。
     *
     * @param key            worker 唯一标识
     * @param reason         失败原因描述
     * @param offerTimeoutMs 入队超时毫秒
     * @param queueSize      失败时刻队列中的元素数量
     */
    public void tryNoticeOfferFailedAsync(String key, String reason, long offerTimeoutMs, int queueSize) {
        NOTIFY_EXECUTOR.execute(() -> doTryNotice(NotifyTypeEnum.OFFER_FAILED,
                new OfferFailedContext(key, reason, offerTimeoutMs, queueSize)));
    }

    /**
     * 批次执行失败告警：flush 回调抛异常（failureHandler 处理完之后）时投递，异步执行立即返回。
     *
     * @param key          worker 唯一标识
     * @param failedSize   本次失败批次的元素数量
     * @param errorMsg     flush 回调抛出的异常信息
     * @param dataLossRisk failureHandler 未配置或也未兜住时为 true
     */
    public void tryNoticeFlushFailedAsync(String key, int failedSize, String errorMsg, boolean dataLossRisk) {
        NOTIFY_EXECUTOR.execute(() -> doTryNotice(NotifyTypeEnum.FLUSH_FAILED,
                new FlushFailedContext(key, failedSize, errorMsg, dataLossRisk)));
    }

    @SuppressWarnings("unchecked")
    private void doTryNotice(NotifyTypeEnum type, NotifyContext context) {
        if (platforms.isEmpty()) {
            log.debug("notify skipped, no platforms configured, type={}, key={}", type, context.getKey());
            return;
        }

        if (!NotifyLimiter.isAllowed(context.getKey(), type)) {
            log.debug("notify skipped, silence period active, type={}, key={}", type, context.getKey());
            return;
        }

        // 注册时按模板 type() 登记，此处 cast 安全
        NoticeTemplate<NotifyContext> template = (NoticeTemplate<NotifyContext>) templates.get(type);
        if (template == null) {
            log.error("template not found: type={}", type);
            return;
        }
        String content = template.build(context);
        if (content == null) {
            log.debug("notify skipped, template built nothing, type={}, key={}", type, context.getKey());
            return;
        }

        // 模板构建成功 → 记录时间戳后再发送
        // （先记录后发送：即使本次发送因网络异常失败，静默期也生效，防止重试刷屏）
        NotifyLimiter.record(context.getKey(), type);

        for (NotifyPlatformPOJO platform : platforms) {
            NotifierRegistry.getInstance().send(platform, content);
        }
    }

    private static class Holder {
        private static final NotifyManager INSTANCE = new NotifyManager();
    }
}
