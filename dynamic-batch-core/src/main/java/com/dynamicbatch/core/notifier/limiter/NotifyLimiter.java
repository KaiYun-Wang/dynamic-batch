package com.dynamicbatch.core.notifier.limiter;

import com.dynamicbatch.common.enums.NotifyTypeEnum;
import com.dynamicbatch.common.pojo.NotifyItemPOJO;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 通知静默期限流器。
 *
 * <p>以 {@code workerKey#通知类型} 为维度，记录上次发送时间戳；
 * 静默期内同一维度重复触发时直接跳过，不发送通知。
 * 零依赖纯 JDK 实现（{@link ConcurrentHashMap}），通过惰性清理防止 key 残留。
 *
 * <p>使用方式：启动时由 {@code NotifyManager.initItems()} 传入配置；
 * 发送前调用 {@link #isAllowed} 判断，通过后调用 {@link #record} 记录时间戳。
 */
public class NotifyLimiter {

    /** key → 上次发送时间戳（毫秒） */
    private static final ConcurrentHashMap<String, Long> CACHE = new ConcurrentHashMap<>();

    /** 通知类型名（大写）→ 静默期秒数；未配置默认 0（不限流） */
    private static final Map<String, Long> SILENCE_PERIODS = new ConcurrentHashMap<>();

    /** put 计数，达到阈值触发惰性清理 */
    private static final int CLEANUP_THRESHOLD = 1000;
    private static int putCounter = 0;

    private NotifyLimiter() {
    }

    /**
     * 从通知项配置初始化静默期参数。
     * 启动时由 NotifyManager 调用，清空并重新加载。
     *
     * @param items 通知项配置列表，可为 null 或空（全部不限流）
     */
    public static void init(List<NotifyItemPOJO> items) {
        SILENCE_PERIODS.clear();
        CACHE.clear();
        if (items == null) {
            return;
        }
        for (NotifyItemPOJO item : items) {
            if (item.getType() == null || item.getType().isEmpty()) {
                continue;
            }
            SILENCE_PERIODS.put(item.getType().toUpperCase(), item.getSilencePeriod());
        }
    }

    /**
     * 判断指定 worker + 通知类型是否允许发送。
     *
     * @param workerKey worker 唯一标识
     * @param type      通知类型
     * @return true = 允许发送（未在静默期）；false = 跳过
     */
    public static boolean isAllowed(String workerKey, NotifyTypeEnum type) {
        long silence = SILENCE_PERIODS.getOrDefault(type.name(), 0L);
        if (silence <= 0) {
            return true;
        }
        String key = genKey(workerKey, type.name());
        Long last = CACHE.get(key);
        if (last == null) {
            return true; // 从未发送过
        }
        if (System.currentTimeMillis() - last >= silence * 1000L) {
            CACHE.remove(key); // 已过静默期，惰性删除过期 key
            return true;
        }
        return false;
    }

    /**
     * 记录本次发送时间戳（发送前调用）。
     *
     * @param workerKey worker 唯一标识
     * @param type      通知类型
     */
    public static void record(String workerKey, NotifyTypeEnum type) {
        String key = genKey(workerKey, type.name());
        CACHE.put(key, System.currentTimeMillis());
        lazyCleanup();
    }

    /**
     * 惰性清理：put 累计达到阈值时遍历删除过期条目，
     * 防止 key 无限增长（worker 销毁后残留）。
     */
    private static void lazyCleanup() {
        if (++putCounter < CLEANUP_THRESHOLD) {
            return;
        }
        putCounter = 0;
        long now = System.currentTimeMillis();
        CACHE.forEach((key, last) -> {
            int sep = key.lastIndexOf('#');
            if (sep < 0) {
                return;
            }
            String type = key.substring(sep + 1);
            long silence = SILENCE_PERIODS.getOrDefault(type, 0L);
            if (silence > 0 && now - last >= silence * 1000L) {
                CACHE.remove(key, last);
            }
        });
    }

    private static String genKey(String workerKey, String type) {
        return workerKey + "#" + type;
    }
}