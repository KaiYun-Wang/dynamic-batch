package com.dynamicbatch.core.stats;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * 统计快照出口全局注册表：一次注册服务全部组，可注册多个；推送时逐个
 * try-catch Throwable，用户钩子异常不带崩采集与其他钩子。
 */
public final class StatsListeners {

    private static final Logger log = LoggerFactory.getLogger(StatsListeners.class);

    /** 注册表：写少读多，CopyOnWrite 快照遍历天然容忍注册并发 */
    private static final List<StatsListener> LISTENERS = new CopyOnWriteArrayList<>();

    private StatsListeners() {
    }

    /**
     * 注册快照钩子；重复注册同一实例会重复回调，去重由调用方自理。
     *
     * @param listener 钩子实现（可为 lambda），null 抛 {@link IllegalArgumentException}
     */
    public static void register(StatsListener listener) {
        if (listener == null) {
            throw new IllegalArgumentException("listener must not be null");
        }
        LISTENERS.add(listener);
        log.info("stats listener registered, listener={}, total={}",
                listener.getClass().getName(), LISTENERS.size());
    }

    /**
     * 注销快照钩子；未注册过的实例静默返回。
     *
     * @param listener 钩子实现
     * @return true 本次注销生效
     */
    public static boolean unregister(StatsListener listener) {
        if (listener == null) {
            return false;
        }
        boolean removed = LISTENERS.remove(listener);
        if (removed) {
            log.info("stats listener unregistered, listener={}, total={}",
                    listener.getClass().getName(), LISTENERS.size());
        }
        return removed;
    }

    /** 当前注册的钩子数（测试与运维观测用） */
    static int size() {
        return LISTENERS.size();
    }

    /** 推送快照给全部钩子，单个钩子异常只 error 留痕 */
    static void notifySnapshot(StatsSnapshot snapshot) {
        for (StatsListener listener : LISTENERS) {
            try {
                listener.onSnapshot(snapshot);
            } catch (Throwable t) {
                log.error("stats listener threw, ignored, listener={}",
                        listener.getClass().getName(), t);
            }
        }
    }
}
