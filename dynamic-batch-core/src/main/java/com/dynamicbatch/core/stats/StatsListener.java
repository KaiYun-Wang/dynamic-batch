package com.dynamicbatch.core.stats;

/**
 * 统计快照出口钩子：经 {@link StatsListeners#register} 全局注册一次，服务全部组，
 * 数据靠 {@code snapshot.getGroupKey()} 区分。钩子应快速返回，重 IO 自行异步
 * （同步调用在采集线程上，慢 IO 只会让下份快照迟到）。
 */
@FunctionalInterface
public interface StatsListener {

    /**
     * 每 tick 差分出一份增量快照回调一次。
     *
     * @param snapshot 本次采集区间的增量与累计
     */
    void onSnapshot(StatsSnapshot snapshot);
}
