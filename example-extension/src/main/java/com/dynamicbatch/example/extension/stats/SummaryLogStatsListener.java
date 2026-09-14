package com.dynamicbatch.example.extension.stats;

import com.dynamicbatch.core.stats.StatsListener;
import com.dynamicbatch.core.stats.StatsSnapshot;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 快照钩子 B：只打关键数字（模拟「告警阈值判断 / 控制台观测」）。
 *
 * <p>与 {@link JsonLogStatsListener} 同时注册：每次采集会<strong>广播</strong>给两个钩子，
 * 证明 {@code StatsListeners} 支持多监听、互不影响。
 */
public class SummaryLogStatsListener implements StatsListener {

    private static final Logger log = LoggerFactory.getLogger(SummaryLogStatsListener.class);

    @Override
    public void onSnapshot(StatsSnapshot snapshot) {
        log.info("[stats-hook-B/summary] group={} submitQps={} callbackQps={} submitTotal={} tp={}",
                snapshot.getGroupKey(),
                String.format("%.1f", snapshot.getSubmitQps()),
                String.format("%.1f", snapshot.getCallbackQps()),
                snapshot.getSubmitTotal(),
                snapshot.getTp());
    }
}
