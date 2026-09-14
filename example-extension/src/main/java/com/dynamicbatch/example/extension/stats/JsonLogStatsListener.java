package com.dynamicbatch.example.extension.stats;

import com.dynamicbatch.common.util.JsonUtil;
import com.dynamicbatch.core.stats.StatsListener;
import com.dynamicbatch.core.stats.StatsSnapshot;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 快照钩子 A：把整份快照打成 JSON 日志（模拟「推监控平台 / 写文件」之类）。
 */
public class JsonLogStatsListener implements StatsListener {

    private static final Logger log = LoggerFactory.getLogger(JsonLogStatsListener.class);

    @Override
    public void onSnapshot(StatsSnapshot snapshot) {
        log.info("[stats-hook-A/json] {}", JsonUtil.toJson(snapshot));
    }
}
