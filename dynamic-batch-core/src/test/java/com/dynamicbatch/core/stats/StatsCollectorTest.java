package com.dynamicbatch.core.stats;

import com.dynamicbatch.core.pojo.StatsConfigPOJO;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * StatsCollector 采集语义：基线差分、查询不推进、间隔摊平、钩子异常隔离、调度不停摆。
 * tick 手动驱动（collectOnce 为 package-private），真调度只做不停摆冒烟。
 */
public class StatsCollectorTest {

    /** 4 边界 → 5 桶：[0,1) [1,2) [2,5) [5,10) [10,+∞) */
    private static final long[] BOUNDARIES = {1, 2, 5, 10};

    private final List<StatsSnapshot> received = new CopyOnWriteArrayList<>();
    private final StatsListener recorder = received::add;

    private CumulativeStats stats;
    private StatsCollector collector;

    @Before
    public void setUp() {
        StatsListeners.register(recorder);
        stats = new CumulativeStats(BOUNDARIES);
        collector = new StatsCollector("collector-test", stats, StatsConfigPOJO.builder().build());
    }

    @After
    public void tearDown() {
        if (collector != null) {
            collector.stop();
        }
        StatsListeners.unregister(recorder);
    }

    /** 首份快照 = 组启动至今：基线从 0 diff，增量与累计相等，落桶增量对账 */
    @Test
    public void firstSnapshotCoversSinceCreation() {
        stats.recordSubmit();
        stats.recordSubmit();
        stats.recordCallback(3);
        stats.recordCallback(30);
        collector.collectOnce();

        assertEquals(1, received.size());
        StatsSnapshot snapshot = received.get(0);
        assertEquals("首份快照增量应与累计一致（基线为 0）", 2L, snapshot.getSubmitCount());
        assertEquals(2L, snapshot.getSubmitTotal());
        assertEquals(2L, snapshot.getCallbackCount());
        assertEquals(snapshot.getCallbackTotal(), snapshot.getCallbackCount());
        assertEquals("rt=3 应落 [2,5) 桶", 1L, snapshot.getBucketCounts()[2]);
        assertEquals("rt=30 超出末边界应落溢出桶", 1L, snapshot.getBucketCounts()[4]);
    }

    /** 基线差分：上次快照即基线（无独立基线变量），增量 = 本次累计 − 上次快照累计 */
    @Test
    public void baselineDiffUsesLastSnapshot() {
        stats.recordSubmit();
        stats.recordSubmit();
        collector.collectOnce();

        stats.recordSubmit();
        stats.recordCallback(7);
        collector.collectOnce();

        assertEquals(2, received.size());
        StatsSnapshot second = received.get(1);
        assertEquals("submit 增量应只含本轮 1 条", 1L, second.getSubmitCount());
        assertEquals("submit 累计应为 3 条", 3L, second.getSubmitTotal());
        assertEquals(1L, second.getCallbackCount());
        assertEquals("累计器从不复位（差分非清零）", 3L, second.getSubmitTotal());
    }

    /** endpoint 查询只读不推进：live 差分不替换基线，下轮 tick 的区间起点仍是上次 tick 终点 */
    @Test
    public void liveSnapshotDoesNotAdvanceBaseline() throws Exception {
        stats.recordSubmit();
        collector.collectOnce();
        StatsSnapshot tick1 = received.get(0);

        Thread.sleep(10);
        StatsSnapshot live = collector.liveSnapshot();
        assertTrue("live 查询应看到当前累计", live.getSubmitTotal() >= tick1.getSubmitTotal());

        Thread.sleep(10);
        stats.recordSubmit();
        collector.collectOnce();
        StatsSnapshot tick2 = received.get(1);
        assertEquals("查询无副作用：tick2 区间起点应仍是 tick1 终点（live 未吃掉区间）",
                tick1.getIntervalEndMillis(), tick2.getIntervalStartMillis());
    }

    /** 卡顿免疫：间隔取实际值（分母摊平），qps = 增量 ÷ 实际间隔，数据只有迟到没有丢失 */
    @Test
    public void stretchedIntervalKeepsQpsCorrect() throws Exception {
        stats.recordSubmit();
        collector.collectOnce();

        Thread.sleep(150);   // 模拟调度延迟 / GC 卡顿
        stats.recordSubmit();
        collector.collectOnce();

        StatsSnapshot tick2 = received.get(1);
        long interval = tick2.getIntervalEndMillis() - tick2.getIntervalStartMillis();
        assertTrue("间隔应取实际值摊进分母（>=150ms）", interval >= 150);
        assertEquals("qps 应按实际间隔折算", 1.0 / (interval / 1000.0), tick2.getSubmitQps(), 1e-6);
    }

    /** 钩子异常隔离（第一层）：单个钩子抛异常被出口吞掉，其余钩子照常收到、采集照跑 */
    @Test
    public void listenerExceptionIsolated() {
        StatsListener bomb = snapshot -> {
            throw new IllegalStateException("boom");
        };
        StatsListeners.register(bomb);
        try {
            stats.recordCallback(1);
            collector.collectOnce();
            assertEquals("炸弹钩子不得影响后续钩子收到快照", 1, received.size());

            collector.collectOnce();
            assertEquals("下轮 tick 应照常产出", 2, received.size());
        } finally {
            StatsListeners.unregister(bomb);
        }
    }

    /** 真调度冒烟：钩子持续抛 Error，快照仍持续产出 */
    @Test(timeout = 15_000)
    public void scheduledTaskSurvivesListenerThrowing() throws Exception {
        StatsListener bomb = snapshot -> {
            throw new Error("boom");
        };
        StatsListeners.register(bomb);
        try {
            StatsCollector scheduled = new StatsCollector("collector-sched",
                    new CumulativeStats(BOUNDARIES),
                    StatsConfigPOJO.builder().collectIntervalMillis(100).build());
            scheduled.start();
            Thread.sleep(700);
            scheduled.stop();
            assertTrue("约 7 个周期应产出多份快照（任务未被静默取消）", received.size() >= 3);
        } finally {
            StatsListeners.unregister(bomb);
        }
    }

    /** stop 幂等；取消调度后 latest 仍可做 live 差分基线（关闭后累计读数可查） */
    @Test
    public void stoppedCollectorStillServesLiveSnapshot() {
        stats.recordSubmit();
        collector.collectOnce();
        assertEquals(1, received.size());

        collector.stop();
        collector.stop();   // 幂等

        stats.recordSubmit();
        StatsSnapshot live = collector.liveSnapshot();
        assertEquals("关闭后累计读数仍可查", 2L, live.getSubmitTotal());
    }

    /** tp 算术经采集配置贯通：percentiles 从 StatsConfigPOJO 一路传入快照 */
    @Test
    public void tpProducedFromCollectorConfig() {
        StatsCollector configured = new StatsCollector("collector-tp", stats,
                StatsConfigPOJO.builder().percentiles(0.5).build());
        try {
            stats.recordCallback(3);
            configured.collectOnce();

            assertEquals(1, received.size());
            assertEquals("rt=3 单条落在 [2,5) 桶，tp50 = 2 + 3×(1/1) = 5.0",
                    5.0, received.get(0).getTp().get("tp50"), 1e-9);
        } finally {
            configured.stop();
        }
    }
}
