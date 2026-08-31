package com.dynamicbatch.common.enums;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/**
 * PausePhase 枚举防护测试：枚举本身零逻辑，仅防手滑改名/删值。
 * 状态机语义见类 javadoc 与 refactor-design.md §5.1，行为测试在阶段 2 的 core 侧。
 */
public class PausePhaseTest {

    @Test
    public void values() {
        PausePhase[] phases = PausePhase.values();
        assertEquals("相位恰好 4 个", 4, phases.length);
        // 按名可取，命名与设计文档 §5.1 一致
        assertNotNull(PausePhase.valueOf("RUNNING"));
        assertNotNull(PausePhase.valueOf("PAUSE_PENDING"));
        assertNotNull(PausePhase.valueOf("PAUSED"));
        assertNotNull(PausePhase.valueOf("RUN_PENDING"));
    }
}
