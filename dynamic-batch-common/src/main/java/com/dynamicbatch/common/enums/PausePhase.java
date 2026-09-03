package com.dynamicbatch.common.enums;

/**
 * 热更新暂停状态机的相位：协调方置 *_PENDING，工作线程以 CAS 切换到终态（实现见 core 的 Dispatcher / Worker）。
 */
public enum PausePhase {
    /** 已运行 */
    RUNNING,
    /** 待暂停 */
    PAUSE_PENDING,
    /** 已暂停 */
    PAUSED,
    /** 待运行 */
    RUN_PENDING
}
