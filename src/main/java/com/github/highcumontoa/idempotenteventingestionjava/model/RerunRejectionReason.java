package com.github.highcumontoa.idempotenteventingestionjava.model;

/** 重跑被拒绝的原因，各类拒绝必须可区分。 */
public enum RerunRejectionReason {
    /** 幂等键不存在。 */
    NOT_FOUND,
    /** 当前状态不允许重跑（仅 TERMINATED 可重跑）。 */
    NOT_TERMINATED,
    /** 重跑轮数已达上限。 */
    RERUN_LIMIT_REACHED,
    /** 在途容量不足，重新入队会超容量。 */
    QUEUE_FULL
}
