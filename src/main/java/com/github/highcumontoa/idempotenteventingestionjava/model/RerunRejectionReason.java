package com.github.highcumontoa.idempotenteventingestionjava.model;

/** 重跑被拒绝的原因，各类拒绝必须可区分。 */
public enum RerunRejectionReason {
    /** 幂等键不存在。 */
    NOT_FOUND,
    /** 事件当前不在终止态（排队中/处理中/已成功），当前不允许重跑。 */
    NOT_TERMINATED,
    /** 重跑轮数已达上限，不允许再重跑。 */
    RERUN_LIMIT_REACHED,
    /** 在途容量不足，重跑会超出队列容量上限。 */
    QUEUE_FULL
}
