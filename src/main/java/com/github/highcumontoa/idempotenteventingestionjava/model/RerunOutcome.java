package com.github.highcumontoa.idempotenteventingestionjava.model;

/** 受控重跑请求的判定结果。 */
public enum RerunOutcome {
    /** 重跑被接受：事件已从终止态放回待处理队列。 */
    ACCEPTED,
    /** 重跑被拒绝：{@link RerunRejectionReason} 给出可区分的具体原因。 */
    REJECTED
}
