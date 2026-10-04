package com.github.highcumontoa.idempotenteventingestionjava.model;

/** 重跑请求的判定结果。 */
public enum RerunOutcome {
    /** 已重新入队，等待完整再跑一轮。 */
    ACCEPTED,
    /** 被拒绝，原因见 {@link RerunRejectionReason}。 */
    REJECTED
}
