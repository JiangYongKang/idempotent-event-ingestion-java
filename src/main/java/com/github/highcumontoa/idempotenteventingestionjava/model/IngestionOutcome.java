package com.github.highcumontoa.idempotenteventingestionjava.model;

/** 接入请求的判定结果，重复投递与拒绝必须可区分。 */
public enum IngestionOutcome {
    ACCEPTED,
    DUPLICATE,
    REJECTED
}
