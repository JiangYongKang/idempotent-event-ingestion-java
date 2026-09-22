package com.github.highcumontoa.idempotenteventingestionjava.model;

/** 进入终止态的原因，各类失败必须可区分。 */
public enum TerminationReason {
    NON_RETRYABLE,
    RETRY_EXHAUSTED
}
