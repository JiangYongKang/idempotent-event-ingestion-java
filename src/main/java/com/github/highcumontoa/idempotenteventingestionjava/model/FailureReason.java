package com.github.highcumontoa.idempotenteventingestionjava.model;

/**
 * 终止/失败原因分类，保证各类失败可区分、不静默丢失。
 */
public enum FailureReason {
    NONE,
    RETRYABLE_ERROR,
    NON_RETRYABLE_ERROR,
    RETRIES_EXHAUSTED,
    RECOVERED_AFTER_RESTART
}
