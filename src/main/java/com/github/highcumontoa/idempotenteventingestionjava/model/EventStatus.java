package com.github.highcumontoa.idempotenteventingestionjava.model;

/**
 * 事件生命周期状态。
 * PENDING（排队）/ PROCESSING（处理中）/ SUCCEEDED（成功）/ TERMINATED（终止，不再重试）。
 */
public enum EventStatus {
    PENDING,
    PROCESSING,
    SUCCEEDED,
    TERMINATED
}
