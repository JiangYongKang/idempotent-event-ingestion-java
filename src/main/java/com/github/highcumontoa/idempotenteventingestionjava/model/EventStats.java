package com.github.highcumontoa.idempotenteventingestionjava.model;

/** 状态统计快照，四类状态互斥且合计等于 total。 */
public record EventStats(long queued, long processing, long succeeded, long terminated, long total) {
}
