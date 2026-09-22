package com.github.highcumontoa.idempotenteventingestionjava.model;

/**
 * 接入结论：首次受理 vs 幂等重复。
 */
public enum IngestionOutcome {
    ACCEPTED,
    DUPLICATE
}
