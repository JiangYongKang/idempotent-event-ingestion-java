package com.github.highcumontoa.idempotenteventingestionjava.model;

import java.util.Objects;

/**
 * 一次接入请求的结果：
 * <ul>
 *   <li>{@link IngestionOutcome#ACCEPTED} —— 首次接受；</li>
 *   <li>{@link IngestionOutcome#DUPLICATE} —— 幂等键重复，返回先前的记录，绝不二次处理；</li>
 *   <li>{@link IngestionOutcome#REJECTED} —— 非法事件或超出容量，{@link #rejectionReason()} 必填。</li>
 * </ul>
 */
public final class IngestionResult {

    private final IngestionOutcome outcome;
    private final RejectionReason rejectionReason;
    private final EventRecord record;
    private final String message;

    private IngestionResult(IngestionOutcome outcome, RejectionReason rejectionReason,
                            EventRecord record, String message) {
        this.outcome = Objects.requireNonNull(outcome, "outcome");
        this.rejectionReason = rejectionReason;
        this.record = record;
        this.message = message;
    }

    public static IngestionResult accepted(EventRecord record) {
        return new IngestionResult(IngestionOutcome.ACCEPTED, null, record, "事件已接受");
    }

    public static IngestionResult duplicate(EventRecord existing) {
        return new IngestionResult(IngestionOutcome.DUPLICATE, null, existing,
                "幂等键重复，返回已有处理状态，未重复处理");
    }

    public static IngestionResult rejected(RejectionReason reason, String message) {
        return new IngestionResult(IngestionOutcome.REJECTED,
                Objects.requireNonNull(reason, "reason"), null, message);
    }

    public IngestionOutcome outcome() {
        return outcome;
    }

    public RejectionReason rejectionReason() {
        return rejectionReason;
    }

    public EventRecord record() {
        return record;
    }

    public String message() {
        return message;
    }
}
