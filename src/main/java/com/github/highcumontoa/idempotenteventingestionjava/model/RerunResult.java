package com.github.highcumontoa.idempotenteventingestionjava.model;

import java.util.Objects;

/**
 * 一次重跑请求的结果：
 * <ul>
 *   <li>{@link RerunOutcome#ACCEPTED} —— 已重新入队，{@link #record()} 为更新后的记录；</li>
 *   <li>{@link RerunOutcome#REJECTED} —— 被拒绝，{@link #rejectionReason()} 必填，
 *     可区分“状态不允许”“轮数到顶”“容量不足”“键不存在”。</li>
 * </ul>
 */
public final class RerunResult {

    private final RerunOutcome outcome;
    private final RerunRejectionReason rejectionReason;
    private final EventRecord record;
    private final String message;

    private RerunResult(RerunOutcome outcome, RerunRejectionReason rejectionReason,
                        EventRecord record, String message) {
        this.outcome = Objects.requireNonNull(outcome, "outcome");
        this.rejectionReason = rejectionReason;
        this.record = record;
        this.message = message;
    }

    public static RerunResult accepted(EventRecord record) {
        return new RerunResult(RerunOutcome.ACCEPTED, null, record, "已重新入队，等待重新处理");
    }

    public static RerunResult rejected(RerunRejectionReason reason, String message) {
        return new RerunResult(RerunOutcome.REJECTED,
                Objects.requireNonNull(reason, "reason"), null, message);
    }

    public RerunOutcome outcome() {
        return outcome;
    }

    public RerunRejectionReason rejectionReason() {
        return rejectionReason;
    }

    public EventRecord record() {
        return record;
    }

    public String message() {
        return message;
    }
}
