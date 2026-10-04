package com.github.highcumontoa.idempotenteventingestionjava.model;

import java.util.Objects;

/**
 * 一次受控重跑请求的结果：
 * <ul>
 *   <li>{@link RerunOutcome#ACCEPTED} —— 事件已重新入队，{@link #record()} 为最新记录；</li>
 *   <li>{@link RerunOutcome#REJECTED} —— 被拒绝，{@link #rejectionReason()} 必填且可区分
 *       （状态不允许 / 轮数到顶 / 容量不足 / 键不存在）。</li>
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
        return new RerunResult(RerunOutcome.ACCEPTED, null, record,
                "重跑已接受，事件重新进入待处理队列（第 " + record.getRerunCount() + " 轮重跑）");
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
