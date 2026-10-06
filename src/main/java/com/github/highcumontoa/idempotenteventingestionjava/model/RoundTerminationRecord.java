package com.github.highcumontoa.idempotenteventingestionjava.model;

import java.io.Serial;
import java.io.Serializable;
import java.time.Instant;
import java.util.Objects;

/**
 * 单轮处理（首轮或某一轮重跑）终止时留下的不可变历史记录。
 *
 * <p>每当一轮处理进入 {@link EventStatus#TERMINATED} 时追加一条，追加后永不修改、永不删除，
 * 因此一条事件被反复重跑多少轮，就能按 {@link #getRound()} 逐轮看清：
 * 该轮为什么终止、该轮处理了几次、该轮最后的失败信息是什么。各轮记录彼此独立，
 * 不覆盖、不串轮；该列表随 {@link EventRecord} 一起持久化，服务重启后完整保留。</p>
 */
public final class RoundTerminationRecord implements Serializable {

    @Serial
    private static final long serialVersionUID = 1L;

    /** 轮次编号：0 = 首轮处理，1 = 第 1 轮重跑，以此类推；与终止发生时的 {@code rerunCount} 一致。 */
    private final int round;

    /** 该轮进入终止态的原因。 */
    private final TerminationReason terminationReason;

    /** 该轮内实际执行的处理次数（仅本轮，不含其他轮次）。 */
    private final int roundAttempts;

    /** 该轮终止时的累计总处理次数（含之前所有轮次），用于与全局累计口径对账。 */
    private final int totalAttemptsAtTermination;

    /** 该轮最后一次失败的错误信息。 */
    private final String lastError;

    /** 该轮最后一次失败是否可重试。 */
    private final boolean lastErrorRetryable;

    /** 该轮进入终止态的时刻。 */
    private final Instant terminatedAt;

    public RoundTerminationRecord(int round, TerminationReason terminationReason, int roundAttempts,
                                  int totalAttemptsAtTermination, String lastError,
                                  boolean lastErrorRetryable, Instant terminatedAt) {
        this.round = round;
        this.terminationReason = Objects.requireNonNull(terminationReason, "terminationReason");
        this.roundAttempts = roundAttempts;
        this.totalAttemptsAtTermination = totalAttemptsAtTermination;
        this.lastError = lastError;
        this.lastErrorRetryable = lastErrorRetryable;
        this.terminatedAt = Objects.requireNonNull(terminatedAt, "terminatedAt");
    }

    public int getRound() {
        return round;
    }

    public TerminationReason getTerminationReason() {
        return terminationReason;
    }

    public int getRoundAttempts() {
        return roundAttempts;
    }

    public int getTotalAttemptsAtTermination() {
        return totalAttemptsAtTermination;
    }

    public String getLastError() {
        return lastError;
    }

    public boolean isLastErrorRetryable() {
        return lastErrorRetryable;
    }

    public Instant getTerminatedAt() {
        return terminatedAt;
    }

    @Override
    public String toString() {
        return "RoundTerminationRecord{round=" + round + ", reason=" + terminationReason
                + ", roundAttempts=" + roundAttempts + ", totalAttempts=" + totalAttemptsAtTermination
                + ", error=" + lastError + ", terminatedAt=" + terminatedAt + '}';
    }
}
