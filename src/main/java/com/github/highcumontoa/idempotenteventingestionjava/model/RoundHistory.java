package com.github.highcumontoa.idempotenteventingestionjava.model;

import java.io.Serial;
import java.io.Serializable;
import java.time.Instant;
import java.util.Objects;

/**
 * 一轮处理（首轮或某一轮重跑）跑完后的不可变历史条目。
 *
 * <p>条目只在该轮“到达结局”（成功 SUCCEEDED 或终止 TERMINATED）时追加一次，
 * 此后永不变更、永不被后续轮次覆盖，因此多轮重跑后每一轮的终止原因、
 * 该轮处理次数与该轮失败信息仍能逐轮看清、互不串轮。</p>
 *
 * <p>该对象随 {@link EventRecord} 一起持久化到本地文件，实现 {@link Serializable}。</p>
 */
public final class RoundHistory implements Serializable {

    @Serial
    private static final long serialVersionUID = 1L;

    /** 轮次序号：0 表示首轮，1 表示第 1 轮重跑，以此类推。 */
    private final int round;

    /** 本轮实际执行的处理尝试次数（本轮计数，不含其他轮次）。 */
    private final int attempts;

    /** 本轮结局状态：SUCCEEDED 或 TERMINATED。 */
    private final EventStatus outcome;

    /** 本轮终止原因；成功结束时为 null。 */
    private final TerminationReason terminationReason;

    /** 本轮最后一次失败的错误信息；成功结束时为 null。 */
    private final String lastError;

    /** 本轮最后一次失败是否可重试；成功结束时为 false。 */
    private final boolean lastErrorRetryable;

    /** 本轮开始（首轮接入或重跑重新入队）的时刻。 */
    private final Instant startedAt;

    /** 本轮到达结局的时刻。 */
    private final Instant finishedAt;

    public RoundHistory(int round, int attempts, EventStatus outcome, TerminationReason terminationReason,
                        String lastError, boolean lastErrorRetryable, Instant startedAt, Instant finishedAt) {
        this.round = round;
        this.attempts = attempts;
        this.outcome = Objects.requireNonNull(outcome, "outcome");
        this.terminationReason = terminationReason;
        this.lastError = lastError;
        this.lastErrorRetryable = lastErrorRetryable;
        this.startedAt = startedAt;
        this.finishedAt = Objects.requireNonNull(finishedAt, "finishedAt");
    }

    public int getRound() {
        return round;
    }

    public int getAttempts() {
        return attempts;
    }

    public EventStatus getOutcome() {
        return outcome;
    }

    public TerminationReason getTerminationReason() {
        return terminationReason;
    }

    public String getLastError() {
        return lastError;
    }

    public boolean isLastErrorRetryable() {
        return lastErrorRetryable;
    }

    public Instant getStartedAt() {
        return startedAt;
    }

    public Instant getFinishedAt() {
        return finishedAt;
    }
}
