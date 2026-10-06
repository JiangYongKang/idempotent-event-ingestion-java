package com.github.highcumontoa.idempotenteventingestionjava.model;

import java.io.Serial;
import java.io.Serializable;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * 一个幂等键对应的完整处理状态。
 *
 * <p>该对象会被持久化到本地文件，因此实现 {@link Serializable}。所有字段的读写都必须在
 * {@code EventStore} 的监视器保护下进行（单进程内串行化状态迁移），因此字段本身不再额外加锁。</p>
 */
public class EventRecord implements Serializable {

    @Serial
    private static final long serialVersionUID = 1L;

    /** 幂等键：同一键的重复投递只允许产生一次有效处理。 */
    private final String idempotencyKey;

    /** 原始事件内容（演示用文本载荷）。 */
    private final String payload;

    private final Instant createdAt;
    private Instant updatedAt;

    private EventStatus status;

    /** 已执行（含正在执行）的处理尝试总次数：跨重跑轮次累计，永不重置。 */
    private int attempts;

    /** 当前轮次（首轮或某轮重跑）内已执行的尝试次数；每次发起重跑时重置为 0。 */
    private int roundAttempts;

    /** 已发起的重跑轮数：0 表示仍为首轮处理。 */
    private int rerunCount;

    /** 最近一次进入终止态的原因；重跑会清空 {@link #terminationReason}，但历史终止原因在此保留。 */
    private TerminationReason lastTerminationReason;

    /**
     * 逐轮终止历史：每轮（首轮或某轮重跑）进入终止态时追加一条，只增不改不删。
     * 反序列化旧版本数据时该字段为 null，读取时惰性初始化。
     */
    private List<RoundTerminationRecord> roundHistory;

    /** 最近一次进入终止态的时刻；未终止过为 null。 */
    private Instant lastTerminatedAt;

    private Instant lastAttemptAt;

    /** 下次允许重试的时间；非重试态为 null。 */
    private Instant nextAttemptAt;

    /** 最近一次失败的错误信息，用于可解释性。 */
    private String lastError;

    /** 最近一次失败是否可重试。 */
    private boolean lastErrorRetryable;

    /** 进入终止态的原因；未终止为 null。 */
    private TerminationReason terminationReason;

    /** 成功后处理器返回的结果凭证。 */
    private String result;

    public EventRecord(String idempotencyKey, String payload, Instant now) {
        this.idempotencyKey = Objects.requireNonNull(idempotencyKey, "idempotencyKey");
        this.payload = payload;
        this.createdAt = now;
        this.updatedAt = now;
        this.status = EventStatus.QUEUED;
    }

    /** 服务重启后调用：把崩溃前停留在 PROCESSING 的事件安全回退为 QUEUED，等待重新处理。 */
    public void resetProcessingAfterRestart(Instant now) {
        if (this.status == EventStatus.PROCESSING) {
            this.status = EventStatus.QUEUED;
            this.nextAttemptAt = now;
            this.updatedAt = now;
        }
    }

    public String getIdempotencyKey() {
        return idempotencyKey;
    }

    public String getPayload() {
        return payload;
    }

    public Instant getCreatedAt() {
        return createdAt;
    }

    public Instant getUpdatedAt() {
        return updatedAt;
    }

    public void setUpdatedAt(Instant updatedAt) {
        this.updatedAt = updatedAt;
    }

    public EventStatus getStatus() {
        return status;
    }

    public void setStatus(EventStatus status) {
        this.status = status;
    }

    public int getAttempts() {
        return attempts;
    }

    public void setAttempts(int attempts) {
        this.attempts = attempts;
    }

    public int getRoundAttempts() {
        return roundAttempts;
    }

    public void setRoundAttempts(int roundAttempts) {
        this.roundAttempts = roundAttempts;
    }

    public int getRerunCount() {
        return rerunCount;
    }

    public void setRerunCount(int rerunCount) {
        this.rerunCount = rerunCount;
    }

    public TerminationReason getLastTerminationReason() {
        return lastTerminationReason;
    }

    public void setLastTerminationReason(TerminationReason lastTerminationReason) {
        this.lastTerminationReason = lastTerminationReason;
    }

    /**
     * 把当前轮的终止信息归档为一条逐轮历史。必须在状态迁移为 TERMINATED 的同一临界区内调用，
     * 归档内容（轮次、本轮次数、失败信息）全部取自当前记录，归档后本轮计数才可被重跑重置。
     */
    public void archiveCurrentRoundTermination(Instant terminatedAt) {
        roundHistory().add(new RoundTerminationRecord(
                rerunCount, terminationReason, roundAttempts, attempts,
                lastError, lastErrorRetryable, terminatedAt));
    }

    /** 逐轮终止历史（只读视图，按轮次升序）。 */
    public List<RoundTerminationRecord> getRoundHistory() {
        return Collections.unmodifiableList(roundHistory());
    }

    private List<RoundTerminationRecord> roundHistory() {
        if (roundHistory == null) {
            roundHistory = new ArrayList<>();
        }
        return roundHistory;
    }

    public Instant getLastTerminatedAt() {
        return lastTerminatedAt;
    }

    public void setLastTerminatedAt(Instant lastTerminatedAt) {
        this.lastTerminatedAt = lastTerminatedAt;
    }

    public Instant getLastAttemptAt() {
        return lastAttemptAt;
    }

    public void setLastAttemptAt(Instant lastAttemptAt) {
        this.lastAttemptAt = lastAttemptAt;
    }

    public Instant getNextAttemptAt() {
        return nextAttemptAt;
    }

    public void setNextAttemptAt(Instant nextAttemptAt) {
        this.nextAttemptAt = nextAttemptAt;
    }

    public String getLastError() {
        return lastError;
    }

    public void setLastError(String lastError) {
        this.lastError = lastError;
    }

    public boolean isLastErrorRetryable() {
        return lastErrorRetryable;
    }

    public void setLastErrorRetryable(boolean lastErrorRetryable) {
        this.lastErrorRetryable = lastErrorRetryable;
    }

    public TerminationReason getTerminationReason() {
        return terminationReason;
    }

    public void setTerminationReason(TerminationReason terminationReason) {
        this.terminationReason = terminationReason;
    }

    public String getResult() {
        return result;
    }

    public void setResult(String result) {
        this.result = result;
    }
}
