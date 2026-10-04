package com.github.highcumontoa.idempotenteventingestionjava.model;

import java.io.Serial;
import java.io.Serializable;
import java.time.Instant;
import java.util.ArrayList;
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

    /** 已执行（含正在执行）的处理尝试次数：跨全部轮次累计，重跑后不清零。 */
    private int attempts;

    /** 当前轮次内已执行的尝试次数：重试上限/退避按此计数，重跑时清零重新计。 */
    private int attemptsInRound;

    /** 已发起的重跑轮数：0 表示仍在最初一轮，N 表示当前处于第 N 次重跑。 */
    private int rerunCount;

    /**
     * 已结束轮次的历史快照（重跑发起时归档上一轮终局）。
     * 反序列化旧版本数据文件时该字段可能为 null，读取必须走 {@link #getRerunHistory()}。
     */
    private List<RerunRecord> rerunHistory;

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

    /**
     * 发起一轮重跑（仅限 TERMINATED 态调用，调用方必须在存储监视器内完成状态判断）：
     * 把当前轮次的终局（终止原因/本轮尝试次数/最后错误）归档进 {@link #rerunHistory}，
     * 然后清零本轮计数、清除终止痕迹，重新进入 QUEUED 等待完整再跑一轮。
     * 累计尝试次数 {@link #attempts} 与历史归档不清除，保证重跑痕迹可解释。
     */
    public void beginRerun(Instant now) {
        if (this.status != EventStatus.TERMINATED) {
            throw new IllegalStateException("仅终止态事件可发起重跑: " + idempotencyKey
                    + " 当前状态=" + this.status);
        }
        if (this.rerunHistory == null) {
            this.rerunHistory = new ArrayList<>();
        }
        this.rerunHistory.add(new RerunRecord(this.rerunCount, this.terminationReason,
                this.attemptsInRound, this.lastError, this.updatedAt));
        this.rerunCount++;
        this.attemptsInRound = 0;
        this.status = EventStatus.QUEUED;
        this.terminationReason = null;
        this.lastError = null;
        this.lastErrorRetryable = false;
        this.result = null;
        this.lastAttemptAt = null;
        this.nextAttemptAt = now;
        this.updatedAt = now;
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

    public int getAttemptsInRound() {
        return attemptsInRound;
    }

    public void setAttemptsInRound(int attemptsInRound) {
        this.attemptsInRound = attemptsInRound;
    }

    public int getRerunCount() {
        return rerunCount;
    }

    /** 已结束轮次的历史归档（按轮次升序），永不返回 null。 */
    public List<RerunRecord> getRerunHistory() {
        return rerunHistory == null ? List.of() : List.copyOf(rerunHistory);
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
