package com.github.highcumontoa.idempotenteventingestionjava.model;

import java.io.IOException;
import java.io.ObjectInputStream;
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

    /** 已执行（含正在执行）的处理尝试总次数：跨重跑轮次累计，永不重置。 */
    private int attempts;

    /** 当前轮次（首轮或某轮重跑）内已执行的尝试次数；每次发起重跑时重置为 0。 */
    private int roundAttempts;

    /** 已发起的重跑轮数：0 表示仍为首轮处理。 */
    private int rerunCount;

    /** 当前轮次开始时刻（首轮为接入时间，每发起一次重跑刷新为重跑入队时间）。 */
    private Instant roundStartedAt;

    /**
     * 已跑完各轮的历史条目（按轮次升序，round=0 为首轮）。条目只追加、不改写、不删除，
     * 因此多轮重跑后每一轮的终止原因、处理次数、失败信息都独立可查，互不覆盖、互不串轮。
     */
    private List<RoundHistory> roundHistory = new ArrayList<>();

    /** 最近一次进入终止态的原因；重跑会清空 {@link #terminationReason}，但历史终止原因在此保留。 */
    private TerminationReason lastTerminationReason;

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
        this.roundStartedAt = now;
        this.status = EventStatus.QUEUED;
    }

    /**
     * 兼容旧版本数据文件：旧记录没有逐轮历史 / 本轮开始时间字段，反序列化后补默认值，
     * 保证升级后重启不报错，后续新跑完的轮次照常追加历史。
     */
    @Serial
    private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
        in.defaultReadObject();
        if (roundHistory == null) {
            roundHistory = new ArrayList<>();
        }
        if (roundStartedAt == null) {
            roundStartedAt = createdAt;
        }
    }

    /**
     * 追加一条“本轮结局”历史。只在某轮成功或终止时于存储监视器内调用一次；
     * 已追加的条目永不再被修改。列表自身锁保证与并发读取快照互不干扰。
     */
    public void appendRoundHistory(RoundHistory entry) {
        synchronized (roundHistory) {
            roundHistory.add(Objects.requireNonNull(entry, "entry"));
        }
    }

    /** 逐轮历史的快照副本（按轮次升序），调用方可在锁外安全遍历。 */
    public List<RoundHistory> getRoundHistory() {
        synchronized (roundHistory) {
            return new ArrayList<>(roundHistory);
        }
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

    public Instant getRoundStartedAt() {
        return roundStartedAt;
    }

    public void setRoundStartedAt(Instant roundStartedAt) {
        this.roundStartedAt = roundStartedAt;
    }

    public TerminationReason getLastTerminationReason() {
        return lastTerminationReason;
    }

    public void setLastTerminationReason(TerminationReason lastTerminationReason) {
        this.lastTerminationReason = lastTerminationReason;
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
