package com.github.highcumontoa.idempotenteventingestionjava.model;

import java.io.Serial;
import java.io.Serializable;
import java.time.Instant;
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

    /** 已执行（含正在执行）的处理尝试次数。 */
    private int attempts;

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
