package com.github.highcumontoa.idempotenteventingestionjava.model;

/**
 * 事件的持久化记录（状态机唯一事实来源）。
 */
public class EventRecord {
    private String idempotencyKey;
    private String type;
    private String payload;
    private EventStatus status;
    private int attemptCount;
    private FailureReason failureReason;
    private String detail;
    private String result;
    private long createdAt;
    private long updatedAt;
    private long nextAttemptAt;
    private int version;

    public EventRecord() {
    }

    public String getIdempotencyKey() { return idempotencyKey; }
    public void setIdempotencyKey(String idempotencyKey) { this.idempotencyKey = idempotencyKey; }
    public String getType() { return type; }
    public void setType(String type) { this.type = type; }
    public String getPayload() { return payload; }
    public void setPayload(String payload) { this.payload = payload; }
    public EventStatus getStatus() { return status; }
    public void setStatus(EventStatus status) { this.status = status; }
    public int getAttemptCount() { return attemptCount; }
    public void setAttemptCount(int attemptCount) { this.attemptCount = attemptCount; }
    public FailureReason getFailureReason() { return failureReason; }
    public void setFailureReason(FailureReason failureReason) { this.failureReason = failureReason; }
    public String getDetail() { return detail; }
    public void setDetail(String detail) { this.detail = detail; }
    public String getResult() { return result; }
    public void setResult(String result) { this.result = result; }
    public long getCreatedAt() { return createdAt; }
    public void setCreatedAt(long createdAt) { this.createdAt = createdAt; }
    public long getUpdatedAt() { return updatedAt; }
    public void setUpdatedAt(long updatedAt) { this.updatedAt = updatedAt; }
    public long getNextAttemptAt() { return nextAttemptAt; }
    public void setNextAttemptAt(long nextAttemptAt) { this.nextAttemptAt = nextAttemptAt; }
    public int getVersion() { return version; }
    public void setVersion(int version) { this.version = version; }
}
