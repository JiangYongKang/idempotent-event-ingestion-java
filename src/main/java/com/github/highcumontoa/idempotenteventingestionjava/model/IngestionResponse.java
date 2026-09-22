package com.github.highcumontoa.idempotenteventingestionjava.model;

/**
 * 接入响应：区分首次受理与重复投递。
 */
public class IngestionResponse {
    private IngestionOutcome outcome;
    private String idempotencyKey;
    private EventStatus status;
    private String message;
    private int attemptCount;

    public IngestionResponse() {
    }

    public IngestionResponse(IngestionOutcome outcome, String idempotencyKey, EventStatus status,
                             String message, int attemptCount) {
        this.outcome = outcome;
        this.idempotencyKey = idempotencyKey;
        this.status = status;
        this.message = message;
        this.attemptCount = attemptCount;
    }

    public IngestionOutcome getOutcome() { return outcome; }
    public void setOutcome(IngestionOutcome outcome) { this.outcome = outcome; }
    public String getIdempotencyKey() { return idempotencyKey; }
    public void setIdempotencyKey(String idempotencyKey) { this.idempotencyKey = idempotencyKey; }
    public EventStatus getStatus() { return status; }
    public void setStatus(EventStatus status) { this.status = status; }
    public String getMessage() { return message; }
    public void setMessage(String message) { this.message = message; }
    public int getAttemptCount() { return attemptCount; }
    public void setAttemptCount(int attemptCount) { this.attemptCount = attemptCount; }
}
