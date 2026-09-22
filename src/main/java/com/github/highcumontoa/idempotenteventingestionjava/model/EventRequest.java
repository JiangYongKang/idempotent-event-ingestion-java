package com.github.highcumontoa.idempotenteventingestionjava.model;

/**
 * 事件接入请求。
 */
public class EventRequest {
    private String idempotencyKey;
    private String type;
    private String payload;

    public String getIdempotencyKey() { return idempotencyKey; }
    public void setIdempotencyKey(String idempotencyKey) { this.idempotencyKey = idempotencyKey; }
    public String getType() { return type; }
    public void setType(String type) { this.type = type; }
    public String getPayload() { return payload; }
    public void setPayload(String payload) { this.payload = payload; }
}
