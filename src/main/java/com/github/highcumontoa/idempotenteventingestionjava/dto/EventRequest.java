package com.github.highcumontoa.idempotenteventingestionjava.dto;

/** 事件接入请求体。 */
public class EventRequest {

    private String idempotencyKey;
    private String payload;

    public String getIdempotencyKey() {
        return idempotencyKey;
    }

    public void setIdempotencyKey(String idempotencyKey) {
        this.idempotencyKey = idempotencyKey;
    }

    public String getPayload() {
        return payload;
    }

    public void setPayload(String payload) {
        this.payload = payload;
    }
}
