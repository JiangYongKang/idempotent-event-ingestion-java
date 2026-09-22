package com.github.highcumontoa.idempotenteventingestionjava.handler;

import java.util.Objects;

/**
 * 业务处理器主动抛出的受控异常，通过 {@code retryable} 明确区分
 * “可重试失败”与“不可重试失败”，避免失败被静默吞掉或盲目重试。
 */
@SuppressWarnings("serial")
public class EventProcessingException extends Exception {

    private final boolean retryable;

    public EventProcessingException(String message, boolean retryable) {
        super(Objects.requireNonNull(message, "message"));
        this.retryable = retryable;
    }

    public EventProcessingException(String message, boolean retryable, Throwable cause) {
        super(Objects.requireNonNull(message, "message"), cause);
        this.retryable = retryable;
    }

    public boolean isRetryable() {
        return retryable;
    }
}
