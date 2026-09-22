package com.github.highcumontoa.idempotenteventingestionjava.model;

/**
 * 业务处理器对单次尝试的结论。
 */
public class HandlerResult {
    private final boolean success;
    private final boolean retryable;
    private final String result;
    private final String errorMessage;

    private HandlerResult(boolean success, boolean retryable, String result, String errorMessage) {
        this.success = success;
        this.retryable = retryable;
        this.result = result;
        this.errorMessage = errorMessage;
    }

    public static HandlerResult success(String result) {
        return new HandlerResult(true, false, result, null);
    }

    public static HandlerResult failure(boolean retryable, String errorMessage) {
        return new HandlerResult(false, retryable, null, errorMessage);
    }

    public boolean isSuccess() { return success; }
    public boolean isRetryable() { return retryable; }
    public String getResult() { return result; }
    public String getErrorMessage() { return errorMessage; }
}
