package com.github.highcumontoa.idempotenteventingestionjava.model;

/**
 * 统计口径快照：每个事件按当前状态恰好归入一个桶，不重不漏。
 */
public class StatsResponse {
    private long pending;
    private long processing;
    private long succeeded;
    private long terminated;
    private long total;
    private long rejected;
    private long rateLimited;
    private long terminalRetryableError;
    private long terminalNonRetryableError;
    private long terminalRetriesExhausted;
    private long terminalRecoveredAfterRestart;

    public long getPending() { return pending; }
    public void setPending(long pending) { this.pending = pending; }
    public long getProcessing() { return processing; }
    public void setProcessing(long processing) { this.processing = processing; }
    public long getSucceeded() { return succeeded; }
    public void setSucceeded(long succeeded) { this.succeeded = succeeded; }
    public long getTerminated() { return terminated; }
    public void setTerminated(long terminated) { this.terminated = terminated; }
    public long getTotal() { return total; }
    public void setTotal(long total) { this.total = total; }
    public long getRejected() { return rejected; }
    public void setRejected(long rejected) { this.rejected = rejected; }
    public long getRateLimited() { return rateLimited; }
    public void setRateLimited(long rateLimited) { this.rateLimited = rateLimited; }
    public long getTerminalRetryableError() { return terminalRetryableError; }
    public void setTerminalRetryableError(long terminalRetryableError) { this.terminalRetryableError = terminalRetryableError; }
    public long getTerminalNonRetryableError() { return terminalNonRetryableError; }
    public void setTerminalNonRetryableError(long terminalNonRetryableError) { this.terminalNonRetryableError = terminalNonRetryableError; }
    public long getTerminalRetriesExhausted() { return terminalRetriesExhausted; }
    public void setTerminalRetriesExhausted(long terminalRetriesExhausted) { this.terminalRetriesExhausted = terminalRetriesExhausted; }
    public long getTerminalRecoveredAfterRestart() { return terminalRecoveredAfterRestart; }
    public void setTerminalRecoveredAfterRestart(long terminalRecoveredAfterRestart) { this.terminalRecoveredAfterRestart = terminalRecoveredAfterRestart; }
}
