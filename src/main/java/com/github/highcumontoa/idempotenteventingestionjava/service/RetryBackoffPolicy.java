package com.github.highcumontoa.idempotenteventingestionjava.service;

import java.time.Duration;
import java.time.Instant;

/**
 * 可控的指数退避策略：{@code delay = min(baseDelay * multiplier^(attempts-1), maxDelay)}。
 *
 * <p>重试上限由 {@code maxAttempts} 控制：当失败可重试且已尝试次数尚未达到上限时才允许重试；
 * 达到上限（或失败本身不可重试）后必须进入终止态，不得静默丢失或无限重试。</p>
 */
public final class RetryBackoffPolicy {

    private final int maxAttempts;
    private final Duration baseDelay;
    private final Duration maxDelay;
    private final double multiplier;

    public RetryBackoffPolicy(int maxAttempts, Duration baseDelay, Duration maxDelay, double multiplier) {
        if (maxAttempts < 1) {
            throw new IllegalArgumentException("maxAttempts 必须 >= 1");
        }
        if (baseDelay == null || baseDelay.isNegative() || maxDelay == null || maxDelay.compareTo(baseDelay) < 0) {
            throw new IllegalArgumentException("退避时间配置非法");
        }
        if (multiplier < 1.0) {
            throw new IllegalArgumentException("multiplier 必须 >= 1");
        }
        this.maxAttempts = maxAttempts;
        this.baseDelay = baseDelay;
        this.maxDelay = maxDelay;
        this.multiplier = multiplier;
    }

    /** 是否还允许重试。 */
    public boolean canRetry(boolean failureRetryable, int attemptsSoFar) {
        return failureRetryable && attemptsSoFar < maxAttempts;
    }

    /** 计算第 {@code attemptsSoFar} 次尝试失败后，下一次重试的等待时长。 */
    public Duration delayFor(int attemptsSoFar) {
        long base = baseDelay.toMillis();
        long delay = base;
        for (int i = 1; i < attemptsSoFar; i++) {
            if (delay >= maxDelay.toMillis()) {
                break;
            }
            delay = (long) Math.min(maxDelay.toMillis(), Math.ceil(delay * multiplier));
        }
        return Duration.ofMillis(Math.min(delay, maxDelay.toMillis()));
    }

    /** 下次允许重试的绝对时间。 */
    public Instant nextAttemptAt(Instant now, int attemptsSoFar) {
        return now.plus(delayFor(attemptsSoFar));
    }

    public int getMaxAttempts() {
        return maxAttempts;
    }

    public Duration getBaseDelay() {
        return baseDelay;
    }

    public Duration getMaxDelay() {
        return maxDelay;
    }
}
