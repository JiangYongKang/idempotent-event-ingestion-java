package com.github.highcumontoa.idempotenteventingestionjava.tests;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.github.highcumontoa.idempotenteventingestionjava.service.RetryBackoffPolicy;
import java.time.Duration;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class RetryBackoffPolicyTest {

    private static final Logger log = LoggerFactory.getLogger(RetryBackoffPolicyTest.class);
    /** 测试内统一使用该幂等键并在日志中打印重试次数与判定依据。 */
    private static final String KEY = "key-backoff-policy";

    private final RetryBackoffPolicy policy =
            new RetryBackoffPolicy(3, Duration.ofMillis(100), Duration.ofSeconds(5), 2.0);

    @Test
    void exponentialBackoffIsCapped() {
        assertEquals(Duration.ofMillis(100), policy.delayFor(1));
        assertEquals(Duration.ofMillis(200), policy.delayFor(2));
        assertEquals(Duration.ofMillis(400), policy.delayFor(3));
        log.info("幂等键={} 重试次数=1/2/3 判定依据=指数退避100/200/400ms", KEY);

        RetryBackoffPolicy capped =
                new RetryBackoffPolicy(10, Duration.ofSeconds(4), Duration.ofSeconds(5), 2.0);
        assertEquals(Duration.ofSeconds(5), capped.delayFor(3));
        log.info("幂等键={} 重试次数=3 判定依据=退避达到上限5000ms", KEY);
    }

    @Test
    void retryGateRespectsRetryableAndMaxAttempts() {
        assertTrue(policy.canRetry(true, 1));
        assertTrue(policy.canRetry(true, 2));
        assertFalse(policy.canRetry(true, 3));
        assertFalse(policy.canRetry(false, 1));
        log.info("幂等键={} 重试次数=1,2,3 判定依据=可重试且未达上限放行;达到上限3或不可重试则终止", KEY);
    }
}
