package com.github.highcumontoa.idempotenteventingestionjava.tests;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.github.highcumontoa.idempotenteventingestionjava.handler.DemoLedgerEventHandler;
import com.github.highcumontoa.idempotenteventingestionjava.model.EventRecord;
import com.github.highcumontoa.idempotenteventingestionjava.model.EventStats;
import com.github.highcumontoa.idempotenteventingestionjava.model.EventStatus;
import com.github.highcumontoa.idempotenteventingestionjava.model.IngestionOutcome;
import com.github.highcumontoa.idempotenteventingestionjava.model.IngestionResult;
import com.github.highcumontoa.idempotenteventingestionjava.model.TerminationReason;
import com.github.highcumontoa.idempotenteventingestionjava.service.EventIngestionService;
import com.github.highcumontoa.idempotenteventingestionjava.service.EventProcessingService;
import com.github.highcumontoa.idempotenteventingestionjava.service.EventQueryService;
import com.github.highcumontoa.idempotenteventingestionjava.service.RetryBackoffPolicy;
import com.github.highcumontoa.idempotenteventingestionjava.store.FileEventStore;
import com.github.highcumontoa.idempotenteventingestionjava.tests.support.MutableClock;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 失败重试与终止语义测试：
 * 可重试失败按指数退避重试、达到上限进入 RETRY_EXHAUSTED；不可重试失败立即 NON_RETRYABLE。
 * 全程在单测日志中打印幂等键、重试次数与判定依据。
 */
class RetryAndTerminationTest {

    private static final Logger log = LoggerFactory.getLogger(RetryAndTerminationTest.class);

    private record Harness(EventIngestionService ingest, EventProcessingService processor,
                           EventQueryService query, DemoLedgerEventHandler handler,
                           MutableClock clock, int maxAttempts) {
    }

    private Harness harness(Path dir) {
        MutableClock clock = MutableClock.startAt(Instant.parse("2026-09-23T00:00:00Z"));
        int maxAttempts = 3;
        FileEventStore store = new FileEventStore(dir.resolve("events.dat"));
        DemoLedgerEventHandler handler = new DemoLedgerEventHandler();
        RetryBackoffPolicy policy =
                new RetryBackoffPolicy(maxAttempts, Duration.ofMillis(100), Duration.ofSeconds(5), 2.0);
        EventProcessingService processor = new EventProcessingService(store, handler, policy, clock);
        EventIngestionService ingest =
                new EventIngestionService(store, processor, defaultProps(maxAttempts), clock);
        return new Harness(ingest, processor, new EventQueryService(store), handler, clock, maxAttempts);
    }

    private com.github.highcumontoa.idempotenteventingestionjava.config.EventingProperties defaultProps(
            int maxAttempts) {
        var p = new com.github.highcumontoa.idempotenteventingestionjava.config.EventingProperties();
        p.setMaxAttempts(maxAttempts);
        return p;
    }

    @Test
    void retryableFailureRetriesWithBackoffAndTerminatesAtLimit(@TempDir Path dir) {
        Harness h = harness(dir);
        String key = "order-retryable-1";

        IngestionResult accepted = h.ingest.ingest(key, "RETRYABLE:下游超时");
        assertEquals(IngestionOutcome.ACCEPTED, accepted.outcome());
        log.info("[测试] 幂等键={} 重试次数=0 判定依据=首次接受,进入QUEUED", key);

        // 第 1 次尝试失败：仍可重试 -> QUEUED
        h.processor.processDueBatch();
        EventRecord r1 = h.query.get(key);
        assertEquals(EventStatus.QUEUED, r1.getStatus());
        assertEquals(1, r1.getAttempts());
        assertTrue(r1.isLastErrorRetryable());
        assertEquals(Instant.parse("2026-09-23T00:00:00.100Z"), r1.getNextAttemptAt());
        log.info("[测试] 幂等键={} 重试次数=1 判定依据=可重试失败,退避100ms后重试", key);

        // 未到退避时间：不处理
        h.clock.advance(Duration.ofMillis(50));
        h.processor.processDueBatch();
        assertEquals(1, h.query.get(key).getAttempts());
        log.info("[测试] 幂等键={} 重试次数=1 判定依据=未到退避时刻,跳过", key);

        // 第 2 次尝试失败：仍可重试 -> QUEUED，退避 200ms
        h.clock.advance(Duration.ofMillis(60));
        h.processor.processDueBatch();
        EventRecord r2 = h.query.get(key);
        assertEquals(EventStatus.QUEUED, r2.getStatus());
        assertEquals(2, r2.getAttempts());
        assertEquals(Instant.parse("2026-09-23T00:00:00.310Z"), r2.getNextAttemptAt());
        log.info("[测试] 幂等键={} 重试次数=2 判定依据=可重试失败,退避200ms后重试", key);

        // 第 3 次尝试（达到上限 3）仍失败 -> TERMINATED / RETRY_EXHAUSTED
        h.clock.advance(Duration.ofSeconds(1));
        h.processor.processDueBatch();
        EventRecord r3 = h.query.get(key);
        assertEquals(EventStatus.TERMINATED, r3.getStatus());
        assertEquals(TerminationReason.RETRY_EXHAUSTED, r3.getTerminationReason());
        assertEquals(3, r3.getAttempts());
        assertNotNull(r3.getLastError());
        log.info("[测试] 幂等键={} 重试次数=3 判定依据=达到上限{},判定RETRY_EXHAUSTED终止", key, h.maxAttempts);

        // 终止后不再被任何轮询捞起
        h.clock.advance(Duration.ofHours(1));
        h.processor.processDueBatch();
        assertEquals(3, h.query.get(key).getAttempts());
        assertEquals(EventStatus.TERMINATED, h.query.get(key).getStatus());
        log.info("[测试] 幂等键={} 重试次数=3 判定依据=终止态不再被轮询,失败未静默丢失(lastError保留)", key);

        // 失败终止的事件不得记账
        assertEquals(0, h.handler.getAppliedEffectCount());
        assertFalse(h.handler.isLedgerContains(key));

        // 统计口径：1 条终止，合计一致
        EventStats stats = h.query.stats();
        assertEquals(1, stats.total());
        assertEquals(1, stats.terminated());
        assertEquals(0, stats.queued() + stats.processing() + stats.succeeded());
    }

    @Test
    void nonRetryableFailureTerminatesImmediately(@TempDir Path dir) {
        Harness h = harness(dir);
        String key = "order-fatal-1";

        h.ingest.ingest(key, "FATAL:金额非法");
        h.processor.processDueBatch();

        EventRecord r = h.query.get(key);
        assertEquals(EventStatus.TERMINATED, r.getStatus());
        assertEquals(TerminationReason.NON_RETRYABLE, r.getTerminationReason());
        assertEquals(1, r.getAttempts());
        assertNotNull(r.getLastError());
        log.info("[测试] 幂等键={} 重试次数=1 判定依据=不可重试失败,立即NON_RETRYABLE终止,不浪费重试", key);

        // 原因可与 RETRY_EXHAUSTED 区分
        log.info("[测试] 幂等键={} 判定依据=terminationReason={} 与RETRY_EXHAUSTED可区分",
                key, r.getTerminationReason());
    }
}
