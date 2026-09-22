package com.github.highcumontoa.idempotenteventingestionjava.tests;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import com.github.highcumontoa.idempotenteventingestionjava.handler.DemoLedgerEventHandler;
import com.github.highcumontoa.idempotenteventingestionjava.model.EventRecord;
import com.github.highcumontoa.idempotenteventingestionjava.model.EventStats;
import com.github.highcumontoa.idempotenteventingestionjava.model.EventStatus;
import com.github.highcumontoa.idempotenteventingestionjava.model.IngestionOutcome;
import com.github.highcumontoa.idempotenteventingestionjava.model.TerminationReason;
import com.github.highcumontoa.idempotenteventingestionjava.service.EventIngestionService;
import com.github.highcumontoa.idempotenteventingestionjava.service.EventProcessingService;
import com.github.highcumontoa.idempotenteventingestionjava.service.EventQueryService;
import com.github.highcumontoa.idempotenteventingestionjava.service.RetryBackoffPolicy;
import com.github.highcumontoa.idempotenteventingestionjava.store.FileEventStore;
import com.github.highcumontoa.idempotenteventingestionjava.tests.support.MutableClock;
import java.io.ObjectOutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 服务重启后的状态恢复测试（通过重建 FileEventStore 模拟进程重启，数据文件即持久状态）：
 * 成功/终止/排队态持久保留；崩溃时停留在 PROCESSING 的事件重启后回退 QUEUED 重新处理；
 * 重启后重复投递仍只识别为 DUPLICATE，统计口径不重不漏。
 */
class RestartRecoveryTest {

    private static final Logger log = LoggerFactory.getLogger(RestartRecoveryTest.class);

    private record Ctx(FileEventStore store, EventProcessingService processor,
                      EventIngestionService ingest, EventQueryService query,
                      DemoLedgerEventHandler handler, MutableClock clock) {
    }

    private Ctx boot(Path file, MutableClock clock) {
        FileEventStore store = new FileEventStore(file);
        DemoLedgerEventHandler handler = new DemoLedgerEventHandler();
        RetryBackoffPolicy policy =
                new RetryBackoffPolicy(3, Duration.ofMillis(100), Duration.ofSeconds(5), 2.0);
        EventProcessingService processor = new EventProcessingService(store, handler, policy, clock);
        var props = new com.github.highcumontoa.idempotenteventingestionjava.config.EventingProperties();
        EventIngestionService ingest = new EventIngestionService(store, processor, props, clock);
        return new Ctx(store, processor, ingest, new EventQueryService(store), handler, clock);
    }

    @Test
    void statesSurviveRestartAndDuplicatesAreNotDoubleCounted(@TempDir Path dir) {
        Path file = dir.resolve("events-restart.dat");
        MutableClock clock = MutableClock.startAt(Instant.parse("2026-09-23T00:00:00Z"));

        // ---- 第一次启动：制造 成功 / 终止 两种终态事件 ----
        Ctx first = boot(file, clock);
        first.ingest.ingest("evt-ok", "NORMAL");
        first.ingest.ingest("evt-fatal", "FATAL:脏数据");
        first.processor.processDueBatch();
        assertEquals(EventStatus.SUCCEEDED, first.query.get("evt-ok").getStatus());
        assertEquals(EventStatus.TERMINATED, first.query.get("evt-fatal").getStatus());
        log.info("[测试] 幂等键=evt-ok/evt-fatal 判定依据=重启前分别为SUCCEEDED/TERMINATED(NON_RETRYABLE)");

        // ---- 第二次启动：状态应从文件恢复 ----
        Ctx second = boot(file, clock);
        assertEquals(EventStatus.SUCCEEDED, second.query.get("evt-ok").getStatus());
        assertEquals(EventStatus.TERMINATED, second.query.get("evt-fatal").getStatus());
        assertEquals(TerminationReason.NON_RETRYABLE,
                second.query.get("evt-fatal").getTerminationReason());
        assertEquals("LEDGER-1", second.query.get("evt-ok").getResult());
        log.info("[测试] 幂等键=evt-ok/evt-fatal 判定依据=重启后SUCCEEDED/TERMINATED及结果凭证完整恢复");

        // 重启后再次投递既有键 -> 仍是 DUPLICATE，不会因为重启而二次计入
        assertEquals(IngestionOutcome.DUPLICATE, second.ingest.ingest("evt-ok", "NORMAL").outcome());
        assertEquals(IngestionOutcome.DUPLICATE, second.ingest.ingest("evt-fatal", "FATAL:脏数据").outcome());
        log.info("[测试] 幂等键=evt-ok 判定依据=重启后重复投递仍为DUPLICATE,不二次处理/记账");

        EventStats stats = second.query.stats();
        assertEquals(2, stats.total());
        assertEquals(1, stats.succeeded());
        assertEquals(1, stats.terminated());
        assertEquals(0, stats.queued() + stats.processing());
    }

    @Test
    void processingStateAtCrashIsRecoveredToQueuedAndRetried(@TempDir Path dir) throws Exception {
        Path file = dir.resolve("events-crash.dat");
        MutableClock clock = MutableClock.startAt(Instant.parse("2026-09-23T00:00:00Z"));

        // 第一次启动：摄入一条 QUEUED 事件后，手工把状态改成 PROCESSING（模拟处理中途断电）
        Ctx first = boot(file, clock);
        first.ingest.ingest("evt-crash", "FLAKY:1");
        EventRecord queued = first.query.get("evt-crash");
        synchronized (first.store) {
            queued.setStatus(EventStatus.PROCESSING);
            queued.setAttempts(1);
            queued.setNextAttemptAt(null);
            first.store.persist();
        }
        log.info("[测试] 幂等键=evt-crash 重试次数=1 判定依据=模拟处理中途崩溃,状态残留PROCESSING");

        // 第二次启动：加载时必须回退为 QUEUED
        Ctx second = boot(file, clock);
        EventRecord recovered = second.query.get("evt-crash");
        assertEquals(EventStatus.QUEUED, recovered.getStatus());
        assertEquals(1, recovered.getAttempts());
        assertNotNull(recovered.getNextAttemptAt());
        log.info("[测试] 幂等键=evt-crash 重试次数=1 判定依据=重启恢复PROCESSING->QUEUED,等待重新处理");

        // 到点后继续处理：FLAKY:1 第 2 次尝试成功，且记账恰好一次
        clock.advance(Duration.ofSeconds(1));
        second.processor.processDueBatch();
        EventRecord done = second.query.get("evt-crash");
        assertEquals(EventStatus.SUCCEEDED, done.getStatus());
        assertEquals(2, done.getAttempts());
        assertEquals(1, second.handler.getAppliedEffectCount());
        log.info("[测试] 幂等键=evt-crash 重试次数=2 判定依据=恢复后重试成功,记账恰好一次");

        // 再重启：仍是成功态，统计一致
        Ctx third = boot(file, clock);
        assertEquals(EventStatus.SUCCEEDED, third.query.get("evt-crash").getStatus());
        EventStats s = third.query.stats();
        assertEquals(1, s.total());
        assertEquals(1, s.succeeded());
    }
}
