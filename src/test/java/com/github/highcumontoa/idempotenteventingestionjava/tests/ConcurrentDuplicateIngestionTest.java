package com.github.highcumontoa.idempotenteventingestionjava.tests;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.github.highcumontoa.idempotenteventingestionjava.handler.DemoLedgerEventHandler;
import com.github.highcumontoa.idempotenteventingestionjava.model.EventRecord;
import com.github.highcumontoa.idempotenteventingestionjava.model.EventStats;
import com.github.highcumontoa.idempotenteventingestionjava.model.EventStatus;
import com.github.highcumontoa.idempotenteventingestionjava.model.IngestionOutcome;
import com.github.highcumontoa.idempotenteventingestionjava.model.IngestionResult;
import com.github.highcumontoa.idempotenteventingestionjava.model.RejectionReason;
import com.github.highcumontoa.idempotenteventingestionjava.service.EventIngestionService;
import com.github.highcumontoa.idempotenteventingestionjava.service.EventProcessingService;
import com.github.highcumontoa.idempotenteventingestionjava.service.EventQueryService;
import com.github.highcumontoa.idempotenteventingestionjava.service.RetryBackoffPolicy;
import com.github.highcumontoa.idempotenteventingestionjava.store.FileEventStore;
import com.github.highcumontoa.idempotenteventingestionjava.tests.support.MutableClock;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 并发重复投递测试：
 * 多线程同时投递同一幂等键，只允许一次 ACCEPTED 与一次有效处理/记账，
 * 其余全部为可区分的 DUPLICATE；后台处理与查询并发进行时统计口径一致。
 */
class ConcurrentDuplicateIngestionTest {

    private static final Logger log = LoggerFactory.getLogger(ConcurrentDuplicateIngestionTest.class);

    @Test
    void concurrentDuplicatesAreAcceptedOnceAndProcessedOnce(@TempDir Path dir) throws Exception {
        String key = "payment-dup-concurrent";
        MutableClock clock = MutableClock.startAt(Instant.parse("2026-09-23T00:00:00Z"));
        FileEventStore store = new FileEventStore(dir.resolve("events.dat"), clock);
        DemoLedgerEventHandler handler = new DemoLedgerEventHandler();
        RetryBackoffPolicy policy =
                new RetryBackoffPolicy(5, Duration.ofMillis(1), Duration.ofMillis(10), 2.0);
        EventProcessingService processor = new EventProcessingService(store, handler, policy, clock);
        var props = new com.github.highcumontoa.idempotenteventingestionjava.config.EventingProperties();
        EventIngestionService ingest = new EventIngestionService(store, processor, props, clock);
        EventQueryService query = new EventQueryService(store);

        int threads = 32;
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch start = new CountDownLatch(1);
        CountDownLatch done = new CountDownLatch(threads);
        AtomicInteger accepted = new AtomicInteger();
        AtomicInteger duplicates = new AtomicInteger();
        ConcurrentLinkedQueue<String> failures = new ConcurrentLinkedQueue<>();

        for (int i = 0; i < threads; i++) {
            pool.submit(() -> {
                try {
                    start.await();
                    IngestionResult r = ingest.ingest(key, "NORMAL-PAYLOAD");
                    if (r.outcome() == IngestionOutcome.ACCEPTED) {
                        accepted.incrementAndGet();
                    } else if (r.outcome() == IngestionOutcome.DUPLICATE) {
                        duplicates.incrementAndGet();
                    } else {
                        failures.add("意外的拒绝: " + r.outcome());
                    }
                } catch (Exception e) {
                    failures.add(e.toString());
                } finally {
                    done.countDown();
                }
            });
        }
        start.countDown();
        assertTrue(done.await(10, TimeUnit.SECONDS), "并发接入未在时限内完成");
        pool.shutdownNow();

        log.info("[测试] 幂等键={} 判定依据=并发{}投遞, ACCEPTED={} DUPLICATE={} (同一临界区find+putIfAbsent)",
                key, threads, accepted.get(), duplicates.get());
        assertEquals(1, accepted.get(), "只允许一次 ACCEPTED");
        assertEquals(threads - 1, duplicates.get(), "其余必须是 DUPLICATE");
        assertTrue(failures.isEmpty(), "不应出现失败: " + failures);

        // 后台 worker 并发反复扫描处理，同时另一线程不断读取统计
        ExecutorService bg = Executors.newFixedThreadPool(2);
        for (int w = 0; w < 2; w++) {
            bg.submit(() -> {
                while (!Thread.currentThread().isInterrupted()) {
                    processor.processDueBatch();
                    try {
                        Thread.sleep(1);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                }
            });
        }
        AtomicInteger inconsistentSnapshots = new AtomicInteger();
        ExecutorService readerPool = Executors.newSingleThreadExecutor();
        readerPool.submit(() -> {
            for (int i = 0; i < 200; i++) {
                EventStats s = query.stats();
                if (s.queued() + s.processing() + s.succeeded() + s.terminated() != s.total()) {
                    inconsistentSnapshots.incrementAndGet();
                }
                try {
                    Thread.sleep(1);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        }).get(10, TimeUnit.SECONDS);
        bg.shutdownNow();
        readerPool.shutdownNow();

        EventRecord finalRecord = query.get(key);
        assertEquals(EventStatus.SUCCEEDED, finalRecord.getStatus());
        assertEquals(1, finalRecord.getAttempts());
        assertEquals(1, handler.getAppliedEffectCount(), "副作用（记账）只允许发生一次");
        log.info("[测试] 幂等键={} 重试次数={} 判定依据=仅一次有效处理, 记账次数={} 统计不一致快照={}",
                key, finalRecord.getAttempts() - 1, handler.getAppliedEffectCount(),
                inconsistentSnapshots.get());
        assertEquals(0, inconsistentSnapshots.get(), "并发统计口径必须始终不重不漏");

        EventStats stats = query.stats();
        assertEquals(1, stats.total());
        assertEquals(1, stats.succeeded());
    }

    @Test
    void distinctKeysAreAllAccepted(@TempDir Path dir) {
        MutableClock clock = MutableClock.startAt(Instant.now());
        FileEventStore store = new FileEventStore(dir.resolve("events2.dat"), clock);
        DemoLedgerEventHandler handler = new DemoLedgerEventHandler();
        RetryBackoffPolicy policy =
                new RetryBackoffPolicy(5, Duration.ofMillis(1), Duration.ofMillis(10), 2.0);
        EventProcessingService processor = new EventProcessingService(store, handler, policy, clock);
        var props = new com.github.highcumontoa.idempotenteventingestionjava.config.EventingProperties();
        EventIngestionService ingest = new EventIngestionService(store, processor, props, clock);

        List<String> keys = List.of("k-1", "k-2", "k-3");
        for (String k : keys) {
            assertEquals(IngestionOutcome.ACCEPTED, ingest.ingest(k, "p").outcome());
            // 第二次投递同一键 -> DUPLICATE
            assertEquals(IngestionOutcome.DUPLICATE, ingest.ingest(k, "p").outcome());
        }
        log.info("[测试] 幂等键={} 判定依据=不同键全部接受,同键第二次DUPLICATE", keys);
    }

    @Test
    void invalidAndOverCapacityRequestsAreRejectedNotSilentlyAccepted(@TempDir Path dir) {
        MutableClock clock = MutableClock.startAt(Instant.now());
        FileEventStore store = new FileEventStore(dir.resolve("events3.dat"), clock);
        DemoLedgerEventHandler handler = new DemoLedgerEventHandler();
        RetryBackoffPolicy policy =
                new RetryBackoffPolicy(5, Duration.ofMillis(1), Duration.ofMillis(10), 2.0);
        EventProcessingService processor = new EventProcessingService(store, handler, policy, clock);
        var props = new com.github.highcumontoa.idempotenteventingestionjava.config.EventingProperties();
        props.setMaxQueueCapacity(2);
        EventIngestionService ingest = new EventIngestionService(store, processor, props, clock);

        assertEquals(RejectionReason.INVALID_EVENT,
                ingest.ingest("", "p").rejectionReason());
        assertEquals(RejectionReason.INVALID_EVENT,
                ingest.ingest("k", null).rejectionReason());
        log.info("[测试] 幂等键=<空> 判定依据=INVALID_EVENT 明确拒绝400,不误报成功");

        assertEquals(IngestionOutcome.ACCEPTED, ingest.ingest("a", "RETRYABLE:x").outcome());
        assertEquals(IngestionOutcome.ACCEPTED, ingest.ingest("b", "RETRYABLE:x").outcome());
        IngestionResult over = ingest.ingest("c", "p");
        assertEquals(IngestionOutcome.REJECTED, over.outcome());
        assertEquals(RejectionReason.QUEUE_FULL, over.rejectionReason());
        log.info("[测试] 幂等键=c 判定依据=QUEUE_FULL 在途2/2 明确限流503,不阻塞调用");

        EventStats stats = new EventQueryService(store).stats();
        assertEquals(2, stats.queued());
        assertEquals(2, stats.total());
    }
}
