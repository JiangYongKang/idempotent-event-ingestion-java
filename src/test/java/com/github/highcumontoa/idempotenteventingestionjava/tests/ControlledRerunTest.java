package com.github.highcumontoa.idempotenteventingestionjava.tests;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.github.highcumontoa.idempotenteventingestionjava.config.EventingProperties;
import com.github.highcumontoa.idempotenteventingestionjava.handler.DemoLedgerEventHandler;
import com.github.highcumontoa.idempotenteventingestionjava.model.EventRecord;
import com.github.highcumontoa.idempotenteventingestionjava.model.EventStats;
import com.github.highcumontoa.idempotenteventingestionjava.model.EventStatus;
import com.github.highcumontoa.idempotenteventingestionjava.model.RerunOutcome;
import com.github.highcumontoa.idempotenteventingestionjava.model.RerunRejectionReason;
import com.github.highcumontoa.idempotenteventingestionjava.model.RerunResult;
import com.github.highcumontoa.idempotenteventingestionjava.model.TerminationReason;
import com.github.highcumontoa.idempotenteventingestionjava.service.EventIngestionService;
import com.github.highcumontoa.idempotenteventingestionjava.service.EventProcessingService;
import com.github.highcumontoa.idempotenteventingestionjava.service.EventQueryService;
import com.github.highcumontoa.idempotenteventingestionjava.service.EventRerunService;
import com.github.highcumontoa.idempotenteventingestionjava.service.RetryBackoffPolicy;
import com.github.highcumontoa.idempotenteventingestionjava.store.FileEventStore;
import com.github.highcumontoa.idempotenteventingestionjava.tests.support.MutableClock;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
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
 * 终止事件受控重跑测试：
 * 并发重跑只生效一次、重跑轮数到顶被拒、状态不合适被拒、容量不足被拒、
 * 重跑轮次仍按退避/上限/终止规则推进且轮次可区分、重跑后重启能继续处理。
 * 全程日志打印幂等键、处理/重试次数与判定依据。
 */
class ControlledRerunTest {

    private static final Logger log = LoggerFactory.getLogger(ControlledRerunTest.class);

    private record Harness(FileEventStore store, EventIngestionService ingest, EventRerunService rerun,
                           EventProcessingService processor, EventQueryService query,
                           DemoLedgerEventHandler handler, MutableClock clock) {
    }

    private Harness harness(Path file, MutableClock clock, int maxAttempts, int maxReruns,
                            int maxQueueCapacity) {
        FileEventStore store = new FileEventStore(file, clock);
        DemoLedgerEventHandler handler = new DemoLedgerEventHandler();
        RetryBackoffPolicy policy =
                new RetryBackoffPolicy(maxAttempts, Duration.ofMillis(100), Duration.ofSeconds(5), 2.0);
        EventProcessingService processor = new EventProcessingService(store, handler, policy, clock);
        EventingProperties props = new EventingProperties();
        props.setMaxAttempts(maxAttempts);
        props.setMaxReruns(maxReruns);
        props.setMaxQueueCapacity(maxQueueCapacity);
        EventIngestionService ingest = new EventIngestionService(store, processor, props, clock);
        EventRerunService rerun = new EventRerunService(store, props, clock);
        return new Harness(store, ingest, rerun, processor,
                new EventQueryService(store), handler, clock);
    }

    @Test
    void concurrentRerunTakesEffectOnlyOnce(@TempDir Path dir) throws Exception {
        MutableClock clock = MutableClock.startAt(Instant.parse("2026-10-04T00:00:00Z"));
        Harness h = harness(dir.resolve("events.dat"), clock, 2, 3, 100);
        String key = "rerun-concurrent";

        // 先制造一条终止事件：FLAKY:2 两次尝试都失败，达到上限终止
        h.ingest.ingest(key, "FLAKY:2");
        h.processor.processDueBatch();
        h.clock.advance(Duration.ofSeconds(1));
        h.processor.processDueBatch();
        EventRecord terminated = h.query.get(key);
        assertEquals(EventStatus.TERMINATED, terminated.getStatus());
        assertEquals(TerminationReason.RETRY_EXHAUSTED, terminated.getTerminationReason());
        assertEquals(2, terminated.getAttempts());
        log.info("[测试] 幂等键={} 处理次数=2 判定依据=达到上限终止(RETRY_EXHAUSTED),具备重跑前提", key);

        // 16 个线程同时对同一终止事件发起重跑
        int threads = 16;
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch start = new CountDownLatch(1);
        CountDownLatch done = new CountDownLatch(threads);
        AtomicInteger accepted = new AtomicInteger();
        AtomicInteger notTerminated = new AtomicInteger();
        ConcurrentLinkedQueue<String> unexpected = new ConcurrentLinkedQueue<>();
        for (int i = 0; i < threads; i++) {
            pool.submit(() -> {
                try {
                    start.await();
                    RerunResult r = h.rerun.rerun(key);
                    if (r.outcome() == RerunOutcome.ACCEPTED) {
                        accepted.incrementAndGet();
                    } else if (r.rejectionReason() == RerunRejectionReason.NOT_TERMINATED) {
                        notTerminated.incrementAndGet();
                    } else {
                        unexpected.add("意外结果: " + r.outcome() + "/" + r.rejectionReason());
                    }
                } catch (Exception e) {
                    unexpected.add(e.toString());
                } finally {
                    done.countDown();
                }
            });
        }
        start.countDown();
        assertTrue(done.await(10, TimeUnit.SECONDS), "并发重跑未在时限内完成");
        pool.shutdownNow();

        log.info("[测试] 幂等键={} 判定依据=并发{}次重跑, ACCEPTED={} NOT_TERMINATED={} (同一临界区判定+入队)",
                key, threads, accepted.get(), notTerminated.get());
        assertEquals(1, accepted.get(), "同一事件最多只允许一次真正重新入队");
        assertEquals(threads - 1, notTerminated.get(), "其余必须拿到可区分的 NOT_TERMINATED 拒绝");
        assertTrue(unexpected.isEmpty(), "不应出现意外结果: " + unexpected);

        // 重新入队后只入队一次：QUEUED、第 1 轮重跑、累计次数保留、上轮终止原因保留
        EventRecord requeued = h.query.get(key);
        assertEquals(EventStatus.QUEUED, requeued.getStatus());
        assertEquals(1, requeued.getRerunCount());
        assertEquals(2, requeued.getAttempts());
        assertEquals(0, requeued.getRoundAttempts());
        assertEquals(TerminationReason.RETRY_EXHAUSTED, requeued.getLastTerminationReason());
        assertNull(requeued.getTerminationReason());
        log.info("[测试] 幂等键={} 重跑轮数=1 判定依据=重新入队QUEUED,累计处理次数=2保留,上轮终止原因保留", key);

        // 跑完这一轮：FLAKY:2 第 3 次（累计）成功，与正常成功一样拿到结果凭证，副作用恰好一次
        h.processor.processDueBatch();
        EventRecord succeeded = h.query.get(key);
        assertEquals(EventStatus.SUCCEEDED, succeeded.getStatus());
        assertNotNull(succeeded.getResult());
        assertEquals(3, succeeded.getAttempts());
        assertEquals(1, succeeded.getRoundAttempts());
        assertEquals(1, succeeded.getRerunCount());
        assertEquals(TerminationReason.RETRY_EXHAUSTED, succeeded.getLastTerminationReason());
        assertEquals(1, h.handler.getAppliedEffectCount());
        log.info("[测试] 幂等键={} 重跑轮数=1 本轮处理次数=1 累计处理次数=3 判定依据=重跑成功,凭证={} 记账恰好一次",
                key, succeeded.getResult());

        EventStats stats = h.query.stats();
        assertEquals(1, stats.total());
        assertEquals(1, stats.succeeded());
        assertEquals(0, stats.queued() + stats.processing() + stats.terminated());
    }

    @Test
    void rerunLimitReachedIsRejectedWithDistinctReason(@TempDir Path dir) {
        MutableClock clock = MutableClock.startAt(Instant.parse("2026-10-04T00:00:00Z"));
        Harness h = harness(dir.resolve("events.dat"), clock, 1, 1, 100);
        String key = "rerun-limit";

        // maxAttempts=1：一次可重试失败即终止
        h.ingest.ingest(key, "RETRYABLE:下游超时");
        h.processor.processDueBatch();
        assertEquals(EventStatus.TERMINATED, h.query.get(key).getStatus());
        assertEquals(TerminationReason.RETRY_EXHAUSTED, h.query.get(key).getTerminationReason());
        log.info("[测试] 幂等键={} 处理次数=1 判定依据=达到上限终止,允许重跑轮数=1", key);

        // 第 1 轮重跑：接受；再次失败仍按规则终止
        RerunResult first = h.rerun.rerun(key);
        assertEquals(RerunOutcome.ACCEPTED, first.outcome());
        h.processor.processDueBatch();
        EventRecord again = h.query.get(key);
        assertEquals(EventStatus.TERMINATED, again.getStatus());
        assertEquals(1, again.getRerunCount());
        assertEquals(2, again.getAttempts());
        log.info("[测试] 幂等键={} 重跑轮数=1 累计处理次数=2 判定依据=重跑轮再次终止,轮数已用满", key);

        // 第 2 次重跑：到顶，明确拒绝且原因可与“状态不允许”区分；记录保持原状
        RerunResult rejected = h.rerun.rerun(key);
        assertEquals(RerunOutcome.REJECTED, rejected.outcome());
        assertEquals(RerunRejectionReason.RERUN_LIMIT_REACHED, rejected.rejectionReason());
        EventRecord after = h.query.get(key);
        assertEquals(EventStatus.TERMINATED, after.getStatus());
        assertEquals(1, after.getRerunCount());
        assertEquals(2, after.getAttempts());
        assertEquals(TerminationReason.RETRY_EXHAUSTED, after.getLastTerminationReason());
        log.info("[测试] 幂等键={} 重跑轮数=1/1 判定依据=RERUN_LIMIT_REACHED 明确拒绝,记录保持终止态不变", key);
    }

    @Test
    void rerunRejectedWhenStatusNotTerminated(@TempDir Path dir) {
        MutableClock clock = MutableClock.startAt(Instant.parse("2026-10-04T00:00:00Z"));
        Harness h = harness(dir.resolve("events.dat"), clock, 3, 3, 100);

        // 不存在的键
        RerunResult missing = h.rerun.rerun("no-such-key");
        assertEquals(RerunOutcome.REJECTED, missing.outcome());
        assertEquals(RerunRejectionReason.NOT_FOUND, missing.rejectionReason());
        log.info("[测试] 幂等键=no-such-key 判定依据=NOT_FOUND 明确拒绝");

        // 排队中：不允许重跑
        h.ingest.ingest("queued-key", "NORMAL");
        RerunResult queued = h.rerun.rerun("queued-key");
        assertEquals(RerunOutcome.REJECTED, queued.outcome());
        assertEquals(RerunRejectionReason.NOT_TERMINATED, queued.rejectionReason());
        log.info("[测试] 幂等键=queued-key 判定依据=NOT_TERMINATED(QUEUED) 当前不允许重跑");

        // 成功后：不允许重跑（拒绝原因与“轮数到顶”可区分）
        h.processor.processDueBatch();
        assertEquals(EventStatus.SUCCEEDED, h.query.get("queued-key").getStatus());
        RerunResult succeeded = h.rerun.rerun("queued-key");
        assertEquals(RerunOutcome.REJECTED, succeeded.outcome());
        assertEquals(RerunRejectionReason.NOT_TERMINATED, succeeded.rejectionReason());
        assertTrue(succeeded.rejectionReason() != RerunRejectionReason.RERUN_LIMIT_REACHED,
                "状态不允许与轮数到顶必须是不同拒绝原因");
        log.info("[测试] 幂等键=queued-key 判定依据=NOT_TERMINATED(SUCCEEDED) 与RERUN_LIMIT_REACHED可区分");

        // 处理中的事件同样不允许重跑（手工置为 PROCESSING 模拟）
        h.ingest.ingest("processing-key", "NORMAL");
        synchronized (h.store) {
            h.query.get("processing-key").setStatus(EventStatus.PROCESSING);
            h.store.persist();
        }
        RerunResult processing = h.rerun.rerun("processing-key");
        assertEquals(RerunRejectionReason.NOT_TERMINATED, processing.rejectionReason());
        log.info("[测试] 幂等键=processing-key 判定依据=NOT_TERMINATED(PROCESSING) 当前不允许重跑");
    }

    @Test
    void rerunRespectsQueueCapacity(@TempDir Path dir) {
        MutableClock clock = MutableClock.startAt(Instant.parse("2026-10-04T00:00:00Z"));
        Harness h = harness(dir.resolve("events.dat"), clock, 1, 3, 1);

        // 先制造一条终止事件（此时容量空）
        h.ingest.ingest("cap-terminated", "FATAL:脏数据");
        h.processor.processDueBatch();
        assertEquals(EventStatus.TERMINATED, h.query.get("cap-terminated").getStatus());

        // 占满在途容量（1/1）
        h.ingest.ingest("cap-occupier", "RETRYABLE:占坑");
        // 容量满时发起重跑：立即拒绝，不阻塞、不改变任何状态
        RerunResult full = h.rerun.rerun("cap-terminated");
        assertEquals(RerunOutcome.REJECTED, full.outcome());
        assertEquals(RerunRejectionReason.QUEUE_FULL, full.rejectionReason());
        assertEquals(EventStatus.TERMINATED, h.query.get("cap-terminated").getStatus());
        assertEquals(0, h.query.get("cap-terminated").getRerunCount());
        log.info("[测试] 幂等键=cap-terminated 判定依据=QUEUE_FULL 在途1/1 立即拒绝,不阻塞不计数");

        // 占坑事件终止后容量释放，重跑即可接受
        h.processor.processDueBatch();
        assertEquals(EventStatus.TERMINATED, h.query.get("cap-occupier").getStatus());
        RerunResult accepted = h.rerun.rerun("cap-terminated");
        assertEquals(RerunOutcome.ACCEPTED, accepted.outcome());
        assertEquals(EventStatus.QUEUED, h.query.get("cap-terminated").getStatus());
        log.info("[测试] 幂等键=cap-terminated 重跑轮数=1 判定依据=容量释放后重跑接受,统计口径未被打乱");

        EventStats stats = h.query.stats();
        assertEquals(2, stats.total());
        assertEquals(1, stats.queued());
        assertEquals(1, stats.terminated());
    }

    @Test
    void rerunRoundFollowsSameBackoffAndTerminationRules(@TempDir Path dir) {
        MutableClock clock = MutableClock.startAt(Instant.parse("2026-10-04T00:00:00Z"));
        Harness h = harness(dir.resolve("events.dat"), clock, 2, 3, 100);
        String key = "rerun-rounds";

        // 首轮：2 次尝试后 RETRY_EXHAUSTED 终止
        h.ingest.ingest(key, "RETRYABLE:下游超时");
        h.processor.processDueBatch();
        h.clock.advance(Duration.ofSeconds(1));
        h.processor.processDueBatch();
        EventRecord t1 = h.query.get(key);
        assertEquals(EventStatus.TERMINATED, t1.getStatus());
        assertEquals(2, t1.getAttempts());
        assertEquals(2, t1.getRoundAttempts());
        log.info("[测试] 幂等键={} 重跑轮数=0 本轮处理次数=2 判定依据=首轮达到上限终止", key);

        // 发起重跑：本轮计数清零，累计保留
        assertEquals(RerunOutcome.ACCEPTED, h.rerun.rerun(key).outcome());
        EventRecord requeued = h.query.get(key);
        assertEquals(0, requeued.getRoundAttempts());
        assertEquals(2, requeued.getAttempts());
        assertEquals(clock.instant(), requeued.getNextAttemptAt());

        // 重跑轮第 1 次失败：仍按退避重新排队（退避从本轮第 1 次重新计算）
        h.processor.processDueBatch();
        EventRecord r1 = h.query.get(key);
        assertEquals(EventStatus.QUEUED, r1.getStatus());
        assertEquals(1, r1.getRoundAttempts());
        assertEquals(3, r1.getAttempts());
        assertEquals(clock.instant().plus(Duration.ofMillis(100)), r1.getNextAttemptAt());
        log.info("[测试] 幂等键={} 重跑轮数=1 本轮处理次数=1 累计=3 判定依据=重跑轮可重试失败,退避100ms", key);

        // 未到退避时间不处理；到点后第 2 次失败达到本轮上限，再次终止
        h.processor.processDueBatch();
        assertEquals(3, h.query.get(key).getAttempts());
        h.clock.advance(Duration.ofMillis(150));
        h.processor.processDueBatch();
        EventRecord t2 = h.query.get(key);
        assertEquals(EventStatus.TERMINATED, t2.getStatus());
        assertEquals(TerminationReason.RETRY_EXHAUSTED, t2.getTerminationReason());
        assertEquals(2, t2.getRoundAttempts());
        assertEquals(4, t2.getAttempts());
        assertEquals(1, t2.getRerunCount());
        assertEquals(TerminationReason.RETRY_EXHAUSTED, t2.getLastTerminationReason());
        log.info("[测试] 幂等键={} 重跑轮数=1 本轮处理次数=2 累计=4 判定依据=重跑轮达到上限再次终止,轮次可区分", key);
    }

    @Test
    void rerunQueuedEventContinuesAfterRestart(@TempDir Path dir) {
        Path file = dir.resolve("events-restart.dat");
        MutableClock clock = MutableClock.startAt(Instant.parse("2026-10-04T00:00:00Z"));
        String key = "rerun-restart";

        // 第一次启动：终止一条事件并发起重跑，重跑入队后立刻“重启”
        Harness first = harness(file, clock, 1, 3, 100);
        first.ingest.ingest(key, "FLAKY:1");
        first.processor.processDueBatch();
        assertEquals(EventStatus.TERMINATED, first.query.get(key).getStatus());
        assertEquals(RerunOutcome.ACCEPTED, first.rerun.rerun(key).outcome());
        assertEquals(EventStatus.QUEUED, first.query.get(key).getStatus());
        log.info("[测试] 幂等键={} 重跑轮数=1 判定依据=重跑已入队,模拟服务重启", key);

        // 第二次启动：重跑入队状态必须恢复，且能继续被处理（不停在原地）
        Harness second = harness(file, clock, 1, 3, 100);
        EventRecord recovered = second.query.get(key);
        assertEquals(EventStatus.QUEUED, recovered.getStatus());
        assertEquals(1, recovered.getRerunCount());
        assertEquals(1, recovered.getAttempts());
        assertEquals(TerminationReason.RETRY_EXHAUSTED, recovered.getLastTerminationReason());
        assertNotNull(recovered.getNextAttemptAt());
        log.info("[测试] 幂等键={} 重跑轮数=1 判定依据=重启后重跑状态/历史完整恢复,等待继续处理", key);

        clock.advance(Duration.ofSeconds(1));
        second.processor.processDueBatch();
        EventRecord done = second.query.get(key);
        assertEquals(EventStatus.SUCCEEDED, done.getStatus());
        assertNotNull(done.getResult());
        assertEquals(2, done.getAttempts());
        assertEquals(1, done.getRerunCount());
        assertEquals(1, second.handler.getAppliedEffectCount());
        log.info("[测试] 幂等键={} 重跑轮数=1 累计处理次数=2 判定依据=重启后重跑事件继续处理并成功,凭证={}",
                key, done.getResult());

        EventStats stats = second.query.stats();
        assertEquals(1, stats.total());
        assertEquals(1, stats.succeeded());
    }
}
