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
import com.github.highcumontoa.idempotenteventingestionjava.model.RerunRecord;
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
 * 终止事件受控重跑测试：
 * 并发重跑只生效一次、轮数到顶拒绝、状态不允许拒绝、容量不足拒绝、
 * 重跑成功拿凭证且历史可查、重跑再失败按原退避/上限再次终止、重跑后重启能继续处理。
 * 全程日志打印幂等键、轮次/重试次数与判定依据。
 */
class RerunControlTest {

    private static final Logger log = LoggerFactory.getLogger(RerunControlTest.class);

    private record Harness(FileEventStore store, EventIngestionService ingest,
                           EventProcessingService processor, EventQueryService query,
                           EventRerunService rerun, DemoLedgerEventHandler handler,
                           MutableClock clock, EventingProperties props) {
    }

    private Harness harness(Path file, MutableClock clock, int maxAttempts, int maxReruns,
                            int capacity) {
        FileEventStore store = new FileEventStore(file, clock);
        DemoLedgerEventHandler handler = new DemoLedgerEventHandler();
        RetryBackoffPolicy policy =
                new RetryBackoffPolicy(maxAttempts, Duration.ofMillis(100), Duration.ofSeconds(5), 2.0);
        EventProcessingService processor = new EventProcessingService(store, handler, policy, clock);
        EventingProperties props = new EventingProperties();
        props.setMaxAttempts(maxAttempts);
        props.setMaxReruns(maxReruns);
        props.setMaxQueueCapacity(capacity);
        EventIngestionService ingest = new EventIngestionService(store, processor, props, clock);
        EventRerunService rerun = new EventRerunService(store, props, clock);
        return new Harness(store, ingest, processor, new EventQueryService(store), rerun,
                handler, clock, props);
    }

    /** 让一条 FIXABLE 事件在故障未修复时跑满 maxAttempts 次尝试直至终止。 */
    private void driveToTerminated(Harness h, String key) {
        for (int i = 0; i < h.props.getMaxAttempts(); i++) {
            h.processor.processDueBatch();
            h.clock.advance(Duration.ofSeconds(10));
        }
        EventRecord r = h.query.get(key);
        assertEquals(EventStatus.TERMINATED, r.getStatus());
        assertEquals(TerminationReason.RETRY_EXHAUSTED, r.getTerminationReason());
    }

    @Test
    void concurrentRerunTakesEffectOnlyOnce(@TempDir Path dir) throws Exception {
        MutableClock clock = MutableClock.startAt(Instant.parse("2026-10-04T00:00:00Z"));
        Harness h = harness(dir.resolve("events.dat"), clock, 2, 3, 10);
        String key = "evt-rerun-concurrent";

        h.ingest.ingest(key, "FIXABLE:下游临时挂了");
        driveToTerminated(h, key);
        log.info("[测试] 幂等键={} 轮次=0 重试次数=2 判定依据=故障未修复,达到上限RETRY_EXHAUSTED终止", key);

        // 模拟“下游恢复/数据改对了”
        h.handler.markFixed(key);

        // 多人同时发起重跑：只允许一个真正把事件重新入队
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

        log.info("[测试] 幂等键={} 判定依据=并发{}次重跑, ACCEPTED={} NOT_TERMINATED={} (同一监视器内状态迁移)",
                key, threads, accepted.get(), notTerminated.get());
        assertEquals(1, accepted.get(), "同一条事件最多只允许真正重新入队一次");
        assertEquals(threads - 1, notTerminated.get(), "其余重跑必须因状态已变被可区分地拒绝");
        assertTrue(unexpected.isEmpty(), "不应出现其他结果: " + unexpected);

        // 重新入队后的记录：轮次推进、历史归档、统计口径平
        EventRecord queued = h.query.get(key);
        assertEquals(EventStatus.QUEUED, queued.getStatus());
        assertEquals(1, queued.getRerunCount());
        assertEquals(0, queued.getAttemptsInRound());
        assertEquals(2, queued.getAttempts());
        assertNull(queued.getTerminationReason());
        List<RerunRecord> history = queued.getRerunHistory();
        assertEquals(1, history.size());
        assertEquals(0, history.get(0).round());
        assertEquals(TerminationReason.RETRY_EXHAUSTED, history.get(0).terminationReason());
        assertEquals(2, history.get(0).attempts());
        assertNotNull(history.get(0).lastError());
        log.info("[测试] 幂等键={} 轮次=1 判定依据=重跑入队,上一轮终止原因={}已归档,累计尝试={}保留",
                key, history.get(0).terminationReason(), queued.getAttempts());

        EventStats afterRerun = h.query.stats();
        assertEquals(1, afterRerun.total());
        assertEquals(1, afterRerun.queued());
        assertEquals(0, afterRerun.terminated());
        assertEquals(afterRerun.total(), afterRerun.queued() + afterRerun.processing()
                + afterRerun.succeeded() + afterRerun.terminated());

        // 重跑这一轮成功：与正常成功一样拿到结果凭证，副作用恰好一次
        h.processor.processDueBatch();
        EventRecord succeeded = h.query.get(key);
        assertEquals(EventStatus.SUCCEEDED, succeeded.getStatus());
        assertEquals("LEDGER-1", succeeded.getResult());
        assertEquals(1, succeeded.getAttemptsInRound());
        assertEquals(3, succeeded.getAttempts());
        assertEquals(1, succeeded.getRerunCount());
        assertEquals(1, succeeded.getRerunHistory().size());
        assertEquals(1, h.handler.getAppliedEffectCount());
        log.info("[测试] 幂等键={} 轮次=1 重试次数=1 判定依据=故障已修复,重跑成功 凭证={} 历史轮数={} 记账={}",
                key, succeeded.getResult(), succeeded.getRerunHistory().size(),
                h.handler.getAppliedEffectCount());

        EventStats stats = h.query.stats();
        assertEquals(1, stats.total());
        assertEquals(1, stats.succeeded());
    }

    @Test
    void rerunLimitReachedIsRejectedDistinctly(@TempDir Path dir) {
        MutableClock clock = MutableClock.startAt(Instant.parse("2026-10-04T00:00:00Z"));
        Harness h = harness(dir.resolve("events.dat"), clock, 1, 1, 10);
        String key = "evt-rerun-limit";

        h.ingest.ingest(key, "FIXABLE:一直修不好");
        driveToTerminated(h, key);

        // 第 1 次重跑：允许（上限 maxReruns=1）
        RerunResult first = h.rerun.rerun(key);
        assertEquals(RerunOutcome.ACCEPTED, first.outcome());
        log.info("[测试] 幂等键={} 轮次=1 判定依据=第1次重跑接受(上限1)", key);

        // 重跑这一轮又失败终止（maxAttempts=1，立即 RETRY_EXHAUSTED）
        h.processor.processDueBatch();
        EventRecord r = h.query.get(key);
        assertEquals(EventStatus.TERMINATED, r.getStatus());
        assertEquals(1, r.getRerunCount());
        log.info("[测试] 幂等键={} 轮次=1 重试次数=1 判定依据=重跑轮再次达到上限终止", key);

        // 第 2 次重跑：轮数到顶，明确拒绝且与“状态不允许”可区分
        RerunResult limited = h.rerun.rerun(key);
        assertEquals(RerunOutcome.REJECTED, limited.outcome());
        assertEquals(RerunRejectionReason.RERUN_LIMIT_REACHED, limited.rejectionReason());
        log.info("[测试] 幂等键={} 判定依据=重跑轮数到顶({}/{}),拒绝原因=RERUN_LIMIT_REACHED",
                key, r.getRerunCount(), h.props.getMaxReruns());

        // 终止原因与历史均未因拒绝而改变
        EventRecord after = h.query.get(key);
        assertEquals(EventStatus.TERMINATED, after.getStatus());
        assertEquals(1, after.getRerunCount());
        assertEquals(1, after.getRerunHistory().size());
        assertEquals(2, after.getAttempts());
    }

    @Test
    void rerunOnNonTerminatedOrMissingIsRejected(@TempDir Path dir) {
        MutableClock clock = MutableClock.startAt(Instant.parse("2026-10-04T00:00:00Z"));
        Harness h = harness(dir.resolve("events.dat"), clock, 3, 3, 10);

        h.ingest.ingest("evt-queued", "NORMAL");
        RerunResult onQueued = h.rerun.rerun("evt-queued");
        assertEquals(RerunOutcome.REJECTED, onQueued.outcome());
        assertEquals(RerunRejectionReason.NOT_TERMINATED, onQueued.rejectionReason());
        log.info("[测试] 幂等键=evt-queued 判定依据=QUEUED状态不允许重跑,拒绝原因=NOT_TERMINATED");

        h.processor.processDueBatch();
        assertEquals(EventStatus.SUCCEEDED, h.query.get("evt-queued").getStatus());
        RerunResult onSucceeded = h.rerun.rerun("evt-queued");
        assertEquals(RerunOutcome.REJECTED, onSucceeded.outcome());
        assertEquals(RerunRejectionReason.NOT_TERMINATED, onSucceeded.rejectionReason());
        log.info("[测试] 幂等键=evt-queued 判定依据=SUCCEEDED状态不允许重跑,拒绝原因=NOT_TERMINATED");

        RerunResult missing = h.rerun.rerun("evt-missing");
        assertEquals(RerunOutcome.REJECTED, missing.outcome());
        assertEquals(RerunRejectionReason.NOT_FOUND, missing.rejectionReason());
        log.info("[测试] 幂等键=evt-missing 判定依据=事件不存在,拒绝原因=NOT_FOUND");

        // 与“轮数到顶”是不同枚举值，调用方可区分
        assertTrue(RerunRejectionReason.NOT_TERMINATED != RerunRejectionReason.RERUN_LIMIT_REACHED);

        // 拒绝不产生任何状态变化与统计扰动
        EventStats stats = h.query.stats();
        assertEquals(1, stats.total());
        assertEquals(1, stats.succeeded());
    }

    @Test
    void rerunRespectsQueueCapacity(@TempDir Path dir) {
        MutableClock clock = MutableClock.startAt(Instant.parse("2026-10-04T00:00:00Z"));
        Harness h = harness(dir.resolve("events.dat"), clock, 1, 3, 1);

        // 先制造一条终止事件（容量 1，此时在途只有它自己）
        h.ingest.ingest("evt-dead", "FIXABLE:待修复");
        driveToTerminated(h, "evt-dead");

        // 占满在途容量
        h.ingest.ingest("evt-occupying", "RETRYABLE:占位");
        assertEquals(1, h.query.stats().queued());

        // 容量不足：重跑立即拒绝，不阻塞、不改状态
        RerunResult full = h.rerun.rerun("evt-dead");
        assertEquals(RerunOutcome.REJECTED, full.outcome());
        assertEquals(RerunRejectionReason.QUEUE_FULL, full.rejectionReason());
        assertEquals(EventStatus.TERMINATED, h.query.get("evt-dead").getStatus());
        assertEquals(0, h.query.get("evt-dead").getRerunCount());
        log.info("[测试] 幂等键=evt-dead 判定依据=在途1/1容量已满,重跑拒绝原因=QUEUE_FULL,不阻塞调用方");

        // 容量释放后：重跑放行
        h.handler.markFixed("evt-dead");
        h.clock.advance(Duration.ofSeconds(10));
        h.processor.processDueBatch(); // 占位事件 RETRYABLE 失败一次后终止(maxAttempts=1)，释放在途
        assertEquals(0, h.query.stats().queued() + h.query.stats().processing());
        RerunResult ok = h.rerun.rerun("evt-dead");
        assertEquals(RerunOutcome.ACCEPTED, ok.outcome());
        log.info("[测试] 幂等键=evt-dead 轮次=1 判定依据=容量释放后重跑放行入队");

        EventStats stats = h.query.stats();
        assertEquals(2, stats.total());
        assertEquals(stats.total(),
                stats.queued() + stats.processing() + stats.succeeded() + stats.terminated());
    }

    @Test
    void rerunFailureFollowsSameBackoffAndTerminatesWithDistinguishableRounds(@TempDir Path dir) {
        MutableClock clock = MutableClock.startAt(Instant.parse("2026-10-04T00:00:00Z"));
        Harness h = harness(dir.resolve("events.dat"), clock, 2, 3, 10);
        String key = "evt-rerun-refail";

        h.ingest.ingest(key, "RETRYABLE:一直失败");
        driveToTerminated(h, key);

        // 重跑进入第 1 轮，仍然失败：退避与上限按新一轮重新计数
        assertEquals(RerunOutcome.ACCEPTED, h.rerun.rerun(key).outcome());
        h.processor.processDueBatch();
        EventRecord firstTry = h.query.get(key);
        assertEquals(EventStatus.QUEUED, firstTry.getStatus());
        assertEquals(1, firstTry.getAttemptsInRound());
        assertEquals(3, firstTry.getAttempts());
        assertNotNull(firstTry.getNextAttemptAt());
        log.info("[测试] 幂等键={} 轮次=1 重试次数=1 判定依据=重跑轮可重试失败,按新一轮退避重排", key);

        h.clock.advance(Duration.ofSeconds(10));
        h.processor.processDueBatch();
        EventRecord terminated = h.query.get(key);
        assertEquals(EventStatus.TERMINATED, terminated.getStatus());
        assertEquals(TerminationReason.RETRY_EXHAUSTED, terminated.getTerminationReason());
        assertEquals(2, terminated.getAttemptsInRound());
        assertEquals(4, terminated.getAttempts());
        assertEquals(1, terminated.getRerunCount());

        // 两轮终止可区分：历史归档第 0 轮，当前字段是第 1 轮
        List<RerunRecord> history = terminated.getRerunHistory();
        assertEquals(1, history.size());
        assertEquals(0, history.get(0).round());
        assertEquals(2, history.get(0).attempts());
        assertEquals(TerminationReason.RETRY_EXHAUSTED, history.get(0).terminationReason());
        assertNotNull(terminated.getLastError());
        log.info("[测试] 幂等键={} 轮次=1 重试次数=2 判定依据=重跑轮再次RETRY_EXHAUSTED终止,"
                        + "第0轮归档(尝试={})与本轮(尝试={})可区分,累计={}",
                key, history.get(0).attempts(), terminated.getAttemptsInRound(),
                terminated.getAttempts());
    }

    @Test
    void rerunQueuedEventContinuesAfterRestart(@TempDir Path dir) {
        Path file = dir.resolve("events.dat");
        MutableClock clock = MutableClock.startAt(Instant.parse("2026-10-04T00:00:00Z"));

        // 第一次启动：终止后发起重跑，事件处于 QUEUED 等待重跑轮处理
        Harness first = harness(file, clock, 1, 3, 10);
        String key = "evt-rerun-restart";
        first.ingest.ingest(key, "FIXABLE:重启前未修复");
        driveToTerminated(first, key);
        first.handler.markFixed(key);
        assertEquals(RerunOutcome.ACCEPTED, first.rerun.rerun(key).outcome());
        assertEquals(EventStatus.QUEUED, first.query.get(key).getStatus());
        log.info("[测试] 幂等键={} 轮次=1 判定依据=重跑已入队,此时模拟服务重启", key);

        // 第二次启动：重跑入队状态、轮次与历史完整恢复，且能继续被处理（不停在原地）
        Harness second = harness(file, clock, 1, 3, 10);
        EventRecord recovered = second.query.get(key);
        assertEquals(EventStatus.QUEUED, recovered.getStatus());
        assertEquals(1, recovered.getRerunCount());
        assertEquals(1, recovered.getRerunHistory().size());
        assertEquals(TerminationReason.RETRY_EXHAUSTED,
                recovered.getRerunHistory().get(0).terminationReason());
        log.info("[测试] 幂等键={} 轮次=1 判定依据=重启后重跑轮次/历史归档完整恢复,仍为QUEUED", key);

        second.handler.markFixed(key);
        second.processor.processDueBatch();
        EventRecord done = second.query.get(key);
        assertEquals(EventStatus.SUCCEEDED, done.getStatus());
        assertNotNull(done.getResult());
        assertEquals(1, done.getRerunCount());
        assertEquals(2, done.getAttempts());
        assertEquals(1, second.handler.getAppliedEffectCount());
        log.info("[测试] 幂等键={} 轮次=1 重试次数=1 判定依据=重启后重跑轮继续处理成功 凭证={} 记账={}",
                key, done.getResult(), second.handler.getAppliedEffectCount());

        EventStats stats = second.query.stats();
        assertEquals(1, stats.total());
        assertEquals(1, stats.succeeded());
    }
}
