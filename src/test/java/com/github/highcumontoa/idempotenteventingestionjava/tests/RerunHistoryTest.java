package com.github.highcumontoa.idempotenteventingestionjava.tests;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.github.highcumontoa.idempotenteventingestionjava.config.EventingProperties;
import com.github.highcumontoa.idempotenteventingestionjava.handler.EventHandler;
import com.github.highcumontoa.idempotenteventingestionjava.handler.EventProcessingException;
import com.github.highcumontoa.idempotenteventingestionjava.model.EventRecord;
import com.github.highcumontoa.idempotenteventingestionjava.model.EventStats;
import com.github.highcumontoa.idempotenteventingestionjava.model.EventStatus;
import com.github.highcumontoa.idempotenteventingestionjava.model.IngestionOutcome;
import com.github.highcumontoa.idempotenteventingestionjava.model.IngestionResult;
import com.github.highcumontoa.idempotenteventingestionjava.model.RerunOutcome;
import com.github.highcumontoa.idempotenteventingestionjava.model.RerunRejectionReason;
import com.github.highcumontoa.idempotenteventingestionjava.model.RerunResult;
import com.github.highcumontoa.idempotenteventingestionjava.model.RoundTerminationRecord;
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
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 多轮重跑的逐轮历史保留测试：
 * 跑过两轮以上重跑后，每一轮的终止原因、该轮处理次数、该轮失败信息都能单独看清，
 * 各轮互不覆盖、互不串轮；历史在服务重启后完整保留且事件继续处理；
 * 并发重跑/重复接入/后台重试下历史不被冲掉、四状态计数恒平。
 * 全程日志打印幂等键、每轮处理/重试次数与判定依据。
 */
class RerunHistoryTest {

    private static final Logger log = LoggerFactory.getLogger(RerunHistoryTest.class);

    /**
     * 按“累计尝试次数”编排行为的处理器：脚本命中则按约定失败，否则成功记账。
     * 账本按幂等键去重，保证副作用恰好一次。
     */
    private static final class ScriptedHandler implements EventHandler {
        private final Map<Integer, String> script = new ConcurrentHashMap<>();
        private final Map<String, String> ledger = new ConcurrentHashMap<>();
        private final AtomicLong sequence = new AtomicLong();
        private final AtomicInteger appliedEffects = new AtomicInteger();

        /** attempt 从 1 起计；动作前缀 R:=可重试失败，F:=不可重试失败。 */
        ScriptedHandler onAttempt(int attempt, String action) {
            script.put(attempt, action);
            return this;
        }

        @Override
        public String handle(EventRecord record) throws EventProcessingException {
            String key = record.getIdempotencyKey();
            String existing = ledger.get(key);
            if (existing != null) {
                return existing;
            }
            String action = script.get(record.getAttempts());
            if (action != null) {
                if (action.startsWith("R:")) {
                    throw new EventProcessingException(action.substring(2), true);
                }
                if (action.startsWith("F:")) {
                    throw new EventProcessingException(action.substring(2), false);
                }
            }
            String voucher = "LEDGER-" + sequence.incrementAndGet();
            String winner = ledger.putIfAbsent(key, voucher);
            if (winner != null) {
                return winner;
            }
            appliedEffects.incrementAndGet();
            return voucher;
        }

        int appliedEffects() {
            return appliedEffects.get();
        }
    }

    private record Harness(FileEventStore store, EventIngestionService ingest, EventRerunService rerun,
                           EventProcessingService processor, EventQueryService query,
                           ScriptedHandler handler, MutableClock clock) {
    }

    private Harness harness(Path file, MutableClock clock, int maxAttempts, int maxReruns,
                            int maxQueueCapacity, ScriptedHandler handler) {
        FileEventStore store = new FileEventStore(file, clock);
        RetryBackoffPolicy policy =
                new RetryBackoffPolicy(maxAttempts, Duration.ofMillis(100), Duration.ofSeconds(5), 2.0);
        EventProcessingService processor = new EventProcessingService(store, handler, policy, clock);
        EventingProperties props = new EventingProperties();
        props.setMaxAttempts(maxAttempts);
        props.setMaxReruns(maxReruns);
        props.setMaxQueueCapacity(maxQueueCapacity);
        EventIngestionService ingest = new EventIngestionService(store, processor, props, clock);
        EventRerunService rerun = new EventRerunService(store, props, clock);
        return new Harness(store, ingest, rerun, processor, new EventQueryService(store), handler, clock);
    }

    /** 处理一轮直到终止：每次失败后推进时钟越过退避再处理，直到事件离开 QUEUED。 */
    private void drainRound(Harness h, String key) {
        for (int i = 0; i < 20; i++) {
            EventRecord r = h.query.get(key);
            if (r.getStatus() != EventStatus.QUEUED) {
                return;
            }
            h.clock.advance(Duration.ofMillis(150));
            h.processor.processDueBatch();
        }
        throw new IllegalStateException("事件未在预期批次内离开QUEUED: " + key);
    }

    @Test
    void multiRoundHistoryKeepsEachRoundDistinct(@TempDir Path dir) {
        MutableClock clock = MutableClock.startAt(Instant.parse("2026-10-04T00:00:00Z"));
        // 三轮三种失败形态：首轮2次可重试失败、第2轮1次不可重试失败、第3轮2次可重试失败
        ScriptedHandler handler = new ScriptedHandler()
                .onAttempt(1, "R:首轮-下游超时-第1次")
                .onAttempt(2, "R:首轮-下游超时-第2次")
                .onAttempt(3, "F:第二轮-脏数据")
                .onAttempt(4, "R:第三轮-抖动-第1次")
                .onAttempt(5, "R:第三轮-抖动-第2次");
        Harness h = harness(dir.resolve("events.dat"), clock, 2, 5, 100, handler);
        String key = "history-multi-round";

        // 首轮：2 次可重试失败，RETRY_EXHAUSTED 终止
        h.ingest.ingest(key, "SCRIPTED");
        drainRound(h, key);
        EventRecord t0 = h.query.get(key);
        assertEquals(EventStatus.TERMINATED, t0.getStatus());
        assertEquals(TerminationReason.RETRY_EXHAUSTED, t0.getTerminationReason());
        assertEquals(2, t0.getAttempts());
        assertEquals(1, t0.getRoundHistory().size());
        log.info("[测试] 幂等键={} 轮次=0 本轮处理次数=2 累计=2 判定依据=首轮RETRY_EXHAUSTED终止,历史已归档1条", key);

        // 第 1 轮重跑：1 次不可重试失败，NON_RETRYABLE 终止
        assertEquals(RerunOutcome.ACCEPTED, h.rerun.rerun(key).outcome());
        drainRound(h, key);
        EventRecord t1 = h.query.get(key);
        assertEquals(EventStatus.TERMINATED, t1.getStatus());
        assertEquals(TerminationReason.NON_RETRYABLE, t1.getTerminationReason());
        assertEquals(3, t1.getAttempts());
        assertEquals(2, t1.getRoundHistory().size());
        log.info("[测试] 幂等键={} 轮次=1 本轮处理次数=1 累计=3 判定依据=重跑轮NON_RETRYABLE终止,历史2条互不覆盖", key);

        // 第 2 轮重跑：2 次可重试失败，再次 RETRY_EXHAUSTED 终止
        assertEquals(RerunOutcome.ACCEPTED, h.rerun.rerun(key).outcome());
        drainRound(h, key);
        EventRecord t2 = h.query.get(key);
        assertEquals(EventStatus.TERMINATED, t2.getStatus());
        assertEquals(TerminationReason.RETRY_EXHAUSTED, t2.getTerminationReason());
        assertEquals(5, t2.getAttempts());
        assertEquals(2, t2.getRoundAttempts());
        assertEquals(2, t2.getRerunCount());

        // 逐轮历史：三轮各自独立，终止原因/本轮次数/失败信息都能单独看清
        List<RoundTerminationRecord> history = t2.getRoundHistory();
        assertEquals(3, history.size(), "三轮终止必须各留一条历史");

        RoundTerminationRecord r0 = history.get(0);
        assertEquals(0, r0.getRound());
        assertEquals(TerminationReason.RETRY_EXHAUSTED, r0.getTerminationReason());
        assertEquals(2, r0.getRoundAttempts());
        assertEquals(2, r0.getTotalAttemptsAtTermination());
        assertEquals("首轮-下游超时-第2次", r0.getLastError());
        assertTrue(r0.isLastErrorRetryable());
        assertNotNull(r0.getTerminatedAt());

        RoundTerminationRecord r1 = history.get(1);
        assertEquals(1, r1.getRound());
        assertEquals(TerminationReason.NON_RETRYABLE, r1.getTerminationReason());
        assertEquals(1, r1.getRoundAttempts());
        assertEquals(3, r1.getTotalAttemptsAtTermination());
        assertEquals("第二轮-脏数据", r1.getLastError());
        assertTrue(!r1.isLastErrorRetryable());

        RoundTerminationRecord r2 = history.get(2);
        assertEquals(2, r2.getRound());
        assertEquals(TerminationReason.RETRY_EXHAUSTED, r2.getTerminationReason());
        assertEquals(2, r2.getRoundAttempts());
        assertEquals(5, r2.getTotalAttemptsAtTermination());
        assertEquals("第三轮-抖动-第2次", r2.getLastError());
        assertTrue(!r2.getTerminatedAt().isBefore(r1.getTerminatedAt()));

        // 口径对账：各轮处理次数之和 == 累计处理次数；轮次编号连续不重不漏
        int sum = history.stream().mapToInt(RoundTerminationRecord::getRoundAttempts).sum();
        assertEquals(t2.getAttempts(), sum, "各轮处理次数之和必须等于累计处理次数");
        for (int i = 0; i < history.size(); i++) {
            assertEquals(i, history.get(i).getRound(), "轮次编号必须连续且不重不漏");
        }
        log.info("[测试] 幂等键={} 重跑轮数=2 累计处理次数=5 判定依据=三轮历史各自独立: "
                        + "轮0=RETRY_EXHAUSTED/2次 轮1=NON_RETRYABLE/1次 轮2=RETRY_EXHAUSTED/2次, 各轮之和=累计",
                key);

        EventStats stats = h.query.stats();
        assertEquals(1, stats.total());
        assertEquals(1, stats.terminated());
        assertEquals(0, h.handler.appliedEffects());
    }

    @Test
    void roundHistorySurvivesRestartAndEventContinues(@TempDir Path dir) {
        Path file = dir.resolve("events-restart.dat");
        MutableClock clock = MutableClock.startAt(Instant.parse("2026-10-04T00:00:00Z"));
        ScriptedHandler handler = new ScriptedHandler()
                .onAttempt(1, "R:首轮-超时")
                .onAttempt(2, "R:首轮-超时再试")
                .onAttempt(3, "F:第二轮-坏数据");
        String key = "history-restart";

        // 第一次“启动”：首轮终止 -> 重跑 -> 第2轮终止 -> 再重跑入队后立刻模拟重启
        Harness first = harness(file, clock, 2, 5, 100, handler);
        first.ingest.ingest(key, "SCRIPTED");
        drainRound(first, key);
        assertEquals(RerunOutcome.ACCEPTED, first.rerun.rerun(key).outcome());
        drainRound(first, key);
        assertEquals(RerunOutcome.ACCEPTED, first.rerun.rerun(key).outcome());
        EventRecord beforeRestart = first.query.get(key);
        assertEquals(EventStatus.QUEUED, beforeRestart.getStatus());
        assertEquals(2, beforeRestart.getRerunCount());
        assertEquals(3, beforeRestart.getAttempts());
        List<RoundTerminationRecord> historyBefore = beforeRestart.getRoundHistory();
        assertEquals(2, historyBefore.size());
        log.info("[测试] 幂等键={} 重跑轮数=2 累计处理次数=3 判定依据=两轮历史已归档,重跑已入队,模拟服务重启", key);

        // 第二次“启动”：每一轮历史必须逐字段一致地恢复
        Harness second = harness(file, clock, 2, 5, 100, handler);
        EventRecord recovered = second.query.get(key);
        assertNotNull(recovered);
        assertEquals(EventStatus.QUEUED, recovered.getStatus());
        assertEquals(2, recovered.getRerunCount());
        assertEquals(3, recovered.getAttempts());
        assertEquals(0, recovered.getRoundAttempts());
        assertNotNull(recovered.getNextAttemptAt());
        List<RoundTerminationRecord> historyAfter = recovered.getRoundHistory();
        assertEquals(historyBefore.size(), historyAfter.size(), "重启后历史条数必须一致");
        for (int i = 0; i < historyBefore.size(); i++) {
            RoundTerminationRecord b = historyBefore.get(i);
            RoundTerminationRecord a = historyAfter.get(i);
            assertEquals(b.getRound(), a.getRound());
            assertEquals(b.getTerminationReason(), a.getTerminationReason());
            assertEquals(b.getRoundAttempts(), a.getRoundAttempts());
            assertEquals(b.getTotalAttemptsAtTermination(), a.getTotalAttemptsAtTermination());
            assertEquals(b.getLastError(), a.getLastError());
            assertEquals(b.isLastErrorRetryable(), a.isLastErrorRetryable());
            assertEquals(b.getTerminatedAt(), a.getTerminatedAt());
        }
        log.info("[测试] 幂等键={} 判定依据=重启后逐轮历史完整恢复: 轮0={}/{}次 轮1={}/{}次",
                key, historyAfter.get(0).getTerminationReason(), historyAfter.get(0).getRoundAttempts(),
                historyAfter.get(1).getTerminationReason(), historyAfter.get(1).getRoundAttempts());

        // 重启后本该继续的事件照常处理：第 3 轮（重跑轮数=2）成功，历史不被成功轮冲掉
        clock.advance(Duration.ofSeconds(1));
        second.processor.processDueBatch();
        EventRecord done = second.query.get(key);
        assertEquals(EventStatus.SUCCEEDED, done.getStatus());
        assertNotNull(done.getResult());
        assertEquals(4, done.getAttempts());
        assertEquals(1, done.getRoundAttempts());
        assertEquals(2, done.getRerunCount());
        assertEquals(2, done.getRoundHistory().size(), "成功轮不产生终止历史,已有历史必须原样保留");
        assertEquals(1, second.handler.appliedEffects());
        log.info("[测试] 幂等键={} 重跑轮数=2 本轮处理次数=1 累计=4 判定依据=重启后继续处理并成功,凭证={} 历史仍2条",
                key, done.getResult());

        EventStats stats = second.query.stats();
        assertEquals(1, stats.total());
        assertEquals(1, stats.succeeded());
    }

    @Test
    void concurrentRerunIngestAndRetryKeepHistoryAndStatsConsistent(@TempDir Path dir) throws Exception {
        MutableClock clock = MutableClock.startAt(Instant.parse("2026-10-04T00:00:00Z"));
        // 目标事件 K：第 1、2 次（首轮）与第 3、4 次（重跑轮）都可重试失败
        ScriptedHandler handler = new ScriptedHandler()
                .onAttempt(1, "R:首轮-超时1").onAttempt(2, "R:首轮-超时2")
                .onAttempt(3, "R:重跑轮-超时1").onAttempt(4, "R:重跑轮-超时2");
        Harness h = harness(dir.resolve("events.dat"), clock, 2, 3, 100, handler);
        String key = "history-concurrent";

        // 先制造一条终止事件（首轮历史归档），再制造 5 条后台重试事件
        // （脚本按各自记录的累计次数命中：后台事件第 1、2 次尝试同样可重试失败，随后终止）
        h.ingest.ingest(key, "SCRIPTED");
        drainRound(h, key);
        assertEquals(EventStatus.TERMINATED, h.query.get(key).getStatus());
        assertEquals(1, h.query.get(key).getRoundHistory().size());
        for (int i = 0; i < 5; i++) {
            h.ingest.ingest("bg-" + i, "SCRIPTED");
        }
        log.info("[测试] 幂等键={} 判定依据=首轮终止历史1条,另有5条后台重试事件在途,开始并发阶段", key);

        int rerunThreads = 8;
        int ingestThreads = 8;
        ExecutorService pool = Executors.newFixedThreadPool(rerunThreads + ingestThreads + 1);
        CountDownLatch start = new CountDownLatch(1);
        AtomicInteger accepted = new AtomicInteger();
        AtomicInteger notTerminated = new AtomicInteger();
        AtomicInteger duplicates = new AtomicInteger();
        ConcurrentLinkedQueue<String> unexpected = new ConcurrentLinkedQueue<>();
        AtomicInteger statsViolations = new AtomicInteger();

        for (int i = 0; i < rerunThreads; i++) {
            pool.submit(() -> {
                try {
                    start.await();
                    RerunResult r = h.rerun.rerun(key);
                    if (r.outcome() == RerunOutcome.ACCEPTED) {
                        accepted.incrementAndGet();
                    } else if (r.rejectionReason() == RerunRejectionReason.NOT_TERMINATED) {
                        notTerminated.incrementAndGet();
                    } else {
                        unexpected.add("意外重跑结果: " + r.outcome() + "/" + r.rejectionReason());
                    }
                } catch (Exception e) {
                    unexpected.add(e.toString());
                }
            });
        }
        for (int i = 0; i < ingestThreads; i++) {
            pool.submit(() -> {
                try {
                    start.await();
                    IngestionResult r = h.ingest.ingest(key, "SCRIPTED");
                    if (r.outcome() == IngestionOutcome.DUPLICATE) {
                        duplicates.incrementAndGet();
                    } else {
                        unexpected.add("意外接入结果: " + r.outcome());
                    }
                } catch (Exception e) {
                    unexpected.add(e.toString());
                }
            });
        }
        // 后台重试线程：并发期间持续处理到期事件，并不停校验四状态计数加得平
        pool.submit(() -> {
            try {
                start.await();
                for (int i = 0; i < 40; i++) {
                    h.clock.advance(Duration.ofMillis(120));
                    h.processor.processDueBatch();
                    EventStats s = h.query.stats();
                    if (s.queued() + s.processing() + s.succeeded() + s.terminated() != s.total()) {
                        statsViolations.incrementAndGet();
                    }
                    Thread.sleep(2);
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });
        start.countDown();
        pool.shutdown();
        assertTrue(pool.awaitTermination(20, TimeUnit.SECONDS), "并发阶段未在时限内完成");

        log.info("[测试] 幂等键={} 判定依据=并发{}次重跑+{}次重复接入+后台重试: ACCEPTED={} NOT_TERMINATED={} DUPLICATE={}",
                key, rerunThreads, ingestThreads, accepted.get(), notTerminated.get(), duplicates.get());
        assertEquals(1, accepted.get(), "同一事件只能真正重新入队一次");
        assertEquals(rerunThreads - 1, notTerminated.get());
        assertEquals(ingestThreads, duplicates.get(), "重复投递必须全部判重");
        assertTrue(unexpected.isEmpty(), "不应出现意外结果: " + unexpected);
        assertEquals(0, statsViolations.get(), "任何时刻四状态计数都必须加得平");

        // 排干所有到期事件后：目标事件第 2 轮也终止，两轮历史各自独立、未被并发冲掉
        for (int i = 0; i < 30; i++) {
            clock.advance(Duration.ofMillis(150));
            h.processor.processDueBatch();
        }
        EventRecord k = h.query.get(key);
        assertEquals(EventStatus.TERMINATED, k.getStatus());
        assertEquals(1, k.getRerunCount());
        assertEquals(4, k.getAttempts());
        List<RoundTerminationRecord> history = k.getRoundHistory();
        assertEquals(2, history.size(), "首轮与重跑轮必须各留一条历史,不允许被并发冲掉或混轮");
        assertEquals(0, history.get(0).getRound());
        assertEquals(2, history.get(0).getRoundAttempts());
        assertEquals("首轮-超时2", history.get(0).getLastError());
        assertEquals(1, history.get(1).getRound());
        assertEquals(2, history.get(1).getRoundAttempts());
        assertEquals("重跑轮-超时2", history.get(1).getLastError());
        assertEquals(k.getAttempts(),
                history.stream().mapToInt(RoundTerminationRecord::getRoundAttempts).sum());
        log.info("[测试] 幂等键={} 重跑轮数=1 累计处理次数=4 判定依据=并发后两轮历史完整: 轮0=2次 轮1=2次,累计口径对账一致",
                key);

        EventStats stats = h.query.stats();
        assertEquals(6, stats.total());
        assertEquals(6, stats.terminated());
        assertEquals(0, stats.queued() + stats.processing() + stats.succeeded());
        assertNull(h.query.get("no-such-key"));
    }
}
