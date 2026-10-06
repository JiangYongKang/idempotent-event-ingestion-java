package com.github.highcumontoa.idempotenteventingestionjava.tests;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.github.highcumontoa.idempotenteventingestionjava.config.EventingProperties;
import com.github.highcumontoa.idempotenteventingestionjava.handler.EventHandler;
import com.github.highcumontoa.idempotenteventingestionjava.handler.EventProcessingException;
import com.github.highcumontoa.idempotenteventingestionjava.model.EventRecord;
import com.github.highcumontoa.idempotenteventingestionjava.model.EventStatus;
import com.github.highcumontoa.idempotenteventingestionjava.model.RoundHistory;
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
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 多轮重跑逐轮历史保留测试：
 * 一条事件反复重跑多轮后，每一轮的终止原因、本轮处理次数、失败信息都独立可查、互不覆盖互不串轮；
 * 累计处理次数与当前重跑轮数始终对得上；逐轮历史重启后完整保留，重启后事件继续处理不停滞；
 * 并发重跑/接入/后台重试同时发生时只真正重新入队一次、统计恒平、历史不被冲掉。
 *
 * <p>为让每一轮的失败信息可逐轮区分，使用按“重跑轮数”脚本化的处理器：
 * 每轮抛出带轮次标记的不同错误文本，从而可以严格断言“哪一轮发生了什么、不串轮”。</p>
 */
class MultiRerunHistoryTest {

    private static final Logger log = LoggerFactory.getLogger(MultiRerunHistoryTest.class);

    @AfterAll
    static void coverageSummary() {
        log.info("""
                [测试覆盖汇总] 多轮重跑逐轮历史业务覆盖:
                 1) 首轮+3轮重跑(共4轮)后逐轮查询: 每轮终止原因(RETRY_EXHAUSTED/NON_RETRYABLE)、
                    本轮处理次数(2/2/2/1)、本轮失败信息各自独立,互不覆盖互不串轮;
                 2) 累计口径: attempts=各轮次数之和,rerunCount 始终对应当前轮数,轮数到顶拒绝不改历史;
                 3) 重启恢复: 终止后重启逐轮历史(原因/次数/失败信息/时刻)完全一致;
                    某轮重试排队中途重启,已结束轮次历史完整、事件继续跑完本轮不停滞;
                 4) 成功轮归档: 重跑轮先失败后成功时,成功条目保留本轮失败信息、记账恰好一次,成功后拒绝重跑;
                 5) 并发: 多人并发重跑(每波12线程,共3波)只真正入队一次,与并发同键接入(DUPLICATE)、
                    后台重试同时发生,历史不被冲掉、轮次严格升序,四种状态计数任何时刻加得平。""");
    }

    private record Harness(FileEventStore store, EventIngestionService ingest, EventRerunService rerun,
                           EventProcessingService processor, EventQueryService query,
                           EventHandler handler, MutableClock clock) {
    }

    private Harness harness(Path file, MutableClock clock, int maxAttempts, int maxReruns,
                            int maxQueueCapacity, EventHandler handler) {
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

    /**
     * 按“当前重跑轮数”脚本化的处理器（仅服务于指定幂等键，其余键退化为成功记账）：
     * <ul>
     *   <li>轮次 0/1/2：前 2 次可重试失败（错误文本带轮次标记），到上限后 RETRY_EXHAUSTED 终止；</li>
     *   <li>轮次 3：第 1 次即不可重试失败，NON_RETRYABLE 立即终止。</li>
     * </ul>
     * 每轮错误文本都不同，可严格验证逐轮失败信息不串轮。
     */
    private static final class RoundScriptedHandler implements EventHandler {
        private final String targetKey;
        private RoundScriptedHandler(String targetKey) {
            this.targetKey = targetKey;
        }

        @Override
        public String handle(EventRecord record) throws EventProcessingException {
            String key = record.getIdempotencyKey();
            if (!key.equals(targetKey)) {
                return "LEDGER-OTHER";
            }
            int round = record.getRerunCount();
            int roundAttempt = record.getRoundAttempts();
            switch (round) {
                case 0, 1, 2 -> throw new EventProcessingException(
                        "可重试错误:round=" + round + ",roundAttempt=" + roundAttempt, true);
                case 3 -> throw new EventProcessingException(
                        "不可重试错误:round=" + round + ",roundAttempt=" + roundAttempt, false);
                default -> throw new IllegalStateException("未脚本化的轮次: " + round);
            }
        }
    }

    /** 驱动一轮重试到终止：每次处理后推进时钟越过退避，直到该事件离开 QUEUED。 */
    private void runRoundToEnd(Harness h, String key) {
        for (int guard = 0; guard < 50; guard++) {
            h.processor.processDueBatch();
            EventStatus s = h.query.get(key).getStatus();
            if (s != EventStatus.QUEUED && s != EventStatus.PROCESSING) {
                return;
            }
            h.clock.advance(Duration.ofSeconds(1));
        }
        throw new IllegalStateException("事件在限定步数内未到达终态: " + h.query.get(key).getStatus());
    }

    @Test
    void eachRerunRoundKeepsIndependentTerminationAttemptsAndError(@TempDir Path dir) {
        MutableClock clock = MutableClock.startAt(Instant.parse("2026-10-05T00:00:00Z"));
        Path file = dir.resolve("multi-round.dat");
        String key = "multi-round-key";
        RoundScriptedHandler handler = new RoundScriptedHandler(key);
        // maxAttempts=2：轮 0/1/2 各 2 次尝试后 RETRY_EXHAUSTED；maxReruns=3 允许 3 轮重跑。
        Harness h = harness(file, clock, 2, 3, 100, handler);

        h.ingest.ingest(key, "SCRIPTED");
        runRoundToEnd(h, key);
        assertEquals(EventStatus.TERMINATED, h.query.get(key).getStatus());

        // 连续发起 3 轮重跑：每轮跑完再次终止后再发起下一轮
        for (int round = 1; round <= 3; round++) {
            assertNotNull(h.rerun.rerun(key));
            runRoundToEnd(h, key);
            EventRecord r = h.query.get(key);
            assertEquals(EventStatus.TERMINATED, r.getStatus(), "第 " + round + " 轮重跑应再次终止");
            assertEquals(round, r.getRerunCount(), "当前重跑轮数应等于 " + round);
            log.info("[测试] 幂等键={} 重跑轮数={} 判定依据=该轮已终止,累计尝试={} 已归档轮次={}",
                    key, round, r.getAttempts(), r.getRoundHistory().size());
        }

        EventRecord finalRecord = h.query.get(key);
        List<RoundHistory> history = finalRecord.getRoundHistory();
        log.info("[测试] 幂等键={} 判定依据=多轮重跑结束,逐轮历史条目数={} 累计尝试={} 重跑轮数={}",
                key, history.size(), finalRecord.getAttempts(), finalRecord.getRerunCount());

        // ---- 逐轮历史：4 条（首轮 + 3 轮重跑），各轮独立、互不覆盖 ----
        assertEquals(4, history.size(), "首轮+3轮重跑应留下 4 条独立历史");
        for (int i = 0; i < history.size(); i++) {
            assertEquals(i, history.get(i).getRound(), "历史条目必须按轮次升序且不串轮");
            assertNotNull(history.get(i).getFinishedAt(), "每轮结束时刻必须保留");
        }

        // 轮 0/1/2：各 2 次尝试、RETRY_EXHAUSTED、失败信息带本轮标记（不串轮）
        for (int round = 0; round <= 2; round++) {
            RoundHistory rh = history.get(round);
            assertEquals(2, rh.getAttempts(), "第 " + round + " 轮处理次数应为 2");
            assertEquals(EventStatus.TERMINATED, rh.getOutcome());
            assertEquals(TerminationReason.RETRY_EXHAUSTED, rh.getTerminationReason(),
                    "第 " + round + " 轮终止原因应独立保留");
            assertTrue(rh.getLastError().contains("round=" + round),
                    "第 " + round + " 轮失败信息必须属于本轮，实际=" + rh.getLastError());
            assertTrue(rh.isLastErrorRetryable(), "第 " + round + " 轮最后一次失败应可重试");
            log.info("[测试] 幂等键={} 轮次={} 本轮处理次数=2 终止原因=RETRY_EXHAUSTED 失败信息='{}' 判定=逐轮独立不串轮",
                    key, round, rh.getLastError());
        }

        // 轮 3：1 次尝试即 NON_RETRYABLE，且失败信息只属于第 3 轮
        RoundHistory round3 = history.get(3);
        assertEquals(1, round3.getAttempts(), "第 3 轮应只处理 1 次即不可重试终止");
        assertEquals(TerminationReason.NON_RETRYABLE, round3.getTerminationReason());
        assertTrue(round3.getLastError().contains("round=3"),
                "第 3 轮失败信息必须属于本轮，实际=" + round3.getLastError());
        assertTrue(!round3.isLastErrorRetryable());
        log.info("[测试] 幂等键={} 轮次=3 本轮处理次数=1 终止原因=NON_RETRYABLE 失败信息='{}' 判定=与前轮原因/次数可区分",
                key, round3.getLastError());

        // ---- 累计口径：2+2+2+1=7，当前第 3 轮重跑，本轮次数=1 ----
        assertEquals(7, finalRecord.getAttempts(), "累计处理次数应为各轮之和 2+2+2+1=7");
        assertEquals(3, finalRecord.getRerunCount());
        assertEquals(1, finalRecord.getRoundAttempts());
        int sumOfRounds = history.stream().mapToInt(RoundHistory::getAttempts).sum();
        assertEquals(finalRecord.getAttempts(), sumOfRounds, "累计次数必须等于逐轮次数之和");
        log.info("[测试] 幂等键={} 判定依据=累计尝试=7=各轮(2+2+2+1)之和,重跑轮数=3,口径对得上", key);

        // 轮数到顶再重跑：拒绝且历史与计数不变
        var limitResult = h.rerun.rerun(key);
        assertEquals(com.github.highcumontoa.idempotenteventingestionjava.model.RerunOutcome.REJECTED,
                limitResult.outcome());
        assertEquals(com.github.highcumontoa.idempotenteventingestionjava.model.RerunRejectionReason.RERUN_LIMIT_REACHED,
                limitResult.rejectionReason());
        assertEquals(4, h.query.get(key).getRoundHistory().size(), "拒绝重跑不得改动历史");
        assertEquals(7, h.query.get(key).getAttempts());
        log.info("[测试] 幂等键={} 判定依据=轮数到顶RERUN_LIMIT_REACHED,历史4条/累计7次保持不变", key);

        // ---- 重启后逐轮历史完整一致 ----
        Harness restarted = harness(file, clock, 2, 3, 100, handler);
        EventRecord recovered = restarted.query.get(key);
        assertEquals(EventStatus.TERMINATED, recovered.getStatus());
        assertEquals(7, recovered.getAttempts());
        assertEquals(3, recovered.getRerunCount());
        List<RoundHistory> recoveredHistory = recovered.getRoundHistory();
        assertEquals(4, recoveredHistory.size(), "重启后 4 条逐轮历史必须完整保留");
        for (int i = 0; i < 4; i++) {
            RoundHistory a = history.get(i);
            RoundHistory b = recoveredHistory.get(i);
            assertEquals(a.getRound(), b.getRound());
            assertEquals(a.getAttempts(), b.getAttempts());
            assertEquals(a.getOutcome(), b.getOutcome());
            assertEquals(a.getTerminationReason(), b.getTerminationReason());
            assertEquals(a.getLastError(), b.getLastError(), "第 " + i + " 轮失败信息重启后必须一致");
            assertEquals(a.isLastErrorRetryable(), b.isLastErrorRetryable());
            assertEquals(a.getStartedAt(), b.getStartedAt());
            assertEquals(a.getFinishedAt(), b.getFinishedAt());
        }
        log.info("[测试] 幂等键={} 判定依据=重启后逐轮历史4条/每轮原因次数失败信息/时刻完全一致,无丢失无串轮", key);

        // 统计恒平
        var stats = restarted.query.stats();
        assertEquals(1, stats.total());
        assertEquals(1, stats.terminated());
        assertEquals(1, stats.queued() + stats.processing() + stats.succeeded() + stats.terminated());
    }

    @Test
    void historySurvivesRestartMidRerunRoundAndEventContinues(@TempDir Path dir) {
        MutableClock clock = MutableClock.startAt(Instant.parse("2026-10-05T08:00:00Z"));
        Path file = dir.resolve("multi-round-restart.dat");
        String key = "multi-round-restart-key";
        RoundScriptedHandler handler = new RoundScriptedHandler(key);
        Harness h = harness(file, clock, 2, 3, 100, handler);

        // 首轮：2 次尝试后 RETRY_EXHAUSTED 终止 -> 归档轮次 0
        h.ingest.ingest(key, "SCRIPTED");
        runRoundToEnd(h, key);
        assertEquals(2, h.query.get(key).getAttempts());
        assertEquals(1, h.query.get(key).getRoundHistory().size());

        // 第 1 轮重跑：只处理 1 次（可重试失败，退避排队中），此时“重启”
        assertNotNull(h.rerun.rerun(key));
        h.processor.processDueBatch();
        EventRecord mid = h.query.get(key);
        assertEquals(EventStatus.QUEUED, mid.getStatus());
        assertEquals(1, mid.getRerunCount());
        assertEquals(1, mid.getRoundAttempts());
        assertEquals(3, mid.getAttempts());
        log.info("[测试] 幂等键={} 重跑轮数=1 本轮处理次数=1 累计=3 判定依据=重试退避排队中途模拟重启,已归档轮次=1", key);

        // 重启：已结束的轮次 0 历史必须原样保留；在途这一轮不产生半截历史
        Harness restarted = harness(file, clock, 2, 3, 100, handler);
        EventRecord recovered = restarted.query.get(key);
        assertEquals(EventStatus.QUEUED, recovered.getStatus());
        assertEquals(1, recovered.getRerunCount());
        assertEquals(1, recovered.getRoundAttempts(), "在途轮次的本轮次数不丢");
        assertEquals(3, recovered.getAttempts(), "累计次数不丢");
        assertNotNull(recovered.getNextAttemptAt(), "到期时刻必须恢复，事件才能继续前进");
        List<RoundHistory> historyAfterRestart = recovered.getRoundHistory();
        assertEquals(1, historyAfterRestart.size(), "重启后只能看到已结束的轮次 0，历史不重复不丢失");
        RoundHistory round0 = historyAfterRestart.get(0);
        assertEquals(0, round0.getRound());
        assertEquals(2, round0.getAttempts());
        assertEquals(TerminationReason.RETRY_EXHAUSTED, round0.getTerminationReason());
        assertTrue(round0.getLastError().contains("round=0"),
                "轮次 0 失败信息必须仍是本轮的，实际=" + round0.getLastError());
        log.info("[测试] 幂等键={} 判定依据=重启后轮次0历史(终止原因/次数2/失败信息)完整,事件停在QUEUED等待继续", key);

        // 推进到点后继续把本轮跑完：第 4 次尝试（本轮第 2 次）后再次终止，事件没有停在原地
        clock.advance(Duration.ofSeconds(1));
        restarted.processor.processDueBatch();
        EventRecord done = restarted.query.get(key);
        assertEquals(EventStatus.TERMINATED, done.getStatus());
        assertEquals(TerminationReason.RETRY_EXHAUSTED, done.getTerminationReason());
        assertEquals(4, done.getAttempts());
        assertEquals(2, done.getRoundAttempts());
        assertEquals(1, done.getRerunCount());

        // 重启后续跑的轮次 1 作为独立条目追加，与轮次 0 互不覆盖
        List<RoundHistory> finalHistory = done.getRoundHistory();
        assertEquals(2, finalHistory.size(), "重启后续跑的轮次必须追加为独立历史");
        assertEquals(0, finalHistory.get(0).getRound());
        assertEquals(2, finalHistory.get(0).getAttempts());
        assertTrue(finalHistory.get(0).getLastError().contains("round=0"));
        assertEquals(1, finalHistory.get(1).getRound());
        assertEquals(2, finalHistory.get(1).getAttempts());
        assertEquals(TerminationReason.RETRY_EXHAUSTED, finalHistory.get(1).getTerminationReason());
        assertTrue(finalHistory.get(1).getLastError().contains("round=1"),
                "轮次 1 失败信息必须属于本轮，实际=" + finalHistory.get(1).getLastError());
        assertEquals(4, finalHistory.stream().mapToInt(RoundHistory::getAttempts).sum(),
                "累计次数仍等于各轮次数之和");
        log.info("[测试] 幂等键={} 判定依据=重启后事件继续跑完轮次1,两轮历史各自独立(round0/round1失败信息不串),累计=4", key);
    }

    @Test
    void concurrentRerunWithIngestionAndWorkerEnqueuesOnceAndKeepsHistoryIntact(@TempDir Path dir)
            throws Exception {
        MutableClock clock = MutableClock.startAt(Instant.parse("2026-10-05T16:00:00Z"));
        Path file = dir.resolve("multi-round-concurrent.dat");
        String key = "multi-round-concurrent-key";
        RoundScriptedHandler handler = new RoundScriptedHandler(key);
        // maxAttempts=1：脚本化轮次 0/1/2 每次处理即终止，方便在并发中快速进入“可重跑”窗口
        Harness h = harness(file, clock, 1, 3, 1000, handler);

        h.ingest.ingest(key, "SCRIPTED");
        h.processor.processDueBatch();
        assertEquals(EventStatus.TERMINATED, h.query.get(key).getStatus());
        assertEquals(TerminationReason.RETRY_EXHAUSTED, h.query.get(key).getTerminationReason());
        assertEquals(1, h.query.get(key).getAttempts());
        log.info("[测试] 幂等键={} 判定依据=首轮1次终止,具备并发重跑前提", key);

        java.util.concurrent.ExecutorService pool =
                java.util.concurrent.Executors.newFixedThreadPool(4);
        java.util.concurrent.ConcurrentLinkedQueue<String> failures =
                new java.util.concurrent.ConcurrentLinkedQueue<>();
        java.util.concurrent.atomic.AtomicBoolean stop = new java.util.concurrent.atomic.AtomicBoolean();
        // 重跑突发窗口内短暂挂起 worker 的“处理动作”，保证每个终止窗口只被一波重跑消费一次；
        // worker 线程仍在并发扫描，接入线程与统计线程全程不停。
        java.util.concurrent.atomic.AtomicBoolean workerPaused =
                new java.util.concurrent.atomic.AtomicBoolean(false);
        java.util.concurrent.atomic.AtomicInteger inconsistent = new java.util.concurrent.atomic.AtomicInteger();
        java.util.concurrent.atomic.AtomicInteger acceptedReruns = new java.util.concurrent.atomic.AtomicInteger();
        java.util.concurrent.atomic.AtomicInteger notTerminated = new java.util.concurrent.atomic.AtomicInteger();
        java.util.concurrent.atomic.AtomicInteger duplicates = new java.util.concurrent.atomic.AtomicInteger();

        // 后台重试 worker：退避 100ms，直接推进时钟并不断扫描，保证重跑事件被立刻处理
        pool.submit(() -> {
            while (!stop.get()) {
                try {
                    h.clock.advance(Duration.ofMillis(150));
                    if (!workerPaused.get()) {
                        h.processor.processDueBatch();
                    }
                } catch (RuntimeException e) {
                    failures.add("worker: " + e);
                }
            }
        });
        // 同键并发接入：必须始终判为 DUPLICATE，绝不产生第二条记录/第二次有效处理
        pool.submit(() -> {
            while (!stop.get()) {
                try {
                    var r = h.ingest.ingest(key, "SCRIPTED");
                    if (r.outcome() == com.github.highcumontoa.idempotenteventingestionjava.model.IngestionOutcome.ACCEPTED) {
                        failures.add("重复接入竟被 ACCEPTED");
                    } else if (r.outcome() == com.github.highcumontoa.idempotenteventingestionjava.model.IngestionOutcome.DUPLICATE) {
                        duplicates.incrementAndGet();
                    }
                } catch (RuntimeException e) {
                    failures.add("ingest: " + e);
                }
            }
        });
        // 统计读取：四种状态计数任何时刻都必须加得平
        pool.submit(() -> {
            while (!stop.get()) {
                var st = h.query.stats();
                if (st.queued() + st.processing() + st.succeeded() + st.terminated() != st.total()) {
                    inconsistent.incrementAndGet();
                }
                try {
                    Thread.sleep(1);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        });

        // 主驱动线程：每当目标事件终止就放一批并发重跑，共推进 3 轮（每批 12 个线程同时打）
        int rerunThreads = 12;
        for (int targetRound = 1; targetRound <= 3; targetRound++) {
            waitUntilTerminated(h, key, 10_000);
            // 挂起 worker 处理：突发窗口内状态不会被推进到下一个终止窗口，保证“一波只入队一次”确定可验
            workerPaused.set(true);
            java.util.concurrent.ExecutorService burst =
                    java.util.concurrent.Executors.newFixedThreadPool(rerunThreads);
            java.util.concurrent.CountDownLatch gate = new java.util.concurrent.CountDownLatch(1);
            java.util.concurrent.CountDownLatch done = new java.util.concurrent.CountDownLatch(rerunThreads);
            for (int i = 0; i < rerunThreads; i++) {
                burst.submit(() -> {
                    try {
                        gate.await();
                        var rr = h.rerun.rerun(key);
                        if (rr.outcome() == com.github.highcumontoa.idempotenteventingestionjava.model.RerunOutcome.ACCEPTED) {
                            acceptedReruns.incrementAndGet();
                        } else if (rr.rejectionReason()
                                == com.github.highcumontoa.idempotenteventingestionjava.model.RerunRejectionReason.NOT_TERMINATED) {
                            notTerminated.incrementAndGet();
                        } else {
                            failures.add("意外重跑结果: " + rr.outcome() + "/" + rr.rejectionReason());
                        }
                    } catch (Exception e) {
                        failures.add("rerun: " + e);
                    } finally {
                        done.countDown();
                    }
                });
            }
            gate.countDown();
            assertTrue(done.await(10, java.util.concurrent.TimeUnit.SECONDS), "并发重跑未在时限内完成");
            burst.shutdownNow();
            // 突发结束：恢复 worker，让它把本轮跑到下一次终止（或跑满 3 轮）
            workerPaused.set(false);
            log.info("[测试] 幂等键={} 重跑目标轮数={} 判定依据=并发{}次重跑同临界区判定,一波仅一次ACCEPTED",
                    key, targetRound, rerunThreads);
        }
        waitUntilTerminated(h, key, 10_000);
        stop.set(true);
        pool.shutdownNow();

        EventRecord finalRecord = h.query.get(key);
        List<RoundHistory> history = finalRecord.getRoundHistory();
        log.info("[测试] 幂等键={} 判定依据=并发结束 ACCEPTED={} NOT_TERMINATED={} DUPLICATE接入={} "
                        + "统计不一致快照={} 归档轮次={} 累计尝试={} 当前轮={}",
                key, acceptedReruns.get(), notTerminated.get(), duplicates.get(), inconsistent.get(),
                history.size(), finalRecord.getAttempts(), finalRecord.getRerunCount());

        assertTrue(failures.isEmpty(), "并发期间不应出现异常/意外结果: " + failures);
        assertEquals(3, acceptedReruns.get(), "3 个终止窗口各只能真正重新入队一次");
        assertEquals(3 * rerunThreads - 3, notTerminated.get(),
                "其余并发重跑必须全部拿到 NOT_TERMINATED");
        assertEquals(0, inconsistent.get(), "任何时刻四种状态计数必须加得平");
        assertEquals(EventStatus.TERMINATED, finalRecord.getStatus());
        assertEquals(3, finalRecord.getRerunCount());
        assertEquals(4, finalRecord.getAttempts(), "maxAttempts=1 时每轮只处理 1 次，共 4 轮=4 次");

        // 历史没有被并发冲掉：4 条，轮次 0..3 严格升序，每轮次数=1，轮 0/1/2 可重试、轮 3 不可重试
        assertEquals(4, history.size());
        for (int i = 0; i < 4; i++) {
            RoundHistory rh = history.get(i);
            assertEquals(i, rh.getRound());
            assertEquals(1, rh.getAttempts());
            assertEquals(EventStatus.TERMINATED, rh.getOutcome());
            assertTrue(rh.getLastError().contains("round=" + i),
                    "第 " + i + " 轮失败信息必须只属于本轮，实际=" + rh.getLastError());
            TerminationReason expected = i == 3
                    ? TerminationReason.NON_RETRYABLE : TerminationReason.RETRY_EXHAUSTED;
            assertEquals(expected, rh.getTerminationReason(), "第 " + i + " 轮终止原因");
        }
        assertEquals(4, history.stream().mapToInt(RoundHistory::getAttempts).sum());
        log.info("[测试] 幂等键={} 判定依据=4条逐轮历史轮次严格0..3/每轮1次/失败信息各属本轮,并发未冲掉历史", key);
    }

    /**
     * 首轮终止 -> 重跑轮“先失败 1 次、再恢复成功”的处理器：
     * 轮次 0：可重试失败（到上限终止）；轮次 1：本轮第 1 次可重试失败、第 2 次成功记账。
     */
    private static final class RecoverOnRerunHandler implements EventHandler {
        private final String targetKey;
        private final AtomicInteger effects = new AtomicInteger();

        private RecoverOnRerunHandler(String targetKey) {
            this.targetKey = targetKey;
        }

        @Override
        public String handle(EventRecord record) throws EventProcessingException {
            int round = record.getRerunCount();
            int roundAttempt = record.getRoundAttempts();
            if (round == 0) {
                throw new EventProcessingException("首轮错误:round=0,roundAttempt=" + roundAttempt, true);
            }
            if (round == 1 && roundAttempt == 1) {
                throw new EventProcessingException("重跑轮临时错误:round=1,roundAttempt=1", true);
            }
            if (record.getIdempotencyKey().equals(targetKey)) {
                return "LEDGER-RECOVER-" + effects.incrementAndGet();
            }
            throw new IllegalStateException("未脚本化的键/轮次: " + record.getIdempotencyKey() + "/" + round);
        }

        int effectCount() {
            return effects.get();
        }
    }

    @Test
    void successfulRerunRoundIsArchivedIndependentlyAndKeepsItsOwnFailureInfo(@TempDir Path dir) {
        MutableClock clock = MutableClock.startAt(Instant.parse("2026-10-05T23:00:00Z"));
        Path file = dir.resolve("multi-round-success.dat");
        String key = "multi-round-success-key";
        RecoverOnRerunHandler handler = new RecoverOnRerunHandler(key);
        // maxAttempts=2：首轮 2 次终止；重跑轮第 1 次失败、第 2 次恢复成功
        Harness h = harness(file, clock, 2, 3, 100, handler);

        h.ingest.ingest(key, "SCRIPTED-RECOVER");
        runRoundToEnd(h, key);
        EventRecord terminated = h.query.get(key);
        assertEquals(EventStatus.TERMINATED, terminated.getStatus());
        assertEquals(TerminationReason.RETRY_EXHAUSTED, terminated.getTerminationReason());
        log.info("[测试] 幂等键={} 判定依据=首轮2次终止,历史1条,准备重跑", key);

        assertNotNull(h.rerun.rerun(key));
        // 重跑轮第 1 次：本轮临时失败、退避
        h.processor.processDueBatch();
        EventRecord firstFail = h.query.get(key);
        assertEquals(EventStatus.QUEUED, firstFail.getStatus());
        assertEquals(1, firstFail.getRoundAttempts());
        assertTrue(firstFail.getLastError().contains("round=1"),
                "在途轮次的最近失败必须属于本轮，实际=" + firstFail.getLastError());
        log.info("[测试] 幂等键={} 重跑轮数=1 本轮处理次数=1 判定依据=本轮临时失败退避,失败信息属于本轮不串到首轮", key);

        // 第 2 次：恢复成功，本轮到达结局
        h.clock.advance(Duration.ofSeconds(1));
        h.processor.processDueBatch();
        EventRecord succeeded = h.query.get(key);
        assertEquals(EventStatus.SUCCEEDED, succeeded.getStatus());
        assertNotNull(succeeded.getResult());
        assertEquals(4, succeeded.getAttempts());
        assertEquals(2, succeeded.getRoundAttempts());
        assertEquals(1, handler.effectCount(), "成功轮只记账一次");

        // 两条历史：轮 0 终止条目 + 轮 1 成功条目，互不覆盖
        List<RoundHistory> history = succeeded.getRoundHistory();
        assertEquals(2, history.size());
        RoundHistory round0 = history.get(0);
        assertEquals(0, round0.getRound());
        assertEquals(2, round0.getAttempts());
        assertEquals(EventStatus.TERMINATED, round0.getOutcome());
        assertEquals(TerminationReason.RETRY_EXHAUSTED, round0.getTerminationReason());
        assertTrue(round0.getLastError().contains("round=0"));

        RoundHistory round1 = history.get(1);
        assertEquals(1, round1.getRound());
        assertEquals(2, round1.getAttempts(), "成功轮处理次数必须是 2（含失败那次）");
        assertEquals(EventStatus.SUCCEEDED, round1.getOutcome());
        assertNull(round1.getTerminationReason(), "成功轮没有终止原因");
        assertTrue(round1.getLastError().contains("round=1"),
                "成功轮归档要保留“本轮最后一次失败”的信息，实际=" + round1.getLastError());
        assertTrue(round1.isLastErrorRetryable());
        assertNotNull(round1.getFinishedAt());
        log.info("[测试] 幂等键={} 轮次=1 本轮处理次数=2 结局=SUCCEEDED 本轮失败信息='{}' 判定=失败后恢复成功仍逐轮可查",
                key, round1.getLastError());

        // 成功后不允许再重跑，历史保持不变
        var rr = h.rerun.rerun(key);
        assertEquals(com.github.highcumontoa.idempotenteventingestionjava.model.RerunOutcome.REJECTED, rr.outcome());
        assertEquals(com.github.highcumontoa.idempotenteventingestionjava.model.RerunRejectionReason.NOT_TERMINATED,
                rr.rejectionReason());
        assertEquals(2, h.query.get(key).getRoundHistory().size());
        assertEquals(4, h.query.get(key).getAttempts());
        log.info("[测试] 幂等键={} 判定依据=成功后NOT_TERMINATED拒绝,历史2条/累计4次不变", key);

        // 重启后成功轮与终止轮历史仍各自完整
        Harness restarted = harness(file, clock, 2, 3, 100, handler);
        EventRecord recovered = restarted.query.get(key);
        assertEquals(EventStatus.SUCCEEDED, recovered.getStatus());
        assertEquals(2, recovered.getRoundHistory().size());
        assertEquals(round1.getLastError(), recovered.getRoundHistory().get(1).getLastError());
        assertEquals(round0.getTerminationReason(), recovered.getRoundHistory().get(0).getTerminationReason());
        assertEquals(4, recovered.getAttempts());
        log.info("[测试] 幂等键={} 判定依据=重启后终止轮+成功轮两条历史完整一致,状态仍SUCCEEDED", key);
    }

    /** 轮询等待目标事件进入 TERMINATED（后台 worker 在另一线程驱动），超时则失败。 */
    private void waitUntilTerminated(Harness h, String key, long timeoutMillis) throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMillis;
        while (System.currentTimeMillis() < deadline) {
            EventRecord r = h.query.get(key);
            if (r != null && r.getStatus() == EventStatus.TERMINATED) {
                return;
            }
            Thread.sleep(2);
        }
        throw new AssertionError("事件未在时限内进入 TERMINATED: "
                + (h.query.get(key) == null ? null : h.query.get(key).getStatus()));
    }
}
