package com.github.highcumontoa.idempotenteventingestionjava.tests;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.highcumontoa.idempotenteventingestionjava.config.EventingProperties;
import com.github.highcumontoa.idempotenteventingestionjava.handler.DefaultEventHandler;
import com.github.highcumontoa.idempotenteventingestionjava.model.EventRecord;
import com.github.highcumontoa.idempotenteventingestionjava.model.EventRequest;
import com.github.highcumontoa.idempotenteventingestionjava.model.EventStatus;
import com.github.highcumontoa.idempotenteventingestionjava.model.FailureReason;
import com.github.highcumontoa.idempotenteventingestionjava.model.IngestionOutcome;
import com.github.highcumontoa.idempotenteventingestionjava.model.IngestionResponse;
import com.github.highcumontoa.idempotenteventingestionjava.model.StatsResponse;
import com.github.highcumontoa.idempotenteventingestionjava.service.CapacityExceededException;
import com.github.highcumontoa.idempotenteventingestionjava.service.EventIngestionService;
import com.github.highcumontoa.idempotenteventingestionjava.service.InvalidEventException;
import com.github.highcumontoa.idempotenteventingestionjava.store.FileEventStore;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 核心服务单测：并发幂等去重、失败重试/终止、统计口径、容量限流、重启恢复。
 * 日志中包含幂等键、重试次数与判定依据（见服务与处理器的 log 输出）。
 */
class EventIngestionServiceTest {

    @TempDir
    Path tempDir;

    private ExecutorService executor;
    private java.util.concurrent.ScheduledExecutorService ticker;
    private EventIngestionService service;
    private FileEventStore store;
    private DefaultEventHandler handler;
    private EventingProperties properties;

    @BeforeEach
    void setUp() throws Exception {
        properties = new EventingProperties();
        properties.setDataDir(tempDir.toString());
        properties.setMaxRetries(2);
        properties.setBackoffInitialMillis(20);
        properties.setBackoffMultiplier(1.5);
        properties.setBackoffMaxMillis(200);
        properties.setSchedulerIntervalMillis(20);
        properties.setWorkerThreads(8);
        ObjectMapper mapper = new ObjectMapper();
        store = new FileEventStore(properties, mapper);
        store.init();
        handler = new DefaultEventHandler();
        executor = new ThreadPoolExecutor(8, 8, 30, TimeUnit.SECONDS,
                new LinkedBlockingQueue<>(1000),
                r -> {
                    Thread t = new Thread(r, "test-event-worker");
                    t.setDaemon(true);
                    return t;
                },
                new ThreadPoolExecutor.CallerRunsPolicy());
        service = new EventIngestionService(store, handler, properties, executor);
        // 模拟 RetryScheduler 的后台扫描，驱动退避到期事件重试
        ticker = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "test-retry-ticker");
            t.setDaemon(true);
            return t;
        });
        ticker.scheduleAtFixedRate(() -> service.dispatchDueEvents(), 10, 20, TimeUnit.MILLISECONDS);
    }

    @AfterEach
    void tearDown() {
        executor.shutdownNow();
        if (ticker != null) {
            ticker.shutdownNow();
        }
    }

    private EventRequest request(String key, String payload) {
        EventRequest r = new EventRequest();
        r.setIdempotencyKey(key);
        r.setType("demo");
        r.setPayload(payload);
        return r;
    }

    @Test
    void concurrentDuplicates_onlyOneAccepted_andSucceedsOnce() throws Exception {
        String key = "dup-key-001";
        int threads = 32;
        ExecutorService callers = Executors.newFixedThreadPool(threads);
        try {
            Callable<IngestionResponse> task = () -> service.ingest(request(key, null));
            List<Future<IngestionResponse>> futures = callers.invokeAll(
                    java.util.stream.IntStream.range(0, threads).mapToObj(i -> task).toList());

            int accepted = 0;
            int duplicate = 0;
            for (Future<IngestionResponse> f : futures) {
                IngestionResponse resp = f.get(5, TimeUnit.SECONDS);
                if (resp.getOutcome() == IngestionOutcome.ACCEPTED) {
                    accepted++;
                } else {
                    duplicate++;
                    assertNotEquals(null, resp.getStatus(), "重复响应应回显当前状态");
                    assertNotEquals(null, resp.getMessage(), "重复响应必须给出可区分的判定说明");
                }
            }
            assertEquals(1, accepted, "32 个并发重复投递中只能有 1 个首次受理");
            assertEquals(threads - 1, duplicate, "其余必须被识别为重复投递");
        } finally {
            callers.shutdownNow();
        }

        service.awaitQuiescence(5000);
        EventRecord rec = service.get(key);
        assertEquals(EventStatus.SUCCEEDED, rec.getStatus());
        assertEquals(1, rec.getAttemptCount(), "成功事件只应处理一次（attempt=1）");
        StatsResponse s = service.stats();
        assertEquals(1, s.getTotal(), "重复投递不得被重复计入");
        assertEquals(1, s.getSucceeded());
        assertEquals(0, s.getPending() + s.getProcessing() + s.getTerminated());
        assertEquals(1, s.getSucceeded(), "四桶之和等于 total");
        assertEquals(1, s.getPending() + s.getProcessing() + s.getSucceeded() + s.getTerminated());
    }

    @Test
    void retryableFailure_retriedWithBackoff_thenSucceeds() throws Exception {
        // 前两次尝试失败（可重试），第三次成功
        String payload = "{\"behavior\":\"FAIL_RETRYABLE\",\"failUntilAttempt\":3,\"effect\":\"acct:A+10\"}";
        IngestionResponse resp = service.ingest(request("retry-ok-001", payload));
        assertEquals(IngestionOutcome.ACCEPTED, resp.getOutcome());

        service.awaitQuiescence(10_000);
        EventRecord rec = service.get("retry-ok-001");
        assertEquals(EventStatus.SUCCEEDED, rec.getStatus());
        assertEquals(3, rec.getAttemptCount(), "应经历 3 次尝试（1 次首试 + 2 次重试）");
        assertEquals(1, handler.getAppliedEffectCount(), "副作用只应记账一次");
        StatsResponse s = service.stats();
        assertEquals(1, s.getTotal());
        assertEquals(1, s.getSucceeded());
    }

    @Test
    void retryableFailure_exhaustingRetries_terminatedWithDistinctReason() throws Exception {
        String payload = "{\"behavior\":\"FAIL_RETRYABLE\"}";
        service.ingest(request("retry-exhaust-001", payload));

        service.awaitQuiescence(10_000);
        EventRecord rec = service.get("retry-exhaust-001");
        assertEquals(EventStatus.TERMINATED, rec.getStatus());
        assertEquals(FailureReason.RETRIES_EXHAUSTED, rec.getFailureReason(),
                "超过重试上限必须进入明确的 RETRIES_EXHAUSTED 终止态");
        assertEquals(properties.getMaxRetries() + 1, rec.getAttemptCount(),
                "尝试次数 = 首次 + maxRetries");
        assertNotNull(rec.getDetail(), "失败细节不得静默丢失");
        StatsResponse s = service.stats();
        assertEquals(1, s.getTerminated());
        assertEquals(1, s.getTerminalRetriesExhausted());
        assertEquals(0, s.getTerminalNonRetryableError());
    }

    @Test
    void nonRetryableFailure_terminatedImmediately() throws Exception {
        String payload = "{\"behavior\":\"FAIL_NON_RETRYABLE\"}";
        service.ingest(request("fatal-001", payload));

        service.awaitQuiescence(5000);
        EventRecord rec = service.get("fatal-001");
        assertEquals(EventStatus.TERMINATED, rec.getStatus());
        assertEquals(FailureReason.NON_RETRYABLE_ERROR, rec.getFailureReason());
        assertEquals(1, rec.getAttemptCount(), "不可重试失败不应发生重试");
    }

    @Test
    void invalidKey_rejectedAndCounted_notAcceptedAsSuccess() {
        InvalidEventException ex = assertThrows(InvalidEventException.class,
                () -> service.ingest(request("  ", null)));
        assertTrue(ex.getMessage().contains("idempotencyKey"));
        StatsResponse s = service.stats();
        assertEquals(0, s.getTotal(), "非法事件不得计入正常状态桶");
        assertEquals(1, s.getRejected(), "非法拒绝应单独计数");
    }

    @Test
    void overCapacity_rejectedWith503Semantics_andCounted() throws InterruptedException {
        properties.setMaxInFlight(2);
        // 直接注入 2 个"占坑"在途记录（nextAttemptAt 在远期，本轮 ticker 不会将其派发），
        // 稳定占满 PENDING 在途容量
        long farFuture = System.currentTimeMillis() + 3_600_000;
        for (int i = 0; i < 2; i++) {
            EventRecord stuck = new EventRecord();
            stuck.setIdempotencyKey("stuck-" + i);
            stuck.setType("demo");
            stuck.setPayload(null);
            stuck.setStatus(EventStatus.PENDING);
            stuck.setAttemptCount(0);
            stuck.setFailureReason(FailureReason.NONE);
            stuck.setCreatedAt(System.currentTimeMillis());
            stuck.setUpdatedAt(stuck.getCreatedAt());
            stuck.setNextAttemptAt(farFuture);
            assertTrue(store.insertIfAbsent(stuck));
        }
        // 容量已满：所有新投递必须立即被限流（不阻塞、不误报成功）
        for (int i = 0; i < 50; i++) {
            final int idx = i;
            assertThrows(CapacityExceededException.class,
                    () -> service.ingest(request("cap-" + idx, null)));
        }
        StatsResponse s = service.stats();
        assertEquals(2, s.getTotal(), "被限流的事件不得计入 total");
        assertEquals(50, s.getRateLimited(), "每次限流都应可计数、口径不重不漏");
        assertEquals(2, s.getPending());
        assertEquals(0, s.getProcessing() + s.getSucceeded() + s.getTerminated());
    }

    @Test
    void recoverAfterRestart_interruptedProcessingRequeued() throws Exception {
        // 场景：首次处理时崩溃在 PROCESSING 窗口，重启后 store 重建并 recover
        String payload = "{\"behavior\":\"FAIL_RETRYABLE\",\"failUntilAttempt\":2,\"effect\":\"acct:B+5\"}";
        service.ingest(request("restart-001", payload));
        service.awaitQuiescence(10_000);
        // 此时已成功。下面单独构造"崩溃窗口"：直接利用 store 快照无法伪造 PROCESSING，
        // 因此通过新 store 实例模拟重启，并手工插入一条 PROCESSING 快照。
        FileEventStore secondStore = new FileEventStore(properties, new ObjectMapper());
        secondStore.init();
        EventRecord crashed = new EventRecord();
        crashed.setIdempotencyKey("restart-crashed");
        crashed.setType("demo");
        crashed.setPayload(payload);
        crashed.setStatus(EventStatus.PENDING);
        crashed.setAttemptCount(0);
        crashed.setFailureReason(FailureReason.NONE);
        long now = System.currentTimeMillis();
        crashed.setCreatedAt(now);
        crashed.setUpdatedAt(now);
        secondStore.insertIfAbsent(crashed);
        // 抢占为 PROCESSING 后模拟 JVM 崩溃（无后续迁移）
        EventRecord processing = secondStore.find("restart-crashed");
        processing.setStatus(EventStatus.PROCESSING);
        processing.setAttemptCount(1);
        assertTrue(secondStore.compareAndUpdate(processing, processing.getVersion()));

        // 重启：新 store 从同一目录加载
        FileEventStore restartedStore = new FileEventStore(properties, new ObjectMapper());
        restartedStore.init();
        EventIngestionService restarted = new EventIngestionService(
                restartedStore, handler, properties, executor);

        EventRecord before = restarted.get("restart-crashed");
        assertEquals(EventStatus.PROCESSING, before.getStatus());
        int recovered = restarted.recover();
        assertEquals(1, recovered);
        EventRecord afterRecover = restarted.get("restart-crashed");
        assertEquals(EventStatus.PENDING, afterRecover.getStatus());
        assertEquals(FailureReason.RECOVERED_AFTER_RESTART, afterRecover.getFailureReason());

        restarted.dispatchDueEvents();
        restarted.awaitQuiescence(10_000);
        EventRecord done = restarted.get("restart-crashed");
        assertEquals(EventStatus.SUCCEEDED, done.getStatus(), "中断事件恢复后应重试成功");
        assertEquals(2, done.getAttemptCount(), "崩溃前已尝试 1 次，恢复后再尝试 1 次");
    }
}
