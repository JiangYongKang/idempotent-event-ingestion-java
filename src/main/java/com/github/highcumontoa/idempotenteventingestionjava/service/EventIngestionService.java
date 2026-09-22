package com.github.highcumontoa.idempotenteventingestionjava.service;

import com.github.highcumontoa.idempotenteventingestionjava.config.EventingProperties;
import com.github.highcumontoa.idempotenteventingestionjava.handler.EventHandler;
import com.github.highcumontoa.idempotenteventingestionjava.model.EventRecord;
import com.github.highcumontoa.idempotenteventingestionjava.model.EventRequest;
import com.github.highcumontoa.idempotenteventingestionjava.model.EventStatus;
import com.github.highcumontoa.idempotenteventingestionjava.model.FailureReason;
import com.github.highcumontoa.idempotenteventingestionjava.model.HandlerResult;
import com.github.highcumontoa.idempotenteventingestionjava.model.IngestionOutcome;
import com.github.highcumontoa.idempotenteventingestionjava.model.IngestionResponse;
import com.github.highcumontoa.idempotenteventingestionjava.model.StatsResponse;
import com.github.highcumontoa.idempotenteventingestionjava.store.EventStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 事件接入与可靠处理核心服务。
 *
 * <p>关键并发约定：
 * <ul>
 *   <li>准入（校验 → 容量 → 插入）在 admissionLock 内串行，容量计数与插入同一临界区，
 *       超额立即抛 {@link CapacityExceededException}，不阻塞、不误报；</li>
 *   <li>同一幂等键的首次插入只有一个成功，其余并发投递拿到 DUPLICATE 响应；</li>
 *   <li>PENDING→PROCESSING→SUCCEEDED/TERMINATED 的每次迁移都经版本号 CAS，
 *       加上 scheduledKeys 在途集合，保证同一事件全局至多一个工作线程；</li>
 *   <li>统计取自 store 的一致性快照，每个事件按当前状态恰好归入一个桶。</li>
 * </ul>
 */
@Service
public class EventIngestionService {

    private static final Logger log = LoggerFactory.getLogger(EventIngestionService.class);

    private final EventStore store;
    private final EventHandler eventHandler;
    private final EventingProperties properties;
    private final ExecutorService executor;

    /** 已提交到线程池、尚未结束本轮尝试的幂等键：防止重复排队。 */
    private final Set<String> scheduledKeys = ConcurrentHashMap.newKeySet();
    /** 非法事件拒绝计数（不进入正常状态桶）。 */
    private final AtomicLong rejectedCount = new AtomicLong();
    /** 容量限流计数（不进入正常状态桶）。 */
    private final AtomicLong rateLimitedCount = new AtomicLong();

    private final Object admissionLock = new Object();

    public EventIngestionService(EventStore store, EventHandler eventHandler,
                                 EventingProperties properties,
                                 ExecutorService eventTaskExecutor) {
        this.store = store;
        this.eventHandler = eventHandler;
        this.properties = properties;
        this.executor = eventTaskExecutor;
    }

    /**
     * 事件接入：非法 -> 400 拒绝；超额 -> 503 限流；重复 -> 200 且 outcome=DUPLICATE；
     * 首次 -> 202 语义的 ACCEPTED，并立即异步处理。
     */
    public IngestionResponse ingest(EventRequest request) {
        if (request == null || request.getIdempotencyKey() == null
                || request.getIdempotencyKey().isBlank()) {
            rejectedCount.incrementAndGet();
            throw new InvalidEventException("idempotencyKey 不能为空");
        }
        String key = request.getIdempotencyKey().trim();
        if (key.length() > 128) {
            rejectedCount.incrementAndGet();
            throw new InvalidEventException("idempotencyKey 长度不能超过 128");
        }

        EventRecord record;
        synchronized (admissionLock) {
            long inFlight = 0;
            for (EventRecord r : store.loadAll()) {
                if (r.getStatus() == EventStatus.PENDING || r.getStatus() == EventStatus.PROCESSING) {
                    inFlight++;
                }
            }
            EventRecord existing = store.find(key);
            if (existing != null) {
                log.info("事件接入 idempotencyKey={} attempt={} 判定依据=重复投递,当前状态={},不重复计数",
                        key, existing.getAttemptCount(), existing.getStatus());
                String message = switch (existing.getStatus()) {
                    case PENDING -> "重复投递：事件排队等待处理";
                    case PROCESSING -> "重复投递：事件正在处理中";
                    case SUCCEEDED -> "重复投递：事件已处理成功";
                    case TERMINATED -> "重复投递：事件已终止(" + existing.getFailureReason() + ")";
                };
                return new IngestionResponse(IngestionOutcome.DUPLICATE, key, existing.getStatus(),
                        message, existing.getAttemptCount());
            }
            if (inFlight >= properties.getMaxInFlight()) {
                rateLimitedCount.incrementAndGet();
                log.warn("事件接入 idempotencyKey={} attempt=0 判定依据=容量超限 inFlight={} maxInFlight={},限流拒绝",
                        key, inFlight, properties.getMaxInFlight());
                throw new CapacityExceededException(
                        "处理容量已满: inFlight=" + inFlight + ", maxInFlight=" + properties.getMaxInFlight());
            }

            long now = System.currentTimeMillis();
            record = new EventRecord();
            record.setIdempotencyKey(key);
            record.setType(request.getType());
            record.setPayload(request.getPayload());
            record.setStatus(EventStatus.PENDING);
            record.setAttemptCount(0);
            record.setFailureReason(FailureReason.NONE);
            record.setCreatedAt(now);
            record.setUpdatedAt(now);
            record.setNextAttemptAt(0L);
            if (!store.insertIfAbsent(record)) {
                // 极端竞态兜底：理论上上面的 find 已排除
                EventRecord raced = store.find(key);
                log.info("事件接入 idempotencyKey={} attempt={} 判定依据=插入竞态落败,按重复投递处理",
                        key, raced == null ? 0 : raced.getAttemptCount());
                return new IngestionResponse(IngestionOutcome.DUPLICATE, key, raced.getStatus(),
                        "重复投递", raced.getAttemptCount());
            }
        }

        log.info("事件接入 idempotencyKey={} attempt=0 判定依据=首次投递,已受理并立即调度", key);
        submitIfNotScheduled(key);
        EventRecord accepted = store.find(key);
        return new IngestionResponse(IngestionOutcome.ACCEPTED, key, EventStatus.PENDING,
                "事件已受理", accepted == null ? 0 : accepted.getAttemptCount());
    }

    /** 查询单个事件；不存在返回 null（由控制层转 404）。 */
    public EventRecord get(String idempotencyKey) {
        return store.find(idempotencyKey);
    }

    /** 一致性统计：对 store 快照计数，四桶互斥且总和等于已受理事件总数。 */
    public StatsResponse stats() {
        List<EventRecord> snapshot = store.loadAll();
        StatsResponse s = new StatsResponse();
        for (EventRecord r : snapshot) {
            switch (r.getStatus()) {
                case PENDING -> s.setPending(s.getPending() + 1);
                case PROCESSING -> s.setProcessing(s.getProcessing() + 1);
                case SUCCEEDED -> s.setSucceeded(s.getSucceeded() + 1);
                case TERMINATED -> {
                    s.setTerminated(s.getTerminated() + 1);
                    switch (r.getFailureReason()) {
                        case NON_RETRYABLE_ERROR ->
                                s.setTerminalNonRetryableError(s.getTerminalNonRetryableError() + 1);
                        case RETRIES_EXHAUSTED ->
                                s.setTerminalRetriesExhausted(s.getTerminalRetriesExhausted() + 1);
                        case RETRYABLE_ERROR ->
                                s.setTerminalRetryableError(s.getTerminalRetryableError() + 1);
                        case RECOVERED_AFTER_RESTART ->
                                s.setTerminalRecoveredAfterRestart(s.getTerminalRecoveredAfterRestart() + 1);
                        default -> { /* NONE 不应出现在终止态 */ }
                    }
                }
            }
        }
        s.setTotal(snapshot.size());
        s.setRejected(rejectedCount.get());
        s.setRateLimited(rateLimitedCount.get());
        return s;
    }

    /**
     * 对单个事件执行一次尝试。PENDING→PROCESSING 的 CAS 保证：
     * 立即调度与后台重试即使同时发生，也只有一个线程真正执行处理器。
     */
    public void processOnce(String key) {
        EventRecord current = store.find(key);
        if (current == null || current.getStatus() != EventStatus.PENDING) {
            return;
        }
        int claimedVersion = current.getVersion();
        EventRecord processing = copy(current);
        processing.setStatus(EventStatus.PROCESSING);
        processing.setAttemptCount(current.getAttemptCount() + 1);
        processing.setUpdatedAt(System.currentTimeMillis());
        processing.setNextAttemptAt(0L);
        if (!store.compareAndUpdate(processing, claimedVersion)) {
            log.info("事件处理 idempotencyKey={} attempt={} 判定依据=认领CAS失败,已有其他线程处理",
                    key, processing.getAttemptCount());
            return;
        }
        int attempt = processing.getAttemptCount();

        HandlerResult result;
        try {
            result = eventHandler.handle(processing);
        } catch (Exception e) {
            log.warn("事件处理 idempotencyKey={} attempt={} 判定依据=处理器抛出异常,按可重试失败处理: {}",
                    key, attempt, e.toString());
            result = HandlerResult.failure(true, "处理器异常: " + e);
        }

        if (result.isSuccess()) {
            EventRecord done = copy(processing);
            done.setStatus(EventStatus.SUCCEEDED);
            done.setFailureReason(FailureReason.NONE);
            done.setResult(result.getResult());
            done.setDetail(null);
            done.setUpdatedAt(System.currentTimeMillis());
            if (casOrLog(done, processing.getVersion(), attempt, "成功")) {
                log.info("事件处理 idempotencyKey={} attempt={} 重试次数={} 判定依据=处理成功,迁移至SUCCEEDED",
                        key, attempt, attempt - 1);
            }
            return;
        }

        EventRecord next = copy(processing);
        next.setUpdatedAt(System.currentTimeMillis());
        if (!result.isRetryable()) {
            next.setStatus(EventStatus.TERMINATED);
            next.setFailureReason(FailureReason.NON_RETRYABLE_ERROR);
            next.setDetail(result.getErrorMessage());
            if (casOrLog(next, processing.getVersion(), attempt, "不可重试终止")) {
                log.warn("事件处理 idempotencyKey={} attempt={} 重试次数={} 判定依据=不可重试失败,迁移至TERMINATED: {}",
                        key, attempt, attempt - 1, result.getErrorMessage());
            }
            return;
        }

        if (attempt > properties.getMaxRetries()) {
            next.setStatus(EventStatus.TERMINATED);
            next.setFailureReason(FailureReason.RETRIES_EXHAUSTED);
            next.setDetail(result.getErrorMessage());
            if (casOrLog(next, processing.getVersion(), attempt, "重试耗尽终止")) {
                log.warn("事件处理 idempotencyKey={} attempt={} 重试次数={} 判定依据=可重试失败但已达重试上限maxRetries={},迁移至TERMINATED",
                        key, attempt, attempt - 1, properties.getMaxRetries());
            }
            return;
        }

        long delay = backoffMillis(attempt);
        next.setStatus(EventStatus.PENDING);
        next.setFailureReason(FailureReason.RETRYABLE_ERROR);
        next.setDetail(result.getErrorMessage());
        next.setNextAttemptAt(System.currentTimeMillis() + delay);
        if (casOrLog(next, processing.getVersion(), attempt, "退避重试")) {
            log.info("事件处理 idempotencyKey={} attempt={} 重试次数={} 判定依据=可重试失败,退避{}ms后重试(上限{})",
                    key, attempt, attempt - 1, delay, properties.getMaxRetries());
        }
    }

    /**
     * 启动恢复：崩溃/重启时停留在 PROCESSING 的事件没有完整结论，
     * 一律回到 PENDING 并标注 RECOVERED_AFTER_RESTART，等待重试，绝不静默丢失。
     * @return 恢复的事件数
     */
    public int recover() {
        int recovered = 0;
        for (EventRecord r : store.loadAll()) {
            if (r.getStatus() == EventStatus.PROCESSING) {
                EventRecord reset = copy(r);
                reset.setStatus(EventStatus.PENDING);
                reset.setFailureReason(FailureReason.RECOVERED_AFTER_RESTART);
                reset.setDetail("服务重启时事件处于PROCESSING,判定为未完成并重新入队");
                reset.setNextAttemptAt(0L);
                reset.setUpdatedAt(System.currentTimeMillis());
                if (store.compareAndUpdate(reset, r.getVersion())) {
                    recovered++;
                    log.warn("事件恢复 idempotencyKey={} attempt={} 判定依据=重启恢复:PROCESSING->PENDING,重新排队",
                            r.getIdempotencyKey(), r.getAttemptCount());
                }
            }
        }
        if (recovered > 0) {
            log.info("事件恢复完成: 共恢复 {} 个中断事件", recovered);
        }
        return recovered;
    }

    /**
     * 扫描到期的 PENDING 事件并提交执行（后台重试）。
     * @return 本次新提交的事件数
     */
    public int dispatchDueEvents() {
        int submitted = 0;
        long now = System.currentTimeMillis();
        for (EventRecord r : store.loadAll()) {
            if (r.getStatus() == EventStatus.PENDING && r.getNextAttemptAt() <= now) {
                if (scheduledKeys.contains(r.getIdempotencyKey())) {
                    continue;
                }
                if (submitIfNotScheduled(r.getIdempotencyKey())) {
                    submitted++;
                }
            }
        }
        return submitted;
    }

    /** 测试辅助：等待所有事件到达 SUCCEEDED/TERMINATED。 */
    public void awaitQuiescence(long timeoutMillis) throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMillis;
        while (System.currentTimeMillis() < deadline) {
            StatsResponse s = stats();
            if (s.getPending() == 0 && s.getProcessing() == 0) {
                return;
            }
            Thread.sleep(20);
        }
        throw new AssertionError("等待事件静止超时");
    }

    private boolean submitIfNotScheduled(String key) {
        if (!scheduledKeys.add(key)) {
            return false;
        }
        try {
            executor.submit(() -> {
                try {
                    processOnce(key);
                } finally {
                    scheduledKeys.remove(key);
                }
            });
            return true;
        } catch (RuntimeException e) {
            // 有界线程池满（理论上被容量准入排除）：放弃在途标记，事件留在 PENDING 由调度器兜底
            scheduledKeys.remove(key);
            log.warn("事件提交线程池被拒 idempotencyKey={} 判定依据={},保留PENDING等待调度", key, e.toString());
            return false;
        }
    }

    private boolean casOrLog(EventRecord target, int expectedVersion, int attempt, String what) {
        if (store.compareAndUpdate(target, expectedVersion)) {
            return true;
        }
        log.warn("事件处理 idempotencyKey={} attempt={} 判定依据={}迁移CAS失败,状态已被他人推进,放弃本次结果",
                target.getIdempotencyKey(), attempt, what);
        return false;
    }

    /** 指数退避：initial * multiplier^(attempt-1)，上限 backoffMaxMillis。 */
    private long backoffMillis(int failedAttempt) {
        long delay = (long) (properties.getBackoffInitialMillis()
                * Math.pow(properties.getBackoffMultiplier(), failedAttempt - 1));
        return Math.min(delay, properties.getBackoffMaxMillis());
    }

    private EventRecord copy(EventRecord s) {
        EventRecord t = new EventRecord();
        t.setIdempotencyKey(s.getIdempotencyKey());
        t.setType(s.getType());
        t.setPayload(s.getPayload());
        t.setStatus(s.getStatus());
        t.setAttemptCount(s.getAttemptCount());
        t.setFailureReason(s.getFailureReason());
        t.setDetail(s.getDetail());
        t.setResult(s.getResult());
        t.setCreatedAt(s.getCreatedAt());
        t.setUpdatedAt(s.getUpdatedAt());
        t.setNextAttemptAt(s.getNextAttemptAt());
        t.setVersion(s.getVersion());
        return t;
    }
}
