package com.github.highcumontoa.idempotenteventingestionjava.service;

import com.github.highcumontoa.idempotenteventingestionjava.handler.EventHandler;
import com.github.highcumontoa.idempotenteventingestionjava.handler.EventProcessingException;
import com.github.highcumontoa.idempotenteventingestionjava.model.EventRecord;
import com.github.highcumontoa.idempotenteventingestionjava.model.EventStatus;
import com.github.highcumontoa.idempotenteventingestionjava.model.RoundHistory;
import com.github.highcumontoa.idempotenteventingestionjava.model.TerminationReason;
import com.github.highcumontoa.idempotenteventingestionjava.store.EventStore;
import java.time.Clock;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 事件处理与状态迁移的唯一执行者。
 *
 * <p>所有状态迁移都在 {@link EventStore} 的监视器内完成，保证“读取状态—执行处理器—回写结果”
 * 对同一幂等键是原子的，事件在任意时刻只处于一种状态，统计口径不重不漏。</p>
 */
public class EventProcessingService {

    private static final Logger log = LoggerFactory.getLogger(EventProcessingService.class);

    private final EventStore store;
    private final EventHandler handler;
    private final RetryBackoffPolicy backoffPolicy;
    private final Clock clock;

    public EventProcessingService(EventStore store, EventHandler handler,
                                  RetryBackoffPolicy backoffPolicy, Clock clock) {
        this.store = store;
        this.handler = handler;
        this.backoffPolicy = backoffPolicy;
        this.clock = clock;
    }

    /**
     * 处理一条已存在的记录：执行一次处理器并完成状态迁移。
     * 该方法串行化在存储监视器内，调用方无需再加锁。
     *
     * @return 迁移后的记录
     */
    public EventRecord processOnce(EventRecord record) {
        synchronized (store) {
            return attemptRecord(record);
        }
    }

    /**
     * 扫描并处理所有“到期可处理”的事件（QUEUED 且 nextAttemptAt 已到）。
     * 后台 worker 与接入后的立即处理都走这里，单批在同一监视器内串行完成。
     *
     * @return 本批被处理过的记录（任意状态）
     */
    public List<EventRecord> processDueBatch() {
        Instant now = clock.instant();
        List<EventRecord> due = new ArrayList<>();
        synchronized (store) {
            for (EventRecord r : store.all()) {
                if (r.getStatus() == EventStatus.QUEUED
                        && (r.getNextAttemptAt() == null || !r.getNextAttemptAt().isAfter(now))) {
                    due.add(r);
                }
            }
            for (EventRecord r : due) {
                attemptRecord(r);
            }
        }
        if (!due.isEmpty()) {
            log.debug("本轮处理到期事件 {} 条", due.size());
        }
        return due;
    }

    /** 必须在持有 store 监视器时调用。 */
    private EventRecord attemptRecord(EventRecord record) {
        Instant now = clock.instant();
        // attempts 跨重跑轮次累计（总处理次数），roundAttempts 仅统计本轮，用于退避与上限判定。
        int attempt = record.getAttempts() + 1;
        int roundAttempt = record.getRoundAttempts() + 1;
        record.setAttempts(attempt);
        record.setRoundAttempts(roundAttempt);
        record.setStatus(EventStatus.PROCESSING);
        record.setLastAttemptAt(now);
        record.setNextAttemptAt(null);
        record.setUpdatedAt(now);
        store.persist();

        try {
            String voucher = handler.handle(record);
            Instant doneAt = clock.instant();
            // 成功清空前先截取本轮最后一次失败信息（如 FLAKY 前几次失败后恢复），
            // 让“本轮失败过几次、最后一次报什么”在逐轮历史里仍可回看。
            String roundLastError = record.getLastError();
            boolean roundLastErrorRetryable = record.isLastErrorRetryable();
            record.setStatus(EventStatus.SUCCEEDED);
            record.setResult(voucher);
            record.setLastError(null);
            record.setLastErrorRetryable(false);
            record.setTerminationReason(null);
            record.setNextAttemptAt(null);
            record.setUpdatedAt(doneAt);
            // 本轮到达结局：追加一条不可变历史后再落盘；已结束的轮次不会被重复追加。
            record.appendRoundHistory(new RoundHistory(record.getRerunCount(), roundAttempt,
                    EventStatus.SUCCEEDED, null, roundLastError, roundLastErrorRetryable,
                    record.getRoundStartedAt(), doneAt));
            store.persist();
            log.info("幂等键={} 重跑轮数={} 本轮第{}次尝试(累计第{}次) 判定=成功 进入SUCCEEDED 凭证={} 已归档轮次={}",
                    record.getIdempotencyKey(), record.getRerunCount(), roundAttempt, attempt, voucher,
                    record.getRoundHistory().size());
            return record;
        } catch (EventProcessingException e) {
            return handleFailure(record, attempt, roundAttempt, e.getMessage(), e.isRetryable(), now);
        } catch (RuntimeException e) {
            // 未受控异常按可重试处理，达到上限后同样终止，保证不静默丢失。
            log.warn("幂等键={} 重跑轮数={} 本轮第{}次尝试 抛出未受控异常，按可重试处理: {}",
                    record.getIdempotencyKey(), record.getRerunCount(), roundAttempt, e.toString());
            return handleFailure(record, attempt, roundAttempt, "未受控异常: " + e, true, now);
        }
    }

    /** 必须在持有 store 监视器时调用。 */
    private EventRecord handleFailure(EventRecord record, int attempt, int roundAttempt,
                                      String error, boolean retryable, Instant now) {
        record.setLastError(error);
        record.setLastErrorRetryable(retryable);

        if (backoffPolicy.canRetry(retryable, roundAttempt)) {
            Instant nextAt = backoffPolicy.nextAttemptAt(clock.instant(), roundAttempt);
            record.setStatus(EventStatus.QUEUED);
            record.setNextAttemptAt(nextAt);
            record.setUpdatedAt(clock.instant());
            store.persist();
            log.info("幂等键={} 重跑轮数={} 本轮第{}次尝试(累计第{}次) 判定=可重试失败({}) 退避至{} 进入QUEUED",
                    record.getIdempotencyKey(), record.getRerunCount(), roundAttempt, attempt, error, nextAt);
        } else {
            TerminationReason reason = retryable
                    ? TerminationReason.RETRY_EXHAUSTED
                    : TerminationReason.NON_RETRYABLE;
            Instant terminatedAt = clock.instant();
            record.setStatus(EventStatus.TERMINATED);
            record.setTerminationReason(reason);
            record.setLastTerminationReason(reason);
            record.setLastTerminatedAt(terminatedAt);
            record.setNextAttemptAt(null);
            record.setUpdatedAt(terminatedAt);
            // 本轮终止：把该轮的终止原因、处理次数、失败信息作为独立条目归档，
            // 后续重跑只新增条目，不会冲掉这一轮。
            record.appendRoundHistory(new RoundHistory(record.getRerunCount(), roundAttempt,
                    EventStatus.TERMINATED, reason, error, retryable,
                    record.getRoundStartedAt(), terminatedAt));
            store.persist();
            log.warn("幂等键={} 重跑轮数={} 本轮第{}次尝试(累计第{}次) 判定=终止 原因={} 错误={} 本轮上限={} 已归档轮次={}",
                    record.getIdempotencyKey(), record.getRerunCount(), roundAttempt, attempt,
                    reason, error, backoffPolicy.getMaxAttempts(), record.getRoundHistory().size());
        }
        return record;
    }
}
