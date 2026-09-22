package com.github.highcumontoa.idempotenteventingestionjava.service;

import com.github.highcumontoa.idempotenteventingestionjava.handler.EventHandler;
import com.github.highcumontoa.idempotenteventingestionjava.handler.EventProcessingException;
import com.github.highcumontoa.idempotenteventingestionjava.model.EventRecord;
import com.github.highcumontoa.idempotenteventingestionjava.model.EventStatus;
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
        int attempt = record.getAttempts() + 1;
        record.setAttempts(attempt);
        record.setStatus(EventStatus.PROCESSING);
        record.setLastAttemptAt(now);
        record.setNextAttemptAt(null);
        record.setUpdatedAt(now);
        store.persist();

        try {
            String voucher = handler.handle(record);
            Instant doneAt = clock.instant();
            record.setStatus(EventStatus.SUCCEEDED);
            record.setResult(voucher);
            record.setLastError(null);
            record.setLastErrorRetryable(false);
            record.setTerminationReason(null);
            record.setNextAttemptAt(null);
            record.setUpdatedAt(doneAt);
            store.persist();
            log.info("幂等键={} 重试次数={} 判定=成功 进入SUCCEEDED 凭证={}",
                    record.getIdempotencyKey(), attempt - 1, voucher);
            return record;
        } catch (EventProcessingException e) {
            return handleFailure(record, attempt, e.getMessage(), e.isRetryable(), now);
        } catch (RuntimeException e) {
            // 未受控异常按可重试处理，达到上限后同样终止，保证不静默丢失。
            log.warn("幂等键={} 第{}次尝试 抛出未受控异常，按可重试处理: {}",
                    record.getIdempotencyKey(), attempt, e.toString());
            return handleFailure(record, attempt, "未受控异常: " + e, true, now);
        }
    }

    /** 必须在持有 store 监视器时调用。 */
    private EventRecord handleFailure(EventRecord record, int attempt,
                                      String error, boolean retryable, Instant now) {
        record.setLastError(error);
        record.setLastErrorRetryable(retryable);

        if (backoffPolicy.canRetry(retryable, attempt)) {
            Instant nextAt = backoffPolicy.nextAttemptAt(clock.instant(), attempt);
            record.setStatus(EventStatus.QUEUED);
            record.setNextAttemptAt(nextAt);
            record.setUpdatedAt(clock.instant());
            store.persist();
            log.info("幂等键={} 重试次数={} 判定=可重试失败({}) 退避至{} 进入QUEUED",
                    record.getIdempotencyKey(), attempt - 1, error, nextAt);
        } else {
            TerminationReason reason = retryable
                    ? TerminationReason.RETRY_EXHAUSTED
                    : TerminationReason.NON_RETRYABLE;
            record.setStatus(EventStatus.TERMINATED);
            record.setTerminationReason(reason);
            record.setNextAttemptAt(null);
            record.setUpdatedAt(clock.instant());
            store.persist();
            log.warn("幂等键={} 重试次数={} 判定=终止 原因={} 错误={} 上限={}",
                    record.getIdempotencyKey(), attempt - 1, reason, error,
                    backoffPolicy.getMaxAttempts());
        }
        return record;
    }
}
