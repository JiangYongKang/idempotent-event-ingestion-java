package com.github.highcumontoa.idempotenteventingestionjava.service;

import com.github.highcumontoa.idempotenteventingestionjava.config.EventingProperties;
import com.github.highcumontoa.idempotenteventingestionjava.model.EventRecord;
import com.github.highcumontoa.idempotenteventingestionjava.model.EventStatus;
import com.github.highcumontoa.idempotenteventingestionjava.model.RerunRejectionReason;
import com.github.highcumontoa.idempotenteventingestionjava.model.RerunResult;
import com.github.highcumontoa.idempotenteventingestionjava.store.EventStore;
import java.time.Clock;
import java.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

/**
 * 终止事件的受控重跑入口。
 *
 * <p>语义：仅允许对 {@link EventStatus#TERMINATED} 的事件发起重跑；重跑把事件放回
 * {@link EventStatus#QUEUED} 完整再跑一轮（退避、上限、终止规则与首轮一致，本轮尝试数重新计数），
 * 同时完整保留历史信息：上一轮终止原因（{@code lastTerminationReason}）、累计总尝试次数
 * （{@code attempts} 不清零）、这是第几轮重跑（{@code rerunCount}）。</p>
 *
 * <p>边界与并发：整个“判定 + 状态迁移”在 {@link EventStore} 监视器内原子完成，
 * 因此并发对同一事件发起重跑时最多只有一个成功重新入队，其余拿到可区分的拒绝；
 * 重跑轮数上限（{@code eventing.max-reruns}）与在途容量上限（{@code eventing.max-queue-capacity}）
 * 同样在临界区内校验，容量不足立即拒绝返回，不阻塞调用方、不改变任何计数。</p>
 */
@Service
public class EventRerunService {

    private static final Logger log = LoggerFactory.getLogger(EventRerunService.class);

    private final EventStore store;
    private final EventingProperties properties;
    private final Clock clock;

    public EventRerunService(EventStore store, EventingProperties properties, Clock clock) {
        this.store = store;
        this.properties = properties;
        this.clock = clock;
    }

    /**
     * 对一条已终止的事件发起受控重跑。
     *
     * @return 接受则事件已回到 QUEUED；拒绝则原因可区分且记录保持原状
     */
    public RerunResult rerun(String idempotencyKey) {
        if (idempotencyKey == null || idempotencyKey.isBlank()) {
            log.warn("拒绝重跑 幂等键={} 原因=NOT_FOUND 详情=幂等键为空", idempotencyKey);
            return RerunResult.rejected(RerunRejectionReason.NOT_FOUND, "幂等键不能为空");
        }
        synchronized (store) {
            EventRecord record = store.find(idempotencyKey);
            if (record == null) {
                log.warn("拒绝重跑 幂等键={} 原因=NOT_FOUND", idempotencyKey);
                return RerunResult.rejected(RerunRejectionReason.NOT_FOUND, "事件不存在: " + idempotencyKey);
            }
            if (record.getStatus() != EventStatus.TERMINATED) {
                log.warn("拒绝重跑 幂等键={} 原因=NOT_TERMINATED 当前状态={} 重跑轮数={}",
                        idempotencyKey, record.getStatus(), record.getRerunCount());
                return RerunResult.rejected(RerunRejectionReason.NOT_TERMINATED,
                        "事件当前状态为 " + record.getStatus() + "，仅终止态事件允许重跑");
            }
            if (record.getRerunCount() >= properties.getMaxReruns()) {
                log.warn("拒绝重跑 幂等键={} 原因=RERUN_LIMIT_REACHED 重跑轮数={} 上限={}",
                        idempotencyKey, record.getRerunCount(), properties.getMaxReruns());
                return RerunResult.rejected(RerunRejectionReason.RERUN_LIMIT_REACHED,
                        "重跑轮数已达上限 " + record.getRerunCount() + "/" + properties.getMaxReruns());
            }

            long queuedOrProcessing = 0;
            for (EventRecord r : store.all()) {
                EventStatus s = r.getStatus();
                if (s == EventStatus.QUEUED || s == EventStatus.PROCESSING) {
                    queuedOrProcessing++;
                }
            }
            if (queuedOrProcessing >= properties.getMaxQueueCapacity()) {
                log.warn("拒绝重跑 幂等键={} 原因=QUEUE_FULL 在途={} 容量={}",
                        idempotencyKey, queuedOrProcessing, properties.getMaxQueueCapacity());
                return RerunResult.rejected(RerunRejectionReason.QUEUE_FULL,
                        "处理容量已满，在途事件 " + queuedOrProcessing + "/" + properties.getMaxQueueCapacity());
            }

            Instant now = clock.instant();
            // 保留历史：上一轮终止原因与时刻转入 lastTermination*，terminationReason 清空等待新一轮。
            record.setLastTerminationReason(record.getTerminationReason());
            record.setLastTerminatedAt(now);
            record.setTerminationReason(null);
            record.setRerunCount(record.getRerunCount() + 1);
            record.setRoundAttempts(0);
            record.setStatus(EventStatus.QUEUED);
            record.setNextAttemptAt(now);
            record.setUpdatedAt(now);
            store.persist();
            log.info("接受重跑 幂等键={} 重跑轮数={}/{} 累计尝试={} 上轮终止原因={} 已保留历史轮数={} 判定=重新入队QUEUED",
                    idempotencyKey, record.getRerunCount(), properties.getMaxReruns(),
                    record.getAttempts(), record.getLastTerminationReason(),
                    record.getRoundHistory().size());
            return RerunResult.accepted(record);
        }
    }
}
