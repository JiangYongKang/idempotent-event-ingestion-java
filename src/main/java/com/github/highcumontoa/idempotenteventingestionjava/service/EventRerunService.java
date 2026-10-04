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
 * <p>语义与边界：</p>
 * <ul>
 *   <li>仅 {@code TERMINATED} 事件可重跑；排队/处理中/已成功一律以
 *       {@link RerunRejectionReason#NOT_TERMINATED} 拒绝，与“轮数到顶”可区分；</li>
 *   <li>重跑轮数上限 {@code eventing.max-reruns}，到顶后以
 *       {@link RerunRejectionReason#RERUN_LIMIT_REACHED} 拒绝；</li>
 *   <li>重跑重新入队受与接入相同的在途容量约束，容量不足立即以
 *       {@link RerunRejectionReason#QUEUE_FULL} 拒绝，不阻塞调用方；</li>
 *   <li>状态判断与迁移全部在 {@link EventStore} 监视器内完成：并发重跑同一事件时，
 *       只有一个请求能把事件从 TERMINATED 迁移回 QUEUED，其余看到状态已变而被拒，
 *       从结构上保证同一条事件最多真正重新入队一次；</li>
 *   <li>重跑只改状态不增删记录：total 不变，terminated-1、queued+1，统计口径恒平。</li>
 * </ul>
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
     * 对一条已终止事件发起重跑：归档上一轮终局后重新入队，等待后台完整再跑一轮。
     *
     * @return 重跑判定结果（接受或带可区分原因的拒绝）
     */
    public RerunResult rerun(String idempotencyKey) {
        if (idempotencyKey == null || idempotencyKey.isBlank()) {
            log.warn("重跑拒绝 幂等键=<空> 原因=NOT_FOUND 详情=幂等键不能为空");
            return RerunResult.rejected(RerunRejectionReason.NOT_FOUND, "幂等键不能为空");
        }
        synchronized (store) {
            EventRecord record = store.find(idempotencyKey);
            if (record == null) {
                log.warn("重跑拒绝 幂等键={} 原因=NOT_FOUND", idempotencyKey);
                return RerunResult.rejected(RerunRejectionReason.NOT_FOUND, "事件不存在");
            }
            if (record.getStatus() != EventStatus.TERMINATED) {
                log.warn("重跑拒绝 幂等键={} 原因=NOT_TERMINATED 当前状态={} 重跑轮次={}",
                        idempotencyKey, record.getStatus(), record.getRerunCount());
                return RerunResult.rejected(RerunRejectionReason.NOT_TERMINATED,
                        "当前状态 " + record.getStatus() + " 不允许重跑，仅终止态事件可重跑");
            }
            if (record.getRerunCount() >= properties.getMaxReruns()) {
                log.warn("重跑拒绝 幂等键={} 原因=RERUN_LIMIT_REACHED 重跑轮次={} 上限={}",
                        idempotencyKey, record.getRerunCount(), properties.getMaxReruns());
                return RerunResult.rejected(RerunRejectionReason.RERUN_LIMIT_REACHED,
                        "重跑轮数已达上限 " + properties.getMaxReruns() + "，不允许再次重跑");
            }
            long inFlight = 0;
            for (EventRecord r : store.all()) {
                EventStatus s = r.getStatus();
                if (s == EventStatus.QUEUED || s == EventStatus.PROCESSING) {
                    inFlight++;
                }
            }
            if (inFlight >= properties.getMaxQueueCapacity()) {
                log.warn("重跑拒绝 幂等键={} 原因=QUEUE_FULL 在途={} 容量={}",
                        idempotencyKey, inFlight, properties.getMaxQueueCapacity());
                return RerunResult.rejected(RerunRejectionReason.QUEUE_FULL,
                        "处理容量已满，在途事件 " + inFlight + "/" + properties.getMaxQueueCapacity()
                                + "，重跑未入队");
            }

            Instant now = clock.instant();
            int nextRound = record.getRerunCount() + 1;
            record.beginRerun(now);
            store.persist();
            log.info("重跑接受 幂等键={} 进入第{}轮(重跑第{}次) 累计尝试={} 上一轮终止原因已归档 状态=QUEUED",
                    idempotencyKey, nextRound, nextRound, record.getAttempts());
            return RerunResult.accepted(record);
        }
    }
}
