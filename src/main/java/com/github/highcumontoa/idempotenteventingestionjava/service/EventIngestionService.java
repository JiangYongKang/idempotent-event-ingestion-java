package com.github.highcumontoa.idempotenteventingestionjava.service;

import com.github.highcumontoa.idempotenteventingestionjava.config.EventingProperties;
import com.github.highcumontoa.idempotenteventingestionjava.model.EventRecord;
import com.github.highcumontoa.idempotenteventingestionjava.model.EventStatus;
import com.github.highcumontoa.idempotenteventingestionjava.model.IngestionResult;
import com.github.highcumontoa.idempotenteventingestionjava.model.RejectionReason;
import com.github.highcumontoa.idempotenteventingestionjava.store.EventStore;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

/**
 * 事件接入入口：负责参数校验、容量限流与幂等去重。
 *
 * <p>幂等语义：在 {@link EventStore} 监视器内做 {@code find} + {@code putIfAbsent}，
 * 因此并发重复投递中只有一个线程插入成功（ACCEPTED），其余全部拿到既有记录（DUPLICATE），
 * 从根源上杜绝重复处理与重复记账。</p>
 */
@Service
public class EventIngestionService {

    private static final Logger log = LoggerFactory.getLogger(EventIngestionService.class);

    private final EventStore store;
    private final EventProcessingService processingService;
    private final EventingProperties properties;
    private final Clock clock;

    /** 为 true 时新事件入队后立即同步处理一次（默认后台模式由 worker 处理）。 */
    private volatile boolean processInline = false;

    public EventIngestionService(EventStore store, EventProcessingService processingService,
                                 EventingProperties properties, Clock clock) {
        this.store = store;
        this.processingService = processingService;
        this.properties = properties;
        this.clock = clock;
    }

    public IngestionResult ingest(String idempotencyKey, String payload) {
        String invalid = validate(idempotencyKey, payload);
        if (invalid != null) {
            log.warn("拒绝事件 幂等键={} 原因=INVALID_EVENT 详情={}", idempotencyKey, invalid);
            return IngestionResult.rejected(RejectionReason.INVALID_EVENT, invalid);
        }

        EventRecord inserted;
        EventRecord existing;
        synchronized (store) {
            existing = store.find(idempotencyKey);
            if (existing != null) {
                log.info("重复投递 幂等键={} 当前状态={} 重试次数={} 判定=DUPLICATE 不重复处理",
                        idempotencyKey, existing.getStatus(), existing.getAttempts());
                return IngestionResult.duplicate(existing);
            }

            long queuedOrProcessing = 0;
            for (EventRecord r : store.all()) {
                EventStatus s = r.getStatus();
                if (s == EventStatus.QUEUED || s == EventStatus.PROCESSING) {
                    queuedOrProcessing++;
                }
            }
            if (queuedOrProcessing >= properties.getMaxQueueCapacity()) {
                log.warn("限流拒绝 幂等键={} 原因=QUEUE_FULL 在途={} 容量={}",
                        idempotencyKey, queuedOrProcessing, properties.getMaxQueueCapacity());
                return IngestionResult.rejected(RejectionReason.QUEUE_FULL,
                        "处理容量已满，在途事件 " + queuedOrProcessing + "/" + properties.getMaxQueueCapacity());
            }

            inserted = new EventRecord(idempotencyKey, payload, clock.instant());
            store.putIfAbsent(inserted);
            log.info("首次接受 幂等键={} 判定=ACCEPTED 进入QUEUED", idempotencyKey);
        }

        if (processInline) {
            processingService.processOnce(inserted);
        }
        return IngestionResult.accepted(inserted);
    }

    private String validate(String key, String payload) {
        if (key == null || key.isBlank()) {
            return "幂等键不能为空";
        }
        if (key.length() > properties.getMaxKeyLength()) {
            return "幂等键长度超过上限 " + properties.getMaxKeyLength();
        }
        if (payload == null) {
            return "事件载荷不能为 null";
        }
        int bytes = payload.getBytes(StandardCharsets.UTF_8).length;
        if (bytes > properties.getMaxPayloadBytes()) {
            return "载荷大小 " + bytes + " 字节超过上限 " + properties.getMaxPayloadBytes();
        }
        return null;
    }

    public void setProcessInline(boolean processInline) {
        this.processInline = processInline;
    }
}
