package com.github.highcumontoa.idempotenteventingestionjava.service;

import com.github.highcumontoa.idempotenteventingestionjava.model.EventRecord;
import com.github.highcumontoa.idempotenteventingestionjava.model.EventStats;
import com.github.highcumontoa.idempotenteventingestionjava.model.EventStatus;
import com.github.highcumontoa.idempotenteventingestionjava.store.EventStore;
import java.util.Collection;
import org.springframework.stereotype.Service;

/**
 * 状态与统计查询。统计在 {@link EventStore} 监视器内对一份快照计数：
 * 四种状态互斥，{@code queued + processing + succeeded + terminated == total}，
 * 在摄入与后台重试并发进行时依然不重不漏。
 */
@Service
public class EventQueryService {

    private final EventStore store;

    public EventQueryService(EventStore store) {
        this.store = store;
    }

    public EventRecord get(String idempotencyKey) {
        return store.find(idempotencyKey);
    }

    public EventStats stats() {
        long queued = 0;
        long processing = 0;
        long succeeded = 0;
        long terminated = 0;
        long total;
        synchronized (store) {
            Collection<EventRecord> snapshot = store.all();
            total = snapshot.size();
            for (EventRecord r : snapshot) {
                EventStatus s = r.getStatus();
                switch (s) {
                    case QUEUED -> queued++;
                    case PROCESSING -> processing++;
                    case SUCCEEDED -> succeeded++;
                    case TERMINATED -> terminated++;
                }
            }
        }
        return new EventStats(queued, processing, succeeded, terminated, total);
    }
}
