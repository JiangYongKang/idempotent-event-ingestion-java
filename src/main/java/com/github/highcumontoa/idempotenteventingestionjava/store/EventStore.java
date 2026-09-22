package com.github.highcumontoa.idempotenteventingestionjava.store;

import com.github.highcumontoa.idempotenteventingestionjava.model.EventRecord;
import java.util.Collection;

/** 事件记录存储抽象。 */
public interface EventStore {
    EventRecord find(String idempotencyKey);
    EventRecord putIfAbsent(EventRecord record);
    Collection<EventRecord> all();
    void persist();
}
