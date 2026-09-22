package com.github.highcumontoa.idempotenteventingestionjava.store;

import com.github.highcumontoa.idempotenteventingestionjava.model.EventRecord;

import java.util.List;

/**
 * 事件记录存储抽象（本地文件实现，支持重启恢复）。
 */
public interface EventStore {

    /** 启动时加载全部持久化记录。 */
    List<EventRecord> loadAll();

    /** 按幂等键查询当前记录。 */
    EventRecord find(String idempotencyKey);

    /** 插入新记录；若键已存在返回 false。 */
    boolean insertIfAbsent(EventRecord record);

    /**
     * 基于版本号的条件更新（CAS），状态机迁移的唯一入口。
     * expectedVersion 不匹配返回 false，保证并发下状态迁移精确一次。
     */
    boolean compareAndUpdate(EventRecord record, int expectedVersion);
}
