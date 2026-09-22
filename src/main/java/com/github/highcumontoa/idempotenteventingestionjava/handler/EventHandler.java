package com.github.highcumontoa.idempotenteventingestionjava.handler;

import com.github.highcumontoa.idempotenteventingestionjava.model.EventRecord;
import com.github.highcumontoa.idempotenteventingestionjava.model.HandlerResult;

/**
 * 业务事件处理器接口。
 */
public interface EventHandler {
    HandlerResult handle(EventRecord record);
}
