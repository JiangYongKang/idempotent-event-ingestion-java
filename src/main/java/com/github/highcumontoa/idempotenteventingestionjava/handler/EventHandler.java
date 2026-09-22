package com.github.highcumontoa.idempotenteventingestionjava.handler;

import com.github.highcumontoa.idempotenteventingestionjava.model.EventRecord;

/**
 * 事件处理器：所有业务副作用（演示中为“记账”）的唯一边界。
 *
 * <p>约束：处理器可以被同一幂等键多次调用（例如处理中途崩溃后重试），
 * 但演示副作用账本会以幂等键去重，确保重复调用不会重复记账。</p>
 *
 * @return 处理结果凭证（成功时写入事件记录）
 */
public interface EventHandler {

    String handle(EventRecord record) throws EventProcessingException;
}
