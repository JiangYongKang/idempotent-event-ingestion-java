package com.github.highcumontoa.idempotenteventingestionjava.handler;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.highcumontoa.idempotenteventingestionjava.model.EventRecord;
import com.github.highcumontoa.idempotenteventingestionjava.model.HandlerResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 本地默认事件处理器（无需外部服务）。
 *
 * <p>payload 为可选 JSON，支持以下测试/演示协议（缺省一律成功）：
 * <pre>
 * {
 *   "behavior": "SUCCEED | FAIL_RETRYABLE | FAIL_NON_RETRYABLE",
 *   "failUntilAttempt": 2,          // 可重试失败时，第 N 次尝试起转为成功（默认不转）
 *   "effect": "account-1:+10"       // 声明副作用；处理器内置幂等账本，同键只记一次
 * }
 * </pre>
 *
 * <p>处理器自身以幂等键做副作用去重：即使框架在崩溃恢复后重投（at-least-once 投递），
 * 同一事件的副作用也不会重复记账。
 */
@Component
public class DefaultEventHandler implements EventHandler {

    private static final Logger log = LoggerFactory.getLogger(DefaultEventHandler.class);

    private final ObjectMapper objectMapper = new ObjectMapper();
    /** 副作用幂等账本：已成功应用副作用的幂等键集合。 */
    private final ConcurrentHashMap<String, Boolean> appliedEffects = new ConcurrentHashMap<>();
    private final AtomicLong successCount = new AtomicLong();

    @Override
    public HandlerResult handle(EventRecord record) {
        int attempt = record.getAttemptCount();
        String key = record.getIdempotencyKey();
        JsonNode node = parse(record.getPayload());

        String behavior = text(node, "behavior", "SUCCEED");
        int failUntilAttempt = intOr(node, "failUntilAttempt", Integer.MAX_VALUE);
        String effect = text(node, "effect", null);

        switch (behavior) {
            case "FAIL_NON_RETRYABLE" -> {
                log.info("处理事件 idempotencyKey={} attempt={} 判定依据=payload声明不可重试失败", key, attempt);
                return HandlerResult.failure(false, "业务不可重试失败: " + behavior);
            }
            case "FAIL_RETRYABLE" -> {
                if (attempt < failUntilAttempt) {
                    log.info("处理事件 idempotencyKey={} attempt={} 判定依据=可重试失败(attempt<{}),进入退避重试",
                            key, attempt, failUntilAttempt);
                    return HandlerResult.failure(true,
                            "可重试失败: 第 " + attempt + " 次尝试, 计划第 " + failUntilAttempt + " 次起成功");
                }
                return applyEffect(key, attempt, effect, "重试达到阈值后成功");
            }
            case "SUCCEED" -> {
                return applyEffect(key, attempt, effect, "处理成功");
            }
            default -> {
                log.info("处理事件 idempotencyKey={} attempt={} 判定依据=未知behavior={}按不可重试失败处理",
                        key, attempt, behavior);
                return HandlerResult.failure(false, "未知 behavior: " + behavior);
            }
        }
    }

    private HandlerResult applyEffect(String key, int attempt, String effect, String reason) {
        if (effect != null) {
            // 幂等账本：同键副作用只应用一次（含崩溃恢复后的重复投递）
            if (appliedEffects.putIfAbsent(key, Boolean.TRUE) != null) {
                log.info("处理事件 idempotencyKey={} attempt={} 判定依据=副作用已记账,跳过重复副作用", key, attempt);
                return HandlerResult.success("effect-already-applied");
            }
            successCount.incrementAndGet();
            log.info("处理事件 idempotencyKey={} attempt={} 判定依据={},副作用已记账 effect={}", key, attempt, reason, effect);
            return HandlerResult.success("effect-applied:" + effect);
        }
        log.info("处理事件 idempotencyKey={} attempt={} 判定依据={},无副作用", key, attempt, reason);
        return HandlerResult.success("ok");
    }

    private JsonNode parse(String payload) {
        if (payload == null || payload.isBlank()) {
            return null;
        }
        try {
            return objectMapper.readTree(payload);
        } catch (Exception e) {
            // 协议非法属于不可重试错误
            return objectMapper.createObjectNode().put("behavior", "FAIL_NON_RETRYABLE");
        }
    }

    private String text(JsonNode node, String field, String def) {
        return node != null && node.hasNonNull(field) ? node.get(field).asText() : def;
    }

    private int intOr(JsonNode node, String field, int def) {
        return node != null && node.hasNonNull(field) ? node.get(field).asInt() : def;
    }

    /** 已应用的副作用总数（测试断言"不重复记账"用）。 */
    public long getAppliedEffectCount() {
        return successCount.get();
    }

    /** 测试辅助：清空副作用账本。 */
    public void resetForTesting() {
        appliedEffects.clear();
        successCount.set(0);
    }
}
