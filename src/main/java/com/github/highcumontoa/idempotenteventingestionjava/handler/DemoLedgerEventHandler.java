package com.github.highcumontoa.idempotenteventingestionjava.handler;

import com.github.highcumontoa.idempotenteventingestionjava.model.EventRecord;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 演示用“记账”处理器：所有副作用只在本类的内存账本上发生。
 *
 * <p>载荷约定（纯文本，前缀驱动，便于本地验证各种语义，无需任何外部服务）：</p>
 * <ul>
 *   <li>{@code RETRYABLE:<reason>} —— 每次都抛“可重试”异常，用于演示重试直至上限终止；</li>
 *   <li>{@code FATAL:<reason>}     —— 抛“不可重试”异常，用于演示立即终止；</li>
 *   <li>{@code FLAKY:N}            —— 前 N 次尝试可重试失败，第 N+1 次起成功；</li>
 *   <li>其他载荷                    —— 正常成功。</li>
 * </ul>
 *
 * <p>副作用幂等：账本按幂等键记录。即便因崩溃重放导致处理器被同一键调用多次，
 * 也只在首次真正记账一次；重复调用直接返回原凭证，不重复记账。</p>
 */
public class DemoLedgerEventHandler implements EventHandler {

    private static final Logger log = LoggerFactory.getLogger(DemoLedgerEventHandler.class);

    /** 幂等键 -> 记账凭证。putIfAbsent 保证同一键只记账一次。 */
    private final Map<String, String> ledger = new ConcurrentHashMap<>();
    private final AtomicLong sequence = new AtomicLong();

    /** 实际发生的记账次数（不含被幂等拦截的重复调用）。 */
    private final AtomicLong appliedEffectCount = new AtomicLong();

    @Override
    public String handle(EventRecord record) throws EventProcessingException {
        String key = record.getIdempotencyKey();
        int attempt = record.getAttempts();
        String payload = record.getPayload() == null ? "" : record.getPayload();

        // 崩溃重放 / 重复调用的副作用防线：已记过账则直接返回原凭证。
        String existing = ledger.get(key);
        if (existing != null) {
            log.info("幂等键={} 第{}次尝试 判定=副作用已存在,直接返回既有凭证,不重复记账", key, attempt);
            return existing;
        }

        if (payload.startsWith("RETRYABLE:")) {
            log.info("幂等键={} 第{}次尝试 判定=可重试失败({})", key, attempt,
                    payload.substring("RETRYABLE:".length()));
            throw new EventProcessingException("可重试失败: " + payload.substring("RETRYABLE:".length()), true);
        }
        if (payload.startsWith("FATAL:")) {
            log.info("幂等键={} 第{}次尝试 判定=不可重试失败({})", key, attempt,
                    payload.substring("FATAL:".length()));
            throw new EventProcessingException("不可重试失败: " + payload.substring("FATAL:".length()), false);
        }
        if (payload.startsWith("FLAKY:")) {
            int failTimes = 0;
            try {
                failTimes = Integer.parseInt(payload.substring("FLAKY:".length()).trim());
            } catch (NumberFormatException ignored) {
                throw new EventProcessingException("FLAKY 载荷格式非法: " + payload, false);
            }
            if (attempt <= failTimes) {
                log.info("幂等键={} 第{}次尝试 判定=可重试失败(计划前{}次失败)", key, attempt, failTimes);
                throw new EventProcessingException(
                        "临时性失败，计划在第 " + (failTimes + 1) + " 次恢复", true);
            }
        }

        // 成功路径：原子记账，保证恰好一次副作用。
        String voucher = "LEDGER-" + sequence.incrementAndGet();
        String winner = ledger.putIfAbsent(key, voucher);
        if (winner != null) {
            log.info("幂等键={} 第{}次尝试 判定=并发副作用竞争失败,复用既有凭证,不重复记账", key, attempt);
            return winner;
        }
        appliedEffectCount.incrementAndGet();
        log.info("幂等键={} 第{}次尝试 判定=成功记账 凭证={} 累计记账={}", key, attempt, voucher,
                appliedEffectCount.get());
        return voucher;
    }

    public long getAppliedEffectCount() {
        return appliedEffectCount.get();
    }

    public boolean isLedgerContains(String idempotencyKey) {
        return ledger.containsKey(idempotencyKey);
    }
}
