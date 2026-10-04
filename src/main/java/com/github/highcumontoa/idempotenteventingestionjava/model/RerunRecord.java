package com.github.highcumontoa.idempotenteventingestionjava.model;

import java.io.Serial;
import java.io.Serializable;
import java.time.Instant;

/**
 * 一轮已结束处理的历史快照：重跑发起时把上一轮（含最初一轮）的终局信息归档，
 * 保证“上一轮为什么终止、那一轮处理了几次”在重跑后依然可查。
 *
 * @param round             轮次序号，0 表示最初一轮，1 表示第 1 次重跑，以此类推
 * @param terminationReason 该轮终止原因
 * @param attempts          该轮实际执行的处理尝试次数
 * @param lastError         该轮最后一次失败的错误信息
 * @param terminatedAt      该轮进入终止态的时间
 */
public record RerunRecord(int round, TerminationReason terminationReason, int attempts,
                          String lastError, Instant terminatedAt) implements Serializable {

    @Serial
    private static final long serialVersionUID = 1L;
}
