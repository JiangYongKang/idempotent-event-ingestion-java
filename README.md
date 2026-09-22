# 幂等事件接入与可靠处理（本地可运行，无外部依赖）

一套基于 Spring Boot 3 / Java 21 的本地事件接入服务。不依赖数据库、MQ 或任何外部服务与真实凭据：
状态保存在本地 JSON 快照文件中，业务处理器为内置的内存实现。支持：

- 并发重复投递下的**幂等去重**（同一幂等键只产生一次有效处理，重复请求得到可区分反馈）；
- 处理失败后的**指数退避重试与明确终止**（失败原因可区分，不静默丢失）；
- 摄入与后台重试并发进行时**口径一致、不重不漏**的状态统计；
- 非法事件**拒绝**、超容量**限流**，不阻塞调用、不误报成功；
- **服务重启后的状态恢复**（快照重放 + PROCESSING 中断事件重新入队）。

## 1. 运行与验证

```bash
# JDK 21+、Maven 3.9+；无需任何外部服务或凭据
mvn spring-boot:run                     # 启动，监听 8080
mvn test                                # 全部 12 个测试（含并发/重试/终止/重启恢复）
```

手工验证：

```bash
# 首次受理 -> 202 Accepted
curl -i -X POST localhost:8080/api/events -H 'Content-Type: application/json' \
  -d '{"idempotencyKey":"k-1","type":"demo","payload":"{\"effect\":\"cash+1\"}"}'

# 重复投递 -> 200 OK，body.outcome = DUPLICATE（与首次的 202/ACCEPTED 明确区分）
curl -i -X POST localhost:8080/api/events -H 'Content-Type: application/json' \
  -d '{"idempotencyKey":"k-1","type":"demo","payload":"{\"effect\":\"cash+1\"}"}'

# 可重试失败：第 3 次尝试起成功（退避后自动重试）
curl -X POST localhost:8080/api/events -H 'Content-Type: application/json' \
  -d '{"idempotencyKey":"k-2","type":"demo",
       "payload":"{\"behavior\":\"FAIL_RETRYABLE\",\"failUntilAttempt\":3}"}'

# 查询单事件 / 统计
curl localhost:8080/api/events/k-2
curl localhost:8080/api/events/stats
```

## 2. HTTP 接口与响应语义

| 方法/路径 | 说明 | 状态码 |
|---|---|---|
| `POST /api/events` | 事件接入 | 首次受理 **202**；重复 **200**（`outcome=DUPLICATE`，回显当前状态与说明）；非法 **400**；容量超限 **503** |
| `GET /api/events/{key}` | 查询单事件状态 | 存在 **200**；不存在 **404** |
| `GET /api/events/stats` | 一致性统计快照 | **200** |

- 400：`{"error":"INVALID_EVENT","message":"idempotencyKey 不能为空"}`，事件**不进入**任何状态桶，计入 `rejected`。
- 503：`{"error":"CAPACITY_EXCEEDED","message":"...","retryable":true}`，事件**不进入**任何状态桶，计入 `rateLimited`。
- 200 重复响应的 `message` 按当前状态区分：排队 / 处理中 / 已成功 / 已终止（含终止原因）。

## 3. 状态机与幂等语义

```
                 插入(首次, CAS)
   不存在 ───────────────────────► PENDING ──认领(CAS)──► PROCESSING
     ▲                              ▲  │                    │
     │重复(不插入)                  │  │退避到期             ├─ 成功 ─► SUCCEEDED（终态）
   任意已存在                        │  └─重启恢复           ├─ 不可重试失败 ─► TERMINATED（终态）
                                     └────────────────────  └─ 可重试且超过上限 ─► TERMINATED（终态）
```

- **幂等键为主键**。准入临界区（校验 → 容量判断 → 插入）串行；`insertIfAbsent` 保证并发首次投递只有一个成功，其余全部得到 `DUPLICATE`，`total` 不增加、无重复副作用。
- 每次状态迁移都走存储层的**版本号 CAS**（`compareAndUpdate(record, expectedVersion)`），旧结果无法覆盖新状态。
- 内存中的 `scheduledKeys` 在途集合保证同一事件全局至多一个工作线程；立即调度与后台重试即使同时触发，PENDING→PROCESSING 的认领 CAS 也只有一方成功。
- 内置 `DefaultEventHandler` 还有一层**副作用幂等账本**（按幂等键去重）：当崩溃恢复导致 at-least-once 重投时，同一副作用也不会重复记账。

> 投递语义：接受即持久化为 PENDING（至少一次），状态机迁移精确一次；崩溃发生在 PROCESSING 窗口时，业务处理器需保持幂等（内置处理器已示范），可安全重放。

## 4. 重试与终止语义

- 处理器返回三类结论：成功 / 可重试失败 / 不可重试失败；处理器**抛出异常**按可重试失败处理，不允许静默吞掉。
- 可重试失败：事件回到 PENDING，`nextAttemptAt = now + 退避`，退避为
  `min(backoffInitialMillis * backoffMultiplier^(attempt-1), backoffMaxMillis)`，由后台调度器（默认每 100ms 扫描）到期重新提交。
- 尝试次数 `attemptCount`：首次为 1，每次重试 +1；当 `attemptCount > maxRetries` 仍失败，进入 **TERMINATED**。
- 终止原因（`failureReason`，统计分项可区分）：

| failureReason | 含义 |
|---|---|
| `NON_RETRYABLE_ERROR` | 不可重试失败，首次尝试即终止，不发生重试 |
| `RETRIES_EXHAUSTED` | 可重试失败但已达到重试上限 |
| `RECOVERED_AFTER_RESTART` | 服务重启时发现事件停留在 PROCESSING，重置为 PENDING 时的标注（重试成功后清除；若后续再次失败终止，则由对应终态原因取代） |

  终止事件的错误文本保存在 `detail` 字段，**不静默丢失**；终止事件不会再被调度。

## 5. 状态与统计口径

`GET /api/events/stats` 对存储做一次一致性快照后计数：

- `pending` / `processing` / `succeeded` / `terminated` 四桶**互斥**，每个已受理事件按当前状态恰好归入一桶，故
  **四桶之和 = total = 已受理（首次插入成功）事件数**，重复投递、被拒绝、被限流都不计入。
- `terminated` 再按 `failureReason` 细分为 `terminalNonRetryableError` / `terminalRetriesExhausted` / `terminalRetryableError` / `terminalRecoveredAfterRestart`，细分之和等于 `terminated`。
- `rejected`（非法请求）、`rateLimited`（容量限流）为独立计数器，在请求被拒绝时递增，重启后归零（它们不对应持久化事件）。

容量口径：准入时统计 `PENDING + PROCESSING` 数量，达到 `eventing.max-in-flight` 即对新投递返回 503；容量判断与插入在同一临界区，因此不存在"超额插入后补报失败"。

## 6. 服务重启与恢复

- 每次插入/状态迁移都在同一把存储锁内将全量快照经临时文件 + `ATOMIC_MOVE` 原子落盘到 `events.json`，无撕裂文件；启动时加载该快照。
- 应用就绪事件中先执行 `recover()`：快照里停留在 **PROCESSING** 的事件没有完整结论，一律重置为 PENDING、标注 `RECOVERED_AFTER_RESTART`、`nextAttemptAt=0` 立即重新排队；PENDING（含退避中）、SUCCEEDED、TERMINATED 维持原状。
- 随后调度器扫描到期事件继续重试。测试 `restartRecovery_processingInterrupted_thenRetriedToSuccess` 与 `recoverAfterRestart_interruptedProcessingRequeued` 覆盖该链路。

## 7. 可配置项（`src/main/resources/application.properties`，前缀 `eventing.`）

| 配置项 | 默认值 | 说明 |
|---|---|---|
| `data-dir` | `data/eventing` | 本地快照存储目录 |
| `max-in-flight` | `1000` | 在途（PENDING+PROCESSING）容量上限，超出返回 503 |
| `max-retries` | `3` | 最大重试次数（不含首次尝试） |
| `backoff-initial-millis` | `200` | 指数退避初始延迟 |
| `backoff-multiplier` | `2.0` | 退避倍数 |
| `backoff-max-millis` | `5000` | 退避上限 |
| `scheduler-interval-millis` | `100` | 后台重试扫描间隔 |
| `worker-threads` | `4` | 事件处理工作线程数 |

## 8. 测试与可解释日志

- `EventIngestionServiceTest`（直接装配，快）：32 线程并发重复投递去重、重试至成功（副作用计数=1）、重试耗尽终止、不可重试立即终止、非法 400 计数、超容量 503 计数、重启恢复。
- `EventLifecycleIntegrationTest`（真实 Spring MVC + 调度器 + 快照）：HTTP 状态码语义、24 路并发重复、终止原因分项、复制快照模拟重启后恢复。

关键日志在摄入、处理、重试、终止、恢复各环节统一打印**幂等键、尝试/重试次数与判定依据**，例如：

```
事件接入 idempotencyKey=k-1 attempt=0 判定依据=首次投递,已受理并立即调度
事件接入 idempotencyKey=k-1 attempt=1 判定依据=重复投递,当前状态=SUCCEEDED,不重复计数
事件处理 idempotencyKey=k-2 attempt=2 重试次数=1 判定依据=可重试失败,退避200ms后重试(上限3)
事件处理 idempotencyKey=k-2 attempt=3 重试次数=2 判定依据=处理成功,迁移至SUCCEEDED
事件恢复 idempotencyKey=k-9 attempt=1 判定依据=重启恢复:PROCESSING->PENDING,重新排队
```

## 9. 内置处理器 payload 协议（便于演示/测试）

`payload` 为可选 JSON；缺省一律成功：

```json
{
  "behavior": "SUCCEED | FAIL_RETRYABLE | FAIL_NON_RETRYABLE",
  "failUntilAttempt": 2,
  "effect": "account-1:+10"
}
```

- `FAIL_RETRYABLE` + `failUntilAttempt=N`：前 N-1 次可重试失败，第 N 次起成功；不设则一直可重试失败直到上限。
- `effect`：声明记账副作用，用于断言"重复投递/恢复重投不重复记账"（处理器内置幂等账本）。
