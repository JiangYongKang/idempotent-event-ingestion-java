# 幂等事件接入与可靠处理（本地可运行）

一个基于 Spring Boot 3 / Java 21 的**纯本地**事件接入与可靠处理样例：
无需数据库、消息队列或任何真实凭据，状态持久化使用本地文件（Java 序列化 + 原子替换）。

它保证：外部事件在**并发重复投递、处理失败、服务重启**等情况下，仍得到一致且可解释的结果。

---

## 1. 事件生命周期与幂等语义

每个事件以 `idempotencyKey` 为唯一键，状态机如下：

```
                首次接入(校验通过且未超容量)
                         │
                         ▼
                      QUEUED ──(到点)──► PROCESSING ──成功──► SUCCEEDED（终态，结果凭证）
                        ▲                   │
                        │                   ├─ 可重试失败且未达上限 ──退避──┘
                        │                   │
                        └─ 重启时 PROCESSING ┘
                                            ├─ 不可重试失败 ──────────► TERMINATED(NON_RETRYABLE)
                        ┌─── 人工重跑 ◄─────┴─ 可重试失败且达到上限 ──► TERMINATED(RETRY_EXHAUSTED)
                        │   (仅终止态,轮数有上限)
                        └──────────────► 重新进入 QUEUED 完整再跑一轮
```

- **幂等去重**：接入时在存储的同一把监视器锁内完成 `find + putIfAbsent`。
  并发重复投递中只有一个线程得到 `ACCEPTED`，其余线程都得到 `DUPLICATE` 并返回既有记录，
  **绝不二次处理、二次记账**。
- **重复请求可区分**：`ACCEPTED`（首次，HTTP 202）与 `DUPLICATE`（重复，HTTP 200）是不同结果；
  即使事件最终成功、终止，或在服务重启后重复投递，结论都一致。
- **副作用恰好一次**：演示处理器 `DemoLedgerEventHandler` 内部以幂等键维护账本（`putIfAbsent`），
  即便因崩溃重放导致处理器被同一键再次调用，也只记账一次。

## 2. 重试与终止语义

- 处理器通过 `EventProcessingException(message, retryable)` 明确声明失败是否可重试：
  - `retryable=true`：按**指数退避**重新排队，等待 `nextAttemptAt` 到期后再次处理；
  - `retryable=false`：**立即终止**，原因 `NON_RETRYABLE`，不浪费重试；
  - 可重试失败达到 `eventing.max-attempts` 上限：终止，原因 `RETRY_EXHAUSTED`；
  - 处理器抛出未受控 `RuntimeException`：按可重试处理，达上限同样终止——**任何失败都不静默丢失**。
- 退避公式：`delay = min(baseDelay * multiplier^(attempts-1), maxDelay)`。
- 终止态事件保留 `lastError`、`terminationReason`、`attempts`，不再被后台自动捞起，
  可随时查询解释；如需再处理只能由人工显式发起受控重跑（见第 3 节）。

演示载荷约定（纯文本前缀）：

| 载荷 | 行为 |
| --- | --- |
| `NORMAL`（或任意普通文本） | 直接成功记账 |
| `RETRYABLE:<原因>` | 每次都可重试失败，用于演示重试到上限终止 |
| `FATAL:<原因>` | 不可重试失败，立即 `NON_RETRYABLE` 终止 |
| `FLAKY:N` | 每轮前 N 次可重试失败，第 N+1 次起成功（按本轮计数） |
| `FIXABLE:<原因>` | 可重试失败，直到“故障被修复”（演示终止后重跑成功） |

## 3. 终止事件受控重跑

终止（`TERMINATED`）不等于彻底没救：下游恢复、数据改对之后，可以由人工对同一幂等键
发起一次**受控重跑**，事件重新进入 `QUEUED` 完整再跑一轮，无需换新键重新提交。

- **语义**：
  - 仅 `TERMINATED` 状态可重跑；重跑把事件迁移回 `QUEUED`（`nextAttemptAt=现在`），
    由后台 worker 按与首次接入完全相同的退避、上限、终止规矩再跑一轮；
  - 重跑成功后与正常成功完全一致：`SUCCEEDED` + 结果凭证（`result`）；
  - 历史不丢：`rerunCount` 记录已发起的重跑轮数，`rerunHistory[]` 归档每一轮的
    `round / terminationReason / attempts / lastError / terminatedAt`，
    `attempts` 跨轮累计（`attemptsInRound` 为当前轮计数，重试上限按本轮计）——
    单查一条事件即可看清“被重跑过几轮、上一轮为什么终止、总共处理过几次”。
- **边界（拒绝原因互相可区分）**：
  - 键不存在 → `NOT_FOUND`（HTTP 404）；
  - 状态不是 `TERMINATED`（排队中/处理中/已成功）→ `NOT_TERMINATED`（HTTP 409）；
  - 重跑轮数达到 `eventing.max-reruns` → `RERUN_LIMIT_REACHED`（HTTP 409，
    与状态不允许是同码不同因，`rejectionReason` 字段区分）；
  - 在途容量（`QUEUED+PROCESSING`）已满 → `QUEUE_FULL`（HTTP 503），
    与接入限流同一口径，**立即拒绝返回，不阻塞调用方，事件保持终止态不变**。
- **并发安全**：状态判断与迁移在存储监视器内原子完成。多人同时重跑同一事件时，
  只有第一个请求能把 `TERMINATED → QUEUED`，其余看到状态已变，得到
  `NOT_TERMINATED` 拒绝——同一条事件最多真正重新入队一次，不重复入队、不重复副作用。
- **与统计口径的兼容**：重跑只是状态迁移（`terminated-1 / queued+1`），不增删记录，
  `total` 不变，四类计数恒等式不受影响。
- **与幂等/重试/终止语义的兼容**：重跑不改变幂等键与载荷；重跑轮若再次失败，
  仍按指数退避、`max-attempts` 上限、`TERMINATED` 终止的老规矩走，
  且新一轮的历史会与前面轮次分开归档，不混成一笔。

## 4. 状态与统计口径

- 四种状态 `QUEUED / PROCESSING / SUCCEEDED / TERMINATED` **互斥**，事件任意时刻只处于其中一种。
- `GET /api/stats` 在同一存储监视器内对一份快照计数，恒有：

  `queued + processing + succeeded + terminated == total`（响应中的 `consistent` 字段即该校验）。

  因此在“事件摄入 + 后台重试 + 统计查询”并发进行时，口径**不重不漏**。
- 容量限流按在途事件（`QUEUED + PROCESSING`）计数，达到 `eventing.max-queue-capacity` 时
  立即返回 `REJECTED/QUEUE_FULL`（HTTP 503），**不阻塞调用线程，也不误报成功**；
  非法输入（空键、超长键、null/超大载荷）返回 `REJECTED/INVALID_EVENT`（HTTP 400）。

## 5. 重启恢复

- 每次状态迁移都先更新内存再同步落盘（临时文件 + 原子 `move`，避免半截文件）。
- 启动时从数据文件恢复全部事件；崩溃前残留在 `PROCESSING` 的事件安全回退为 `QUEUED`
  （处理非提交型：记账副作用由处理器自身的幂等账本兜底），随后由后台 worker 重新处理。
- **恢复调度与处理使用同一时钟**：`FileEventStore` 构造时注入与处理/调度相同的 `Clock`，
  回退 `PROCESSING` 时计算的 `nextAttemptAt` 与后续“到点判断”同源，
  保证重启后本该继续处理的事件真的会被捞起，不会因时钟来源不一致而一直停在原地。
- 成功/终止态及其结果、错误原因在重启后完整保留；重启后重复投递仍为 `DUPLICATE`。
- 重跑产生的状态同样持久：`rerunCount` / `rerunHistory` / 重跑入队的 `QUEUED`
  在重启后完整恢复，重跑轮会继续被处理（见 `RerunControlTest#rerunQueuedEventContinuesAfterRestart`）。

## 6. HTTP 接口

| 方法/路径 | 说明 | 状态码 |
| --- | --- | --- |
| `POST /api/events` | 请求体 `{"idempotencyKey":"...","payload":"..."}` | 202 首次接受；200 重复；400 非法；503 限流 |
| `POST /api/events/{key}/rerun` | 对终止事件发起受控重跑 | 202 已重新入队；404 键不存在；409 状态不允许(`NOT_TERMINATED`)或轮数到顶(`RERUN_LIMIT_REACHED`)；503 容量不足(`QUEUE_FULL`) |
| `GET /api/events/{key}` | 查询单个事件的完整状态 | 200 / 404 |
| `GET /api/stats` | 四类状态计数与一致性标记 | 200 |

单条事件视图包含：`status / attempts(跨轮累计) / attemptsInRound(本轮) / rerunCount /
rerunHistory(每轮 round/terminationReason/attempts/lastError/terminatedAt) /
lastError / lastErrorRetryable / terminationReason / result / lastAttemptAt / nextAttemptAt`
等可解释字段。

## 7. 可配置项（`src/main/resources/application.properties`，前缀 `eventing`）

| 配置项 | 默认值 | 含义 |
| --- | --- | --- |
| `eventing.data-file` | `data/events.dat` | 本地持久化文件路径 |
| `eventing.max-attempts` | `5` | 最大尝试次数（含首次），达到后 `RETRY_EXHAUSTED` |
| `eventing.retry-base-delay` | `200ms` | 指数退避基础延迟 |
| `eventing.retry-multiplier` | `2.0` | 退避乘子 |
| `eventing.retry-max-delay` | `10s` | 退避上限 |
| `eventing.max-queue-capacity` | `1000` | 在途容量，超限返回 503（接入与重跑共用此口径） |
| `eventing.max-reruns` | `3` | 同一事件最大重跑轮数，到顶返回 409/`RERUN_LIMIT_REACHED` |
| `eventing.max-key-length` | `128` | 幂等键最大长度 |
| `eventing.max-payload-bytes` | `65536` | 载荷最大字节数（UTF-8） |
| `eventing.worker-interval` | `100ms` | 后台重试轮询间隔 |
| `eventing.worker-enabled` | `true` | 是否启用后台 worker（测试可关闭） |

## 8. 本地验证

前置：JDK 21+、Maven 3.9+（依赖可离线使用，无需任何外部服务/凭据）。

```bash
# 1) 运行全部测试（并发重复投递 / 失败重试直至终止 / 受控重跑 / 重启恢复 / HTTP 语义）
mvn test

# 2) 启动应用
mvn spring-boot:run
# 或：mvn package -DskipTests && java -jar target/idempotent-event-ingestion-java-1.0-SNAPSHOT.jar

# 3) 另开终端，手工验证幂等与重试
curl -s -X POST localhost:8080/api/events -H 'Content-Type: application/json' \
  -d '{"idempotencyKey":"k1","payload":"NORMAL"}'                 # 202 ACCEPTED
curl -s -X POST localhost:8080/api/events -H 'Content-Type: application/json' \
  -d '{"idempotencyKey":"k1","payload":"NORMAL"}'                 # 200 DUPLICATE（不重复处理）
curl -s -X POST localhost:8080/api/events -H 'Content-Type: application/json' \
  -d '{"idempotencyKey":"k2","payload":"RETRYABLE:下游超时"}'     # 退避重试 5 次后终止
curl -s localhost:8080/api/events/k2                              # TERMINATED/RETRY_EXHAUSTED
curl -s localhost:8080/api/stats                                  # 四类计数 consistent=true

# 4) 手工验证受控重跑（k2 已终止）
curl -s -X POST localhost:8080/api/events/k2/rerun                # 202 ACCEPTED：重新入队，rerunCount=1
curl -s -X POST localhost:8080/api/events/k2/rerun                # 409 NOT_TERMINATED：处理中不允许重复重跑
curl -s localhost:8080/api/events/k2
#   -> 若载荷仍是 RETRYABLE，重跑轮会再次退避重试到上限终止，
#      rerunHistory[0] 记录第 0 轮的终止原因/尝试次数，与本轮互不混淆；
#      反复重跑至 eventing.max-reruns 轮后，再发起得到 409 RERUN_LIMIT_REACHED。
curl -s -X POST localhost:8080/api/events/k1/rerun                # 409 NOT_TERMINATED：成功态不允许重跑
curl -s -X POST localhost:8080/api/events/nope/rerun              # 404 NOT_FOUND
curl -s localhost:8080/api/stats                                  # 重跑只是状态迁移，total 不变，consistent=true

# 5) 重启验证：Ctrl+C 停止后再次启动，观察日志“恢复事件 N 条”，
#    GET /api/events/k1 仍为 SUCCEEDED 且 result 凭证不变；对 k1 再投递仍为 DUPLICATE；
#    若 k2 重跑后处于 QUEUED/退避中，重启后会被继续捞起处理，不会停在原地。
```

## 9. 测试如何打印“幂等键 / 重试次数 / 判定依据”

- 生产代码日志（`INFO`/`WARN`）在接入、每次尝试、重试排队、终止、成功、重跑接受/拒绝时都输出
  `幂等键=... 轮次=R 本轮第N次尝试 判定=...`（含退避时刻、终止原因或重跑拒绝原因）。
- 测试日志以 `[测试] 幂等键=... 重试次数=N 判定依据=...` 形式显式记录每个断言的依据。
- 查看某次测试输出：`mvn test -Dtest=RetryAndTerminationTest`
  （或 `target/surefire-reports/...txt`，完整输出见控制台）。

## 10. 关键设计取舍

- **单进程串行状态迁移**：所有状态判断与迁移在 `EventStore` 监视器内完成，
  后台重试使用单线程调度器，从结构上消除同键并发处理，统计天然一致。
- **文件原子持久化**：写 `*.tmp` 后原子 `move` 覆盖；以简单可靠换取非高吞吐场景的可解释性。
- **作用域说明**：本方案面向单实例本地可靠处理；多实例水平扩展需要共享存储/分布式锁，超出本样例范围。
