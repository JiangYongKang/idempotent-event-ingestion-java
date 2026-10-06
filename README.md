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
                        ▲                   ├─ 不可重试失败 ──────────► TERMINATED(NON_RETRYABLE)
                        │                   └─ 可重试失败且达到上限 ──► TERMINATED(RETRY_EXHAUSTED)
                        │                                                            │
                        └────────────── 人工受控重跑（轮数未达上限且容量充足） ◄──────────┘
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
- 终止态事件保留 `lastError`、`terminationReason`、`attempts`，永久不再被后台捞起，可随时查询解释。

## 2.1 终止事件的受控重跑

终止（`TERMINATED`，无论 `NON_RETRYABLE` 还是 `RETRY_EXHAUSTED`）的事件可由人工发起一次
**受控重跑**（`POST /api/events/{key}/rerun`）：事件被放回 `QUEUED` 完整再跑一轮，
退避、重试上限、终止规则与首轮完全一致。

- **历史不丢**：重跑后单条事件仍可查清楚——
  - `rerunCount`：这是第几轮重跑（0 = 首轮）；
  - `attempts`：累计总处理次数，跨轮次只增不减；
  - `roundAttempts`：本轮已处理次数（每轮重新计 0，退避与上限判定都按本轮计）；
  - `lastTerminationReason` / `lastTerminatedAt`：上一轮为什么终止、何时终止。
- **结果凭证**：重跑轮成功后与正常成功完全一致，`result` 写入新凭证，副作用仍恰好一次
  （处理器账本按幂等键去重）。
- **轮数上限**：同一事件最多重跑 `eventing.max-reruns` 轮。到顶后再发起返回
  `REJECTED / RERUN_LIMIT_REACHED`（HTTP 409）。
- **状态边界**：仅终止态允许重跑。对排队中 / 处理中 / 已成功 / 不存在的事件发起，
  分别返回 `NOT_TERMINATED`（HTTP 409）/ `NOT_FOUND`（HTTP 404）——
  与“轮数到顶”是**不同的拒绝原因**，调用方可明确区分。
- **容量约束**：重跑与首次接入共用同一在途容量口径（`QUEUED + PROCESSING`），
  容量不足立即返回 `QUEUE_FULL`（HTTP 503），不阻塞调用方、不改变任何状态与计数。
- **并发安全**：判定与状态迁移在同一存储监视器内原子完成。多人同时重跑同一事件，
  最多一个 `ACCEPTED` 真正重新入队，其余拿到 `NOT_TERMINATED`；重跑与新事件接入、
  后台重试并发进行时，四种状态计数仍恒满足 `queued+processing+succeeded+terminated == total`。
- **轮次可区分**：重跑轮若再次失败，仍按退避 → 上限 → 终止推进；
  日志与记录中 `重跑轮数 / 本轮次数 / 累计次数` 三者并列，不会与之前轮次混成一笔。

### 2.2 多轮重跑的逐轮历史

一条事件被反复重跑多轮后，**每一轮的终止情况都单独归档、永久保留**，查询单条事件时
通过 `roundHistory` 字段按轮次升序逐轮看清，各轮彼此独立、互不覆盖、互不串轮：

- **何时归档**：每当一轮（首轮或某轮重跑）迁移到 `TERMINATED` 时，在与状态迁移**同一个
  临界区**内追加一条 `RoundTerminationRecord`，只增、不改、不删；成功的轮次不产生终止记录，
  已有历史也不会被后续任何轮次（无论成功、失败还是再次重跑）冲掉。
- **每条历史包含**：
  - `round`：轮次编号，0 = 首轮，1 = 第 1 轮重跑……与终止发生时的 `rerunCount` 一致，连续不重不漏；
  - `terminationReason`：该轮为什么终止（`NON_RETRYABLE` / `RETRY_EXHAUSTED`）；
  - `roundAttempts`：该轮实际处理了几次（仅本轮）；
  - `totalAttemptsAtTermination`：该轮终止时的累计总次数（用于与全局口径对账）；
  - `lastError` / `lastErrorRetryable`：该轮最后的失败信息及是否可重试；
  - `terminatedAt`：该轮终止时刻。
- **口径对账**：`attempts`（累计处理次数）恒等于“各轮历史 `roundAttempts` 之和 + 当前轮
  `roundAttempts`”；`rerunCount`（当前是第几轮重跑）与历史条数也始终对得上——
  当前处于终止态时 `rerunCount == roundHistory.size() - 1`（本轮终止已归档），
  其余状态 `rerunCount == roundHistory.size()`。第 i 轮（0 起）的终止细节看
  `roundHistory[i]`，不会只剩最近一轮。
- **重启保留**：`roundHistory` 随事件记录一起原子落盘，重启前后逐字段一致；
  重启后处于 `QUEUED` 的重跑事件照常继续处理，历史的存在不影响推进。
- **并发安全**：归档发生在存储监视器内，与并发重跑、重复接入、后台重试互斥；
  多人同时重跑同一事件仍只有一个真正入队，历史不会出现重复条目或串轮。
- **与既有口径兼容**：`roundHistory` 是纯附加的可解释性元数据——状态机、幂等去重、
  退避重试、终止判定、四状态统计（`queued+processing+succeeded+terminated == total`）
  的规则与口径完全不变；`attempts / roundAttempts / rerunCount / lastTerminationReason`
  等原有字段含义不变，逐轮历史只是让它们“按轮可拆”。旧版本持久化的数据文件可直接加载
  （历史列表惰性初始化为空），无需迁移。

演示载荷约定（纯文本前缀）：

| 载荷 | 行为 |
| --- | --- |
| `NORMAL`（或任意普通文本） | 直接成功记账 |
| `RETRYABLE:<原因>` | 每次都可重试失败，用于演示重试到上限终止 |
| `FATAL:<原因>` | 不可重试失败，立即 `NON_RETRYABLE` 终止 |
| `FLAKY:N` | 前 N 次可重试失败，第 N+1 次起成功 |

## 3. 状态与统计口径

- 四种状态 `QUEUED / PROCESSING / SUCCEEDED / TERMINATED` **互斥**，事件任意时刻只处于其中一种。
- `GET /api/stats` 在同一存储监视器内对一份快照计数，恒有：

  `queued + processing + succeeded + terminated == total`（响应中的 `consistent` 字段即该校验）。

  因此在“事件摄入 + 后台重试 + 统计查询”并发进行时，口径**不重不漏**。
- 容量限流按在途事件（`QUEUED + PROCESSING`）计数，达到 `eventing.max-queue-capacity` 时
  立即返回 `REJECTED/QUEUE_FULL`（HTTP 503），**不阻塞调用线程，也不误报成功**；
  非法输入（空键、超长键、null/超大载荷）返回 `REJECTED/INVALID_EVENT`（HTTP 400）。

## 4. 重启恢复

- 每次状态迁移都先更新内存再同步落盘（临时文件 + 原子 `move`，避免半截文件）。
- 启动时从数据文件恢复全部事件；崩溃前残留在 `PROCESSING` 的事件安全回退为 `QUEUED`
  （处理非提交型：记账副作用由处理器自身的幂等账本兜底），随后由后台 worker 重新处理。
- 恢复回退使用与处理服务**同一个时钟**计算 `nextAttemptAt`，保证重启后到期判断正确，
  事件不会停在 `QUEUED` 不再前进。
- 成功/终止态及其结果、错误原因在重启后完整保留；重跑入队（`QUEUED`）的事件重启后
  同样继续被处理，`rerunCount`、`lastTerminationReason` 以及逐轮终止历史 `roundHistory`
  一并逐字段恢复；重启后重复投递仍为 `DUPLICATE`。

## 5. HTTP 接口

| 方法/路径 | 说明 | 状态码 |
| --- | --- | --- |
| `POST /api/events` | 请求体 `{"idempotencyKey":"...","payload":"..."}` | 202 首次接受；200 重复；400 非法；503 限流 |
| `POST /api/events/{key}/rerun` | 对终止事件发起受控重跑 | 202 重新入队；404 键不存在；409 状态不允许（`NOT_TERMINATED`）或轮数到顶（`RERUN_LIMIT_REACHED`）；503 容量不足（`QUEUE_FULL`） |
| `GET /api/events/{key}` | 查询单个事件的完整状态 | 200 / 404 |
| `GET /api/stats` | 四类状态计数与一致性标记 | 200 |

单条事件视图包含：`status / attempts（累计）/ roundAttempts（本轮）/ rerunCount /
lastError / lastErrorRetryable / terminationReason / lastTerminationReason / lastTerminatedAt /
roundHistory（逐轮终止历史，见 2.2 节）/ result / lastAttemptAt / nextAttemptAt` 等可解释字段。

## 6. 可配置项（`src/main/resources/application.properties`，前缀 `eventing`）

| 配置项 | 默认值 | 含义 |
| --- | --- | --- |
| `eventing.data-file` | `data/events.dat` | 本地持久化文件路径 |
| `eventing.max-attempts` | `5` | 最大尝试次数（含首次），达到后 `RETRY_EXHAUSTED` |
| `eventing.retry-base-delay` | `200ms` | 指数退避基础延迟 |
| `eventing.retry-multiplier` | `2.0` | 退避乘子 |
| `eventing.retry-max-delay` | `10s` | 退避上限 |
| `eventing.max-queue-capacity` | `1000` | 在途容量，超限返回 503（首次接入与重跑共用此口径） |
| `eventing.max-reruns` | `3` | 同一事件允许的最大重跑轮数，到顶返回 `RERUN_LIMIT_REACHED` |
| `eventing.max-key-length` | `128` | 幂等键最大长度 |
| `eventing.max-payload-bytes` | `65536` | 载荷最大字节数（UTF-8） |
| `eventing.worker-interval` | `100ms` | 后台重试轮询间隔 |
| `eventing.worker-enabled` | `true` | 是否启用后台 worker（测试可关闭） |

## 7. 本地验证

前置：JDK 21+、Maven 3.9+（依赖可离线使用，无需任何外部服务/凭据）。

```bash
# 1) 运行全部测试（并发重复投递 / 失败重试直至终止 / 受控重跑 / 多轮重跑历史 / 重启恢复 / HTTP 语义）
mvn test

# 2) 启动应用
mvn spring-boot:run
# 或：mvn package -DskipTests && java -jar target/idempotent-event-ingestion-java-1.0-SNAPSHOT.jar

# 3) 另开终端，手工验证
curl -s -X POST localhost:8080/api/events -H 'Content-Type: application/json' \
  -d '{"idempotencyKey":"k1","payload":"NORMAL"}'                 # 202 ACCEPTED
curl -s -X POST localhost:8080/api/events -H 'Content-Type: application/json' \
  -d '{"idempotencyKey":"k1","payload":"NORMAL"}'                 # 200 DUPLICATE（不重复处理）
curl -s -X POST localhost:8080/api/events -H 'Content-Type: application/json' \
  -d '{"idempotencyKey":"k2","payload":"RETRYABLE:下游超时"}'     # 退避重试 5 次后终止
curl -s localhost:8080/api/events/k2                              # TERMINATED/RETRY_EXHAUSTED
curl -s localhost:8080/api/stats                                  # 四类计数 consistent=true

# 3.1) 受控重跑验证
curl -s -X POST localhost:8080/api/events/k2/rerun                # 202 ACCEPTED：重新入队，rerunCount=1
curl -s localhost:8080/api/events/k2
#   -> status=QUEUED/最终 TERMINATED，attempts 累计增长，roundAttempts 本轮重计，
#      lastTerminationReason=RETRY_EXHAUSTED 保留上一轮终止原因
curl -s -X POST localhost:8080/api/events/k1/rerun                # 409 NOT_TERMINATED（已成功，不允许重跑）
curl -s -X POST localhost:8080/api/events/no-such/rerun           # 404 NOT_FOUND
#   连续把 k2 重跑到第 3 轮（eventing.max-reruns=3）后再次发起：
curl -s -X POST localhost:8080/api/events/k2/rerun                # 409 RERUN_LIMIT_REACHED（轮数到顶）

# 3.2) 多轮重跑逐轮历史验证（k2 载荷为 RETRYABLE，每轮都会再次终止）
#   对 k2 依次执行：重跑 -> 等它再次终止 -> 再重跑，跑过两轮以上后查询：
curl -s -X POST localhost:8080/api/events/k2/rerun                # 第 1 轮重跑入队
sleep 3                                                           # 等后台 worker 把这一轮跑到终止
curl -s -X POST localhost:8080/api/events/k2/rerun                # 第 2 轮重跑入队
sleep 3
curl -s localhost:8080/api/events/k2
#   -> roundHistory 按轮次升序列出每一轮：round=0/1/2 各自的 terminationReason、
#      roundAttempts（该轮处理次数）、lastError（该轮失败信息）、terminatedAt；
#      attempts == 各轮 roundAttempts 之和 + 当前轮 roundAttempts，rerunCount 与历史条数对得上
# 4) 重启验证：Ctrl+C 停止后再次启动，观察日志“恢复事件 N 条”，
#    GET /api/events/k1 仍为 SUCCEEDED 且 result 凭证不变；对 k1 再投递仍为 DUPLICATE；
#    GET /api/events/k2 的 roundHistory 与重启前逐条一致；
#    若 k2 重跑入队后立刻重启，重启后它会继续被处理（不会停在 QUEUED）。
```

## 8. 测试如何打印“幂等键 / 重试次数 / 判定依据”

- 生产代码日志（`INFO`/`WARN`）在接入、每次尝试、重试排队、终止、重跑接受/拒绝、成功时都输出
  `幂等键=... 重跑轮数=R 本轮第N次尝试(累计第M次) 判定=...`（含退避时刻或终止/拒绝原因）。
- 测试日志以 `[测试] 幂等键=... 判定依据=...` 形式显式记录每个断言的依据。
- 查看某次测试输出：`mvn test -Dtest=ControlledRerunTest`
  （或 `target/surefire-reports/...txt`，完整输出见控制台）。
- 多轮重跑历史专项：`mvn test -Dtest=RerunHistoryTest`，覆盖
  “三轮重跑后每轮终止原因/处理次数/失败信息各自独立”“重启后逐轮历史完整且事件继续处理”
  “并发重跑+重复接入+后台重试下历史不丢、四状态计数恒平”三类场景。

## 9. 关键设计取舍

- **单进程串行状态迁移**：所有状态判断与迁移在 `EventStore` 监视器内完成，
  后台重试使用单线程调度器，从结构上消除同键并发处理，统计天然一致。
- **文件原子持久化**：写 `*.tmp` 后原子 `move` 覆盖；以简单可靠换取非高吞吐场景的可解释性。
- **作用域说明**：本方案面向单实例本地可靠处理；多实例水平扩展需要共享存储/分布式锁，超出本样例范围。
