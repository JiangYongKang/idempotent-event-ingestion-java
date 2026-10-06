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

- **历史不丢（逐轮归档，多轮不被压成一条）**：每一轮（首轮或某轮重跑）一旦跑到结局
  （成功或终止），系统就把这一轮**不可变地归档为一条独立历史**，之后任何重跑都只**追加**
  新条目，绝不修改、覆盖或删除旧条目。单条事件视图里的 `roundHistory` 数组按轮次升序，
  每条都能单独看清：
  - `round`：轮次序号（0 = 首轮，1 = 第 1 轮重跑……），历史条目轮次严格升序、互不串轮；
  - `outcome`：本轮结局（`SUCCEEDED` / `TERMINATED`）；
  - `terminationReason`：**这一轮**为什么终止（`RETRY_EXHAUSTED` / `NON_RETRYABLE`），
    成功轮为 `null`；前几轮因何终止不会被后面的轮次冲掉；
  - `attempts`：**这一轮**处理了几次（不含其他轮次）；
  - `lastError` / `lastErrorRetryable`：**这一轮**最后一次失败的错误信息及是否可重试，
    成功轮若曾失败后恢复（如 `FLAKY`）也保留本轮失败信息；
  - `startedAt` / `finishedAt`：本轮开始（接入或重跑入队）与到达结局的时刻。
- **累计口径始终对得上**：
  - `rerunCount`：当前是第几轮重跑（0 = 首轮）；
  - `attempts`：累计总处理次数，跨轮次只增不减，
    **恒等于 `roundHistory` 中各轮 `attempts` 之和 + 在途当前轮的 `roundAttempts`**；
  - `roundAttempts`：**当前轮**已处理次数（每轮重新计 0，退避与上限判定都按本轮计）；
  - `lastTerminationReason` / `lastTerminatedAt`：最近一次终止的原因与时刻
    （即最后一条终止类历史的摘要，方便不展开数组时快速查看；完整逐轮信息以 `roundHistory` 为准）。
- **重跑不串轮**：发起重跑时只重置本轮计数（`roundAttempts=0`）、刷新 `roundStartedAt`、
  清空记录级“最近失败信息”（上轮错误文本已归档进 `roundHistory`，不会丢失），
  **不清空、不改写任何历史条目**；新一轮的失败只写入新一轮条目。
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
  日志与记录中 `重跑轮数 / 本轮次数 / 累计次数` 三者并列；跑完的每一轮在 `roundHistory`
  中各占一条独立条目，不会与之前轮次混成一笔。多轮反复重跑后，仍能逐轮回答
  “第几轮为什么终止、那轮处理了几次、报了什么错”。
- **与原语义的兼容**：逐轮历史只是**在原有状态迁移上追加归档**，不改变幂等去重、
  退避重试、终止判定、容量与轮数上限、四态统计中的任何一条规则；旧版本数据文件
  （没有 `roundHistory` 字段）可直接加载，升级后新跑完的轮次照常追加历史。

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
- 成功/终止态及其结果、错误原因在重启后完整保留；**逐轮历史 `roundHistory` 随数据文件
  一并持久化**，多轮重跑后重启，每一轮的终止原因、本轮处理次数、失败信息与时刻逐条一致、
  不丢不串；在某一轮重试排队中途重启时，已结束轮次的历史完整保留、在途轮次不产生半截条目，
  恢复后该轮继续被处理（不会停在 `QUEUED`）。
  重跑入队（`QUEUED`）的事件重启后同样继续被处理，`rerunCount`、`lastTerminationReason`
  等重跑信息一并恢复；重启后重复投递仍为 `DUPLICATE`。

## 5. HTTP 接口

| 方法/路径 | 说明 | 状态码 |
| --- | --- | --- |
| `POST /api/events` | 请求体 `{"idempotencyKey":"...","payload":"..."}` | 202 首次接受；200 重复；400 非法；503 限流 |
| `POST /api/events/{key}/rerun` | 对终止事件发起受控重跑 | 202 重新入队；404 键不存在；409 状态不允许（`NOT_TERMINATED`）或轮数到顶（`RERUN_LIMIT_REACHED`）；503 容量不足（`QUEUE_FULL`） |
| `GET /api/events/{key}` | 查询单个事件的完整状态 | 200 / 404 |
| `GET /api/stats` | 四类状态计数与一致性标记 | 200 |

单条事件视图包含：`status / attempts（累计）/ roundAttempts（当前轮）/ rerunCount /
roundStartedAt / lastError / lastErrorRetryable / terminationReason /
lastTerminationReason / lastTerminatedAt / roundHistory（逐轮历史数组，每条含
round / outcome / terminationReason / attempts / lastError / lastErrorRetryable /
startedAt / finishedAt）/ result / lastAttemptAt / nextAttemptAt` 等可解释字段。

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
# 1) 运行全部测试（并发重复投递 / 失败重试直至终止 / 受控重跑 / 多轮重跑逐轮历史 /
#    重启恢复 / HTTP 语义）
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

# 3.2) 多轮重跑逐轮历史验证（一轮一条，互不覆盖）
curl -s -X POST localhost:8080/api/events -H 'Content-Type: application/json' \
  -d '{"idempotencyKey":"k3","payload":"FATAL:脏数据"}'           # 首轮 NON_RETRYABLE 终止
curl -s -X POST localhost:8080/api/events/k3/rerun                # 202 第 1 轮重跑（仍终止）
sleep 1                                                           # 等 worker 把这轮跑完
curl -s -X POST localhost:8080/api/events/k3/rerun                # 202 第 2 轮重跑（仍终止）
sleep 1
curl -s localhost:8080/api/events/k3 | python3 -m json.tool
#   预期：
#   rerunCount=2, attempts=3（首轮1次+两轮各1次）, roundAttempts=1,
#   roundHistory 共 3 条，round 分别为 0/1/2，互不覆盖：
#     [0] outcome=TERMINATED terminationReason=NON_RETRYABLE attempts=1 lastError="不可重试失败: 脏数据"
#     [1] outcome=TERMINATED terminationReason=NON_RETRYABLE attempts=1 lastError="不可重试失败: 脏数据"
#     [2] outcome=TERMINATED terminationReason=NON_RETRYABLE attempts=1 lastError="不可重试失败: 脏数据"
#   （想逐轮看到“不同终止原因/不同次数”：首轮用 RETRYABLE 载荷、重跑轮用 FATAL 语义无法在线切换载荷，
#    因此“不同轮不同原因”的严格验证见下面的自动化测试 MultiRerunHistoryTest。）
curl -s localhost:8080/api/stats                                  # consistent=true，重跑不改统计口径

# 3.3) 只跑多轮历史相关测试（日志含幂等键、每轮处理/重试次数与判定依据）
mvn test -Dtest=MultiRerunHistoryTest
#   覆盖：4 轮(首轮+3轮重跑)逐轮原因/次数/失败信息独立可查、累计=各轮之和、
#         终止后重启历史一致、某轮重试中途重启后续跑、失败后恢复成功的成功轮归档、
#         12 线程×3 波并发重跑只入队一次且统计恒平。

# 4) 重启验证：Ctrl+C 停止后再次启动，观察日志“恢复事件 N 条”，
#    GET /api/events/k1 仍为 SUCCEEDED 且 result 凭证不变；对 k1 再投递仍为 DUPLICATE；
#    GET /api/events/k3 的 roundHistory 与重启前逐条一致；
#    若某事件重跑入队（或重试退避排队中）立刻重启，重启后它会继续被处理（不会停在 QUEUED）。
```

## 8. 测试如何打印“幂等键 / 重试次数 / 判定依据”

- 生产代码日志（`INFO`/`WARN`）在接入、每次尝试、重试排队、终止、重跑接受/拒绝、成功时都输出
  `幂等键=... 重跑轮数=R 本轮第N次尝试(累计第M次) 判定=...`（含退避时刻或终止/拒绝原因）。
- 测试日志以 `[测试] 幂等键=... 判定依据=...` 形式显式记录每个断言的依据。
- 查看某次测试输出：`mvn test -Dtest=ControlledRerunTest` 或
  `mvn test -Dtest=MultiRerunHistoryTest`（多轮重跑逐轮历史专项）
  （完整输出见控制台与 `target/surefire-reports/TEST-*.xml`）。
- `MultiRerunHistoryTest` 结尾会打印一段 `[测试覆盖汇总]`，逐条列出所覆盖的多轮历史业务场景。

## 9. 关键设计取舍

- **单进程串行状态迁移**：所有状态判断与迁移在 `EventStore` 监视器内完成，
  后台重试使用单线程调度器，从结构上消除同键并发处理，统计天然一致。
- **文件原子持久化**：写 `*.tmp` 后原子 `move` 覆盖；以简单可靠换取非高吞吐场景的可解释性。
- **作用域说明**：本方案面向单实例本地可靠处理；多实例水平扩展需要共享存储/分布式锁，超出本样例范围。
