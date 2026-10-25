# Craft 定时任务运行取消（Cancel This Run）- 产品需求文档

## Overview

- **Summary**: 为 Craft 定时任务的单次运行增加"取消本次运行"能力。任务所有者可在运行历史中取消排队中或执行中的单次运行；排队中的运行不再启动智能体，执行中的运行停止其对应会话，确认停止后显示"已取消"。
- **Purpose**: 当前暂停任务只能阻止后续调度，无法中止一次已经排队或正在执行、但提示内容已发现有误的运行，错误的运行会继续消耗资源并可能执行外部动作。
- **Target Users**: Craft 定时任务的所有者（所有定时任务接口均为用户级作用域，无管理员视图）。

## Goals

- 任务所有者能从任务详情页的运行历史中取消**本次**运行。
- 同时支持定时触发（`SCHEDULED`）与手动触发（`MANUAL_RUN_NOW`）的运行。
- 排队中（QUEUED）取消：运行不再启动智能体，直接结束为 `CANCELLED`。
- 执行中（RUNNING）取消：仅中止该运行对应的 opencode 会话；执行器确认事件流停止后将运行结束为 `CANCELLED`。
- 取消后已保存的会话内容仍可从运行历史打开查看。
- 用户主动取消不发送失败通知。
- 停止失败时返回失败原因，运行保持可取消状态，允许重试；在确认停止前不展示取消成功。
- 取消只影响该次运行：不改变任务启停状态与 `next_run_at`，不停止其他会话，不影响其他运行。

## Non-Goals

- 不做批量取消、不做"取消所有未来运行"（暂停任务已覆盖该需求）。
- 不取消/删除已生成的 BuildSession 及其消息记录。
- 不为取消引入新的通知类型。
- 不改变定时任务的调度、跳过（SKIPPED）、卡住清扫（stuck sweeper）的既有策略，仅扩展其状态集合。
- 不提供管理员/非所有者取消他人运行的能力。
- 不做取消后的自动重跑。

## Background & Context

现状依据 `.agents/feature-map/components/craft-scheduled-tasks.md`（验证于 `268e4d5a3d`）：

- 运行行 `ScheduledTaskRun` 状态机（`backend/onyx/db/enums.py: ScheduledTaskRunStatus`）：`QUEUED / RUNNING / SUCCEEDED / FAILED / SKIPPED / AWAITING_APPROVAL`；终态判定 `is_terminal()` 仅含 `SUCCEEDED/FAILED/SKIPPED`。状态列为非原生枚举（VARCHAR，`native_enum=False`）。
- 派发：`dispatch_due_scheduled_tasks`（primary 队列，每 30 秒）以 `FOR UPDATE SKIP LOCKED` 认领到期任务，插入 QUEUED 运行并推进 `next_run_at`，然后向 `scheduled_tasks` 队列发送执行器任务（`expires=15min`）。`run-now` 走同一插入路径。
- 执行：`run_scheduled_task_logic`（`backend/onyx/server/features/build/scheduled_tasks/executor.py`）启动后先复核运行仍为 QUEUED，唤醒沙箱，再置 RUNNING；`_drive_agent` 创建 `BuildSession(origin=SCHEDULED)`、写入用户提示、关联 `run.session_id`，通过 `SessionManager.yield_sandbox_events` 驱动智能体。opencode 的中止（`MessageAbortedError`）当前被翻译为 `PromptResponse(stopReason="cancelled")`，执行器将其判为 FAILED（"Scheduled runs have no interrupt mechanism"）。
- 既有中止设施：`_ServeMixin.prepare_opencode_abort` / `OpencodeServeClient.abort` 向沙箱内 opencode `POST /session/{id}/abort`；交互式中断 `SessionManager.interrupt_message`（`POST /build/sessions/{id}/interrupt`）使用该设施，但其中 `abort()` 吞掉异常且面向"活动回合"缓存，不直接适用于计划任务（计划执行器不注册活动回合缓存）。
- 运行历史前端：`web/src/app/craft/v1/tasks/components/RunHistoryTable.tsx` 每 5 秒 SWR 刷新；`StatusBadge.tsx` 渲染状态；`utils.ts` 的 `getNonClickableReason` 决定可打开性（RUNNING/FAILED/SUCCEEDED/AWAITING_APPROVAL 且有 session_id 时可打开；QUEUED/SKIPPED 不可）。
- 失败不变量：执行器每个失败出口都调用 `_notify(SCHEDULED_TASK_FAILED)`；唯一例外是卡住清扫器（不通知）。
- 运行行 API：`GET /build/scheduled-tasks/{task_id}/runs`；所有接口经 BFF `/api/build/...` 访问，要求 `Permission.BASIC_ACCESS` 且按任务所有者鉴权。

关键设计决策：

1. **新增两个状态值**：终态 `CANCELLED` 与非终态 `CANCEL_REQUESTED`（"停止中"）。RUNNING 取消先做条件更新进入 CANCEL_REQUESTED，再向该运行会话发 abort；执行器观察到流结束且行为 CANCEL_REQUESTED 时终结为 CANCELLED。这样前端只有在服务端确认后才显示"已取消"，且执行器能区分"用户取消"与"自发性中止"。
2. **QUEUED 取消直接到 CANCELLED**：不存在会话/智能体，条件更新 `WHERE status='QUEUED'` 成功即确认；执行器启动时的认领（claim）采用同样的条件更新，认领失败即退出，绝不启动智能体。
3. **取消接口同步发起 abort，但不长时间轮询等待执行器终结**：abort 成功即返回 `CANCEL_REQUESTED`（"停止中"），前端依靠已有的 5 秒刷新看到 `CANCELLED`；abort 失败则回滚为 RUNNING 并返回原因，允许重试。不提前展示成功。
4. **终态写入全部条件化（CAS）**：执行器、清扫器、取消接口都以当前状态为条件写入，互不覆盖（取消与正常完成竞速时，真实结果优先）。
5. 取消是用户意图，所有 `CANCEL_REQUESTED → CANCELLED` 路径不发送失败通知。

## Functional Requirements

- **FR-1（入口与权限）**：在任务详情页运行历史中，为 QUEUED、RUNNING、CANCEL_REQUESTED 的运行提供"取消本次运行"操作；后端提供 `POST /build/scheduled-tasks/{task_id}/runs/{run_id}/cancel`。仅任务所有者可调用；非所有者（含其他用户）得到 NOT_FOUND（与现有任务接口一致，不泄露资源存在性）。SCHEDULED 与 MANUAL_RUN_NOW 两种触发来源均可取消。
- **FR-2（QUEUED 取消）**：QUEUED 运行被取消后立即为 CANCELLED 并写入 finished_at；已在队列中的 Celery 消息被执行器拾取时，因运行已非 QUEUED 而直接退出，不唤醒会话创建路径、不创建 BuildSession、不发送 prompt。
- **FR-3（RUNNING 取消-停止）**：RUNNING 运行被取消时，条件置为 CANCEL_REQUESTED，并仅对 `run.session_id` 对应的 opencode 会话调用 abort；不得影响同一沙箱内的其他 BuildSession/opencode 会话。
- **FR-4（RUNNING 取消-确认与展示）**：执行器在事件流因 abort 结束后，复核运行状态：CANCEL_REQUESTED 时完成既有落盘（消息/输出卡片）后条件置为 CANCELLED（写 finished_at，可保留摘要），前端随后显示"已取消"。CANCEL_REQUESTED 期间前端显示"取消中"。
- **FR-5（会话内容保留与可查看）**：取消不删除 BuildSession 与已持久化消息；CANCELLED 运行只要存在 session_id 即可从运行历史打开展开为普通会话视图（含定时运行横幅）。
- **FR-6（不通知）**：用户主动取消产生的 CANCELLED 不创建 `SCHEDULED_TASK_FAILED` 或其他通知；既有失败/权限通知路径在非取消情形下保持不变。
- **FR-7（失败可见与重试）**：当停止调用失败（沙箱不可达、abort HTTP 失败等）时，接口返回明确错误原因，运行恢复/保持 RUNNING（或仍为 CANCEL_REQUESTED 的可重试状态），前端展示错误并允许再次取消；在服务端确认前，任何界面不得显示"已取消"。重复点击取消（幂等重试）不产生额外副作用。
- **FR-8（终态与幂等）**：已经处于终态（SUCCEEDED/FAILED/SKIPPED/CANCELLED）的运行不能被改变结果：取消请求返回冲突错误（409）且不写任何状态。重复取消同一在途运行等价于重试，不产生第二个中止目标之外的效果。
- **FR-9（任务与其他会话不受影响）**：取消单次运行不修改 `ScheduledTask.status`、`next_run_at`、prompt/schedule；不终止同任务其他运行；不中止其他会话；不影响后续调度（因取消而空出的在途槽位允许后续正常派发）。
- **FR-10（执行器竞速安全）**：
  - 执行器 QUEUED→RUNNING 改为条件认领；唤醒沙箱期间被取消（QUEUED→CANCELLED）时，认领失败即退出。
  - 会话创建/prompt 发送前再次复核状态：CANCEL_REQUESTED 时直接终结为 CANCELLED（已写入的用户提示保留），不发送 prompt。
  - 取消与智能体正常完成同时发生：若智能体在 abort 生效前已正常结束（非 cancelled 终态），运行保持/落为 SUCCEEDED；接口随后按"运行已结束"处理，结果不被覆盖。
- **FR-11（在途语义与卡住回收）**：CANCEL_REQUESTED 仍视为"在途"：`has_in_flight_run_for_task` 包含它（派发仍按 SKIP_IF_RUNNING 跳过重叠触发）；运行实时查看 SSE 与外部应用授权上下文在 CANCEL_REQUESTED 期间继续按活动运行处理，CANCELLED 后与其他终态一致（不再授权、查看流结束）。卡住清扫器把超时的 CANCEL_REQUESTED 运行回收为 CANCELLED（不通知）；超时的 QUEUED/RUNNING 仍回收为 FAILED(stuck)。
- **FR-12（前端交互）**：运行历史新增操作列；取消中显示进行态并可重试；请求失败以 toast 展示后端原因；取消成功不做乐观更新，以服务端刷新后的状态为准；新增状态在徽章、可打开性、会话内定时运行横幅/流式等待逻辑中正确表现；所有新增文案走 i18n（en.json 为源，补齐全部 locale）。

## Non-Functional Requirements

- **NFR-1（一致性）**：所有运行状态变更必须是数据库条件更新（CAS），任何并发交错（取消接口/执行器/清扫器/重复点击）都不得出现终态被复活或被覆盖。
- **NFR-2（向后兼容）**：既有状态、API 响应形状（仅扩展枚举值）、通知行为保持兼容；VARCHAR 枚举扩值需要数据迁移来应对可能存在的 CHECK 约束（若约束不存在则为空操作的安全迁移），不得要求数据回填。
- **NFR-3（代码规范）**：DB 操作只在 `backend/onyx/db/`；FastAPI 不使用 `response_model`；错误一律使用 `OnyxError` + `OnyxErrorCode`；严格类型（Python 与 TypeScript）；前端遵循 Opal 组件与 i18n 规则。
- **NFR-4（可测试性）**：核心状态机以外部依赖单元测试（真 Postgres + StubSandboxManager）覆盖；HTTP 契约以集成测试覆盖；前端以 Jest 覆盖工具函数与取消控件。不新增需要真实长时运行智能体的脆弱 Playwright 用例，手动验证清单记录在案。

## Constraints

- **Technical**:
  - 后端：FastAPI + SQLAlchemy + Celery（线程池，无时间限制）；取消由 api-server 进程直接经 `SandboxManager` 访问沙箱 Pod（交互式中断已有先例）。
  - 执行器与 api-server 是不同进程，跨进程信号只能依赖：DB 状态 + opencode abort HTTP；不引入 Redis pub/sub。
  - 枚举存储为 VARCHAR（非原生枚举）；迁移需幂等且确定性（无随机值）。
  - 前端：Next.js + SWR（运行历史 5 秒轮询）+ Opal 设计系统 + next-intl。
- **Business**: CE 功能，不引入 EE 门控；不新增通知类型。
- **Dependencies**: 沙箱 opencode serve 的 `/session/{id}/abort` 能力；现有 `_ServeMixin` / `OpencodeServeClient`。

## Assumptions

- 一次计划运行只驱动一个 opencode 会话的一个回合（执行器现状），按 `run.session_id` 精确 abort 即可满足"不停止其他会话"。
- opencode 对已结束/不存在的回合重复 abort 是安全的（幂等），因此 CANCEL_REQUESTED 的重试可直接重发 abort。
- `AWAITING_APPROVAL` 当前无任何代码写入（执行器权限缺口落为 FAILED）；本次不将其纳入可取消集合，保持范围收敛。
- 任务软删除后在途运行仍可被取消（运行通过任务行取得所有者，不受 `deleted` 标志影响）。

## Acceptance Criteria

### AC-1: 取消接口的所有者鉴权与触发来源覆盖
- **Type**: `rule`
- **Given**: 一个任务所有者 A 拥有两次在途运行（一次 SCHEDULED、一次 MANUAL_RUN_NOW），另一用户 B
- **When**: A 分别对两次运行调用取消接口；B 对 A 的运行调用取消接口
- **Then**: A 的两次请求均被接受并进入对应取消流程；B 的请求返回 404 NOT_FOUND 且数据库无任何变化
- **Pass Condition**: 集成测试中两种 trigger_source 的取消均成功；非所有者请求 404 且运行状态字段不变
- **Evidence**: `backend/tests/integration/tests/craft/test_scheduled_tasks_api.py`

### AC-2: QUEUED 运行取消后智能体不启动
- **Type**: `rule`
- **Given**: 一条 QUEUED 运行（含已入队的执行器消息）
- **When**: 所有者取消该运行；随后执行器拾取该消息执行 `run_scheduled_task_logic`
- **Then**: 运行以 CANCELLED 终态返回（finished_at 已写），执行器不创建 BuildSession、不调用 send_message、不发通知；反向竞速（执行器已认领）也不会复活终态
- **Pass Condition**: 外部依赖单元测试断言：状态 CANCELLED、`send_message_count == 0`、无该运行的通知；取消在唤醒沙箱前后两种时序下均不启动智能体
- **Evidence**: `backend/tests/external_dependency_unit/craft/test_scheduled_task_executor.py`、`test_scheduled_task_run_cancellation.py`

### AC-3: RUNNING 运行取消只停止对应会话并终结为 CANCELLED
- **Type**: `rule`
- **Given**: 一条 RUNNING 运行，已有 BuildSession 与 opencode_session_id，stub 事件流在 abort 后产出 `PromptResponse(stopReason="cancelled")`
- **When**: 取消接口置 CANCEL_REQUESTED 并对该会话发 abort；执行器消费完终止事件
- **Then**: 仅记录针对该 (sandbox, run.session_id, opencode_session_id) 的一次 abort 调用；运行终结为 CANCELLED、finished_at 已写；已持久化消息保留且 run.session_id 不变；无通知
- **Pass Condition**: 外部依赖单元测试断言 abort 目标三元组精确匹配、最终状态 CANCELLED、会话消息仍可读取、通知表无新增；同沙箱另一会话不被 abort
- **Evidence**: `backend/tests/external_dependency_unit/craft/` 取消相关测试

### AC-4: 停止失败返回原因且不提前显示成功
- **Type**: `rule`
- **Given**: 一条 RUNNING 运行，沙箱 abort 调用被注入异常
- **When**: 所有者调用取消接口
- **Then**: 接口抛出/返回带原因的错误（BAD_GATEWAY 502 语义），运行恢复为 RUNNING；再次调用且 abort 成功时可正常进入 CANCEL_REQUESTED→CANCELLED；前端在收到 CANCELLED 服务端状态前不显示"已取消"
- **Pass Condition**: 服务测试断言失败时状态仍为 RUNNING（或可重试的 CANCEL_REQUESTED）且错误信息包含底层原因；前端测试断言失败 toast + 不出现"已取消"，成功路径仅在状态刷新为 CANCELLED 后展示
- **Evidence**: 取消服务外部依赖单元测试 + `CancelRunButton` Jest 测试

### AC-5: 终态运行不可改变、重复取消幂等
- **Type**: `rule`
- **Given**: SUCCEEDED/FAILED/SKIPPED/CANCELLED 运行各一，以及一条 CANCEL_REQUESTED 运行
- **When**: 对终态运行调用取消；对 CANCEL_REQUESTED 运行重复调用取消
- **Then**: 终态运行返回 409 CONFLICT 且状态/错误字段/finished_at 完全不变；重复取消只重新尝试同一 abort，不创建重复通知、不改变任务、不影响其他运行
- **Pass Condition**: 集成测试断言 409 与行数据不变；服务测试断言重复调用的 abort 目标一致且无额外副作用
- **Evidence**: 集成测试 + 外部依赖单元测试

### AC-6: 取消不影响任务调度与其他运行
- **Type**: `rule`
- **Given**: ACTIVE 任务（有 next_run_at）存在在途运行，另有同一沙箱的交互式会话与同任务的其他运行
- **When**: 取消该在途运行并等待其终结
- **Then**: 任务 status 仍为 ACTIVE、next_run_at 不变；后续到期派发正常进行；其他会话无 abort 调用；其他运行状态不变
- **Pass Condition**: 集成测试断言任务字段前后一致；服务测试断言 abort 调用集合仅含目标会话
- **Evidence**: 集成测试 + 外部依赖单元测试

### AC-7: 取消与完成/失败竞速时结果不被错误覆盖
- **Type**: `rule`
- **Given**: 处于 CANCEL_REQUESTED 的运行
- **When**: 事件流分别以 (a) 正常 end_turn、(b) terminal Error、(c) 权限请求、(d) 执行器进程死亡由清扫器超时回收 结束
- **Then**: (a) 落为 SUCCEEDED（真实完成优先，不通知）；(b)(c) 落为 CANCELLED（不通知）；(d) 清扫器将其置 CANCELLED（不通知）；任何路径都不把终态改写为 FAILED，也不把 FAILED 改写为 CANCELLED
- **Pass Condition**: 外部依赖单元测试覆盖四种时序的最终状态与通知计数（均为 0 条取消相关通知）
- **Evidence**: `test_scheduled_task_executor.py` / sweeper 测试

### AC-8: CANCEL_REQUESTED 的在途语义与活动门控
- **Type**: `rule`
- **Given**: CANCEL_REQUESTED 运行（有 session_id）
- **When**: 派发器评估同任务新到期触发；实时运行 SSE 接口被调用；外部应用代理解析活动任务上下文；前端判定运行是否仍在途/可否打开
- **Then**: 派发按"已有在途运行"跳过（SKIPPED 行）；SSE 与授权上下文仍视其为活动；前端视为在途（继续轮询/会话输入保持锁定）且有 session_id 时行可打开；终结为 CANCELLED 后上述门控全部按终态处理，CANCELLED 行仍可打开查看
- **Pass Condition**: 后端测试断言 in-flight 查询/门控函数对 CANCEL_REQUESTED 的判定；前端 Jest 断言 `getNonClickableReason`、`isScheduledRunInFlight` 与流式等待分类
- **Evidence**: db 辅助函数测试、`scheduledTaskRuns.test.ts`、`useBuildSessionStore`/`useBuildStreaming` 相关测试

### AC-9: 前端取消交互的可用性与一致性
- **Type**: `rubric`
- **Dimension**: 取消控件在运行历史中的可发现性、状态反馈准确性（取消中/已取消/失败重试）、与行点击导航的事件隔离、样式与 Opal 规范及深色模式一致性
- **Scale**: 1-5
- **Anchors**: 1 = 找不到入口或状态错乱（提前显示成功、误导航、样式违规）；3 = 功能可用但反馈粗糙或缺少取消中/重试态；5 = 入口明确、三态反馈准确、失败原因可见可重试、无乐观假成功、完全符合 Opal/i18n 规范
- **Pass Threshold**: >= 4
- **Evidence**: `CancelRunButton`/`RunHistoryTable` Jest 测试与手动浏览器验证；`en.json` 及全部 locale 补齐

## Open Questions

- 无阻塞性开放问题。以下为已在方案中确定的取舍，评审时可否决：
  - [x] 已终态运行的取消请求返回 409（而非静默成功），以便前端明确提示"运行已结束"。
  - [x] 取消接口对 RUNNING 运行同步发起 abort 后即返回 CANCEL_REQUESTED，由既有 5 秒轮询确认 CANCELLED，不做长轮询阻塞。
