# Craft 定时任务运行取消 - 实施计划

## Task 1: 运行状态机扩展（枚举、迁移、DB 条件更新助手）
- **Status**: `pending`
- **Priority**: high
- **Depends On**: None
- **Description**:
  - 在 `backend/onyx/db/enums.py` 的 `ScheduledTaskRunStatus` 增加 `CANCEL_REQUESTED = "CANCEL_REQUESTED"`（非终态）与 `CANCELLED = "CANCELLED"`（终态）；`is_terminal()` 纳入 CANCELLED。
  - 新增 Alembic 迁移：若 `scheduled_task_run.status` 上存在 CHECK 约束则以包含全部 8 个状态值的新约束替换（不存在则为空操作）；不回填数据。遵循确定性迁移规则（固定字面量，无 `uuid4()/now()`），多 schema 可克隆。
  - 在 `backend/onyx/db/scheduled_task.py` 增加仅做条件更新（CAS）的 DB 操作：`get_owned_run`（join 任务按 user_id 鉴权，软删除任务仍可取回在途运行）、`claim_queued_run`（QUEUED→RUNNING，返回是否成功）、`link_run_session`（仅写 session_id，不改状态）、`cancel_queued_run`（QUEUED→CANCELLED+finished_at）、`request_run_cancellation`（RUNNING→CANCEL_REQUESTED）、`revert_cancel_request`（CANCEL_REQUESTED→RUNNING）、条件化的终态写入助手（执行器：SUCCEEDED/FAILED/CANCELLED 各自允许的来源状态；清扫器复用），并让 `has_in_flight_run_for_task` 与 `find_stuck_runs` 包含 CANCEL_REQUESTED。所有查询仍只在 `onyx/db/` 下。
- **Acceptance Criteria Addressed**: AC-2, AC-7, AC-8, NFR-1, NFR-2
- **Test Requirements**:
  - `rule` TR-1.1: 迁移 upgrade/downgrade 在本地 Postgres 成功执行；约束存在时包含 CANCELLED/CANCEL_REQUESTED，不存在时不报错（`uv run alembic upgrade head` 输出作为证据）。
  - `rule` TR-1.2: 单元/外部依赖测试证明各 CAS 在来源状态不匹配时影响行数为 0 且不写 finished_at、不改其他字段。
- **Notes**: 枚举列为 `native_enum=False`（VARCHAR）；参考迁移 `28429dd43807_scheduled_tasks.py` 注释（声称 varchar+CHECK，需以实际库约束为准做防御式 DDL）。

## Task 2: 执行器取消感知（claim 守卫、prompt 前复核、终态裁决）
- **Status**: `pending`
- **Priority**: high
- **Depends On**: Task 1
- **Description**:
  - `executor.py: run_scheduled_task_logic`：唤醒沙箱后的 QUEUED→RUNNING 改用 `claim_queued_run` CAS；认领失败直接返回（不建会话、不发 prompt、不通知）。沙箱唤醒失败路径的 FAILED 写入也条件化为仅当仍为 QUEUED。
  - `_drive_agent`：关联 session_id 改用 `link_run_session`（不覆盖状态）；进入 prompt_slot/发送 prompt 前 fresh-read 运行状态，CANCEL_REQUESTED 时直接条件终结为 CANCELLED（保留用户提示消息），清理 deadline，不通知。
  - 事件循环结束后 fresh-read 状态做终态裁决：当前为 CANCEL_REQUESTED 时——正常 end_turn（非 cancelled、无 terminal error）落 SUCCEEDED；cancelled/terminal error/权限请求/无 PromptResponse/slot 丢失均落 CANCELLED（可保留已生成摘要），均不调用 `_notify`。当前为 RUNNING 时保持现状（FAILED+通知 / SUCCEEDED）。当前已是终态则不覆盖。
  - 外层 catch-all 的兜底 FAILED 同样条件化：CANCEL_REQUESTED→CANCELLED 不通知；终态不覆盖。
- **Acceptance Criteria Addressed**: AC-2, AC-3, AC-7, FR-6
- **Test Requirements**:
  - `rule` TR-2.1: 外部依赖测试——QUEUED 先被取消再执行：`send_message_count==0`、无新 BuildSession、状态 CANCELLED、通知计数 0。
  - `rule` TR-2.2: 外部依赖测试——RUNNING + CANCEL_REQUESTED + `PromptResponse(cancelled)`：CANCELLED、session_id/消息保留、无通知；自发性 cancelled（状态仍 RUNNING）仍为 FAILED+agent_exception（回归现状）。
  - `rule` TR-2.3: 外部依赖测试——CANCEL_REQUESTED 下分别 end_turn→SUCCEEDED、terminal Error→CANCELLED、RequestPermissionRequest→CANCELLED，且三种均无通知。
- **Notes**: 复用现有 stub 事件装配方式（`tests/common/craft/stubs.py` + monkeypatch skills payload）；fresh-read 用标量 status 查询或 `populate_existing()`，避免 identity map 陈旧值。

## Task 3: 卡住清扫器对 CANCEL_REQUESTED 的回收
- **Status**: `pending`
- **Priority**: medium
- **Depends On**: Task 1, Task 2
- **Description**:
  - `cleanup_stuck_scheduled_runs`（`background/celery/tasks/scheduled_tasks/tasks.py`）：find_stuck_runs 已含 CANCEL_REQUESTED（Task 1）；清扫时 QUEUED/RUNNING→FAILED(stuck)（现状、不通知），CANCEL_REQUESTED→CANCELLED（不通知）。detail 文案区分。
- **Acceptance Criteria Addressed**: AC-7, FR-11
- **Test Requirements**:
  - `rule` TR-3.1: 外部依赖测试——超时 CANCEL_REQUESTED 运行被清扫为 CANCELLED（非 stuck 失败），且不产生通知。

## Task 4: 严格 abort 传输能力（失败可感知、目标精确）
- **Status**: `pending`
- **Priority**: high
- **Depends On**: None
- **Description**:
  - `OpencodeServeClient` 增加 `abort_checked()`：POST `/session/{id}/abort`，网络错误/非 2xx 以异常抛出（区别于吞异常的 `abort()`），保留 2s 超时。
  - `_ServeMixin`（`sandbox/serve_transport.py`）增加 `abort_opencode_session_strict(sandbox_id, session_id, opencode_session_id)`：构建 `with_event_bus=False` 客户端并调用 `abort_checked`，异常向上传播；不改动现有 `prepare_opencode_abort`/`abort_opencode_session` 的吞异常语义（交互式中断依赖它）。
  - 在 `StubSandboxManager` 增加 `abort_opencode_session_strict` 覆写与故障注入钩子（如 `abort_strict_raises`），记录到既有 `abort_calls`。
- **Acceptance Criteria Addressed**: AC-3, AC-4, NFR-3
- **Test Requirements**:
  - `rule` TR-4.1: 单元测试（或随取消服务测试）证明 strict abort 精确接收 (sandbox_id, run.session_id, opencode_session_id) 三元组，注入异常时异常原样抛出，且不影响旧 `abort()` 行为与交互式中断测试。

## Task 5: 取消服务与 HTTP 接口
- **Status**: `pending`
- **Priority**: high
- **Depends On**: Task 1, Task 2, Task 4
- **Description**:
  - 新增 `backend/onyx/server/features/build/scheduled_tasks/cancel.py`：`cancel_scheduled_run(*, db_session, task_id, run_id, user_id) -> ScheduledTaskRun`：
    1. `get_owned_run` 鉴权取运行（NOT_FOUND 对非所有者/不存在）。
    2. 已终态→`OnyxError(CONFLICT)`（不改数据）。
    3. QUEUED→`cancel_queued_run` CAS；CAS 失败则刷新按新状态重入分支。
    4. RUNNING→`request_run_cancellation` CAS 并 commit；CANCEL_REQUESTED（重试）直接继续。
    5. 解析运行的 BuildSession（按 run.session_id + 所有者）与用户沙箱：opencode_session_id 存在且沙箱 active 时调用 `abort_opencode_session_strict`；失败则 `revert_cancel_request` 条件回滚 RUNNING、commit，并抛 `OnyxError(BAD_GATEWAY, 原因)`。
    6. 会话/opencode id 尚未就绪（prompt 前窗口）时不发起 RPC，保留 CANCEL_REQUESTED（执行器 prompt 前复核兜底）。
    7. abort 成功后 refresh 行返回（通常为 CANCEL_REQUESTED，执行器更快时为 CANCELLED；若已被正常完成落为 SUCCEEDED 等终态则原样返回）。
  - `scheduled_tasks/api.py` 增加 `POST /scheduled-tasks/{task_id}/runs/{run_id}/cancel`，返回 `RunSummary` 风格负载（包含最新 status）；不使用 `response_model`；复用 `Permission.BASIC_ACCESS` 依赖。
- **Acceptance Criteria Addressed**: AC-1, AC-3, AC-4, AC-5, AC-6, FR-1, FR-7, FR-8, FR-9
- **Test Requirements**:
  - `rule` TR-5.1: 集成测试——QUEUED 取消返回 CANCELLED；重复取消与 SUCCEEDED/FAILED/SKIPPED/CANCELLED 取消返回 409 且行不变；任务 status/next_run_at 不变；MANUAL_RUN_NOW 与 SCHEDULED 均可取消；非所有者 404。
  - `rule` TR-5.2: 外部依赖测试——RUNNING 取消：stub strict abort 被调用且返回 CANCEL_REQUESTED；abort 注入异常时抛 OnyxError(BAD_GATEWAY) 且行恢复 RUNNING；CANCEL_REQUESTED 重试再次调用同一 abort 目标。
  - `rule` TR-5.3: 外部依赖测试——abort 只针对 run.session_id；同一沙箱另一 BuildSession 不出现在 abort_calls。

## Task 6: 活动门控兼容 CANCEL_REQUESTED
- **Status**: `pending`
- **Priority**: high
- **Depends On**: Task 1
- **Description**:
  - `server/features/build/session/api.py`：`_scheduled_run_is_running` 与 `/scheduled-run-events` 的状态判定纳入 CANCEL_REQUESTED（实时查看流在停止窗口内不中断）。
  - `db/action_policy.py: get_active_scheduled_task_context`：RUNNING 与 CANCEL_REQUESTED 均视为活动运行授权上下文；CANCELLED 与其他终态一致地拒绝。
  - 复查 `get_scheduled_run_context`/banner 负载无需改形状（状态枚举自动透传）。
- **Acceptance Criteria Addressed**: AC-8, FR-11
- **Test Requirements**:
  - `rule` TR-6.1: 单元测试（stub DB 或现有 `test_gate.py` 风格）证明 CANCEL_REQUESTED 被两处门控视为活动、CANCELLED 被拒绝。

## Task 7: 后端测试补齐与全量回归
- **Status**: `pending`
- **Priority**: high
- **Depends On**: Task 2, Task 3, Task 5, Task 6
- **Description**:
  - 扩充 `tests/external_dependency_unit/craft/test_scheduled_task_executor.py`（或新建同目录 `test_scheduled_task_run_cancellation.py`）与 `tests/integration/tests/craft/test_scheduled_tasks_api.py`，覆盖上述全部 TR；运行 scheduled tasks 相关 unit/ext-dep/integration 测试与 `tests/unit/sandbox_proxy/test_gate.py`。
- **Acceptance Criteria Addressed**: AC-1..AC-8（后端证据汇总）
- **Test Requirements**:
  - `rule` TR-7.1: 目标 pytest 选择器全部通过（命令与结果记录为 Completion Evidence）。
  - `rule` TR-7.2: 通知断言——所有取消路径（取消排队、取消执行中、清扫回收）新增通知数为 0；既有失败路径通知行为不回归。

## Task 8: 前端状态模型、API 客户端与取消控件
- **Status**: `pending`
- **Priority**: high
- **Depends On**: Task 5
- **Description**:
  - `web/src/app/craft/v1/tasks/types.ts`：运行状态联合类型增加 `"CANCEL_REQUESTED" | "CANCELLED"`。
  - `api.ts` 增加 `cancelScheduledRun(taskId, runId)`（POST，失败抛带 detail 的 Error）。
  - `utils.ts`：`getNonClickableReason` 允许 CANCELLED/CANCEL_REQUESTED 在有 session_id 时打开；`isScheduledRunInFlight` 纳入 CANCEL_REQUESTED。
  - `StatusBadge.tsx`：CANCEL_REQUESTED 显示"取消中"（spinner/info），CANCELLED 显示"已取消"（中性灰，合适的 @opal/icons 图标）；更新既有硬编码英文标签时保持 i18n 范围最小（徽章现状为硬编码，新增状态沿用同文件既有方式，不扩大重构）。
  - 新增组件 `components/CancelRunButton.tsx`：QUEUED/RUNNING 显示取消按钮，CANCEL_REQUESTED 显示取消中（可点击重试），终态不渲染；`stopPropagation`/`preventDefault` 阻止触发行导航；每行独立 pending；成功仅在响应为 CANCELLED 时给成功 toast，CANCEL_REQUESTED 给"正在停止…"提示，失败 toast 展示后端原因；不做乐观状态修改，成功后触发 runs SWR mutate。
  - `RunHistoryTable.tsx` 增加操作列承载该按钮。
  - `useBuildStreaming.ts`（1401 行附近）与 `useBuildSessionStore.ts`（877-880）的在途分类纳入 CANCEL_REQUESTED；CANCELLED 按正常完成结算（不计 scheduledFailure）。
  - `ScheduledRunBanner.tsx` 增加 CANCELLED 的状态文案分支（其余终态维持"started by"）。
  - i18n：`en.json` 新增 `craft.tasks.runHistory.cancel.*` 等键，并按规范补齐其余 8 个 locale（zh.json 给准确中文）；ICU 形状一致。
- **Acceptance Criteria Addressed**: AC-4, AC-8, AC-9, FR-5, FR-12
- **Test Requirements**:
  - `rule` TR-8.1: Jest——`scheduledTaskRuns.test.ts` 增加 CANCELLED（有/无 session_id）与 CANCEL_REQUESTED 的可打开性/在途判定。
  - `rule` TR-8.2: Jest——`CancelRunButton.test.tsx`：三态渲染、点击触发正确 API、失败显示原因且不显示成功、点击不导航、取消中重试、CANCELLED 后按刷新状态展示。
  - `rule` TR-8.3: `bun run typecheck`/`ods type-coverage` 与 i18n catalog 测试通过（无缺失键）。
  - `rubric` TR-8.4: 可用性维度；1-5；锚点同 spec AC-9；阈值 >= 4；证据为组件测试 + 手动浏览器走查（QUEUED 取消、RUNNING 取消取消中→已取消、失败原因 toast、打开已取消会话）。

## Task 9: 功能地图文档更新
- **Status**: `pending`
- **Priority**: medium
- **Depends On**: Task 5, Task 8
- **Description**:
  - 更新 `.agents/feature-map/components/craft-scheduled-tasks.md`：端点表增加 cancel 接口；§3 状态集合增加两状态；§4 执行器取消裁决与清扫行为；§5 不变量增加"用户取消不通知/终态不被覆盖/不影响任务与其他会话"；§8 测试与手动验证步骤。
- **Acceptance Criteria Addressed**: NFR-3（仓库规范）
- **Test Requirements**:
  - `rule` TR-9.1: `pre-commit run --files <改动文件>` 通过（含 feature-map stale-docs 钩子）。

## Task 10: 端到端手动验证与收尾
- **Status**: `pending`
- **Priority**: medium
- **Depends On**: Task 7, Task 8
- **Description**:
  - 通过 `http://localhost:3000`（经 BFF）手动验证：Run once 后立即取消 QUEUED；长 prompt 运行中取消（观察"取消中"→"已取消"）；打开已取消运行查看已保存会话；取消后任务仍 ACTIVE、next fire 不变；失败注入（如停沙箱）时显示原因并重试成功。重启 scheduled_tasks celery worker 后复测一次（后端改动需重启 worker）。
- **Acceptance Criteria Addressed**: AC-2, AC-3, AC-4, AC-6, AC-9
- **Test Requirements**:
  - `rule` TR-10.1: 手动清单各项有观察记录（日志/截图/接口响应），失败项回到对应 Task 修复。
