# Asana 连接器子任务采集 实施计划

## Repository Research

### 现状
- 连接器代码：[connector.py](file:///Users/zhangxinyu/SWE/tasks/SWE-0082-onyx/backend/onyx/connectors/asana/connector.py)、[asana_api.py](file:///Users/zhangxinyu/SWE/tasks/SWE-0082-onyx/backend/onyx/connectors/asana/asana_api.py)、[config.py](file:///Users/zhangxinyu/SWE/tasks/SWE-0082-onyx/backend/onyx/connectors/asana/config.py)。
- `AsanaConnector.poll_source(start, end)` 把 `start` 转成 ISO 字符串后调用 `AsanaAPI.get_tasks(project_gids, start_date)`；`load_from_state` 以 `start=0` 复用同一路径。
- `get_tasks` 先按 workspace/team/项目筛选枚举项目，再对每个项目调用 `tasks_api.get_tasks_for_project(gid, {"modified_since": ...})`，只用 `seen_task_gids` 对跨项目重复任务去重；每个任务构建 `AsanaTask`（标题、notes、创建/截止/完成信息、评论 stories、modified_at、permalink、项目 gid/名称），单任务失败时 `logger.error(exc_info=True)` + `api_error_count += 1` 后继续。
- 项目筛选：归档项目一律跳过；私有项目仅在未显式指定 `asana_project_ids` 且不属于配置 team 时跳过；显式 allowlist 优先级最高。
- Asana 无 EE 权限同步（`backend/ee` 无 ASANA 引用），访问边界完全由 PAT 凭据的 API 权限 + 文档 metadata（`group=project_gid`、`project=project_name`）决定。
- 配置经 [factory.py](file:///Users/zhangxinyu/SWE/tasks/SWE-0082-onyx/backend/onyx/connectors/factory.py#L169-L206) 中 `config.model_dump(exclude_unset=True)` 直接作为构造器 kwargs，因此 config 字段名必须与 `AsanaConnector.__init__` 参数名一致。
- 字段策略已有 `ScopeToggle(widens_when=True)` 先例（Box `include_web_links`、Confluence `index_recursively` 等）：False→True 判定为 WIDEN，触发正确的重索引规划。
- 前端表单为静态定义：[connectors.tsx](file:///Users/zhangxinyu/SWE/tasks/SWE-0082-onyx/web/src/lib/connectors/connectors.tsx#L1995-L2021) 的 `asana` 块；bool 选项使用 `type: "checkbox"`、`default: false`。i18n 要求 en.json 加键后，其余 8 个语言文件必须同步加翻译（`types:check` 与 `i18n-copy-changes` pre-commit 强制）。
- SDK：`asana==5.0.8`。数组类端点（`get_tasks_for_project`、`get_stories_for_task`）返回自动翻页的 generator（`PageIterator.items`）；`tasks_api.get_subtasks_for_task(task_gid, opts)` 对应 `GET /tasks/{gid}/subtasks`，同样自动翻页，支持 `opt_fields`（含嵌套字段 `parent.name`、`parent.permalink_url`），但不支持 `modified_since`；`tasks_api.get_task(gid, opts)` 可取全量字段。
- 已有单测：[test_asana_connector.py](file:///Users/zhangxinyu/SWE/tasks/SWE-0082-onyx/backend/tests/unit/onyx/connectors/asana/test_asana_connector.py)，全部 mock SDK，构造 helper `_build_api_with_mocks`。

### 需求要点与设计结论
1. **默认关闭**：新配置 `index_subtasks: bool = False`；关闭时 `get_tasks_for_project` 仍带 `modified_since`，采集行为与现状逐字节一致。
2. **各级子任务、含未入项目者**：开启后对范围内每个项目任务，用显式栈迭代调用 `get_subtasks_for_task` 向下遍历全部层级（不使用递归，规避深层嵌套的递归上限）；子任务是否属于项目不影响采集。
3. **父任务未变也要发现子任务**：开启后项目任务列表查询**不带** `modified_since`（每轮全量枚举，这是遍历整棵子树的必要成本）；项目任务自身是否产出文档改为客户端按 `modified_at >= start` 秒级时间戳过滤；子任务枚举时 compact 对象带 `modified_at`，仅对窗口期内变更者才调 `get_task` 全量获取（含评论），未变者不产出、不重索引，但仍继续向下遍历其子代。`start=0` 时全部通过。
4. **每子任务一篇独立文档，内容齐全**：全量 `get_task` 复用现有字段集并增加 `parent.name,parent.permalink_url`；文档文本中新增 “Parent task / Parent task link” 区块（直接父任务标题+链接），其余构成（标题、notes、评论、permalink、时间）与任务文档一致；子任务 metadata 的 `group/project` 取其被发现时的根项目，沿用现有访问限制语义。
5. **全局唯一**：复用并扩展 `seen_task_gids`：项目列表与子树遍历共用一个集合，任何 gid 只处理一次；项目任务本身也可能带 parent（手工加入项目的子任务），此时同样附加父任务区块。
6. **失败隔离**：枚举某父任务的子任务失败、翻页迭代中途失败、单个子任务 `get_task`/构建失败，均记录 gid 与异常原因（`str(e)` + `exc_info`）、累加 `api_error_count`，跳过该项并继续；不产出文档即不会覆盖/删除该任务已有索引（与现有任务级失败语义一致）。
7. **分页完整**：始终迭代 SDK generator 到耗尽（与现有任务/评论端点同一机制），不自行设置截断数量。

## Files and Modules
- `backend/onyx/connectors/asana/config.py`：新增 `index_subtasks` 字段（SCOPE + `ScopeToggle(widens_when=True)`，默认 False）。
- `backend/onyx/connectors/asana/connector.py`：构造器新增 `index_subtasks` 参数并保存；`poll_source` 向 API 传入 `start` 秒值与开关。
- `backend/onyx/connectors/asana/asana_api.py`：
  - `get_tasks` / `_get_tasks_for_project` 增加 `start_seconds`、`include_subtasks` 参数；
  - 抽取 `_build_task(data, project_gid, project_name) -> AsanaTask | None`（封装现有 try/except、评论、文本构建）；
  - 新增 `_iter_subtasks(root_gids, ...)`（显式栈、客户端增量过滤、失败隔离）；
  - `_construct_task_text` 增加父任务标题/链接区块；任务 opt_fields 增加 `parent.name,parent.permalink_url`；
  - `AsanaTask` 增加 `parent_name`、`parent_link` 可选字段（仅用于对象完整性，文档文本已含）。
- `web/src/lib/connectors/connectors.tsx`：asana 表单增加 checkbox `index_subtasks`（`default: false`、`optional: true`）。
- `web/src/i18n/messages/{en,zh,de,es,fr,ja,ko,pt,ar}.json`：新增 `connectors.asana.fields.index_subtasks.label/description`（9 个文件）。
- `backend/tests/unit/onyx/connectors/asana/test_asana_connector.py`：扩展 mock helper 并新增子任务测试用例。

## Implementation Steps
1. `config.py`：import `ScopeToggle`，新增 `index_subtasks` 字段。
2. `asana_api.py`：
   - 定义模块级常量 `_TASK_OPTIMIZATION_FIELDS`（现有字段串 + `parent.name,parent.permalink_url`）与 `_SUBTASK_ENUMERATION_FIELDS = "modified_at"`；
   - 改造 `get_tasks` / `_get_tasks_for_project` 签名与枚举逻辑（开关、客户端过滤、收集 subtree roots）；
   - 实现 `_build_task`、`_iter_subtasks`、`_modified_since`；`_construct_task_text` 加父任务区块；`AsanaTask` 加两个可选字段。
3. `connector.py`：构造器与 `poll_source` 接线；`__main__` 调试块无需改动（新参数有默认值）。
4. 前端：connectors.tsx 加 checkbox；9 个语言文件加文案。
5. 单元测试：
   - 开关关闭：`get_subtasks_for_task` 零调用，现有断言不变；
   - 开关开启：两级子任务（含未入项目者）全部产出，文档文本含父任务标题与链接，metadata 为根项目；
   - 去重：同时出现在项目列表和父任务下的 gid 只产出一篇、评论只抓一次；
   - 增量：父任务 modified_at 早于窗口不产出，新子任务晚于窗口仍产出；
   - 失败隔离：一个子任务 `get_task` 抛错，兄弟/其他分支子任务仍产出，`api_error_count` 增加；
   - 大数量（如 150 条 compact）枚举全部消费，固定无隐式截断；
   - connector 层：`AsanaConnector(index_subtasks=True)` 调用 API 时正确传参；config 默认 False。
6. 不新增迁移、不动权限代码、不动其他连接器。

## Dependencies and Considerations
- 开启后的同步成本：每轮全量枚举项目任务 + 每个任务一次 subtasks 列举；仅窗口期变更的子任务产生全量详情/评论请求。可接受，且是“父任务未变也能发现子任务”的唯一可靠方式（subtasks 端点不支持 `modified_since`）。
- 时间比较：Asana 的 `modified_at` 为带时区 ISO，用 `datetime.fromisoformat(...).timestamp()` 与 `start` 秒值比较，避免 naive/aware 比较错误。
- 同一 gid 跨项目/跨父子路径的 metadata 以首次发现的根项目为准（与现有“跨项目首次命中”行为一致）。
- ScopeToggle 使开关变更加规划系统自动识别（WIDEN/NARROW），无需改 planning rule。
- i18n 9 个文件必须同批改，否则前端校验失败。

## Validation
- `uv run pytest -xv backend/tests/unit/onyx/connectors/asana backend/tests/unit/onyx/connectors/test_planning_rules.py`
- `pre-commit run --files` 针对所有改动文件（ruff/mypy/格式化/i18n 钩子）。
- 9 个 JSON 语法与 key 形状校验（catalog 测试或 python json 解析）。
- 人工检查 diff：开关关闭路径的 opts 与产出与现状一致。

## Risks
- **深层嵌套导致遍历中断**：用显式栈而非递归规避。
- **subtasks 分页中途失败**：按父节点粒度 catch 迭代器异常，记录原因后继续其他父节点分支。
- **compact 缺少 modified_at**：防御性逻辑——缺失时全量获取后再按全量 modified_at 判定，不漏采。
- **历史已索引的“项目内子任务”文档此前无父任务区块**：开启后首次同步会因内容变化更新文档，属预期行为。
