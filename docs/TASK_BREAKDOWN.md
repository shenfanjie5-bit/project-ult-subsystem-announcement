# 项目任务拆解

## 阶段 0：参考子系统骨架

**目标**：打通 `subsystem-sdk` 与公告子系统的最小闭环，使其能作为参考子系统注册、心跳、发送 Ex-0。
**前置依赖**：无

### ISSUE-001: 初始化公告子系统 Python 项目骨架与包结构
**labels**: P0, infrastructure, milestone-0, ready

#### 背景与目标
当前仓库只有 `pyproject.toml`、`README.md`、`CLAUDE.md` 等空壳文件，尚无任何 Python 源码与 package 布局。根据 §14 系统模块拆分与 §25 自动化开发对接的要求，本子系统必须按 `discovery / parse / extract / signals / graph / index / runtime` 分 package 组织；在进入任何业务实现之前需要先把 Python 项目骨架、依赖、测试目录、日志配置和最小化的 CLI 入口落地。本 issue 是整个 `subsystem-announcement` 的 P0 基础，后续所有 issue 都以该骨架为写入根。它不引入任何业务逻辑，但要保证 `pytest`、`python -m subsystem_announcement` 等命令可跑通空流程。

#### 所属模块
- primary writable paths（允许创建/修改）：
  - `pyproject.toml`
  - `src/subsystem_announcement/__init__.py`
  - `src/subsystem_announcement/__main__.py`
  - `src/subsystem_announcement/discovery/__init__.py`
  - `src/subsystem_announcement/parse/__init__.py`
  - `src/subsystem_announcement/extract/__init__.py`
  - `src/subsystem_announcement/signals/__init__.py`
  - `src/subsystem_announcement/graph/__init__.py`
  - `src/subsystem_announcement/index/__init__.py`
  - `src/subsystem_announcement/runtime/__init__.py`
  - `src/subsystem_announcement/config.py`
  - `src/subsystem_announcement/logging_setup.py`
  - `tests/__init__.py`
  - `tests/test_package_layout.py`
  - `docs/PROGRESS.md`（新增内容由 PM 维护，本 issue 只需不破坏）
- adjacent read-only 路径：`CLAUDE.md`、`AGENTS.md`、`docs/subsystem-announcement.project-doc.md`
- off-limits：任何与 `data-platform` / `entity-registry` / `reasoner-runtime` 相关的真实实现；本 issue 一律不允许创建 HTTP 客户端、Docling 调用、LLM 调用等实际业务代码

#### 实现范围
- 骨架 / 打包层：
  - `pyproject.toml`：改为 `src-layout`，声明 `packages = ["subsystem_announcement", "subsystem_announcement.*"]`（通过 `setuptools.find` 发现）、补充基础依赖（`pydantic>=2.6`、`typer>=0.12`、`structlog>=24.1`、`httpx>=0.27`、`pytest>=8.0`、`pytest-asyncio>=0.23`），保留 `requires-python = ">=3.11"`
  - `src/subsystem_announcement/__init__.py`：暴露 `__version__: str = "0.1.0"` 与 `PACKAGE_NAME: str = "subsystem-announcement"`
  - 7 个子 package（discovery / parse / extract / signals / graph / index / runtime）各放一个 `__init__.py`，只暴露 `__all__: list[str] = []`，为后续 issue 预留
- 配置层：
  - `src/subsystem_announcement/config.py`：定义 `class AnnouncementConfig(BaseModel)`，字段 `artifact_root: Path`、`docling_version: str`、`llama_index_version: str`、`reasoner_endpoint: str | None`、`entity_registry_endpoint: str | None`、`heartbeat_interval_seconds: int = 60`；提供 `load_config(path: Path | None = None) -> AnnouncementConfig`，从 `ANNOUNCEMENT_CONFIG` 环境变量或默认路径 `config/announcement.toml` 读取
  - `src/subsystem_announcement/logging_setup.py`：`configure_logging(level: str = "INFO") -> None`，基于 `structlog` 输出 JSON 行
- 入口层：
  - `src/subsystem_announcement/__main__.py`：`def main() -> int`，使用 `typer` 暴露 `version`、`doctor` 两个子命令；`version` 打印 `__version__`，`doctor` 校验 config 可加载
- 测试层：
  - `tests/test_package_layout.py`：验证 7 个子 package 可被 import、`AnnouncementConfig` 可实例化、`python -m subsystem_announcement version` 正确返回 `0.1.0`
- 构建验证：
  - 在 `pyproject.toml` 的 `[tool.pytest.ini_options]` 中补 `pythonpath = ["src"]`，确保 src-layout 下 `pytest` 直接可跑

#### 不在本次范围
- 不实现任何公告发现、解析、抽取、提交逻辑；相关 package 只放空 `__init__.py`
- 不接入 `subsystem-sdk`，该工作归 ISSUE-002
- 不写任何 Docling、LlamaIndex、HTTP 客户端调用；就算依赖已声明也不在本 issue 触发
- 不扩展 CLI 的业务子命令（如 `parse` / `extract` / `submit`）；仅保留 `version` 与 `doctor`
- 不修改 `CLAUDE.md`、`AGENTS.md`、`docs/subsystem-announcement.project-doc.md` 内容
- 若发现需要共享 schema，必须单独起 issue，不能在本 issue 临时加 `contracts` 目录

#### 关键交付物
- `pyproject.toml`：`[project].dependencies` 明确列出 6 个运行依赖 + 2 个测试依赖；`[tool.setuptools.packages.find]` 配置 `where = ["src"]`
- `AnnouncementConfig`：pydantic v2 `BaseModel`，字段类型全标注，含 `model_config = ConfigDict(extra="forbid")`
- `load_config(path: Path | None = None) -> AnnouncementConfig`：当配置文件不存在时返回默认实例而非抛异常；当字段非法时抛 `pydantic.ValidationError`
- `configure_logging(level: str = "INFO") -> None`：幂等，可重复调用无副作用
- `main() -> int`：返回 0 表示成功、非 0 表示失败；异常分支打印到 stderr 并返回 1
- `tests/test_package_layout.py`：≥5 条测试覆盖 import、config 默认值、config 解析失败、CLI version、CLI doctor
- `docs/PROGRESS.md`：首次提交的状态表骨架
- 异常处理：`load_config` 在 IO 错误时抛 `FileNotFoundError` 上报；`configure_logging` 内部吞掉二次初始化异常

#### 验收标准
**基础结构：**
- [ ] `python -c "import subsystem_announcement; print(subsystem_announcement.__version__)"` 输出 `0.1.0`
- [ ] 7 个子 package 全部可被 `importlib.import_module` 成功加载
- [ ] `pyproject.toml` 通过 `python -m build --sdist --wheel`（或 `pip install -e .`）构建成功
**配置：**
- [ ] `AnnouncementConfig()`（零参数）可实例化，`heartbeat_interval_seconds == 60`
- [ ] 传入非法类型触发 `pydantic.ValidationError`
**CLI：**
- [ ] `python -m subsystem_announcement version` 返回 exit code 0 且 stdout 含 `0.1.0`
- [ ] `python -m subsystem_announcement doctor` 返回 exit code 0
**测试：**
- [ ] `tests/test_package_layout.py` ≥5 条用例全部通过
- [ ] `pytest` 在仓库根目录直接执行成功，0 个失败
- [ ] 执行 `pytest` 时不产生 `PytestCollectionWarning` 或 `DeprecationWarning`
**文档：**
- [ ] `docs/PROGRESS.md` 存在且列出阶段 0-3 四个里程碑

#### 验证命令
```bash
# 安装
pip install -e .[dev] || pip install -e .
# Unit tests
pytest tests/test_package_layout.py -v
# Integration check
python -m subsystem_announcement version
python -m subsystem_announcement doctor
python -c "import subsystem_announcement.discovery, subsystem_announcement.parse, subsystem_announcement.extract, subsystem_announcement.signals, subsystem_announcement.graph, subsystem_announcement.index, subsystem_announcement.runtime"
# Regression
pytest -q
```

#### 依赖
无前置依赖

---

### ISSUE-002: 接入 subsystem-sdk 并实现 Ex-0 注册与心跳闭环
**labels**: P0, integration, milestone-0, ready

#### 背景与目标
本 issue 对应项目文档 §21 阶段 0 退出条件"公告子系统可作为参考子系统启动并发送 Ex-0"。依据 §5.2 与 §16.2，子系统必须通过 `subsystem-sdk` 提供的 `SubsystemBaseInterface` 完成注册、心跳、submit 三个动作，不允许直连 Layer B 队列或自建 adapter。本 issue 在 ISSUE-001 的骨架上接入 SDK，实现最小可运行的参考子系统：启动 -> 注册 -> 周期心跳 -> 发送一条 Ex-0 占位 payload。它不做任何公告解析；目的是让后续阶段 1 的真实候选对象有一条可复用的提交通道。

#### 所属模块
- primary writable paths：
  - `src/subsystem_announcement/runtime/__init__.py`
  - `src/subsystem_announcement/runtime/sdk_adapter.py`
  - `src/subsystem_announcement/runtime/lifecycle.py`
  - `src/subsystem_announcement/runtime/registration.py`
  - `src/subsystem_announcement/runtime/heartbeat.py`
  - `src/subsystem_announcement/runtime/ex0.py`
  - `src/subsystem_announcement/__main__.py`（新增 `run` / `ping` 子命令）
  - `src/subsystem_announcement/config.py`（新增 SDK 相关字段）
  - `tests/test_runtime_sdk.py`
  - `tests/conftest.py`（引入 SDK fake / fixture）
- adjacent read-only 路径：`subsystem-sdk` 源码（若本地可见，只读参考其接口）
- off-limits：
  - 不得直接写 `data-platform` / Layer B 的任何存储或 adapter
  - 不得实现 Ex-1/Ex-2/Ex-3，仅允许 Ex-0
  - 不得在此 issue 启动 Docling / LlamaIndex

#### 实现范围
- SDK 适配层：
  - `runtime/sdk_adapter.py`：`class AnnouncementSubsystem(SubsystemBaseInterface)`，构造参数 `config: AnnouncementConfig`；实现 `on_register() -> RegistrationSpec`、`on_heartbeat() -> HeartbeatPayload`、`submit(candidate: ExPayload) -> SubmitResult`
  - 若 `subsystem-sdk` 不可 import，引入 `runtime/sdk_adapter.py` 顶部的 `SDK_AVAILABLE: bool` 守卫，并在缺失时使用 `runtime/_sdk_stub.py` 提供协议兼容 shim（只包含类型占位，不做 IO）
- 生命周期层：
  - `runtime/lifecycle.py`：`async def run(config: AnnouncementConfig, *, stop_event: asyncio.Event | None = None) -> None`，负责启动注册、心跳、Ex-0 首发
  - `runtime/registration.py`：`build_registration_spec(config: AnnouncementConfig) -> RegistrationSpec`，注入 `module_id="subsystem-announcement"`、`owned_ex_types=["Ex-0","Ex-1","Ex-2","Ex-3"]`、`parser_version=config.docling_version`
  - `runtime/heartbeat.py`：`build_heartbeat(now: datetime, last_run_id: str | None) -> HeartbeatPayload`，周期间隔从 `config.heartbeat_interval_seconds` 读取
  - `runtime/ex0.py`：`build_ex0_envelope(run_id: str, reason: str) -> Ex0Payload`，用作阶段 0 占位提交
- CLI 层：
  - `__main__.py` 新增 `run` 子命令：启动异步生命周期；`ping` 子命令：执行一次注册 + 单次心跳 + 单条 Ex-0，然后退出
- 配置层：
  - `config.py` 新增 `sdk_endpoint: str | None`、`registration_ttl_seconds: int = 900`
- 测试层：
  - `tests/conftest.py`：提供 `fake_sdk` fixture，录制 `on_register` / `on_heartbeat` / `submit` 调用
  - `tests/test_runtime_sdk.py`：覆盖注册 spec 正确性、心跳 payload 结构、Ex-0 占位提交、缺失 SDK 时的优雅降级、`stop_event` 触发干净关闭

#### 不在本次范围
- 不实现 Ex-1 / Ex-2 / Ex-3 的任何字段或 payload（归 ISSUE-003+）
- 不实现公告发现 / 解析 / 抽取
- 不接入真实 Layer B；测试全部用 fake SDK
- 不自定义 schema；schema 以 `contracts`（外部）和 SDK 提供为准，本 issue 只做调用
- 若发现 `subsystem-sdk` 接口与项目文档 §16 不一致，必须升级为 blocker，不在本 issue 擅自修改

#### 关键交付物
- `AnnouncementSubsystem`：3 个 override 方法均有类型签名和单测
- `async def run(config, *, stop_event) -> None`：能响应 `stop_event.set()` 在 ≤2s 内退出
- `RegistrationSpec`：`module_id == "subsystem-announcement"`，含正确的 owned_ex_types 与 parser_version
- `HeartbeatPayload`：包含 `run_id`、`timestamp`、`last_ex_id`、`status`
- `Ex0Payload`：包含 `run_id`、`reason`、`emitted_at`
- `python -m subsystem_announcement ping` 能在离线 fake 模式下返回 0
- 测试：≥8 条，覆盖注册、心跳、Ex-0、stop_event、SDK 缺失降级、异常捕获、并发心跳幂等
- 错误处理：SDK submit 抛异常时 `run()` 记录 structlog error 并继续下一轮，不 crash

#### 验收标准
**注册与心跳：**
- [ ] 注册 spec `module_id == "subsystem-announcement"`
- [ ] 心跳间隔等于 `config.heartbeat_interval_seconds`
- [ ] `stop_event.set()` 后 `run()` 在 2 秒内返回
**Ex-0：**
- [ ] 启动后至少发送一次 Ex-0 占位
- [ ] Ex-0 payload 通过 fake SDK 的 schema 校验
**降级：**
- [ ] `SDK_AVAILABLE=False` 时 `ping` 仍可返回 0 并打印降级日志
**CLI：**
- [ ] `python -m subsystem_announcement ping` exit code 0
- [ ] `python -m subsystem_announcement run --once` exit code 0
**测试：**
- [ ] `tests/test_runtime_sdk.py` ≥8 条用例全部通过
- [ ] `pytest` 全量通过，无新 warning

#### 验证命令
```bash
# Unit tests
pytest tests/test_runtime_sdk.py -v
# Integration check
python -m subsystem_announcement ping
python -c "from subsystem_announcement.runtime.sdk_adapter import AnnouncementSubsystem; print(AnnouncementSubsystem.__mro__)"
# Regression
pytest -q
```

#### 依赖
依赖 #ISSUE-001（初始化项目骨架与包结构）

---

## 阶段 1：公告发现 + Ex-1 主干

**目标**：打通"元数据引用 -> 官方正文 -> Docling 解析 -> Ex-1 候选事实 -> SDK 提交"主链。
**前置依赖**：阶段 0 完成

### ISSUE-003: 公告发现与官方正文获取子链
**labels**: P1, feature, milestone-1
**摘要**: 实现 `AnnouncementEnvelope` 消费入口、官方 URL 读取、内容哈希去重与本地 artifact 缓存，形成可重放的公告发现流。
**所属模块**: 主写 `src/subsystem_announcement/discovery/`（`envelope.py`、`fetcher.py`、`dedupe.py`、`cache.py`）+ `tests/test_discovery_*.py`；只读调用 `runtime.config`。
**写入边界**: 允许修改 discovery package 和对应测试；禁止修改 parse/extract/signals/graph/index/runtime 包；禁止引入第二种抓取器或绕过 data-platform 元数据。
**实现顺序**: 先定义 `AnnouncementEnvelope` pydantic 模型与入口 API，再实现官方 URL httpx 读取（含 retry + 超时 + 非官方域名拒绝），再落 content_hash + announcement_id 去重队列，最后补本地 `AnnouncementDocumentArtifact` 文件缓存 + 重放测试。
**依赖**: #ISSUE-002（SDK 闭环已就绪，discovery 产物能向下游复用）

---

### ISSUE-004: Docling 解析与 ParsedAnnouncementArtifact 落地
**labels**: P1, feature, milestone-1
**摘要**: 把官方公告正文（PDF / HTML / Word）通过 Docling 解析为 `ParsedAnnouncementArtifact`，包含章节层级、表格与归一化全文，供抽取与检索复用。
**所属模块**: 主写 `src/subsystem_announcement/parse/`（`docling_client.py`、`normalize.py`、`artifact.py`、`errors.py`）+ `tests/test_parse_*.py` + `tests/fixtures/announcements/`（小样本）。
**写入边界**: 允许修改 parse package、测试与 fixtures 目录；禁止在此处做事实抽取、信号生成或检索索引；禁止引入 Docling 以外的第二个 parser（违反 §5.4）。
**实现顺序**: 先封装 Docling 版本锁定客户端与错误分类，再实现标题/章节/表格归一化到 `ParsedAnnouncementArtifact`，再把 `content_hash` + `parser_version` 写入 artifact 以支持重放，最后跑 10-20 份 A 股公告样本验收并把基线时延记录进测试。
**依赖**: #ISSUE-003（需要 discovery 产出的原文 artifact）

---

### ISSUE-005: Ex-1 公告事实抽取与实体锚点协同
**labels**: P1, algorithm, milestone-1
**摘要**: 基于 `ParsedAnnouncementArtifact` 实现业绩预告 / 重大合同 / 股权变动 / 股权质押 / 监管 / 停复牌 / 募资变更 7 类 `AnnouncementFactCandidate`，全部携带 `EvidenceSpan`，并与 `entity-registry` 做确定性锚点与复杂 mention 交接。
**所属模块**: 主写 `src/subsystem_announcement/extract/`（`classifier.py`、`rules/`、`reasoner_bridge.py`、`evidence.py`、`entity_anchor.py`）+ `tests/test_extract_*.py`；只读调用 `parse.artifact`、`runtime.config`。
**写入边界**: 允许修改 extract package 与测试；禁止修改 discovery/parse/signals/graph；禁止直连 provider SDK（复杂段落必须走 `reasoner-runtime.generate_structured()`）；禁止输出任何 Ex-2 / Ex-3。
**实现顺序**: 先落 `EvidenceSpan` + `AnnouncementFactCandidate` schema，再实现 disclosure type 分类器与确定性规则抽取器，再接入 `reasoner-runtime` 的困难段落辅助通道，随后接 `entity-registry.lookup_alias` / `resolve_mentions` 完成主实体与相关实体锚点，最后用 7 种样本公告做端到端回归。
**依赖**: #ISSUE-004（需要稳定的解析 artifact）

---

### ISSUE-006: 通过 subsystem-sdk 批量提交 Ex-1 候选与 run trace
**labels**: P1, integration, milestone-1
**摘要**: 把 ISSUE-005 产出的 `AnnouncementFactCandidate[]` 经 `subsystem-sdk.submit()` 批量提交，落 `AnnouncementExtractionRun` trace，并补齐 CLI 端到端 `process` 子命令。
**所属模块**: 主写 `src/subsystem_announcement/runtime/submit.py`、`runtime/pipeline.py`、`runtime/trace.py`、`__main__.py`（新增 `process` 子命令）+ `tests/test_pipeline_e2e.py`。
**写入边界**: 允许修改 runtime package 与 CLI 入口；禁止修改 extract 内部逻辑（若不足请回退到 ISSUE-005）；禁止在提交前静默丢弃候选对象。
**实现顺序**: 先把 discovery -> parse -> extract -> submit 串成 `AnnouncementPipeline`，再实现批量 submit 的幂等与失败重试，再落 `AnnouncementExtractionRun` 本地 trace，最后补端到端测试：给定 envelope -> 产出 ≥1 个带 EvidenceSpan 的 Ex-1 + 成功 submit。
**依赖**: #ISSUE-005（依赖 Ex-1 抽取实现）

---

## 阶段 2：Ex-2 与高门槛 Ex-3

**目标**：在 Ex-1 主干稳定后补方向性信号与少量高门槛图谱变更候选。
**前置依赖**：阶段 1 完成

### ISSUE-007: Ex-2 公告信号生成
**labels**: P1, algorithm, milestone-2
**摘要**: 基于 `AnnouncementFactCandidate[]` 按 fact_type -> signal_template 的映射生成 `AnnouncementSignalCandidate`，覆盖 direction / magnitude / time_horizon 与受影响实体。
**所属模块**: 主写 `src/subsystem_announcement/signals/`（`templates.py`、`classifier.py`、`aggregator.py`）+ `tests/test_signals_*.py`。
**写入边界**: 允许修改 signals package 与测试；禁止修改 extract 抽取规则；事实不充分时必须只出 Ex-1，禁止硬凑 Ex-2。
**实现顺序**: 先定义 signal template 表与 direction / magnitude 规则，再实现 `derive_signal_candidates(facts)`，再把 EvidenceSpan 回指到源 fact，最后扩展 pipeline 在 Ex-1 之后条件性出 Ex-2。
**依赖**: #ISSUE-006（依赖稳定 Ex-1 + 提交通道）

---

### ISSUE-008: 高门槛 Ex-3 图谱变更候选
**labels**: P1, algorithm, milestone-2
**摘要**: 仅在"控股股东/实控人变更、持股比例变化、重大合同建立/终止"三类强证据场景下产出 `AnnouncementGraphDeltaCandidate`，通过强证据 + 实体双重锚点校验。
**所属模块**: 主写 `src/subsystem_announcement/graph/`（`rules.py`、`deltas.py`、`guard.py`）+ `tests/test_graph_delta_*.py`。
**写入边界**: 允许修改 graph package 与测试；禁止修改 signals / extract / entity-registry；禁止扩展 Ex-3 覆盖范围（§5.4 不可协商）。
**实现顺序**: 先实现强证据 guard（至少两条 EvidenceSpan + 实体锚点置信度阈值），再落 `delta_type` / `relation_type` 白名单，再串入 pipeline 条件输出，最后增加反例回归防止模棱两可关系被输出。
**依赖**: #ISSUE-007（Ex-2 管线稳定后再并入 Ex-3，避免图谱污染）

---

## 阶段 3：P4b 公告检索能力

**目标**：补齐 Docling + LlamaIndex 公告域检索链，支撑历史公告按章节与事件语义检索。
**前置依赖**：阶段 2 完成

### ISSUE-009: 公告域 chunk / index 与 retrieval artifact
**labels**: P2, feature, milestone-3
**摘要**: 将 `ParsedAnnouncementArtifact` 按章节 / 表格 / 条款切片，通过 chunk_id 锚定的 LlamaIndex 节点构建本地向量索引，并输出 `AnnouncementRetrievalArtifact` 引用。
**所属模块**: 主写 `src/subsystem_announcement/index/`（`chunker.py`、`vector_store.py`、`retrieval_artifact.py`、`sample_query.py`）+ `tests/test_index_*.py`。
**写入边界**: 允许修改 index package 与测试；禁止修改 extract / signals / graph；禁止把索引构建压进日频关键路径，重建脚本须以离线 CLI 形式提供。
**实现顺序**: 先实现章节/表格 chunk，再接 chunk_id 锚定的 LlamaIndex 节点 + `SimpleVectorStore` / FAISS Lite 二选一，再落 `AnnouncementRetrievalArtifact` 持久化，最后实现示例 `query(text)` 并补端到端检索测试。
**依赖**: #ISSUE-008（阶段 2 稳定后再并检索链）

---

### ISSUE-010: Replay、repair 与增强样本回归
**labels**: P2, testing, milestone-3
**摘要**: 补齐公告重解析、失败 repair、样本固化（10-20 份典型 A 股公告）与关键指标回归（§19）自动化检查。
**所属模块**: 主写 `src/subsystem_announcement/runtime/replay.py`、`runtime/repair.py`、`tests/fixtures/announcements/`、`tests/test_metrics_regression.py`、`tests/test_replay_repair.py`。
**写入边界**: 允许扩展 runtime 的 replay/repair、fixtures 与回归测试；禁止修改 discovery / parse / extract / signals / graph / index 的核心行为；禁止弱化 §19 目标值以通过测试。
**实现顺序**: 先落 replay CLI（按 announcement_id 重放解析 + 抽取），再落 repair（Docling 版本升级 / 解析失败重试），再把 10-20 份样本固化入 fixtures，最后实现 §19 六项关键指标的自动化回归门槛测试。
**依赖**: #ISSUE-009（索引与检索就绪后进行最终回归）

---
