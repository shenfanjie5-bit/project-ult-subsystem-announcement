# subsystem-announcement 完整项目文档

> **文档状态**：Draft v1
> **版本**：v0.1.1
> **作者**：Codex
> **创建日期**：2026-04-15
> **最后更新**：2026-04-15
> **文档目的**：把 `subsystem-announcement` 子项目从“抓公告、抽点信息”的宽泛理解收束为可立项、可拆分、可实现、可验收的正式项目，使其成为主项目中唯一负责公告域半结构化文档理解、公告事件抽取、公告信号生成和少量公告驱动图谱变更候选输出的参考子系统。

---

## 变更记录

| 版本 | 日期 | 变更内容 | 作者 |
|------|------|----------|------|
| v0.1 | 2026-04-15 | 初稿 | Codex |
| v0.1.1 | 2026-04-15 | 补充 runtime 接口级依赖与 Docling 基线验收口径 | Codex |

---

## 1. 一句话定义

`subsystem-announcement` 是主项目中**唯一负责围绕交易所官方公告、上市公司披露和相关 PDF/HTML 正文做半结构化解析、事件抽取、影响分类，并以 `Ex-1` 为主、`Ex-2` 为辅、`Ex-3` 少量输出候选对象**的公告理解子系统，它以“官方源优先”“公告元数据与正文理解分层”“证据先于解释”“不直接写 formal object”为不可协商约束。

它不是公告元数据 adapter，也不是 formal recommendation 模块。  
它不拥有公告元数据 canonical 落地，不拥有实体规则，不拥有 Layer B 队列表和主系统发布逻辑。

---

## 2. 文档定位与核心问题

本文解决的问题不是“怎么解析 PDF”，而是：

1. **公告元数据与正文分层问题**：公告是典型半结构化来源，标题、时间、证券代码等结构化元数据和正文/表格/附件里的关键信息必须分层处理，否则会在 data-platform 与子系统之间重复建设。
2. **事件抽取可信度问题**：公告是高价值官方披露，但表达形式复杂，若没有证据锚点、章节定位和结构化事件模型，后续 Layer B、主系统和审计都无法信任公告产出的候选对象。
3. **首个参考子系统问题**：`subsystem-announcement` 是首批 N=2 中更适合先落地的参考子系统，必须既能验证 `subsystem-sdk` 通用性，又不能把合同、实体解析和文档管线一起搅乱。

---

## 3. 术语表

| 术语 | 定义 | 备注 |
|------|------|------|
| Announcement Envelope | 公告元数据引用 | 如公告 ID、证券代码、标题、发布时间、URL |
| Official Announcement | 来自交易所或上市公司正式披露渠道的公告 | 官方源优先 |
| Announcement Body | 公告正文内容 | PDF / HTML / Word 等 |
| Parsed Announcement Artifact | 经 Docling 解析后的结构化 JSON 结果 | 含段落、表格、标题层级 |
| Evidence Span | 支撑某个候选结论的文本/表格片段定位 | 审计必需 |
| Announcement Fact Candidate | 从公告中抽出的结构化候选事实 | 对应 `Ex-1` |
| Announcement Signal Candidate | 从公告事实进一步归纳的候选信号 | 对应 `Ex-2` |
| Announcement Graph Delta Candidate | 由公告明确支持的候选图谱变更 | 对应 `Ex-3` |
| Disclosure Type | 公告类型分类 | 如业绩预告、股权变动、重大合同、监管事项 |
| Materiality Tier | 事项重要性等级 | 用于排序与告警 |
| Retrieval Chunk | 用于历史文档检索的公告切片 | LlamaIndex 消费 |

**规则**：
- Official Announcement 优先于转载、摘要和二手新闻
- 公告元数据由上游提供或对齐，正文理解归 `subsystem-announcement`
- `Ex-1` 是本子系统主输出，`Ex-2` 次之，`Ex-3` 只在证据极强时输出
- 证据片段必须和候选对象一起保留，不能只给结论不给出处
- 证券代码 / 上市公司锚点优先走确定性匹配，模糊情况转交 `entity-registry`

---

## 4. 目标与非目标

### 4.1 项目目标

1. **消费公告元数据引用**：接住交易所/公告元数据入口，形成可重放的公告发现流。
2. **解析公告正文**：用 Docling 对 PDF / HTML / Word 公告做结构化解析，抽出段落、表格和章节。
3. **抽取公告事实**：围绕业绩、股权、合同、监管、停复牌等主题生成高质量 `Ex-1` 候选事实。
4. **生成公告信号**：在证据明确时，从公告事实衍生 `Ex-2` 候选信号。
5. **少量生成图谱变更候选**：在控制权变更、主要股东关系、关键合作/供应关系等强证据场景输出 `Ex-3`。
6. **沉淀公告检索资产**：构建公告域 chunk / index，支持后续历史公告检索与解释。
7. **作为参考子系统落地**：验证 `subsystem-sdk`、`entity-registry`、Layer B 和 Docling 管线在真实场景下可跑通。

### 4.2 非目标

- **不拥有公告元数据 adapter**：公告标题、时间、证券代码、URL 等纯结构化元数据落地归 `data-platform` adapter + dbt。
- **不生成 formal recommendation**：公告子系统只能输出候选对象，正式判断归 `main-core`。
- **不拥有实体规则**：实体解析和 `canonical_entity_id` 规则归 `entity-registry`。
- **不替代 Layer B 权威校验**：本地校验和自检只是 fail-fast，正式接纳仍归 Layer B。
- **不做全局文档平台**：这里只处理公告域文档，不负责所有财报、研报、新闻的统一文档平台治理。
- **不把 Docling/LlamaIndex 放进 cycle 关键路径里做重型离线批处理**：大批量历史解析应离线执行，不阻塞日频主线。
- **不直连 provider SDK**：困难段落、复杂表格和长篇说明的结构化抽取统一通过 `reasoner-runtime` 的公开结构化接口完成。

---

## 5. 与现有工具的关系定位

### 5.1 架构位置

```text
data-platform announcement metadata + subsystem-sdk + entity-registry + reasoner-runtime
  -> subsystem-announcement
      ├── official announcement fetch/read
      ├── Docling parse
      ├── section/table extraction
      ├── announcement event extraction
      ├── Ex-1 facts
      ├── Ex-2 signals
      ├── limited Ex-3 graph deltas
      └── announcement retrieval chunks
  -> Layer B
      -> main-core / graph-engine / audit-eval
```

### 5.2 上游输入

| 来源 | 提供内容 | 说明 |
|------|----------|------|
| `data-platform` | 公告元数据引用、官方 URL、时间、证券代码等 | 纯结构化部分不在本模块重复采集 |
| `subsystem-sdk` | base class、submit / heartbeat、validator、fixtures | 公共子系统框架 |
| `contracts` | Ex-0~Ex-3 schema、错误码 | 本模块只消费正式合同 |
| `entity-registry` | 实体预检、alias / resolvable 查询 | 公告域实体大多可确定性对齐 |
| `reasoner-runtime` | 困难段落/表格的结构化抽取辅助 | 不拥有 provider 直连 |
| `assembly` | Docling / LlamaIndex / backend 配置 | 环境注入不归本模块定义 |

### 5.3 下游输出

| 目标 | 输出内容 | 消费方式 |
|------|----------|----------|
| Layer B / `data-platform` | `Ex-1` / `Ex-2` / `Ex-3` payload | 通过 `subsystem-sdk.submit()` |
| `main-core` | 经 Layer B 接纳后的公告候选事实 / 信号 | 间接消费 |
| `graph-engine` | 经 Layer B 接纳的公告驱动 graph delta | 间接消费 |
| `audit-eval` | 证据片段、源公告引用、解析 trace | 经候选对象和审计链间接消费 |
| 未来检索层 | 公告域 retrieval chunks / index refs | Python API / analytical reads |

### 5.4 核心边界

- **公告元数据 canonical 落地归 `data-platform`，公告正文理解归 `subsystem-announcement`**
- **`subsystem-announcement` 只通过 Ex-0~Ex-3 输出，不直接写 formal object**
- **官方源优先，二手转载不得成为 primary evidence**
- **`Ex-1` 为主、`Ex-2` 次之、`Ex-3` 少量且高门槛**
- **Docling/LlamaIndex 在这里是公告域解析与检索能力，不是全局文档平台 owner**
- **复杂公告理解统一调用 `reasoner-runtime.generate_structured()`，backend / provider 切换只能发生在 runtime 配置层**

---

## 6. 设计哲学

### 6.1 设计原则

#### 原则 1：Official Source First

公告子系统的核心价值来自“官方披露”这个事实本身。  
如果输入被转载摘要替代，就会把可信度最高的数据源降级成普通文本源。

#### 原则 2：Evidence Before Interpretation

公告里真正可用的不是“模型觉得重要”，而是“哪一段、哪张表、哪句表述支持这个结论”。  
因此候选事实、候选信号和候选图谱变更都必须带 Evidence Span。

#### 原则 3：Facts First, Signals Second

公告首先产生的是事实，其次才可能衍生方向性信号。  
如果一上来直接做利多/利空分类，很容易把事实层和判断层混在一起。

#### 原则 4：Deterministic Company Anchoring First

公告域的上市公司锚点通常比新闻更稳定，证券代码、公司简称和正式全称往往共存。  
因此先用确定性映射收敛实体，再把复杂引用留给 `entity-registry`。

#### 原则 5：Parse Once, Extract Many

Docling 解析应作为公告理解的统一结构前端。  
先得到稳定的章节/表格/文本结构，再复用它去做多种抽取，而不是每种任务各扫一遍原文。

### 6.2 反模式清单

| 反模式 | 为什么危险 |
|--------|-----------|
| 只看公告标题不读正文 | 会漏掉真正影响判断的核心条款和附表 |
| 直接用 LLM 对整篇公告做黑盒结论 | 证据链断裂、可解释性不足、成本失控 |
| 公告元数据和正文重复落地两套真相 | data-platform 与子系统边界混乱 |
| 没有强证据也大量产出 `Ex-3` | 会把图谱层污染成猜测关系 |
| 把转载新闻当公告 primary source | 官方源约束失效 |
| 大批量 Docling 离线任务压进 cycle 关键路径 | 会让日频时延不可控 |

---

## 7. 用户与消费方

### 7.1 直接消费方

| 消费方 | 消费内容 | 用途 |
|--------|----------|------|
| Layer B | 公告域 Ex payload | 候选接纳与校验 |
| `subsystem-sdk` | 参考实现反馈 | 验证公共框架 |
| `entity-registry` | 公告中的实体锚点需求 | 实体预检与后续解析 |
| 开发 / reviewer | Docling 解析结果、fixtures | 子系统开发与验收 |

### 7.2 间接用户

| 角色 | 关注点 |
|------|--------|
| `main-core` owner | 公告信号和事实是否稳定进入主系统 |
| `graph-engine` owner | 公告驱动的关系变化是否谨慎、可信 |
| 审计 / 复盘人员 | 每条公告候选是否能回到原文证据 |

---

## 8. 总体系统结构

### 8.1 公告发现主线

```text
announcement metadata refs
  -> fetch/read official document
  -> dedupe by announcement id / url / content hash
  -> cache document artifact
```

### 8.2 公告解析抽取主线

```text
official announcement body
  -> Docling parse to structured JSON
  -> section / table extraction
  -> deterministic rules + optional reasoner assist
  -> Ex-1 facts
  -> optional Ex-2 signals
  -> limited Ex-3 graph deltas
  -> submit through subsystem-sdk
```

### 8.3 公告检索主线

```text
parsed announcement artifact
  -> chunk into retrieval units
  -> LlamaIndex DoclingNodeParser
  -> local vector / index
  -> query by historical context
```

---

## 9. 领域对象设计

### 9.1 持久层对象

| 对象名 | 职责 | 归属 |
|--------|------|------|
| AnnouncementEnvelope | 公告元数据引用 | 输入引用，可映射上游 canonical |
| AnnouncementDocumentArtifact | 原始公告正文缓存 | 本地缓存 / artifact store |
| ParsedAnnouncementArtifact | Docling 结构化 JSON 结果 | 本地 artifact / analytical ref |
| AnnouncementChunkIndex | 公告域检索切片与索引引用 | 本地 index / analytical ref |
| AnnouncementExtractionRun | 一次抽取运行记录 | 本地 trace / optional analytical |

### 9.2 运行时对象

| 对象名 | 职责 | 生命周期 |
|--------|------|----------|
| AnnouncementParseContext | 一次公告解析上下文 | 单次解析期间 |
| AnnouncementSection | 一段正文或表格块 | 单次解析期间 |
| AnnouncementFactCandidate | 公告候选事实 | 单次抽取期间 |
| AnnouncementSignalCandidate | 公告候选信号 | 单次抽取期间 |
| AnnouncementGraphDeltaCandidate | 公告候选图谱变更 | 单次抽取期间 |
| EvidenceSpan | 候选对象的证据定位 | 单次抽取期间 |

### 9.3 核心对象详细设计

#### AnnouncementEnvelope

**角色**：描述一篇公告的发现入口和最小引用信息。

**建议字段**：

| 字段 | 类型 | 说明 |
|------|------|------|
| announcement_id | String | 公告唯一标识 |
| ts_code | String | 上市公司代码 |
| title | String | 公告标题 |
| publish_time | Timestamp | 发布时间 |
| official_url | String | 官方披露链接 |
| source_exchange | String | 交易所来源 |
| attachment_type | String | `pdf` / `html` / `word` |

#### ParsedAnnouncementArtifact

**角色**：统一的公告结构化解析产物。

**建议字段**：

| 字段 | 类型 | 说明 |
|------|------|------|
| announcement_id | String | 公告标识 |
| content_hash | String | 原文哈希 |
| parser_version | String | Docling 版本 |
| title_hierarchy | JSON | 标题层级 |
| sections | JSON | 段落与章节结构 |
| tables | JSON | 表格结构 |
| extracted_text | Text | 归一化全文 |
| parsed_at | Timestamp | 解析时间 |

#### AnnouncementFactCandidate

**角色**：公告域主输出对象，对应 `Ex-1`。

**建议字段**：

| 字段 | 类型 | 说明 |
|------|------|------|
| fact_id | String | 唯一标识 |
| announcement_id | String | 来源公告 |
| fact_type | String | 如 `earnings_preannounce` / `shareholder_change` / `major_contract` |
| primary_entity_id | String | 主实体 |
| related_entity_ids | Array[String] | 相关实体 |
| fact_content | JSON | 结构化事实体 |
| confidence | Number | 抽取置信度 |
| evidence_spans | Array[JSON] | 证据片段列表 |
| extracted_at | Timestamp | 抽取时间 |

#### AnnouncementSignalCandidate

**角色**：由公告事实进一步归纳的候选信号，对应 `Ex-2`。

**建议字段**：

| 字段 | 类型 | 说明 |
|------|------|------|
| signal_id | String | 唯一标识 |
| announcement_id | String | 来源公告 |
| signal_type | String | 如 `event_impact` / `fundamental_change` |
| direction | String | `positive` / `negative` / `neutral` |
| magnitude | Number | 强度 |
| affected_entities | Array[String] | 受影响实体 |
| time_horizon | String | `immediate` / `short_term` / `medium_term` |
| evidence_spans | Array[JSON] | 支撑证据 |
| confidence | Number | 置信度 |

#### AnnouncementGraphDeltaCandidate

**角色**：从公告中抽出的候选图谱变更，对应 `Ex-3`。

**建议字段**：

| 字段 | 类型 | 说明 |
|------|------|------|
| delta_id | String | 唯一标识 |
| announcement_id | String | 来源公告 |
| delta_type | String | `add_edge` / `update_edge` / `add_node` |
| source_node | String | 源实体 |
| target_node | String | 目标实体 |
| relation_type | String | 如 `shareholding` / `control` / `supply_contract` |
| properties | JSON | 关系属性 |
| evidence_spans | Array[JSON] | 强证据片段 |
| confidence | Number | 置信度 |

#### EvidenceSpan

**角色**：把候选结论锚定到公告原文。

**建议字段**：

| 字段 | 类型 | 说明 |
|------|------|------|
| section_id | String | 章节或表格块标识 |
| start_offset | Integer | 起始偏移 |
| end_offset | Integer | 结束偏移 |
| quote | String | 短引用 |
| table_ref | String \| Null | 表格引用 |

---

## 10. 数据模型设计

### 10.1 模型分层策略

- 公告元数据 canonical -> `data-platform`
- 公告正文原始缓存 / Docling 解析 JSON / retrieval chunks -> 子系统本地 artifact 或 analytical 引用
- Ex payload -> 通过 `subsystem-sdk` 提交，不在本子系统内自建权威队列

### 10.2 存储方案

| 存储用途 | 技术选型 | 理由 |
|----------|----------|------|
| 原始公告正文缓存 | 本地文件缓存 / object path ref | 便于重跑解析 |
| Docling 解析产物 | JSON artifact | 结构清晰、可复用 |
| 公告检索索引 | LlamaIndex SimpleVectorStore / FAISS（Lite） | P4 文档检索主线 |
| 抽取 trace | JSON / 本地日志 | 调试与审计辅助 |
| Ex payload | 运行时对象 | authoritative 接纳在 Layer B |

### 10.3 关系模型

- `AnnouncementEnvelope.announcement_id` 对齐上游公告元数据主键
- `ParsedAnnouncementArtifact.announcement_id -> AnnouncementEnvelope.announcement_id`
- `AnnouncementFactCandidate.announcement_id -> AnnouncementEnvelope.announcement_id`
- `AnnouncementSignalCandidate.announcement_id -> AnnouncementEnvelope.announcement_id`
- `AnnouncementGraphDeltaCandidate.announcement_id -> AnnouncementEnvelope.announcement_id`

---

## 11. 核心计算/算法设计

### 11.1 公告发现与去重算法

**输入**：公告元数据引用、官方 URL。

**输出**：待解析公告列表。

**处理流程**：

```text
read announcement metadata ref
  -> fetch official document url
  -> compute content hash if fetched
  -> dedupe by announcement_id / url / content hash
  -> enqueue parse job
```

### 11.2 公告结构解析算法

**输入**：公告正文文件。

**输出**：`ParsedAnnouncementArtifact`。

**处理流程**：

```text
read PDF / HTML / Word
  -> Docling parse
  -> normalize title / section / table hierarchy
  -> materialize structured JSON
  -> emit parse artifact
```

**规则**：

- Docling 是唯一解析前端，不引入第二个 parser
- CPU 批量解析应离线跑，不压在 cycle 关键路径
- 解析失败要可重试、可定位到原公告

### 11.3 事实抽取算法

**输入**：`ParsedAnnouncementArtifact`。

**输出**：`AnnouncementFactCandidate[]`。

**处理流程**：

```text
scan parsed sections and tables
  -> identify disclosure type
  -> run deterministic extractors first
  -> if needed call reasoner-runtime for hard sections
  -> attach EvidenceSpan
  -> emit Ex-1 fact candidates
```

**优先覆盖的 fact_type**：

- `earnings_preannounce`
- `major_contract`
- `shareholder_change`
- `equity_pledge`
- `regulatory_action`
- `trading_halt_resume`
- `fundraising_change`

### 11.4 信号生成算法

**输入**：`AnnouncementFactCandidate[]`。

**输出**：`AnnouncementSignalCandidate[]`。

**处理流程**：

```text
read fact candidates
  -> map fact_type to signal templates
  -> classify direction / magnitude / time_horizon
  -> keep only evidence-backed signals
  -> emit Ex-2
```

**规则**：

- 事实不充分时宁可只出 Ex-1，不强行出 Ex-2
- signal 必须能回指到具体 fact 和 EvidenceSpan

### 11.5 图谱变更候选生成算法

**输入**：`AnnouncementFactCandidate[]`、实体锚点。

**输出**：`AnnouncementGraphDeltaCandidate[]`。

**处理流程**：

```text
read fact candidates
  -> check relation type is explicit and high-confidence
  -> verify source/target entity anchors
  -> build limited Ex-3 candidate
  -> attach strong evidence
```

**只建议覆盖的高门槛场景**：

- 控股股东 / 实控人变更
- 明确持股比例变化
- 明确重大合同或合作关系建立 / 终止

**规则**：没有强证据时不产出 Ex-3。

### 11.6 实体锚点算法

**输入**：公告代码、公司简称、正文中的实体 mention。

**输出**：主实体与相关实体锚点。

**处理流程**：

```text
use ts_code / company name deterministic match first
  -> resolve obvious listed company entities
  -> use entity-registry.lookup_alias() for deterministic fast path
  -> send ambiguous mentions to entity-registry.resolve_mentions()
  -> leave unresolved refs explicit if needed
```

### 11.7 公告检索索引算法

**输入**：`ParsedAnnouncementArtifact`。

**输出**：公告域 chunk / index refs。

**处理流程**：

```text
load parsed announcement JSON
  -> chunk by section / table / clause
  -> LlamaIndex DoclingNodeParser
  -> build local vector index
  -> store retrieval refs
```

---

## 12. 触发/驱动引擎设计

### 12.1 触发源类型

| 类型 | 来源 | 示例 |
|------|------|------|
| 新公告触发 | `data-platform` / polling | 新公告元数据到达 |
| 重解析触发 | manual / repair | Docling 失败、版本升级 |
| 定时心跳触发 | `subsystem-sdk` | Ex-0 |
| 离线索引触发 | batch | 历史公告 chunk / index 重建 |

### 12.2 关键触发流程

```text
new_announcement_ref
  -> fetch_and_parse()
  -> extract_facts()
  -> optional_signals_and_graph_deltas()
  -> submit Ex payloads
```

### 12.3 启动顺序基线

| 阶段 | 动作 | 说明 |
|------|------|------|
| P0 | `contracts` + `subsystem-sdk` 先稳定 | 先有合同和子系统骨架 |
| P1-P2 | `data-platform` 提供公告元数据引用入口 | 元数据与正文理解分层 |
| P2-P3 | `entity-registry` 提供稳定上市公司锚点 | 公告域实体多为上市公司 |
| P4a | `subsystem-announcement` 先打通 Ex-1 主干 | 作为参考子系统 |
| P4b | 接入 Docling + LlamaIndex 深化正文解析与检索 | 完成半结构化主线 |

---

## 13. 输出产物设计

### 13.1 Ex-1 Announcement Facts

**面向**：Layer B

**结构**：

```text
{
  fact_type: String
  primary_entity_id: String
  fact_content: Object
  confidence: Number
  source_reference: Object
  evidence_spans: Array[Object]
}
```

### 13.2 Ex-2 Announcement Signals

**面向**：Layer B

**结构**：

```text
{
  signal_type: String
  direction: String
  magnitude: Number
  affected_entities: Array[String]
  time_horizon: String
  evidence: Object
  confidence: Number
}
```

### 13.3 Ex-3 Announcement Graph Deltas

**面向**：Layer B / 图谱链

**结构**：

```text
{
  delta_type: String
  source_node: String
  target_node: String
  relation_type: String
  properties: Object
  evidence: Object
  confidence: Number
}
```

### 13.4 Announcement Retrieval Artifact

**面向**：后续历史检索与解释

**结构**：

```text
{
  announcement_id: String
  chunk_refs: Array[String]
  index_ref: String
  parser_version: String
}
```

---

## 14. 系统模块拆分

**组织模式**：单个 Python 项目，内部按发现、解析、抽取、提交、检索分 package。

| 模块名 | 语言 | 运行位置 | 职责 |
|--------|------|----------|------|
| `subsystem_announcement.discovery` | Python | 库 | 公告发现、URL 读取、去重 |
| `subsystem_announcement.parse` | Python | 库 | Docling 解析与 artifact 生成 |
| `subsystem_announcement.extract` | Python | 库 | 事件抽取、Ex-1 生成 |
| `subsystem_announcement.signals` | Python | 库 | Ex-2 生成 |
| `subsystem_announcement.graph` | Python | 库 | 高门槛 Ex-3 生成 |
| `subsystem_announcement.index` | Python | 库 | LlamaIndex chunk / index |
| `subsystem_announcement.runtime` | Python | 库 | `subsystem-sdk` 集成、submit、heartbeat |

**关键设计决策**：

- 这是首个参考子系统，优先验证合同、SDK、实体锚点和 Docling 主线
- `Ex-1` 先打稳，再逐步补 `Ex-2` 与高门槛 `Ex-3`
- 公告正文解析与公告元数据输入必须分层，不做双份采集
- Retrieval artifact 是公告域能力，不是全局文档平台能力

---

## 15. 存储与技术路线

| 用途 | 技术选型 | 理由 |
|------|----------|------|
| 子系统骨架 | `subsystem-sdk` | 统一 submit / heartbeat / validator |
| 文档解析 | Docling | P4 唯一解析前端 |
| 索引与检索 | LlamaIndex + SimpleVectorStore / FAISS（Lite） | 公告域检索 |
| 结构化抽取 | 规则 + `reasoner-runtime` | 先确定性，后困难段落辅助 |
| 官方源读取 | Python HTTP / 文件读取 | 足够覆盖官方公告源 |

最低要求：

- 有稳定的公告元数据引用入口
- `subsystem-sdk` 可用
- `entity-registry` 至少能确定性对齐上市公司实体
- Docling / LlamaIndex 版本锁定并可在本地跑通

---

## 16. API 与接口合同

### 16.1 Python 接口

| 名称 | 功能 | 参数 |
|------|------|------|
| `consume_announcement_ref(envelope)` | 消费一条公告引用 | `AnnouncementEnvelope` |
| `parse_announcement(document_ref)` | 解析公告正文 | 文档引用 |
| `extract_fact_candidates(parsed_artifact)` | 抽取 Ex-1 | parse artifact |
| `derive_signal_candidates(facts)` | 生成 Ex-2 | fact list |
| `derive_graph_delta_candidates(facts)` | 生成 Ex-3 | fact list |
| `build_retrieval_artifact(parsed_artifact)` | 建公告索引 | parse artifact |
| `submit_candidates(candidates)` | 提交候选对象 | candidate list |

### 16.2 协议接口

| 名称 | 功能 | 参数 |
|------|------|------|
| `SubsystemBaseInterface` | 子系统公共运行接口 | 由 `subsystem-sdk` 提供 |
| `ExFactSchema` | 公告域 `Ex-1` 结构 | 由 `contracts` 定义 |
| `ExSignalSchema` | 公告域 `Ex-2` 结构 | 由 `contracts` 定义 |
| `ExGraphDeltaSchema` | 公告域 `Ex-3` 结构 | 由 `contracts` 定义 |
| `entity-registry.lookup_alias(name)` | 公告主实体确定性快路径 | alias / code |
| `entity-registry.resolve_mentions(mentions)` | 公告复杂 mention 解析 | mention + context |
| `reasoner-runtime.generate_structured(request)` | 困难段落 / 表格的结构化抽取 | request payload |

### 16.3 版本与兼容策略

- 公告域输出只接受 backward compatible 的 Ex schema 演进
- Docling / LlamaIndex 版本必须锁定，避免 API 漂移导致解析结果不稳
- `Ex-3` 覆盖范围应保守扩展，不能为了“更多输出”牺牲可信度
- 复杂抽取不得私接 provider SDK，统一通过 `reasoner-runtime.generate_structured()` 执行
- 主文档 P4 的 Docling 启动基线仍是“10-20 份典型 A 股财报”；本模块追加的公告样本验收是公告域补充，不替代该基线

---

## 18. 测试与验证策略

### 18.1 单元测试

- 公告类型分类测试
- 证券代码 / 公司简称确定性锚点测试
- EvidenceSpan 生成测试
- `Ex-1` / `Ex-2` / `Ex-3` payload 校验测试
- `Ex-3` 高门槛规则测试

### 18.2 集成测试

| 场景 | 验证目标 |
|------|----------|
| 官方公告元数据 -> PDF 正文解析 | 验证发现到解析主线 |
| 典型业绩预告公告 | 验证 Ex-1 主输出 |
| 重大合同 / 股权变动公告 | 验证 Ex-2 / 少量 Ex-3 |
| Docling parse -> LlamaIndex chunk | 验证公告检索主线 |
| `subsystem-sdk` submit + heartbeat | 验证参考子系统闭环 |

### 18.3 协议 / 契约测试

- 公告域候选对象全部通过 `contracts` Ex schema
- 子系统提交不携带 ingest metadata
- 官方公告引用缺失时不能伪造 source_reference

### 18.4 质量与回归测试

- 主文档级 Docling 启动基线：10-20 份典型 A 股财报样本
- 公告域补充验收：10-20 份典型 A 股公告样本
- 同一公告重复抓取不会重复产出同一候选对象
- 只看标题不看正文的回归防护测试
- 模棱两可关系不产出 Ex-3 的回归测试

---

## 19. 关键评价指标

### 19.1 性能指标

| 指标 | 目标值 | 说明 |
|------|--------|------|
| 单篇典型公告解析耗时 | `< 3 分钟` | Lite / CPU 目标 |
| 公告发现到 Ex-1 产出耗时 | `< 5 分钟` | 非极端长文档 |
| chunk / index 构建耗时 | `< 2 分钟` | 单篇公告 |

### 19.2 质量指标

| 指标 | 目标值 | 说明 |
|------|--------|------|
| 官方源覆盖率 | `100%` | primary source 只能用官方披露 |
| `Ex-1` 有证据片段覆盖率 | `100%` | 不允许裸结论 |
| `Ex-3` 误产出率 | `< 1%` | 高门槛输出 |
| 上市公司主实体确定性锚点成功率 | `> 90%` | 公告域预期 |

---

## 20. 项目交付物清单

### 20.1 公告理解主干

- 公告发现与去重
- Docling 解析主线
- Ex-1 候选事实抽取
- `subsystem-sdk` 集成

### 20.2 公告增强能力

- Ex-2 候选信号
- 高门槛 Ex-3
- retrieval chunks / index

### 20.3 参考子系统支撑

- fixtures 样例公告
- 典型公告类型测试集
- 端到端参考流程

---

## 21. 实施路线图

### 阶段 0：参考子系统骨架（1-2 天）

**阶段目标**：先让 `subsystem-sdk` 与公告子系统连起来。

**交付**：
- registration spec
- heartbeat
- submit 闭环

**退出条件**：公告子系统可作为参考子系统启动并发送 Ex-0。

### 阶段 1：公告发现 + Ex-1 主干（3-5 天）

**阶段目标**：先把“元数据引用 -> 正文 -> Ex-1”主链打通。

**交付**：
- official announcement fetch
- Docling parse
- 业绩/合同/股权类 Ex-1 抽取

**退出条件**：典型公告能稳定产出有证据片段的 Ex-1。

### 阶段 2：Ex-2 与高门槛 Ex-3（3-5 天）

**阶段目标**：在事实主干稳定后补方向性信号与少量图谱变更。

**交付**：
- Ex-2 模板
- 高门槛 Ex-3 规则
- 实体锚点增强

**退出条件**：公告中的高价值事件可以少量稳定产生 Ex-2 / Ex-3。

### 阶段 3：P4b 公告检索能力（3-5 天）

**阶段目标**：补齐 Docling + LlamaIndex 公告域检索链。

**交付**：
- chunk / index
- sample query
- artifact refs

**退出条件**：历史公告可被按章节和事件语义检索。

---

## 22. 主要风险

| 风险 | 影响 | 应对措施 |
|------|------|----------|
| 官方源链接质量波动 | 文档抓取失败 | 保留 metadata ref + retry / manual repair |
| Docling CPU 解析过慢 | 无法稳定跑批 | 提前做样本压测、把重解析放离线 |
| 表格结构复杂导致抽取失真 | 关键事实漏掉或错抽 | 先做财报/公告样本验收，再扩类型 |
| 标题党式信号生成 | 误导 Layer B / main-core | 事实优先，必须回指正文 EvidenceSpan |
| 公告过度产出 Ex-3 | 图谱污染 | 高门槛规则 + 保守默认 |

---

## 23. 验收标准

项目完成的最低标准：

1. `subsystem-announcement` 能从公告元数据引用出发，读取官方正文并解析成结构化 artifact
2. 典型公告类型能稳定产出带 EvidenceSpan 的 `Ex-1` 候选事实
3. `Ex-2` 只在事实充分时产出，`Ex-3` 只在高门槛强证据场景产出
4. 子系统通过 `subsystem-sdk` 提交候选对象与心跳，不直接触碰 Layer B 队列细节
5. 公告域 Docling + LlamaIndex 管线能在样本上跑通，不引入第二个 parser
6. 公告元数据 canonical 落地仍归 `data-platform`，本模块只拥有正文理解和候选输出
7. 文档中定义的 OWN / BAN / EDGE 与主项目 `12 + N` 模块边界一致

---

## 24. 一句话结论

`subsystem-announcement` 子项目不是一个“抓几篇公告做摘要”的工具，而是主项目里第一个真正把半结构化官方披露转成高可信候选事实和候选信号的参考子系统。  
它如果边界不稳，后面 SDK、实体锚点、Layer B 和文档解析主线都会一起失去校准样本。

---

## 25. 自动化开发对接

### 25.1 自动化输入契约

| 项 | 规则 |
|----|------|
| `module_id` | `subsystem-announcement` |
| 脚本先读章节 | `§1` `§4` `§5.2` `§5.4` `§8` `§11` `§14` `§16` `§18` `§21` `§23` |
| 默认 issue 粒度 | 一次只实现一个子链路：source / parse / extract / entities / signals / graph / runtime / fixtures |
| 默认写入范围 | 当前 repo 的公告抓取、Docling 解析、抽取、提交、测试、fixture、文档和版本配置 |
| 内部命名基线 | 以 `§14` 的内部模块名和 `§9` / `§13` 的对象名为准 |
| 禁止越界 | 不处理未批准来源、不引入第二个 parser、不直写 formal object、不把全局文档平台职责拉进本项目 |
| 完成判定 | 同时满足 `§18`、`§21` 当前阶段退出条件和 `§23` 对应条目 |

### 25.2 推荐自动化任务顺序

1. 先落 source reference、正文获取和 `ParsedAnnouncementArtifact`
2. 再落 `Ex-1` 主干、实体协同和 `subsystem-sdk` 提交
3. 再落 `Ex-2` / 高门槛 `Ex-3`、Docling + LlamaIndex 检索链
4. 最后补 replay、repair 和增强样本

补充规则：

- 单个 issue 默认只改一个子链路；解析、抽取、提交三类不要混成超大 PR
- 在 source_reference、Docling 样本和 SDK 提交未稳定前，不进入图谱候选或检索增强

### 25.3 Blocker 升级条件

- 公告来源、法务边界或官方引用不清，无法形成 `source_reference`
- 需要引入第二个 parser 或把重型离线批处理压进日频关键路径
- Docling / LlamaIndex 版本无法锁定或样本验收基线缺失
- 需要把公告正文理解结果直接写成 formal object
