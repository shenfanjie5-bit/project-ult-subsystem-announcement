# subsystem-announcement 项目进度总览

> 最后更新：2026-04-18
> 源文档：`docs/subsystem-announcement.project-doc.md`
> 任务拆解：`docs/TASK_BREAKDOWN.md`

## 里程碑状态

| 里程碑 | 标签 | 目标 | Issue 范围 | 状态 | 退出条件（§21） |
|--------|------|------|------------|------|-----------------|
| 阶段 0 | milestone-0 | 参考子系统骨架，SDK 注册 / 心跳 / Ex-0 闭环 | ISSUE-001 ~ ISSUE-002 | ☐ Not started | 公告子系统可作为参考子系统启动并发送 Ex-0 |
| 阶段 1 | milestone-1 | 公告发现 + Docling 解析 + Ex-1 主干 + SDK 提交 | ISSUE-003 ~ ISSUE-006 | ☐ Not started | 典型公告能稳定产出带 EvidenceSpan 的 Ex-1 |
| 阶段 2 | milestone-2 | Ex-2 信号 + 高门槛 Ex-3 图谱变更 | ISSUE-007 ~ ISSUE-008 | ☐ Not started | 高价值事件可少量稳定产生 Ex-2 / Ex-3 |
| 阶段 3 | milestone-3 | 公告检索能力 + replay/repair + 样本回归 | ISSUE-009 ~ ISSUE-010 | ☐ Not started | 历史公告可按章节与事件语义检索 |

图例：☐ Not started  🟡 In progress  ✅ Done  ⛔ Blocked

## Issue 明细

| Issue | 标题 | 里程碑 | 优先级 | 状态 | 依赖 |
|-------|------|--------|--------|------|------|
| ISSUE-001 | 初始化公告子系统 Python 项目骨架与包结构 | milestone-0 | P0 | ☐ | — |
| ISSUE-002 | 接入 subsystem-sdk 并实现 Ex-0 注册与心跳闭环 | milestone-0 | P0 | ☐ | #ISSUE-001 |
| ISSUE-003 | 公告发现与官方正文获取子链 | milestone-1 | P1 | ☐ | #ISSUE-002 |
| ISSUE-004 | Docling 解析与 ParsedAnnouncementArtifact 落地 | milestone-1 | P1 | ☐ | #ISSUE-003 |
| ISSUE-005 | Ex-1 公告事实抽取与实体锚点协同 | milestone-1 | P1 | ☐ | #ISSUE-004 |
| ISSUE-006 | 通过 subsystem-sdk 批量提交 Ex-1 候选与 run trace | milestone-1 | P1 | ☐ | #ISSUE-005 |
| ISSUE-007 | Ex-2 公告信号生成 | milestone-2 | P1 | ☐ | #ISSUE-006 |
| ISSUE-008 | 高门槛 Ex-3 图谱变更候选 | milestone-2 | P1 | ☐ | #ISSUE-007 |
| ISSUE-009 | 公告域 chunk / index 与 retrieval artifact | milestone-3 | P2 | ☐ | #ISSUE-008 |
| ISSUE-010 | Replay、repair 与增强样本回归 | milestone-3 | P2 | ☐ | #ISSUE-009 |

## 关键指标跟踪（§19 目标）

| 指标 | 目标值 | 当前值 | 达标 Issue |
|------|--------|--------|-----------|
| 官方源覆盖率 | 100% | — | ISSUE-003 |
| Ex-1 有 EvidenceSpan 覆盖率 | 100% | — | ISSUE-005 |
| Ex-3 误产出率 | < 1% | — | ISSUE-008 |
| 上市公司主实体确定性锚点成功率 | > 90% | — | ISSUE-005 |
| 单篇典型公告解析耗时 | < 3 分钟 | — | ISSUE-004 |
| 公告发现到 Ex-1 产出耗时 | < 5 分钟 | — | ISSUE-006 |
| chunk / index 构建耗时 | < 2 分钟 | — | ISSUE-009 |

## 边界守护（§5.4 不可协商约束）

- [ ] primary source 仅来自交易所 / 上市公司官方披露
- [ ] 公告元数据 canonical 归 `data-platform`，本模块不重复采集
- [ ] Docling 是唯一解析前端，不引入第二个 parser
- [ ] 不直接写 formal object，仅通过 Ex-0~Ex-3 候选对象提交
- [ ] 不直连 provider SDK，复杂抽取统一走 `reasoner-runtime.generate_structured()`
- [ ] Ex-3 保持高门槛，仅覆盖控股股东变更 / 持股比例变化 / 重大合同建立或终止
- [ ] 大批量 Docling 离线任务不压关键路径

## Blocker 升级记录（§25.3）

暂无。出现以下情况立即升级：
- 公告来源或官方引用不清，无法形成合法 `source_reference`
- 需要引入第二个 parser
- 需要把重型离线批处理压进日频关键路径
- Docling / LlamaIndex 版本无法锁定或样本验收基线缺失
- 需要把公告正文理解结果直接写成 formal object
