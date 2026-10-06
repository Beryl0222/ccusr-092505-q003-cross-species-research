# 远缘细胞研究证据流水

本项目提供远缘细胞研究证据流水所需的领域事件交换约定、基础校验库与领域流程实现。接入方使用统一的聚合标识、事件版本和带时区的发生时间，保证业务事实在不同环节之间可以复核。

## 目录

- `contracts/domain.schema.json`：领域事件信封和已登记类型。
- `data/sample.json`：中文联调样例。
- `src/cross_species_research/contracts.py`：不依赖第三方包的基础校验器。
- `src/cross_species_research/pipeline.py`：证据流水领域流程。
- `tests/`：契约边界检查与领域流程测试。

## 领域对象与事件

聚合对象：`species_source`（物种与细胞来源及授权范围）、`specimen_batch`（测序批次及文件校验和）、`annotation_revision`（功能注释版本）、`filter_profile`（过滤参数）、`analysis_run`（分析运行与分片）、`correspondence`（跨物种对应关系）、`research_claim`（公开结论）、`publication_asset`（发布资产）。

事件类型：`SOURCE_REGISTERED`、`BATCH_REGISTERED`、`ANNOTATION_LOCKED`、`FILTER_PROFILE_DEFINED`、`ANALYSIS_STARTED`、`SHARD_COMPLETED`、`ANALYSIS_COMPLETED`、`CORRESPONDENCE_ASSERTED`、`REVIEW_APPROVED`、`REVIEW_REVOKED`、`RANKING_REBUILT`、`CLAIM_RELEASED`、`CLAIM_LIMITED`、`ASSET_PUBLISHED`。

## 领域规则

- **幂等登记与隔离**：来源、批次、注释、过滤参数按内容指纹登记；重复提交复用，标识相同而文件或参数不同抛出 `ConflictError`，必须更换标识隔离。
- **运行复用**：分析运行按批次指纹、注释版本、过滤参数与软件配置派生运行键，重复请求直接复用已有结果。
- **分片计数**：分片任务可重试，同一分片标识只计数一次，全部完成后运行才标记完成。
- **对应关系**：每条跨物种对应关系快照其依据的具体输入（批次指纹、注释版本、过滤参数）与软件配置，且物种必须在运行输入范围内。
- **撤回重建**：质量复核撤回批次时只重建受影响的排名与图表（`RANKING_REBUILT`），已发布的结论保留并以 `CLAIM_LIMITED` 标注限制；被撤回的输入不得再启动运行或发布结论。
- **结论守卫**：发布结论必须引用已复核的对应关系并附不确定性说明；相关性结论只能发布群体层面陈述，不得包含个体诊断字段。
- **发布续跑**：资产按标识幂等发布，中途重启只补齐缺失项（`missing_assets`）。
- **角色视图**：`view_claim` 按 researcher / reviewer / public 裁剪字段，公众看不到文件、校验和与审批人。
- **全程追溯**：`trace_claim` 从一条结论回溯到原始文件校验和、注释与过滤版本、软件配置、审批人和不确定性说明。

## 测试

```bash
python3 -m unittest discover -s tests
```

## 编译检查

```bash
python3 -m compileall -q src tests
```
