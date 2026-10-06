# 远缘细胞研究证据流水

面向七个物种单细胞数据跨实验室协作的**证据流水线**。它不只保存最终图表，而是把
每条结论如何从原始文件、经锁定注释与软件配置、到人工复核与公开发布的全过程登记为
**只追加的领域事件**，使下一位研究者能区分真实观测与计算推断。

## 解决的问题与对应机制

| 需求 | 机制 |
| --- | --- |
| 登记物种、细胞来源、实验室、授权范围、测序文件校验和 | `BATCH_REGISTERED` 登记物种/来源/采集实验室/授权目的/公开授权/逐文件 sha256；`FILES_VERIFIED` 记录逐文件比对结果，**任一不符不产生已验证事实** |
| 功能注释版本会改变排名 | `ANNOTATION_LOCKED` 锁定来源、版本、本体；运行指纹包含注释版本 |
| 过滤阈值、样本批次改变模型排名 | 运行指纹 = 输入文件校验和 + 注释版本 + 过滤参数 + 软件/模型配置的规范化哈希 |
| 重复提交同一批次应复用运行结果 | 同指纹直接返回既有 `run_id`，不产生新事件；同 `request_id` 重试走请求幂等 |
| 标识相同而文件/参数不同必须隔离 | 同 `label` 第二次不同内容提交自动得到隔离标识 `label~iso2`，旧结果不被覆盖 |
| 分片任务可重试但不能重复计数 | 以 `shard_id` 去重；同内容重试返回既有提交、不新增事件；同 id 不同内容报错；完成时总条数按唯一分片求和 |
| 质量复核撤回某批数据 | `REVIEW_REVOKED`（批次）只让**真正消费该批次**的运行产生 `RUN_INVALIDATED`，并仅给相关结论打 `CLAIM_FLAGGED` 限制标注；历史事件与已发布资产**保留不删** |
| 只重建受影响的排名和图表 | `rerun_invalidated()` 以原规格（可替换批次）新建隔离运行；其他运行与资产原样复用 |
| 已用于论文的历史结果保留并标注限制 | 事件只追加；结论带 `restriction.restricted=true` 与 `history_retained=true`；替代发布用 `supersedes` 标记旧结论 |
| 研究者、审稿人、公众字段范围不同 | `project_claim(..., role)` 显式白名单三级视图（见下） |
| 任何相关性不得自动转成个体诊断 | 授权登记拒绝 `individual_diagnosis` 用途；每条发布事件携带 `individual_diagnosis_allowed=false` 与免责声明 |
| 发布中途重启只补齐缺失资产 | 资产由事件重放 + 文件校验和判定：已存在且校验和相符则复用，事件在而文件丢失才补齐，校验和不符则报错拒绝覆盖 |
| 从结论追溯到原始文件、版本、审批人、不确定性 | `trace_claim()` 返回证据链，显式标注 `observed_raw_file`/`manual_review`/`computed`/`reference` |

## 角色视图（字段白名单）

- **public**：结论正文、不确定性、功能对应（仅物种+功能，不含批次/阈值/软件）、
  资产校验和、复核人数、限制标注、诊断禁令。
- **researcher**：增加文件校验和、注释版本、过滤参数、软件与模型配置、分片、
  产出引用、审批人姓名。
- **reviewer**：再增加授权条款与限制、逐文件校验明细、审批检查项、撤回/失效原因、
  源事件列表。

结论自身也有 `visibility`（researcher/reviewer/public）；角色级别低于发布范围时
访问被拒绝（错误码 `access_denied`）。

## 目录

- `contracts/domain.schema.json`：领域事件信封与 12 种已登记事件类型。
- `src/cross_species_research/events.py`：事件存储（聚合版本号、请求幂等表、
  导出/重载）、规范化 JSON 与内容指纹。
- `src/cross_species_research/pipeline.py`：核心应用 `EvidencePipeline`
  （登记、校验、注释锁定、运行、分片、复核、撤回、重建、发布）。
- `src/cross_species_research/provenance.py`：角色视图 `project_claim`
  与溯源 `trace_claim`。
- `examples/demo.py`：端到端中文联调演示，生成 `data/sample_flow.json`。
- `data/sample.json`：单事件信封样例；`data/sample_flow.json`：完整生命周期事件流。
- `tests/`：契约、事件存储、登记门禁、运行复用/隔离、分片幂等、撤回重建、
  发布与资产补齐、角色视图、溯源共 60+ 检查。

## 快速开始

```python
from cross_species_research import EvidencePipeline, EventStore, project_claim, trace_claim

p = EvidencePipeline(EventStore())  # 重启时传入上次 store.export() 的事件即可恢复
p.register_batch(batch_id="B1", species="Homo sapiens", cell_source="小胶质细胞",
                 collection_lab="lab-a",
                 files=[{"file_id": "f1", "name": "f1.h5", "size": 12,
                         "sha256": "<64位十六进制>"}],
                 consent={"allowed_purposes": ["cross_species_correspondence"],
                          "public_release": True},
                 registered_by="curator.li")
p.verify_files("B1", {"f1": "<同一sha256>"}, verified_by="qc.wang")
# … lock_annotation → submit_run → commit_shard* → complete_run →
#    approve_run → release_claim → publish_assets …
view = project_claim(p, "CLAIM-1", "public")
chain = trace_claim(p, "CLAIM-1")
```

运行指纹示例：仅改 `filter_params` 中的一个阈值，同 `label` 提交会得到
`label~iso2`；完全相同的输入+配置再次提交则返回 `reused=True` 与原 `run_id`。

## 测试

```bash
python3 examples/demo.py            # 重新生成联调事件流
python3 -m unittest discover -s tests
python3 -m compileall -q src tests examples
```

## 设计说明

- 全部状态由事件回放得到（event sourcing），无第三方依赖，便于审计与跨语言对接；
  `EventStore.export()` 即持久化格式。
- 错误统一为 `PipelineError(code, message)`，接入方按稳定错误码处理
  （如 `checksum_mismatch`、`consent_scope_denied`、`idempotency_conflict`、
  `shard_conflict`、`evidence_invalidated`、`asset_corrupted`、`access_denied`）。
- 相似性排名、对应关系一律标记为 `computed` / `computed_correspondence`，
  只有测序文件与人工复核标记为 observed；该区分同时出现在溯源图与图例中。
