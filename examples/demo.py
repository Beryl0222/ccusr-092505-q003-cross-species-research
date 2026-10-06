"""远缘细胞研究证据流水端到端演示（中文联调样例）。

运行：python3 examples/demo.py
产物：data/sample_flow.json（完整事件日志），并在标准输出打印关键节点。
"""

from __future__ import annotations

import hashlib
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from cross_species_research.pipeline import EvidencePipeline, RESEARCH_PURPOSE
from cross_species_research.events import EventStore
from cross_species_research.provenance import project_claim, trace_claim


def sha(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def files(prefix: str, texts: dict[str, str]) -> list[dict]:
    return [
        {"file_id": fid, "name": f"{fid}.h5", "size": len(text.encode("utf-8")),
         "sha256": sha(text)}
        for fid, text in texts.items()
    ]


SOFTWARE = [
    {"name": "scanpy", "version": "1.10.2",
     "config": {"n_top_genes": 2000, "resolution": 0.8}},
    {"name": "orthomap", "version": "0.4.1",
     "config": {"aligner": "embedding-cosine", "seed": 7}},
]
FILTERS = {"min_genes": 200, "max_mito_pct": 10.0, "min_cells": 3,
           "doublet_threshold": 0.25}
MODEL = {"name": "cross-species-embedder", "version": "3.1", "seed": 7}


def register_and_verify(p: EvidencePipeline, batch_id: str, species: str,
                        source: str, lab: str, batch_files: list[dict],
                        *, public: bool = True) -> None:
    p.register_batch(
        batch_id=batch_id, species=species, cell_source=source,
        collection_lab=lab, files=batch_files,
        consent={"allowed_purposes": [RESEARCH_PURPOSE], "public_release": public,
                 "restrictions": "仅限学术研究"},
        registered_by="curator.li",
    )
    p.verify_files(batch_id, {f["file_id"]: f["sha256"] for f in batch_files},
                   verified_by="qc.wang")


def commit_complete_approve(p: EvidencePipeline, run_id: str,
                            outputs: list[dict], reviewer: str = "reviewer.sun") -> None:
    p.commit_shard(run_id, shard_id="shard-1", item_count=1200,
                   payload={"rows": list(range(1200))}, committed_by="worker-a",
                   request_id=f"req-{run_id}-s1")
    p.commit_shard(run_id, shard_id="shard-2", item_count=800,
                   payload={"rows": list(range(800))}, committed_by="worker-b",
                   request_id=f"req-{run_id}-s2")
    p.complete_run(run_id, outputs=outputs)
    p.approve_run(run_id, reviewer=reviewer,
                  checklist=["批次与授权核验", "文件校验和一致",
                             "过滤参数可复现", "结果与原始数据抽查一致"],
                  notes="未见批次效应主导相似性")


def main() -> None:
    schema = json.loads((ROOT / "contracts" / "domain.schema.json").read_text("utf-8"))
    p = EvidencePipeline(EventStore(), schema=schema)

    # 1-2. 登记两个物种的批次并完成测序文件校验
    human_files = files("h", {"h-rna-001": "human-microglia-rna-0001",
                              "h-rna-002": "human-microglia-rna-0002"})
    fish_files = files("z", {"z-rna-001": "zebrafish-microglia-like-rna-0001"})
    register_and_verify(p, "B-HUMAN", "Homo sapiens", "成年大脑小胶质细胞",
                        "神经科学实验室A", human_files)
    register_and_verify(p, "B-ZFISH", "Danio rerio", "脑组织小胶质样细胞",
                        "发育生物学实验室B", fish_files)

    # 3. 锁定功能注释版本
    p.lock_annotation(annotation_id="ANN-HOMOLOG-2026_09",
                      source="HomoloGene+CellOntology", version="2026-09",
                      ontology="CL:0000129", locked_by="annotator.chen")

    # 4. 提交运行（重复提交同批次会复用，参数变化会隔离）
    submit = p.submit_run(label="microglia-match",
                          batch_ids=["B-HUMAN", "B-ZFISH"],
                          annotation_id="ANN-HOMOLOG-2026_09",
                          filter_params=FILTERS, software=SOFTWARE,
                          model_config=MODEL, submitted_by="analyst.zhao",
                          request_id="req-run-v1")
    run_id = submit["run_id"]
    outputs = [
        {"ref": "ranking.csv", "kind": "ranking", "checksum": "sha256:" + sha("ranking-v1")},
        {"ref": "correspondences.json", "kind": "correspondence",
         "checksum": "sha256:" + sha("correspondences-v1")},
    ]
    commit_complete_approve(p, run_id, outputs)

    # 5. 发布结论，跨物种对应关系必须引用具体运行产出与软件配置
    correspondence = {
        "correspondence_id": "C-MGL-001",
        "functional_description": "人小胶质细胞与斑马鱼小胶质样细胞的免疫吞噬功能对应",
        "link": {
            "left": {"run_id": run_id, "output_ref": "correspondences.json",
                     "software_cited": ["scanpy@1.10.2", "orthomap@0.4.1"],
                     "species": "Homo sapiens", "entity": "Homo sapiens microglia"},
            "right": {"run_id": run_id, "output_ref": "ranking.csv",
                      "software_cited": ["scanpy@1.10.2", "orthomap@0.4.1"],
                      "species": "Danio rerio", "entity": "Danio rerio microglia-like"},
        },
        "similarity": {"score": 0.71, "rank": 3, "method": "embedding-cosine"},
    }
    p.release_claim(
        claim_id="CLAIM-MGL-2026-01",
        title="人与斑马鱼小胶质细胞吞噬功能对应",
        statement="在锁定注释与统一过滤阈值下，两物种细胞嵌入相似性排名支持吞噬功能对应。",
        run_ids=[run_id], correspondences=[correspondence],
        uncertainty={
            "level": "medium",
            "statement": "相似性排名对注释版本与过滤阈值敏感，跨物种同源性仍为计算推断。",
            "basis": "三种阈值下排名前 5 中有 3 个稳定",
        },
        released_by="pi.zhou", visibility="public",
    )
    p.publish_assets("CLAIM-MGL-2026-01", [
        {"asset_id": "fig1-similarity", "kind": "figure",
         "title": "图1 跨物种相似性排名",
         "source_run_id": run_id, "source_output_ref": "ranking.csv"},
        {"asset_id": "tab1-correspondence", "kind": "table",
         "title": "表1 功能对应明细",
         "source_run_id": run_id, "source_output_ref": "correspondences.json"},
    ])

    # 6. 质量复核撤回斑马鱼批次：仅受影响运行失效，历史结论保留并标注限制
    p.revoke_batch("B-ZFISH", revoked_by="qc.lead", reason="发现样本标签错误")

    # 7. 用替换批次重建受影响链路
    fixed_files = files("z2", {"z-rna-101": "zebrafish-relabeled-clean"})
    register_and_verify(p, "B-ZFISH-FIX", "Danio rerio", "脑组织小胶质样细胞",
                        "发育生物学实验室B", fixed_files)
    rebuilt = p.rerun_invalidated(old_run_id=run_id, submitted_by="analyst.zhao",
                                  request_id="req-run-v2",
                                  batch_replacements={"B-ZFISH": "B-ZFISH-FIX"})
    new_run_id = rebuilt["run_id"]
    commit_complete_approve(p, new_run_id, outputs, reviewer="reviewer.ma")
    correspondence_v2 = json.loads(json.dumps(correspondence))
    correspondence_v2["correspondence_id"] = "C-MGL-002"
    for side in correspondence_v2["link"].values():
        side["run_id"] = new_run_id
    p.release_claim(
        claim_id="CLAIM-MGL-2026-02",
        title="人与斑马鱼小胶质细胞吞噬功能对应（标签修正重建版）",
        statement="以重新标记的斑马鱼批次重建后，吞噬功能对应仍然成立。",
        run_ids=[new_run_id], correspondences=[correspondence_v2],
        uncertainty={
            "level": "low",
            "statement": "标签修正后两批次结果一致；跨物种同源性仍属计算推断。",
            "basis": "修正批次与原批次排名前 5 重合 4 个",
        },
        released_by="pi.zhou", visibility="public",
        supersedes=["CLAIM-MGL-2026-01"],
    )

    # 8. 落盘事件日志
    out = ROOT / "data" / "sample_flow.json"
    out.write_text(json.dumps(p.store.export(), ensure_ascii=False, indent=2), "utf-8")

    # 9. 三种角色视图 + 溯源摘要
    public_view = project_claim(p, "CLAIM-MGL-2026-02", "public")
    trace = trace_claim(p, "CLAIM-MGL-2026-02")
    old_public = project_claim(p, "CLAIM-MGL-2026-01", "public")

    print(f"事件总数: {len(p.store.export())}（已写入 {out.relative_to(ROOT)}）")
    print(f"运行: {run_id} 已失效={p.runs[run_id].invalidated is not None}；"
          f"重建运行={new_run_id}")
    print(f"公众可见字段: {sorted(public_view.keys())}")
    print(f"旧结论限制标注: {old_public['restriction']['reason']}")
    print(f"溯源链步骤: {[s['step'] for s in trace['evidence_chain']]}")
    print(f"原始文件数: {sum(len(s['files']) for s in trace['evidence_chain'] if s['step'] == 'raw_files')}")
    print(f"诊断禁令: {public_view['diagnostics_disclaimer']}")


if __name__ == "__main__":
    main()
