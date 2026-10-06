"""测试共享构造器：搭建七选二物种的标准研究场景。"""

from __future__ import annotations

import hashlib
import json
import sys
from datetime import datetime, timezone, timedelta
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from cross_species_research.events import EventStore
from cross_species_research.pipeline import EvidencePipeline, RESEARCH_PURPOSE

TZ = timezone(timedelta(hours=8))


def sha256_text(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def fixed_clock(marker: str = "2026-10-06T09:00:00+08:00"):
    current = {"value": datetime.fromisoformat(marker)}

    def clock() -> datetime:
        return current["value"]

    def advance(minutes: int = 1) -> None:
        current["value"] += timedelta(minutes=minutes)

    clock.advance = advance  # type: ignore[attr-defined]
    return clock


def load_schema() -> dict:
    return json.loads((ROOT / "contracts" / "domain.schema.json").read_text(encoding="utf-8"))


def file_entry(file_id: str, text: str, name: str | None = None) -> dict:
    return {
        "file_id": file_id,
        "name": name or f"{file_id}.h5",
        "size": len(text.encode("utf-8")),
        "sha256": sha256_text(text),
    }


def file_hashes(files: list[dict]) -> dict[str, str]:
    return {f["file_id"]: f["sha256"] for f in files}


def human_batch(public: bool = True) -> tuple[str, list[dict]]:
    files = [
        file_entry("h-rna-001", "human-microglia-rna-0001", "human_microglia_r1.h5"),
        file_entry("h-rna-002", "human-microglia-rna-0002", "human_microglia_r2.h5"),
    ]
    return "B-HUMAN", files


def fish_batch(public: bool = True) -> tuple[str, list[dict]]:
    files = [
        file_entry("z-rna-001", "zebrafish-microglia-like-rna-0001", "zf_mgl_r1.h5"),
    ]
    return "B-ZFISH", files


def consent(public: bool = True) -> dict:
    return {
        "allowed_purposes": [RESEARCH_PURPOSE],
        "public_release": public,
        "restrictions": "仅限学术研究" if public else "禁止公开发布",
    }


SOFTWARE = [
    {
        "name": "scanpy",
        "version": "1.10.2",
        "config": {"n_top_genes": 2000, "resolution": 0.8},
    },
    {
        "name": "orthomap",
        "version": "0.4.1",
        "config": {"aligner": "embedding-cosine", "seed": 7},
    },
]

FILTERS = {
    "min_genes": 200,
    "max_mito_pct": 10.0,
    "min_cells": 3,
    "doublet_threshold": 0.25,
}

MODEL = {"name": "cross-species-embedder", "version": "3.1", "seed": 7}


def new_pipeline(*, asset_root: str | None = None, clock=None) -> EvidencePipeline:
    return EvidencePipeline(
        EventStore(),
        schema=load_schema(),
        clock=clock or fixed_clock(),
        asset_root=asset_root,
    )


def register_verified(pipeline: EvidencePipeline, batch_id: str, species: str,
                      source: str, files: list[dict], *, public: bool = True,
                      purposes: list[str] | None = None) -> None:
    c = consent(public)
    if purposes is not None:
        c["allowed_purposes"] = purposes
    pipeline.register_batch(
        batch_id=batch_id,
        species=species,
        cell_source=source,
        collection_lab=f"lab-{batch_id.lower()}",
        files=files,
        consent=c,
        registered_by="curator.li",
    )
    pipeline.verify_files(batch_id, file_hashes(files), verified_by="qc.wang")


def standard_pipeline(*, second_batch: bool = True, asset_root: str | None = None) -> EvidencePipeline:
    p = new_pipeline(asset_root=asset_root)
    hid, hfiles = human_batch()
    register_verified(p, hid, "Homo sapiens", "成年大脑小胶质细胞", hfiles)
    if second_batch:
        zid, zfiles = fish_batch()
        register_verified(p, zid, "Danio rerio", "脑组织小胶质样细胞", zfiles)
    p.lock_annotation(
        annotation_id="ANN-HOMOLOG-2026_09",
        source="HomoloGene+CellOntology",
        version="2026-09",
        ontology="CL:0000129",
        locked_by="annotator.chen",
    )
    return p


def submit_completed_run(
    pipeline: EvidencePipeline,
    *,
    run_label: str = "microglia-match",
    batch_ids: list[str] | None = None,
    annotation_id: str = "ANN-HOMOLOG-2026_09",
    filters: dict | None = None,
    software: list[dict] | None = None,
    model: dict | None = None,
    request_id: str = "req-run-1",
) -> str:
    batch_ids = batch_ids or ["B-HUMAN", "B-ZFISH"]
    result = pipeline.submit_run(
        label=run_label,
        batch_ids=batch_ids,
        annotation_id=annotation_id,
        filter_params=filters or FILTERS,
        software=software or SOFTWARE,
        model_config=model or MODEL,
        submitted_by="analyst.zhao",
        request_id=request_id,
    )
    run_id = result["run_id"]
    pipeline.commit_shard(
        run_id,
        shard_id="shard-1",
        item_count=1200,
        payload={"rows": list(range(1200))},
        committed_by="worker-a",
        request_id=f"req-{run_id}-shard-1",
    )
    pipeline.commit_shard(
        run_id,
        shard_id="shard-2",
        item_count=800,
        payload={"rows": list(range(800))},
        committed_by="worker-b",
        request_id=f"req-{run_id}-shard-2",
    )
    pipeline.complete_run(
        run_id,
        outputs=[
            {"ref": "ranking.csv", "kind": "ranking", "checksum": "sha256:" + sha256_text("ranking-v1")},
            {"ref": "correspondences.json", "kind": "correspondence",
             "checksum": "sha256:" + sha256_text("correspondences-v1")},
        ],
    )
    return run_id


def approve(pipeline: EvidencePipeline, run_id: str, reviewer: str = "reviewer.sun") -> None:
    pipeline.approve_run(
        run_id,
        reviewer=reviewer,
        checklist=[
            "批次与授权核验",
            "文件校验和一致",
            "过滤参数可复现",
            "结果与原始数据抽查一致",
        ],
        notes="未见批次效应主导相似性",
    )


def correspondence(run_id: str = "microglia-match") -> dict:
    return {
        "correspondence_id": "C-MGL-001",
        "functional_description": "人小胶质细胞与斑马鱼小胶质样细胞的免疫吞噬功能对应",
        "link": {
            "left": {
                "run_id": run_id,
                "output_ref": "correspondences.json",
                "software_cited": ["scanpy@1.10.2", "orthomap@0.4.1"],
                "species": "Homo sapiens",
                "entity": "Homo sapiens microglia",
            },
            "right": {
                "run_id": run_id,
                "output_ref": "ranking.csv",
                "software_cited": ["scanpy@1.10.2", "orthomap@0.4.1"],
                "species": "Danio rerio",
                "entity": "Danio rerio microglia-like",
            },
        },
        "similarity": {"score": 0.71, "rank": 3, "method": "embedding-cosine"},
    }


UNCERTAINTY = {
    "level": "medium",
    "statement": "相似性排名对注释版本与过滤阈值敏感，跨物种同源性仍为计算推断。",
    "basis": "三种阈值下排名前 5 中有 3 个稳定",
}


def release_claim(
    pipeline: EvidencePipeline,
    *,
    claim_id: str = "CLAIM-MGL-2026-01",
    run_id: str = "microglia-match",
    visibility: str = "public",
    with_correspondence: bool = True,
) -> dict:
    return pipeline.release_claim(
        claim_id=claim_id,
        title="人与斑马鱼小胶质细胞吞噬功能对应",
        statement="在锁定注释与统一过滤阈值下，两物种细胞嵌入相似性排名支持吞噬功能对应。",
        run_ids=[run_id],
        correspondences=[correspondence(run_id)] if with_correspondence else [],
        uncertainty=dict(UNCERTAINTY),
        released_by="pi.zhou",
        visibility=visibility,
    )


ASSETS = [
    {
        "asset_id": "fig1-similarity",
        "kind": "figure",
        "title": "图1 跨物种相似性排名",
        "source_run_id": "microglia-match",
        "source_output_ref": "ranking.csv",
    },
    {
        "asset_id": "tab1-correspondence",
        "kind": "table",
        "title": "表1 功能对应明细",
        "source_run_id": "microglia-match",
        "source_output_ref": "correspondences.json",
    },
]
