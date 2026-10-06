"""分角色视图与结论溯源。

三个角色看到的字段范围不同（显式白名单，而非“先全量再隐藏”）：
- public：结论正文、不确定性、功能对应摘要、资产校验和、限制标注，
  不含批次、授权条款、过滤阈值、软件配置与人员姓名；
- researcher：可复现科研字段（文件校验和、注释版本、过滤参数、
  软件与模型配置、产出引用、审批结论）；
- reviewer：在 researcher 基础上增加审计字段（授权限制、校验明细、
  审批检查项、撤回与失效原因）。

溯源图显式区分证据性质：observed（真实观测/人工复核）与
computed（计算推断），避免把相关性当作事实或个体诊断。
"""

from __future__ import annotations

from typing import Any

from .pipeline import (
    ANNOTATION,
    BATCH,
    CLAIM,
    RUN,
    Batch,
    ClaimRecord,
    EvidencePipeline,
    PipelineError,
    Run,
)

ROLE_RANK = {"public": 0, "researcher": 1, "reviewer": 2}
CLAIM_RANK = {"public": 0, "researcher": 1, "reviewer": 2}

DIAGNOSTICS_DISCLAIMER = (
    "本结论来自跨物种相关性计算，不构成、也不得自动转换为任何个体诊断依据。"
)


def _events_by_aggregate(pipeline: EvidencePipeline, aggregate_type: str, aggregate_id: str):
    return pipeline.store.stream(aggregate_type, aggregate_id)


def _event_time(pipeline: EvidencePipeline, aggregate_type: str, aggregate_id: str, event_type: str) -> str | None:
    for event in reversed(_events_by_aggregate(pipeline, aggregate_type, aggregate_id)):
        if event["event_type"] == event_type:
            return event["occurred_at"]
    return None


def _source_refs(pipeline: EvidencePipeline, aggregate_type: str, aggregate_id: str) -> list[dict[str, Any]]:
    return [
        {
            "event_id": e["event_id"],
            "event_type": e["event_type"],
            "version": e["version"],
            "occurred_at": e["occurred_at"],
        }
        for e in _events_by_aggregate(pipeline, aggregate_type, aggregate_id)
    ]


def _can_view(role: str, claim: ClaimRecord) -> None:
    if ROLE_RANK[role] < CLAIM_RANK[claim.visibility]:
        raise PipelineError(
            "access_denied",
            f"角色 {role} 无权查看可见范围为 {claim.visibility} 的结论",
        )


# ------------------------------------------------------------ 批处理投影


def _batch_public(batch: Batch) -> dict[str, Any]:
    return {
        "batch_id": batch.batch_id,
        "species": batch.species,
        "cell_source": batch.cell_source,
        "status": batch.status,
    }


def _batch_researcher(batch: Batch) -> dict[str, Any]:
    return {
        **_batch_public(batch),
        "collection_lab": batch.collection_lab,
        "registered_by": batch.registered_by,
        "consent": {
            "allowed_purposes": sorted(batch.consent.allowed_purposes),
            "individual_diagnosis_allowed": False,
        },
        "files": [
            {
                "file_id": f.file_id,
                "name": f.name,
                "size": f.size,
                "sha256": f.sha256,
            }
            for f in sorted(batch.files.values(), key=lambda x: x.file_id)
        ],
        "files_verified": batch.status in ("verified", "revoked"),
    }


def _batch_reviewer(pipeline: EvidencePipeline, batch: Batch) -> dict[str, Any]:
    data = _batch_researcher(batch)
    data["registered_by"] = batch.registered_by
    data["consent"] = {
        "allowed_purposes": sorted(batch.consent.allowed_purposes),
        "public_release": batch.consent.public_release,
        "restrictions": batch.consent.restrictions,
        "individual_diagnosis_allowed": False,
    }
    data["verification"] = {
        "verified_by": batch.verified_by,
        "files": sorted(batch.verification.values(), key=lambda x: x["file_id"]),
    }
    if batch.revoke is not None:
        data["revoke"] = batch.revoke
    data["source_events"] = _source_refs(pipeline, BATCH, batch.batch_id)
    return data


# ------------------------------------------------------------ 运行投影


def _run_researcher(run: Run, pipeline: EvidencePipeline) -> dict[str, Any]:
    annotation = pipeline.annotations[run.spec["annotation"]["annotation_id"]]
    return {
        "run_id": run.run_id,
        "status": run.status,
        "current": run.is_current,
        "label": run.label,
        "run_fingerprint": run.fingerprint,
        "evidence_nature": "computed",
        "batch_ids": list(run.batch_ids),
        "spec": {
            "input_files": run.spec["input_files"],
            "annotation": {
                "annotation_id": annotation.annotation_id,
                "source": annotation.source,
                "version": annotation.version,
                "ontology": annotation.ontology,
                "locked_by": annotation.locked_by,
            },
            "filter_params": run.spec["filter_params"],
            "software": run.spec["software"],
            "model_config": run.spec["model_config"],
        },
        "shards": [
            {
                "shard_id": s.shard_id,
                "item_count": s.item_count,
                "fingerprint": s.fingerprint,
            }
            for s in sorted(run.shards.values(), key=lambda x: x.shard_id)
        ],
        "total_items": run.total_items,
        "outputs": list(run.outputs),
        "approved": run.approval is not None,
        "approved_by": run.approval["reviewer"] if run.approval else None,
        "approved_at": run.approval["occurred_at"] if run.approval else None,
        "submitted_by": run.submitted_by,
    }


def _run_reviewer(run: Run, pipeline: EvidencePipeline) -> dict[str, Any]:
    data = _run_researcher(run, pipeline)
    if run.approval is not None:
        data["approval"] = run.approval
    if run.invalidated is not None:
        data["invalidated"] = run.invalidated
    data["source_events"] = _source_refs(pipeline, RUN, run.run_id)
    return data


# ------------------------------------------------------------ 结论视图


def _correspondence_public(
    pipeline: EvidencePipeline, correspondence: dict[str, Any]
) -> dict[str, Any]:
    def species_of(endpoint_key: str) -> list[str]:
        endpoint = correspondence["link"][endpoint_key]
        if endpoint.get("species"):
            return [endpoint["species"]]
        rid = endpoint["run_id"]
        run = pipeline.runs.get(rid)
        if run is None:
            return []
        return sorted({pipeline.batches[b].species for b in run.batch_ids})

    return {
        "correspondence_id": correspondence["correspondence_id"],
        "functional_description": correspondence["functional_description"],
        "evidence_nature": "computed_correspondence",
        "species": {
            "left": species_of("left"),
            "right": species_of("right"),
        },
        "similarity": correspondence.get("similarity", {}),
    }


def _correspondence_full(correspondence: dict[str, Any]) -> dict[str, Any]:
    return {
        **correspondence,
        "evidence_nature": "computed_correspondence",
    }


def project_claim(pipeline: EvidencePipeline, claim_id: str, role: str) -> dict[str, Any]:
    """按角色返回结论视图。字段采用显式白名单。"""
    if role not in ROLE_RANK:
        raise PipelineError("unknown_role", f"未知角色: {role}")
    claim = pipeline.claims.get(claim_id)
    if claim is None:
        raise PipelineError("claim_missing", f"结论 {claim_id} 不存在")
    _can_view(role, claim)
    released_at = _event_time(pipeline, CLAIM, claim_id, "CLAIM_RELEASED")

    base = {
        "claim_id": claim.claim_id,
        "title": claim.title,
        "statement": claim.statement,
        "visibility": claim.visibility,
        "released_at": released_at,
        "uncertainty": {
            "level": claim.uncertainty["level"],
            "statement": claim.uncertainty["statement"],
        },
        "individual_diagnosis_allowed": False,
        "diagnostics_disclaimer": DIAGNOSTICS_DISCLAIMER,
    }
    if claim.flag is not None:
        base["restriction"] = {
            "restricted": True,
            "reason": claim.flag["reason"],
            "history_retained": True,
        }
    if claim.superseded_by:
        base["superseded_by"] = claim.superseded_by

    if role == "public":
        base["peer_review"] = {
            "approved": bool(claim.approved_by),
            "reviewer_count": len(claim.approved_by),
        }
        base["correspondences"] = [
            _correspondence_public(pipeline, c) for c in claim.correspondences
        ]
        base["assets"] = [
            {
                "asset_id": a["asset_id"],
                "kind": a["kind"],
                "title": a["title"],
                "checksum": a["checksum"],
            }
            for a in sorted(claim.assets.values(), key=lambda x: x["asset_id"])
        ]
        return base

    # researcher / reviewer
    base["uncertainty"]["basis"] = claim.uncertainty.get("basis", "")
    base["approved_by"] = list(claim.approved_by)
    base["released_by"] = claim.released_by
    base["correspondences"] = [_correspondence_full(c) for c in claim.correspondences]
    base["assets"] = [dict(a) for a in sorted(claim.assets.values(), key=lambda x: x["asset_id"])]

    runs: dict[str, Any] = {}
    batches: dict[str, Any] = {}
    for rid in claim.run_ids:
        run = pipeline.runs[rid]
        runs[rid] = _run_researcher(run, pipeline)
        for bid in run.batch_ids:
            batch = pipeline.batches[bid]
            batches[bid] = _batch_researcher(batch)
    base["runs"] = runs
    base["batches"] = batches

    if role == "reviewer":
        for rid, run in ((rid, pipeline.runs[rid]) for rid in claim.run_ids):
            runs[rid] = _run_reviewer(run, pipeline)
        for bid in list(batches):
            batches[bid] = _batch_reviewer(pipeline, pipeline.batches[bid])
        if claim.flag is not None:
            base["restriction"]["detail"] = claim.flag["detail"]
            base["restriction"]["flagged_at"] = claim.flag["occurred_at"]
        base["source_events"] = _source_refs(pipeline, CLAIM, claim.claim_id)
    return base


# ------------------------------------------------------------ 溯源图


def trace_claim(pipeline: EvidencePipeline, claim_id: str) -> dict[str, Any]:
    """从一条结论追溯到原始文件、版本、审批人与不确定性说明。"""
    claim = pipeline.claims.get(claim_id)
    if claim is None:
        raise PipelineError("claim_missing", f"结论 {claim_id} 不存在")

    batches: dict[str, Any] = {}
    runs: dict[str, Any] = {}
    annotations: dict[str, Any] = {}

    for rid in claim.run_ids:
        run = pipeline.runs[rid]
        runs[rid] = _run_reviewer(run, pipeline)
        annotation_id = run.spec["annotation"]["annotation_id"]
        if annotation_id not in annotations:
            ann = pipeline.annotations[annotation_id]
            annotations[annotation_id] = {
                "annotation_id": ann.annotation_id,
                "source": ann.source,
                "version": ann.version,
                "ontology": ann.ontology,
                "locked_by": ann.locked_by,
                "evidence_nature": "reference",
                "source_events": _source_refs(pipeline, ANNOTATION, annotation_id),
            }
        for bid in run.batch_ids:
            batch = pipeline.batches[bid]
            if bid in batches:
                continue
            batches[bid] = {
                **_batch_reviewer(pipeline, batch),
                "evidence_nature": "observed_raw_file",
            }

    chain: list[dict[str, Any]] = []
    for rid in claim.run_ids:
        run = pipeline.runs[rid]
        chain.append(
            {
                "step": "run",
                "run_id": rid,
                "status": run.status,
                "current": run.is_current,
                "invalidated": run.invalidated,
                "evidence_nature": "computed",
            }
        )
        for bid in run.batch_ids:
            chain.append(
                {
                    "step": "raw_files",
                    "run_id": rid,
                    "batch_id": bid,
                    "evidence_nature": "observed_raw_file",
                    "files": [
                        {
                            "file_id": f.file_id,
                            "sha256": f.sha256,
                            "verified_match": (
                                f.file_id in pipeline.batches[bid].verification
                            ),
                            "evidence_nature": "observed_raw_file",
                        }
                        for f in sorted(pipeline.batches[bid].files.values(), key=lambda x: x.file_id)
                    ],
                }
            )
        if run.approval is not None:
            chain.append(
                {
                    "step": "manual_review",
                    "run_id": rid,
                    "reviewer": run.approval["reviewer"],
                    "checklist": run.approval["checklist"],
                    "occurred_at": run.approval["occurred_at"],
                    "evidence_nature": "manual_review",
                }
            )

    return {
        "claim": {
            "claim_id": claim.claim_id,
            "title": claim.title,
            "statement": claim.statement,
            "visibility": claim.visibility,
            "released_at": _event_time(pipeline, CLAIM, claim_id, "CLAIM_RELEASED"),
            "released_by": claim.released_by,
            "uncertainty": dict(claim.uncertainty),
            "restriction": claim.flag,
            "superseded_by": claim.superseded_by,
            "individual_diagnosis_allowed": False,
            "diagnostics_disclaimer": DIAGNOSTICS_DISCLAIMER,
            "source_events": _source_refs(pipeline, CLAIM, claim.claim_id),
        },
        "evidence_chain": chain,
        "runs": runs,
        "batches": batches,
        "annotations": annotations,
        "correspondences": [_correspondence_full(c) for c in claim.correspondences],
        "assets": [dict(a) for a in sorted(claim.assets.values(), key=lambda x: x["asset_id"])],
        "legend": {
            "observed_raw_file": "测序文件的登记校验和与人工比对结果，属真实观测",
            "manual_review": "人工复核结论，属真实观测",
            "computed": "软件运行产出，属计算推断",
            "computed_correspondence": "跨物种对应关系与相似性排名，属计算推断",
            "reference": "锁定的功能注释版本，属外部参考",
        },
    }
