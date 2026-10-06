"""远缘细胞研究证据流水核心应用。

所有业务状态都由领域事件回放得到：进程重启后只需重新加载事件日志，
读模型即可重建。命令方法只做三件事——校验、计算指纹、追加事件。

关键规则：
- 测序文件校验和全部匹配才能进入分析；
- 运行提交指纹 = 输入文件校验和 + 注释版本 + 过滤参数 + 软件配置，
  同指纹复用运行，同标识不同内容自动隔离为新运行；
- 分片按 shard_id 幂等，重试不重复计数；
- 复核撤回批次后，仅消费该批次的运行被失效、相关结论被标注限制，
  历史事件与已发布资产保留不删；
- 结论发布必须通过审批、授权与引用完整性门禁，并强制附带
  “相关性不得用于个体诊断”声明。
"""

from __future__ import annotations

import hashlib
import os
from collections.abc import Callable, Sequence
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

from .contracts import validate_event
from .events import EventStore, canonical_json, content_fingerprint

RESEARCH_PURPOSE = "cross_species_correspondence"

BATCH = "specimen_batch"
ANNOTATION = "annotation_revision"
RUN = "analysis_run"
CLAIM = "research_claim"

# 相关性分析在制度上永远不构成个体诊断依据，无论授权条款如何书写。
INDIVIDUAL_DIAGNOSIS_ALLOWED = False

UNCERTAINTY_LEVELS = ("low", "medium", "high")
ASSET_KINDS = ("figure", "table", "ranking", "correspondence", "dataset")


class PipelineError(Exception):
    """业务规则冲突。code 供接入方按类处理。"""

    def __init__(self, code: str, message: str) -> None:
        super().__init__(f"[{code}] {message}")
        self.code = code
        self.message = message


# ---------------------------------------------------------------- 读模型


@dataclass(frozen=True)
class SequencedFile:
    file_id: str
    name: str
    size: int
    sha256: str


@dataclass(frozen=True)
class Consent:
    allowed_purposes: frozenset[str]
    public_release: bool
    restrictions: str = ""


@dataclass
class Batch:
    batch_id: str
    species: str
    cell_source: str
    collection_lab: str
    consent: Consent
    files: dict[str, SequencedFile]
    registered_by: str
    status: str = "registered"  # registered -> verified -> revoked
    verification: dict[str, dict[str, Any]] = field(default_factory=dict)
    verified_by: str | None = None
    revoke: dict[str, Any] | None = None


@dataclass
class Annotation:
    annotation_id: str
    source: str
    version: str
    ontology: str
    locked_by: str


@dataclass(frozen=True)
class Shard:
    shard_id: str
    item_count: int
    fingerprint: str
    committed_by: str
    occurred_at: str


@dataclass
class Run:
    run_id: str
    label: str
    batch_ids: list[str]
    fingerprint: str
    spec: dict[str, Any]
    submitted_by: str
    request_id: str
    status: str = "started"  # started -> completed
    shards: dict[str, Shard] = field(default_factory=dict)
    total_items: int = 0
    outputs: list[dict[str, Any]] = field(default_factory=list)
    approval: dict[str, Any] | None = None
    invalidated: dict[str, Any] | None = None

    @property
    def is_current(self) -> bool:
        return self.invalidated is None


@dataclass
class ClaimRecord:
    claim_id: str
    title: str
    statement: str
    visibility: str
    run_ids: list[str]
    correspondences: list[dict[str, Any]]
    uncertainty: dict[str, Any]
    approved_by: list[str]
    released_by: str
    assets: dict[str, dict[str, Any]] = field(default_factory=dict)
    flag: dict[str, Any] | None = None
    superseded_by: str | None = None


# ---------------------------------------------------------------- 应用


class EvidencePipeline:
    def __init__(
        self,
        store: EventStore | None = None,
        *,
        schema: dict[str, Any] | None = None,
        clock: Callable[[], datetime] | None = None,
        id_prefix: str = "evt",
        asset_root: str | None = None,
    ) -> None:
        self.store = store or EventStore()
        self.schema = schema
        self._clock = clock or (lambda: datetime.now(timezone.utc))
        self._id_prefix = id_prefix
        self._counter = 0
        self.asset_root = asset_root
        self.batches: dict[str, Batch] = {}
        self.annotations: dict[str, Annotation] = {}
        self.runs: dict[str, Run] = {}
        self.claims: dict[str, ClaimRecord] = {}
        self._run_by_fingerprint: dict[str, str] = {}
        self._labels: dict[str, int] = {}
        self._rebuild()

    # ------------------------------------------------------------ 回放

    def _rebuild(self) -> None:
        self.batches.clear()
        self.annotations.clear()
        self.runs.clear()
        self.claims.clear()
        self._run_by_fingerprint.clear()
        self._labels.clear()
        for event in self.store.all_events():
            self._apply(event)

    def _apply(self, event: dict[str, Any]) -> None:
        kind = event["event_type"]
        atype = event["aggregate_type"]
        if atype == BATCH:
            self._apply_batch_event(kind, event)
        elif atype == ANNOTATION:
            if kind == "ANNOTATION_LOCKED":
                self.annotations[event["aggregate_id"]] = Annotation(
                    annotation_id=event["annotation_id"],
                    source=event["source"],
                    version=event["annotation_version"],
                    ontology=event["ontology"],
                    locked_by=event["locked_by"],
                )
        elif atype == RUN:
            self._apply_run_event(kind, event)
        elif atype == CLAIM:
            self._apply_claim_event(kind, event)

    def _apply_batch_event(self, kind: str, event: dict[str, Any]) -> None:
        bid = event["aggregate_id"]
        if kind == "BATCH_REGISTERED":
            consent = event["consent"]
            self.batches[bid] = Batch(
                batch_id=bid,
                species=event["species"],
                cell_source=event["cell_source"],
                collection_lab=event["collection_lab"],
                consent=Consent(
                    allowed_purposes=frozenset(consent["allowed_purposes"]),
                    public_release=bool(consent["public_release"]),
                    restrictions=consent.get("restrictions", ""),
                ),
                files={
                    f["file_id"]: SequencedFile(
                        file_id=f["file_id"],
                        name=f["name"],
                        size=f["size"],
                        sha256=f["sha256"],
                    )
                    for f in event["files"]
                },
                registered_by=event["registered_by"],
            )
        elif kind == "FILES_VERIFIED":
            batch = self.batches[bid]
            batch.status = "verified"
            batch.verified_by = event["verified_by"]
            batch.verification = {f["file_id"]: dict(f) for f in event["files"]}
        elif kind == "REVIEW_REVOKED":
            self.batches[bid].status = "revoked"
            self.batches[bid].revoke = {
                "revoked_by": event["revoked_by"],
                "reason": event["reason"],
                "occurred_at": event["occurred_at"],
            }

    def _apply_run_event(self, kind: str, event: dict[str, Any]) -> None:
        rid = event["aggregate_id"]
        if kind == "ANALYSIS_STARTED":
            run = Run(
                run_id=rid,
                label=event["label"],
                batch_ids=list(event["batch_ids"]),
                fingerprint=event["run_fingerprint"],
                spec=event["spec"],
                submitted_by=event["submitted_by"],
                request_id=event["submitted_request_id"],
            )
            self.runs[rid] = run
            self._labels.setdefault(event["label"], 0)
            self._labels[event["label"]] += 1
            if run.invalidated is None:
                self._run_by_fingerprint[run.fingerprint] = rid
        elif kind == "SHARD_COMMITTED":
            shard = Shard(
                shard_id=event["shard_id"],
                item_count=event["item_count"],
                fingerprint=event["shard_fingerprint"],
                committed_by=event["committed_by"],
                occurred_at=event["occurred_at"],
            )
            self.runs[rid].shards[shard.shard_id] = shard
        elif kind == "ANALYSIS_COMPLETED":
            run = self.runs[rid]
            run.status = "completed"
            run.total_items = event["total_items"]
            run.outputs = list(event["outputs"])
        elif kind == "REVIEW_APPROVED":
            self.runs[rid].approval = {
                "reviewer": event["reviewer"],
                "checklist": list(event["checklist"]),
                "notes": event.get("notes", ""),
                "occurred_at": event["occurred_at"],
            }
        elif kind == "REVIEW_REVOKED":
            self.runs[rid].approval = None
            self.runs[rid].invalidated = {
                "cause": "review_revoked",
                "revoked_by": event["revoked_by"],
                "reason": event["reason"],
                "occurred_at": event["occurred_at"],
            }
            self._run_by_fingerprint.pop(self.runs[rid].fingerprint, None)
        elif kind == "RUN_INVALIDATED":
            run = self.runs[rid]
            run.invalidated = {
                "cause": event["cause"],
                "detail": event.get("detail", {}),
                "occurred_at": event["occurred_at"],
            }
            self._run_by_fingerprint.pop(run.fingerprint, None)

    def _apply_claim_event(self, kind: str, event: dict[str, Any]) -> None:
        cid = event["aggregate_id"]
        if kind == "CLAIM_RELEASED":
            self.claims[cid] = ClaimRecord(
                claim_id=cid,
                title=event["title"],
                statement=event["statement"],
                visibility=event["visibility"],
                run_ids=list(event["evidence"]["run_ids"]),
                correspondences=list(event["evidence"].get("correspondences", [])),
                uncertainty=dict(event["uncertainty"]),
                approved_by=list(event["approved_by"]),
                released_by=event["released_by"],
            )
        elif kind == "CLAIM_ASSET_PUBLISHED":
            self.claims[cid].assets[event["asset_id"]] = {
                "asset_id": event["asset_id"],
                "kind": event["kind"],
                "title": event.get("title", ""),
                "path": event["path"],
                "checksum": event["checksum"],
                "size": event["size"],
                "source_run_id": event["source_run_id"],
                "source_output_ref": event["source_output_ref"],
                "occurred_at": event["occurred_at"],
            }
        elif kind == "CLAIM_FLAGGED":
            claim = self.claims[cid]
            claim.flag = {
                "reason": event["reason"],
                "detail": event.get("detail", {}),
                "restricted": bool(event.get("restricted", True)),
                "history_retained": bool(event.get("history_retained", True)),
                "occurred_at": event["occurred_at"],
            }
            if event.get("superseded_by"):
                claim.superseded_by = event["superseded_by"]

    # ------------------------------------------------------------ 追加

    def _next_event_id(self) -> str:
        self._counter += 1
        return f"{self._id_prefix}-{self._counter:05d}"

    def _append(
        self,
        *,
        event_type: str,
        aggregate_type: str,
        aggregate_id: str,
        summary: str,
        payload: dict[str, Any],
        expected_version: int | None = None,
        request_scope: tuple[str, str] | None = None,
        request_result: Any = None,
    ) -> dict[str, Any]:
        event = {
            "event_id": self._next_event_id(),
            "event_type": event_type,
            "aggregate_type": aggregate_type,
            "aggregate_id": aggregate_id,
            "occurred_at": self._now(),
            "version": self.store.version_of(aggregate_type, aggregate_id) + 1,
            "summary": summary,
            **payload,
        }
        if self.schema is not None:
            issues = validate_event(event, self.schema)
            if issues:  # pragma: no cover - 防御性：命令层应已保证
                raise PipelineError(
                    "contract_violation",
                    "；".join(f"{i.field}:{i.code}" for i in issues),
                )
        stored = self.store.append(event, expected_version=expected_version)
        if request_scope is not None:
            scope, request_id = request_scope
            self.store.remember_request(scope, request_id, request_result)
        self._apply(stored)
        return stored

    def _now(self) -> str:
        return self._clock().astimezone(timezone.utc).isoformat()

    # ------------------------------------------------------------ 登记

    def register_batch(
        self,
        *,
        batch_id: str,
        species: str,
        cell_source: str,
        collection_lab: str,
        files: Sequence[dict[str, Any]],
        consent: dict[str, Any],
        registered_by: str,
    ) -> dict[str, Any]:
        if batch_id in self.batches:
            raise PipelineError("batch_exists", f"批次 {batch_id} 已登记")
        if not species.strip() or not cell_source.strip():
            raise PipelineError("field_required", "物种与细胞来源为必填")
        if not files:
            raise PipelineError("files_required", "至少登记一个测序文件")
        normalized_files: list[dict[str, Any]] = []
        seen: set[str] = set()
        for f in files:
            for key in ("file_id", "name", "size", "sha256"):
                if not f.get(key) and f.get("size") != 0:
                    raise PipelineError("file_field_required", f"文件缺少字段 {key}")
            if f["file_id"] in seen:
                raise PipelineError("duplicate_file", f"文件标识重复: {f['file_id']}")
            if not _is_sha256(f["sha256"]):
                raise PipelineError("bad_checksum", f"文件 {f['file_id']} 的 sha256 不合法")
            if not isinstance(f["size"], int) or isinstance(f["size"], bool) or f["size"] < 0:
                raise PipelineError("bad_size", f"文件 {f['file_id']} 的 size 必须是非负整数")
            seen.add(f["file_id"])
            normalized_files.append(
                {
                    "file_id": f["file_id"],
                    "name": f["name"],
                    "size": int(f["size"]),
                    "sha256": f["sha256"].lower(),
                }
            )
        normalized_consent = self._normalize_consent(consent)
        return self._append(
            event_type="BATCH_REGISTERED",
            aggregate_type=BATCH,
            aggregate_id=batch_id,
            summary=f"登记 {species} {cell_source} 批次 {batch_id}（{collection_lab}）",
            payload={
                "species": species,
                "cell_source": cell_source,
                "collection_lab": collection_lab,
                "consent": normalized_consent,
                "files": normalized_files,
                "registered_by": registered_by,
            },
        )

    @staticmethod
    def _normalize_consent(consent: dict[str, Any]) -> dict[str, Any]:
        purposes = consent.get("allowed_purposes")
        if not purposes or not isinstance(purposes, (list, tuple, set, frozenset)):
            raise PipelineError("consent_purposes_required", "授权范围 allowed_purposes 不能为空")
        if consent.get("individual_diagnosis", False):
            raise PipelineError(
                "diagnostic_use_prohibited",
                "相关性研究授权不得登记个体诊断用途",
            )
        return {
            "allowed_purposes": sorted(purposes),
            "public_release": bool(consent.get("public_release", False)),
            "restrictions": str(consent.get("restrictions", "")),
            "individual_diagnosis_allowed": INDIVIDUAL_DIAGNOSIS_ALLOWED,
        }

    def verify_files(
        self,
        batch_id: str,
        observed: dict[str, str],
        *,
        verified_by: str,
    ) -> dict[str, Any]:
        batch = self._require_batch(batch_id)
        if batch.status == "revoked":
            raise PipelineError("batch_revoked", f"批次 {batch_id} 已撤回")
        if batch.status == "verified":
            raise PipelineError("already_verified", f"批次 {batch_id} 已完成校验")
        if set(observed) != set(batch.files):
            missing = sorted(set(batch.files) - set(observed))
            extra = sorted(set(observed) - set(batch.files))
            raise PipelineError(
                "file_set_mismatch",
                f"校验文件集合不一致，缺失 {missing}，多余 {extra}",
            )
        records = []
        mismatches = []
        for file_id, sequenced in sorted(batch.files.items()):
            observed_hash = observed[file_id].lower()
            if not _is_sha256(observed_hash):
                raise PipelineError("bad_checksum", f"观测校验和不合法: {file_id}")
            ok = observed_hash == sequenced.sha256
            records.append(
                {
                    "file_id": file_id,
                    "expected_sha256": sequenced.sha256,
                    "observed_sha256": observed_hash,
                    "match": ok,
                }
            )
            if not ok:
                mismatches.append(file_id)
        if mismatches:
            # 校验失败不落任何“已验证”事实，返回明细供调用方处置。
            raise PipelineError(
                "checksum_mismatch",
                f"批次 {batch_id} 文件校验和不匹配: {', '.join(mismatches)}",
            )
        return self._append(
            event_type="FILES_VERIFIED",
            aggregate_type=BATCH,
            aggregate_id=batch_id,
            summary=f"批次 {batch_id} 的 {len(records)} 个测序文件校验全部通过",
            payload={"files": records, "verified_by": verified_by, "all_match": True},
        )

    # ------------------------------------------------------------ 注释

    def lock_annotation(
        self,
        *,
        annotation_id: str,
        source: str,
        version: str,
        ontology: str,
        locked_by: str,
    ) -> dict[str, Any]:
        if annotation_id in self.annotations:
            raise PipelineError("annotation_exists", f"注释版本 {annotation_id} 已锁定")
        if not source.strip() or not version.strip():
            raise PipelineError("field_required", "注释来源与版本为必填")
        return self._append(
            event_type="ANNOTATION_LOCKED",
            aggregate_type=ANNOTATION,
            aggregate_id=annotation_id,
            summary=f"锁定功能注释 {source}@{version}",
            payload={
                "annotation_id": annotation_id,
                "source": source,
                "annotation_version": version,
                "ontology": ontology,
                "locked_by": locked_by,
            },
        )

    # ------------------------------------------------------------ 运行

    @staticmethod
    def _software_fingerprint(software: Sequence[dict[str, Any]]) -> list[dict[str, Any]]:
        normalized = []
        seen = set()
        for item in software:
            name = item.get("name")
            version_ = item.get("version")
            if not name or not version_:
                raise PipelineError("software_field_required", "软件配置必须包含 name 与 version")
            key = (name, version_)
            if key in seen:
                raise PipelineError("duplicate_software", f"软件重复登记: {name}@{version_}")
            seen.add(key)
            normalized.append(
                {
                    "name": name,
                    "version": str(version_),
                    "config": item.get("config", {}),
                    "config_fingerprint": content_fingerprint(item.get("config", {})),
                }
            )
        return sorted(normalized, key=lambda s: (s["name"], s["version"]))

    def _run_spec(
        self,
        *,
        batch_ids: Sequence[str],
        annotation_id: str,
        filter_params: dict[str, Any],
        software: Sequence[dict[str, Any]],
        model_config: dict[str, Any],
    ) -> dict[str, Any]:
        if not batch_ids:
            raise PipelineError("batch_required", "运行至少引用一个批次")
        input_files: list[dict[str, Any]] = []
        for bid in batch_ids:
            batch = self._require_batch(bid)
            if batch.status == "revoked":
                raise PipelineError("batch_revoked", f"批次 {bid} 已撤回，不能用于分析")
            if batch.status != "verified":
                raise PipelineError("files_unverified", f"批次 {bid} 尚未通过文件校验")
            if RESEARCH_PURPOSE not in batch.consent.allowed_purposes:
                raise PipelineError(
                    "consent_scope_denied",
                    f"批次 {bid} 授权范围不包含 {RESEARCH_PURPOSE}",
                )
            for f in sorted(batch.files.values(), key=lambda x: x.file_id):
                input_files.append(
                    {"batch_id": bid, "file_id": f.file_id, "sha256": f.sha256}
                )
        annotation = self.annotations.get(annotation_id)
        if annotation is None:
            raise PipelineError("annotation_missing", f"注释版本 {annotation_id} 未锁定")
        if not isinstance(filter_params, dict) or not filter_params:
            raise PipelineError("filter_params_required", "过滤参数必须是非空对象")
        software_norm = self._software_fingerprint(software)
        if not software_norm:
            raise PipelineError("software_required", "至少登记一个软件配置")
        if not isinstance(model_config, dict) or not model_config.get("name"):
            raise PipelineError("model_config_required", "模型配置必须包含 name")
        return {
            "batch_ids": sorted(batch_ids),
            "input_files": input_files,
            "annotation": {
                "annotation_id": annotation_id,
                "source": annotation.source,
                "version": annotation.version,
                "ontology": annotation.ontology,
            },
            "filter_params": filter_params,
            "software": software_norm,
            "model_config": model_config,
        }

    @staticmethod
    def _fingerprint_for(spec: dict[str, Any]) -> str:
        basis = {
            "input_files": spec["input_files"],
            "annotation": spec["annotation"],
            "filter_params": spec["filter_params"],
            "software": spec["software"],
            "model_config": spec["model_config"],
        }
        return content_fingerprint(basis)

    def submit_run(
        self,
        *,
        label: str,
        batch_ids: Sequence[str],
        annotation_id: str,
        filter_params: dict[str, Any],
        software: Sequence[dict[str, Any]],
        model_config: dict[str, Any],
        submitted_by: str,
        request_id: str,
    ) -> dict[str, Any]:
        if not label.strip():
            raise PipelineError("field_required", "运行标识 label 为必填")
        prior = self.store.recalled_request("run_submit", request_id) if request_id else None
        spec = self._run_spec(
            batch_ids=batch_ids,
            annotation_id=annotation_id,
            filter_params=filter_params,
            software=software,
            model_config=model_config,
        )
        fingerprint = self._fingerprint_for(spec)
        if prior is not None:
            if prior["run_fingerprint"] != fingerprint:
                raise PipelineError(
                    "idempotency_conflict",
                    f"请求 {request_id} 曾提交不同内容，拒绝复用",
                )
            return {
                "reused": True,
                "reason": "request_id",
                "run_id": prior["run_id"],
                "run_fingerprint": fingerprint,
            }
        existing = self._run_by_fingerprint.get(fingerprint)
        if existing is not None:
            return {
                "reused": True,
                "reason": "fingerprint",
                "run_id": existing,
                "run_fingerprint": fingerprint,
            }
        # 标识相同而文件/参数不同：隔离为新的运行聚合，绝不覆盖旧结果。
        occurrence = self._labels.get(label, 0) + 1
        run_id = label if occurrence == 1 else f"{label}~iso{occurrence}"
        result = {"run_id": run_id, "run_fingerprint": fingerprint}
        self._append(
            event_type="ANALYSIS_STARTED",
            aggregate_type=RUN,
            aggregate_id=run_id,
            summary=f"启动分析运行 {run_id}（{len(spec['input_files'])} 个输入文件）",
            payload={
                "label": label,
                "batch_ids": spec["batch_ids"],
                "spec": spec,
                "submitted_by": submitted_by,
                "submitted_request_id": request_id,
                "run_fingerprint": fingerprint,
            },
            request_scope=("run_submit", request_id) if request_id else None,
            request_result=result,
        )
        return {"reused": False, **result}

    def commit_shard(
        self,
        run_id: str,
        *,
        shard_id: str,
        item_count: int,
        payload: Any,
        committed_by: str,
        request_id: str,
    ) -> dict[str, Any]:
        run = self._require_run(run_id)
        if run.invalidated is not None:
            raise PipelineError("run_invalidated", f"运行 {run_id} 已失效，拒绝分片提交")
        if run.status == "completed":
            raise PipelineError("run_completed", f"运行 {run_id} 已完成")
        if not isinstance(item_count, int) or isinstance(item_count, bool) or item_count <= 0:
            raise PipelineError("bad_item_count", "分片 item_count 必须是正整数")
        fingerprint = content_fingerprint(payload)
        existing = run.shards.get(shard_id)
        if existing is not None:
            if existing.fingerprint != fingerprint or existing.item_count != item_count:
                raise PipelineError(
                    "shard_conflict",
                    f"分片 {shard_id} 曾提交不同内容，不能覆盖",
                )
            # 重试：返回既有提交，不新增事件、不重复计数。
            return {"reused": True, "shard_id": shard_id, "item_count": item_count}
        prior = self.store.recalled_request("shard_commit", request_id) if request_id else None
        if prior is not None and (
            prior["run_id"] != run_id
            or prior["shard_id"] != shard_id
            or prior["shard_fingerprint"] != fingerprint
        ):
            raise PipelineError(
                "idempotency_conflict",
                f"请求 {request_id} 绑定了不同的分片提交",
            )
        result = {
            "run_id": run_id,
            "shard_id": shard_id,
            "shard_fingerprint": fingerprint,
            "item_count": item_count,
        }
        self._append(
            event_type="SHARD_COMMITTED",
            aggregate_type=RUN,
            aggregate_id=run_id,
            summary=f"运行 {run_id} 分片 {shard_id} 提交 {item_count} 条记录",
            payload={
                "shard_id": shard_id,
                "item_count": item_count,
                "shard_fingerprint": fingerprint,
                "committed_by": committed_by,
                "request_id": request_id,
            },
            request_scope=("shard_commit", request_id) if request_id else None,
            request_result=result,
        )
        return {"reused": False, **result}

    def complete_run(
        self,
        run_id: str,
        *,
        outputs: Sequence[dict[str, Any]],
    ) -> dict[str, Any]:
        run = self._require_run(run_id)
        if run.invalidated is not None:
            raise PipelineError("run_invalidated", f"运行 {run_id} 已失效")
        if run.status == "completed":
            raise PipelineError("run_completed", f"运行 {run_id} 已完成")
        if not run.shards:
            raise PipelineError("no_shards", "至少提交一个分片才能完成运行")
        norm_outputs = []
        seen = set()
        for out in outputs:
            ref = out.get("ref")
            kind = out.get("kind")
            if not ref or not kind:
                raise PipelineError("output_field_required", "产出必须包含 ref 与 kind")
            if ref in seen:
                raise PipelineError("duplicate_output", f"产出引用重复: {ref}")
            seen.add(ref)
            norm_outputs.append(
                {
                    "ref": ref,
                    "kind": kind,
                    "checksum": out.get("checksum", content_fingerprint(out.get("data", {}))),
                }
            )
        total_items = sum(s.item_count for s in run.shards.values())
        return self._append(
            event_type="ANALYSIS_COMPLETED",
            aggregate_type=RUN,
            aggregate_id=run_id,
            summary=(
                f"运行 {run_id} 完成：{len(run.shards)} 个分片、{total_items} 条记录、"
                f"{len(norm_outputs)} 项产出"
            ),
            payload={
                "total_shards": len(run.shards),
                "total_items": total_items,
                "outputs": norm_outputs,
            },
        )

    # ------------------------------------------------------------ 复核

    def approve_run(
        self,
        run_id: str,
        *,
        reviewer: str,
        checklist: Sequence[str],
        notes: str = "",
    ) -> dict[str, Any]:
        run = self._require_run(run_id)
        if run.invalidated is not None:
            raise PipelineError("run_invalidated", f"运行 {run_id} 已失效，不能审批")
        if run.status != "completed":
            raise PipelineError("run_not_completed", "只能审批已完成的运行")
        if not checklist:
            raise PipelineError("checklist_required", "审批必须勾选检查项")
        return self._append(
            event_type="REVIEW_APPROVED",
            aggregate_type=RUN,
            aggregate_id=run_id,
            summary=f"复核人 {reviewer} 批准运行 {run_id}",
            payload={"reviewer": reviewer, "checklist": list(checklist), "notes": notes},
        )

    def revoke_run(self, run_id: str, *, revoked_by: str, reason: str) -> dict[str, Any]:
        run = self._require_run(run_id)
        if run.invalidated is not None:
            raise PipelineError("run_invalidated", "运行已处于失效状态")
        event = self._append(
            event_type="REVIEW_REVOKED",
            aggregate_type=RUN,
            aggregate_id=run_id,
            summary=f"复核人 {revoked_by} 撤回运行 {run_id}：{reason}",
            payload={"revoked_by": revoked_by, "reason": reason, "scope": "run"},
        )
        self._flag_claims_for_runs(
            [run_id],
            reason=f"运行 {run_id} 被复核撤回：{reason}",
            detail={"run_id": run_id, "revoked_by": revoked_by},
        )
        return event

    def revoke_batch(self, batch_id: str, *, revoked_by: str, reason: str) -> dict[str, Any]:
        batch = self._require_batch(batch_id)
        if batch.status == "revoked":
            raise PipelineError("batch_revoked", "批次已撤回")
        event = self._append(
            event_type="REVIEW_REVOKED",
            aggregate_type=BATCH,
            aggregate_id=batch_id,
            summary=f"复核人 {revoked_by} 撤回批次 {batch_id}：{reason}",
            payload={"revoked_by": revoked_by, "reason": reason, "scope": "batch"},
        )
        # 仅失效真正消费该批次的运行；其他运行与图表保持有效。
        affected = [
            rid
            for rid, run in self.runs.items()
            if run.is_current and batch_id in run.batch_ids
        ]
        for rid in affected:
            self._invalidate_run(
                rid,
                cause="upstream_batch_revoked",
                detail={"batch_id": batch_id, "revoked_by": revoked_by, "reason": reason},
            )
        self._flag_claims_for_runs(
            affected,
            reason=f"上游批次 {batch_id} 被撤回：{reason}",
            detail={"batch_id": batch_id, "revoked_by": revoked_by, "affected_runs": affected},
        )
        return event

    def _invalidate_run(self, run_id: str, *, cause: str, detail: dict[str, Any]) -> None:
        self._append(
            event_type="RUN_INVALIDATED",
            aggregate_type=RUN,
            aggregate_id=run_id,
            summary=f"运行 {run_id} 因 {cause} 失效，需要重建",
            payload={"cause": cause, "detail": detail},
        )

    def _flag_claims_for_runs(
        self, run_ids: Sequence[str], *, reason: str, detail: dict[str, Any]
    ) -> None:
        for claim in self.claims.values():
            if claim.flag is not None:
                continue
            if any(rid in claim.run_ids for rid in run_ids):
                self._append(
                    event_type="CLAIM_FLAGGED",
                    aggregate_type=CLAIM,
                    aggregate_id=claim.claim_id,
                    summary=f"结论 {claim.claim_id} 标注限制：{reason}",
                    payload={
                        "reason": reason,
                        "detail": detail,
                        "restricted": True,
                        "history_retained": True,
                    },
                )

    def rerun_invalidated(
        self,
        *,
        old_run_id: str,
        submitted_by: str,
        request_id: str,
        batch_replacements: dict[str, str] | None = None,
    ) -> dict[str, Any]:
        """以原运行规格重建；可用新批次替换被撤回的批次。

        只重建受影响运行的链路，未受影响的运行与资产原样复用。
        """
        old = self._require_run(old_run_id)
        if old.invalidated is None:
            raise PipelineError("run_not_invalidated", "只能重建已失效运行")
        batch_replacements = batch_replacements or {}
        new_batch_ids = [batch_replacements.get(b, b) for b in old.spec["batch_ids"]]
        label = f"{old.label}~rebuild"
        return self.submit_run(
            label=label,
            batch_ids=new_batch_ids,
            annotation_id=old.spec["annotation"]["annotation_id"],
            filter_params=old.spec["filter_params"],
            software=[
                {"name": s["name"], "version": s["version"], "config": s["config"]}
                for s in old.spec["software"]
            ],
            model_config=old.spec["model_config"],
            submitted_by=submitted_by,
            request_id=request_id,
        )

    # ------------------------------------------------------------ 发布

    def _validate_correspondences(
        self, correspondences: Sequence[dict[str, Any]], runs: dict[str, Run]
    ) -> list[dict[str, Any]]:
        normalized = []
        for c in correspondences:
            cid = c.get("correspondence_id")
            if not cid:
                raise PipelineError("correspondence_id_required", "跨物种对应关系必须有标识")
            link = c.get("link") or {}
            for side in ("left", "right"):
                endpoint = link.get(side)
                if not endpoint:
                    raise PipelineError("link_endpoint_required", f"对应关系 {cid} 缺少 {side}")
                run = runs.get(endpoint.get("run_id"))
                if run is None:
                    raise PipelineError(
                        "citation_run_missing",
                        f"对应关系 {cid} 的 {side} 未引用具体运行",
                    )
                if run.status != "completed" or run.invalidated is not None:
                    raise PipelineError(
                        "citation_run_unusable",
                        f"对应关系 {cid} 的 {side} 引用运行未完成或已失效",
                    )
                output_ref = endpoint.get("output_ref")
                if not any(o["ref"] == output_ref for o in run.outputs):
                    raise PipelineError(
                        "citation_output_missing",
                        f"对应关系 {cid} 的 {side} 未引用运行产出 {output_ref}",
                    )
                if not endpoint.get("software_cited"):
                    raise PipelineError(
                        "software_citation_required",
                        f"对应关系 {cid} 的 {side} 必须引用软件配置",
                    )
            if not c.get("functional_description"):
                raise PipelineError(
                    "functional_description_required",
                    f"对应关系 {cid} 缺少功能描述",
                )
            normalized.append(
                {
                    "correspondence_id": cid,
                    "functional_description": c["functional_description"],
                    "link": link,
                    "similarity": c.get("similarity", {}),
                }
            )
        return normalized

    def release_claim(
        self,
        *,
        claim_id: str,
        title: str,
        statement: str,
        run_ids: Sequence[str],
        correspondences: Sequence[dict[str, Any]] = (),
        uncertainty: dict[str, Any],
        released_by: str,
        visibility: str = "researcher",
        supersedes: Sequence[str] = (),
    ) -> dict[str, Any]:
        if claim_id in self.claims:
            raise PipelineError("claim_exists", f"结论 {claim_id} 已发布")
        if not title.strip() or not statement.strip():
            raise PipelineError("field_required", "结论标题与正文为必填")
        if visibility not in ("researcher", "reviewer", "public"):
            raise PipelineError(
                "bad_visibility",
                "可见范围必须是 researcher/reviewer/public 之一",
            )
        if not run_ids:
            raise PipelineError("evidence_required", "结论必须引用至少一个运行")
        runs: dict[str, Run] = {}
        for rid in run_ids:
            run = self._require_run(rid)
            if run.invalidated is not None:
                raise PipelineError(
                    "evidence_invalidated",
                    f"运行 {rid} 已失效，结论不得发布；请先重建",
                )
            if run.status != "completed":
                raise PipelineError("evidence_not_completed", f"运行 {rid} 尚未完成")
            if run.approval is None:
                raise PipelineError("evidence_not_approved", f"运行 {rid} 尚未通过人工复核")
            runs[rid] = run
            if visibility == "public":
                for bid in run.batch_ids:
                    if not self.batches[bid].consent.public_release:
                        raise PipelineError(
                            "public_release_not_consented",
                            f"批次 {bid} 未授权公开发布",
                        )
        if not isinstance(uncertainty, dict) or uncertainty.get("level") not in UNCERTAINTY_LEVELS:
            raise PipelineError(
                "uncertainty_required",
                f"不确定性说明必须包含 level（{'/'.join(UNCERTAINTY_LEVELS)}）",
            )
        if not uncertainty.get("statement"):
            raise PipelineError("uncertainty_statement_required", "必须给出不确定性文字说明")
        links = self._validate_correspondences(correspondences, runs)
        approved_by = sorted({run.approval["reviewer"] for run in runs.values()})  # type: ignore[index]
        payload = {
            "title": title,
            "statement": statement,
            "visibility": visibility,
            "evidence": {
                "run_ids": list(run_ids),
                "correspondences": links,
            },
            "uncertainty": {
                "level": uncertainty["level"],
                "statement": uncertainty["statement"],
                "basis": uncertainty.get("basis", ""),
            },
            "approved_by": approved_by,
            "released_by": released_by,
            "individual_diagnosis_allowed": INDIVIDUAL_DIAGNOSIS_ALLOWED,
            "diagnostics_disclaimer": (
                "本结论来自跨物种相关性计算，不构成、也不得自动转换为任何个体诊断依据。"
            ),
        }
        event = self._append(
            event_type="CLAIM_RELEASED",
            aggregate_type=CLAIM,
            aggregate_id=claim_id,
            summary=f"发布结论 {claim_id}：{title}",
            payload=payload,
        )
        for old_id in supersedes:
            old = self.claims.get(old_id)
            if old is None:
                raise PipelineError("claim_missing", f"被替代结论 {old_id} 不存在")
            self._append(
                event_type="CLAIM_FLAGGED",
                aggregate_type=CLAIM,
                aggregate_id=old_id,
                summary=f"结论 {old_id} 被 {claim_id} 替代，历史版本保留",
                payload={
                    "reason": f"被新结论 {claim_id} 替代",
                    "detail": {"superseded_by": claim_id},
                    "restricted": True,
                    "history_retained": True,
                    "superseded_by": claim_id,
                },
            )
        return event

    def publish_assets(
        self,
        claim_id: str,
        assets: Sequence[dict[str, Any]],
        *,
        materializer: Callable[[dict[str, Any]], bytes] | None = None,
    ) -> dict[str, Any]:
        """发布结论资产；重启后重放只会补齐缺失资产。

        materializer 接收资产规格返回字节；默认生成内嵌溯源引用的 JSON。
        已在事件中登记的 asset_id 直接复用；事件存在但文件丢失时补齐；
        校验和不符则报错而不是静默覆盖。
        """
        claim = self._require_claim(claim_id)
        materializer = materializer or _default_materializer
        materialized: list[str] = []
        reused: list[str] = []
        for spec in assets:
            asset_id = spec.get("asset_id")
            kind = spec.get("kind")
            if not asset_id or kind not in ASSET_KINDS:
                raise PipelineError("asset_spec_invalid", f"资产规格不合法: {asset_id}")
            source_run_id = spec.get("source_run_id")
            source_output_ref = spec.get("source_output_ref")
            run = self._require_run(source_run_id) if source_run_id else None
            if run is not None and source_output_ref and not any(
                o["ref"] == source_output_ref for o in run.outputs
            ):
                raise PipelineError(
                    "asset_source_missing",
                    f"资产 {asset_id} 的产出引用 {source_output_ref} 不存在",
                )
            if claim.flag is not None:
                raise PipelineError(
                    "claim_flagged",
                    f"结论 {claim_id} 已标注限制，资产 {asset_id} 不得发布",
                )
            existing = claim.assets.get(asset_id)
            path = _asset_path(self.asset_root, claim_id, spec)
            if existing is not None:
                if path is None:
                    # 无文件根目录：资产以 claim:// URI 登记，存在即复用。
                    reused.append(asset_id)
                    continue
                if os.path.exists(path):
                    if _sha256_file(path) != existing["checksum"]:
                        raise PipelineError(
                            "asset_corrupted",
                            f"资产 {asset_id} 已存在但校验和不符，拒绝覆盖",
                        )
                    reused.append(asset_id)
                    continue
                # 事件在、文件丢：按“补齐缺失资产”重写同一路径。
                data = materializer(spec)
                _write_bytes(path, data)
                materialized.append(asset_id)
                continue
            data = materializer(spec)
            checksum = "sha256:" + hashlib.sha256(data).hexdigest()
            if path is not None:
                _write_bytes(path, data)
            self._append(
                event_type="CLAIM_ASSET_PUBLISHED",
                aggregate_type=CLAIM,
                aggregate_id=claim_id,
                summary=f"结论 {claim_id} 发布资产 {asset_id}（{kind}）",
                payload={
                    "asset_id": asset_id,
                    "kind": kind,
                    "title": spec.get("title", ""),
                    "path": path or f"claim://{claim_id}/{asset_id}",
                    "size": len(data),
                    "checksum": checksum,
                    "source_run_id": source_run_id,
                    "source_output_ref": source_output_ref,
                },
            )
            materialized.append(asset_id)
        return {
            "claim_id": claim_id,
            "materialized": materialized,
            "reused": reused,
            "missing_remaining": [],
        }

    # ------------------------------------------------------------ 查询

    def _require_batch(self, batch_id: str) -> Batch:
        batch = self.batches.get(batch_id)
        if batch is None:
            raise PipelineError("batch_missing", f"批次 {batch_id} 不存在")
        return batch

    def _require_run(self, run_id: str) -> Run:
        run = self.runs.get(run_id)
        if run is None:
            raise PipelineError("run_missing", f"运行 {run_id} 不存在")
        return run

    def _require_claim(self, claim_id: str) -> ClaimRecord:
        claim = self.claims.get(claim_id)
        if claim is None:
            raise PipelineError("claim_missing", f"结论 {claim_id} 不存在")
        return claim

    def runs_affected_by_batch(self, batch_id: str) -> list[str]:
        return sorted(
            rid for rid, run in self.runs.items() if batch_id in run.batch_ids
        )


# ---------------------------------------------------------------- 辅助


def _is_sha256(value: str) -> bool:
    return (
        isinstance(value, str)
        and len(value) == 64
        and all(c in "0123456789abcdef" for c in value.lower())
    )


def _asset_path(root: str | None, claim_id: str, spec: dict[str, Any]) -> str | None:
    if root is None:
        return None
    filename = spec.get("filename") or f"{spec['asset_id']}.{spec['kind']}.json"
    return os.path.join(root, claim_id, filename)


def _write_bytes(path: str | None, data: bytes) -> None:
    if path is None:
        return
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "wb") as handle:
        handle.write(data)


def _sha256_file(path: str) -> str:
    digest = hashlib.sha256()
    with open(path, "rb") as handle:
        for chunk in iter(lambda: handle.read(65536), b""):
            digest.update(chunk)
    return "sha256:" + digest.hexdigest()


def _default_materializer(spec: dict[str, Any]) -> bytes:
    body = {
        "asset_id": spec["asset_id"],
        "kind": spec["kind"],
        "title": spec.get("title", ""),
        "generated_by": "cross-species-research-evidence-pipeline",
        "provenance": {
            "source_run_id": spec.get("source_run_id"),
            "source_output_ref": spec.get("source_output_ref"),
        },
        "notice": (
            "占位资产：接入方应注入实际图表/表格渲染器；"
            "溯源引用必须随资产一并分发。"
        ),
    }
    return canonical_json(body).encode("utf-8")
