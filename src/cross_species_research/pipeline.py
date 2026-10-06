"""远缘细胞研究证据流水的领域流程。

在 contracts.py 的交换层校验之上实现领域规则：

- 物种来源、批次、注释版本与过滤参数按内容指纹登记；重复提交复用，
  标识相同而文件或参数不同必须隔离（冲突拒绝，不得覆盖或复用）；
- 分析运行按输入指纹与软件配置派生运行键，重复请求复用已有结果；
- 分片任务可重试，但同一分片标识只计数一次；
- 质量复核撤回批次时只重建受影响的排名与图表，已发布的结论保留并标注限制；
- 结论发布必须附不确定性说明，相关性结论不得改写成个体诊断；
- 发布资产按标识幂等补齐，中途重启只补缺失项；
- 研究者、审稿人、公众看到不同字段范围；
- 任意结论可追溯到原始文件、版本、审批人与不确定性说明。
"""

from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Mapping, Sequence

__all__ = [
    "ConflictError",
    "DomainError",
    "Pipeline",
]

_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")

#: 相关性结论中禁止出现的个体诊断字段。
_DIAGNOSTIC_KEYS = frozenset({"diagnosis", "individual_diagnosis", "patient_recommendation"})

#: 结论对公众开放的字段；审稿人追加 _REVIEWER_EXTRA_FIELDS；研究者可见全部。
_PUBLIC_FIELDS = (
    "claim_id",
    "statement",
    "claim_kind",
    "scope",
    "uncertainty",
    "limitations",
    "released_at",
)
_REVIEWER_EXTRA_FIELDS = ("correspondences", "approvals")

_ROLES = ("researcher", "reviewer", "public")


class DomainError(Exception):
    """领域规则不满足时抛出。"""


class ConflictError(DomainError):
    """标识相同但内容不同的隔离冲突。"""


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _fingerprint(content: Any) -> str:
    canonical = json.dumps(content, sort_keys=True, ensure_ascii=False, separators=(",", ":"))
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def _require_text(value: Any, field_name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise DomainError(f"{field_name} 必须是非空字符串")
    return value


def _check_files(files: Any) -> list[dict[str, str]]:
    if not isinstance(files, Sequence) or isinstance(files, str) or not files:
        raise DomainError("测序文件列表必须非空")
    checked: list[dict[str, str]] = []
    for item in files:
        if not isinstance(item, Mapping):
            raise DomainError("测序文件条目必须是对象")
        path = _require_text(item.get("path"), "文件路径")
        sha256 = item.get("sha256")
        if not isinstance(sha256, str) or not _SHA256_RE.match(sha256):
            raise DomainError(f"文件 {path} 缺少合法的 sha256 校验和")
        checked.append({"path": path, "sha256": sha256})
    return sorted(checked, key=lambda entry: entry["path"])


@dataclass
class Pipeline:
    """事件溯源的证据流水；所有状态由追加的领域事件推导。"""

    events: list[dict[str, Any]] = field(default_factory=list)
    sources: dict[str, dict[str, Any]] = field(default_factory=dict)
    batches: dict[str, dict[str, Any]] = field(default_factory=dict)
    annotations: dict[str, dict[str, Any]] = field(default_factory=dict)
    filter_profiles: dict[str, dict[str, Any]] = field(default_factory=dict)
    runs: dict[str, dict[str, Any]] = field(default_factory=dict)
    run_keys: dict[str, str] = field(default_factory=dict)
    correspondences: dict[str, dict[str, Any]] = field(default_factory=dict)
    claims: dict[str, dict[str, Any]] = field(default_factory=dict)
    approvals: dict[str, list[dict[str, Any]]] = field(default_factory=dict)
    limitations: dict[str, list[dict[str, Any]]] = field(default_factory=dict)
    assets: dict[str, dict[str, dict[str, Any]]] = field(default_factory=dict)

    # ------------------------------------------------------------------
    # 事件追加
    # ------------------------------------------------------------------

    def _emit(
        self,
        event_type: str,
        aggregate_type: str,
        aggregate_id: str,
        summary: str,
        payload: Mapping[str, Any],
    ) -> dict[str, Any]:
        version = 1 + sum(
            1
            for event in self.events
            if event["aggregate_type"] == aggregate_type and event["aggregate_id"] == aggregate_id
        )
        event = {
            "event_id": f"evt-{len(self.events) + 1:06d}",
            "event_type": event_type,
            "aggregate_type": aggregate_type,
            "aggregate_id": aggregate_id,
            "occurred_at": _now(),
            "version": version,
            "summary": summary,
            "payload": dict(payload),
        }
        self.events.append(event)
        return event

    def _events_of(self, aggregate_id: str, event_type: str) -> list[dict[str, Any]]:
        return [
            event
            for event in self.events
            if event["aggregate_id"] == aggregate_id and event["event_type"] == event_type
        ]

    # ------------------------------------------------------------------
    # 登记：物种来源、批次、注释版本、过滤参数
    # ------------------------------------------------------------------

    def register_source(
        self,
        source_id: str,
        species: str,
        cell_source: str,
        provider_lab: str,
        authorization: Mapping[str, Any],
    ) -> dict[str, Any]:
        """登记物种与细胞来源及其授权范围；重复提交复用，内容不同必须隔离。"""
        _require_text(source_id, "来源标识")
        _require_text(species, "物种")
        _require_text(cell_source, "细胞来源")
        _require_text(provider_lab, "提供方实验室")
        if not isinstance(authorization, Mapping):
            raise DomainError("授权范围必须登记")
        granted_by = _require_text(authorization.get("granted_by"), "授权人")
        scope = authorization.get("scope")
        if not isinstance(scope, Sequence) or isinstance(scope, str) or not scope:
            raise DomainError("授权范围 scope 必须是非空列表")
        record = {
            "source_id": source_id,
            "species": species,
            "cell_source": cell_source,
            "provider_lab": provider_lab,
            "authorization": {"granted_by": granted_by, "scope": sorted(scope)},
        }
        existing = self.sources.get(source_id)
        if existing is not None:
            if existing != record:
                raise ConflictError(f"来源 {source_id} 已登记且内容不同，必须更换标识以隔离")
            return existing
        self.sources[source_id] = record
        self._emit(
            "SOURCE_REGISTERED",
            "species_source",
            source_id,
            f"登记物种来源 {species} / {cell_source}",
            record,
        )
        return record

    def register_batch(
        self,
        batch_id: str,
        source_id: str,
        files: Sequence[Mapping[str, str]],
    ) -> dict[str, Any]:
        """登记测序批次；文件校验和构成内容指纹，同标识不同文件必须隔离。"""
        _require_text(batch_id, "批次标识")
        source = self.sources.get(source_id)
        if source is None:
            raise DomainError(f"物种来源 {source_id} 未登记")
        checked = _check_files(files)
        fingerprint = _fingerprint(checked)
        existing = self.batches.get(batch_id)
        if existing is not None:
            if existing["fingerprint"] != fingerprint:
                raise ConflictError(f"批次 {batch_id} 已登记但文件内容不同，必须更换标识以隔离")
            return existing
        record = {
            "batch_id": batch_id,
            "source_id": source_id,
            "files": checked,
            "fingerprint": fingerprint,
            "revoked": False,
        }
        self.batches[batch_id] = record
        self._emit(
            "BATCH_REGISTERED",
            "specimen_batch",
            batch_id,
            f"登记测序批次 {batch_id}（{len(checked)} 个文件）",
            {"source_id": source_id, "files": checked, "fingerprint": fingerprint},
        )
        return record

    def lock_annotation(
        self,
        annotation_id: str,
        database: str,
        version: str,
        entries: Sequence[str],
    ) -> dict[str, Any]:
        """锁定功能注释版本；同标识不同内容必须隔离。"""
        _require_text(annotation_id, "注释标识")
        _require_text(database, "注释数据库")
        _require_text(version, "注释版本")
        if not isinstance(entries, Sequence) or isinstance(entries, str) or not entries:
            raise DomainError("注释条目必须非空")
        terms = sorted({_require_text(entry, "注释条目") for entry in entries})
        fingerprint = _fingerprint({"database": database, "version": version, "entries": terms})
        existing = self.annotations.get(annotation_id)
        if existing is not None:
            if existing["fingerprint"] != fingerprint:
                raise ConflictError(f"注释 {annotation_id} 已锁定且内容不同，必须更换标识以隔离")
            return existing
        record = {
            "annotation_id": annotation_id,
            "database": database,
            "version": version,
            "entries": terms,
            "fingerprint": fingerprint,
        }
        self.annotations[annotation_id] = record
        self._emit(
            "ANNOTATION_LOCKED",
            "annotation_revision",
            annotation_id,
            f"锁定功能注释 {database} {version}",
            record,
        )
        return record

    def define_filter_profile(self, profile_id: str, params: Mapping[str, Any]) -> dict[str, Any]:
        """登记过滤参数；同标识不同参数必须隔离。"""
        _require_text(profile_id, "过滤参数标识")
        if not isinstance(params, Mapping) or not params:
            raise DomainError("过滤参数必须非空")
        fingerprint = _fingerprint(dict(params))
        existing = self.filter_profiles.get(profile_id)
        if existing is not None:
            if existing["fingerprint"] != fingerprint:
                raise ConflictError(f"过滤参数 {profile_id} 已登记且内容不同，必须更换标识以隔离")
            return existing
        record = {"profile_id": profile_id, "params": dict(params), "fingerprint": fingerprint}
        self.filter_profiles[profile_id] = record
        self._emit(
            "FILTER_PROFILE_DEFINED",
            "filter_profile",
            profile_id,
            f"登记过滤参数 {profile_id}",
            record,
        )
        return record

    # ------------------------------------------------------------------
    # 分析运行与分片
    # ------------------------------------------------------------------

    def request_analysis(
        self,
        batch_ids: Sequence[str],
        annotation_id: str,
        filter_profile_id: str,
        software: Mapping[str, Any],
        shard_count: int,
    ) -> dict[str, Any]:
        """按输入与软件配置派生运行键；重复请求复用已有运行结果。"""
        if not isinstance(shard_count, int) or isinstance(shard_count, bool) or shard_count < 1:
            raise DomainError("分片数必须是正整数")
        batches = []
        for batch_id in batch_ids:
            batch = self.batches.get(batch_id)
            if batch is None:
                raise DomainError(f"批次 {batch_id} 未登记")
            if batch["revoked"]:
                raise DomainError(f"批次 {batch_id} 已被质量复核撤回")
            batches.append(batch)
        annotation = self.annotations.get(annotation_id)
        if annotation is None:
            raise DomainError(f"功能注释 {annotation_id} 未锁定")
        profile = self.filter_profiles.get(filter_profile_id)
        if profile is None:
            raise DomainError(f"过滤参数 {filter_profile_id} 未登记")
        if not isinstance(software, Mapping):
            raise DomainError("软件配置必须登记")
        software_record = {
            "name": _require_text(software.get("name"), "软件名称"),
            "version": _require_text(software.get("version"), "软件版本"),
        }
        if software.get("image_digest"):
            software_record["image_digest"] = software["image_digest"]
        run_key = _fingerprint(
            {
                "batches": sorted((b["batch_id"], b["fingerprint"]) for b in batches),
                "annotation": (annotation["annotation_id"], annotation["fingerprint"]),
                "filter_profile": (profile["profile_id"], profile["fingerprint"]),
                "software": software_record,
            }
        )
        existing_id = self.run_keys.get(run_key)
        if existing_id is not None:
            return self.runs[existing_id]
        run_id = f"run-{len(self.runs) + 1:04d}"
        shard_ids = [f"{run_id}-shard-{index:03d}" for index in range(shard_count)]
        record = {
            "run_id": run_id,
            "run_key": run_key,
            "batch_ids": sorted(b["batch_id"] for b in batches),
            "annotation_id": annotation_id,
            "filter_profile_id": filter_profile_id,
            "software": software_record,
            "shards": {shard_id: {"completed": False, "attempts": 0} for shard_id in shard_ids},
            "status": "running",
            "stale": False,
        }
        self.runs[run_id] = record
        self.run_keys[run_key] = run_id
        self._emit(
            "ANALYSIS_STARTED",
            "analysis_run",
            run_id,
            f"启动分析运行 {run_id}（{shard_count} 个分片）",
            {
                "run_key": run_key,
                "batch_ids": record["batch_ids"],
                "annotation_id": annotation_id,
                "filter_profile_id": filter_profile_id,
                "software": software_record,
                "shard_ids": shard_ids,
            },
        )
        return record

    def complete_shard(self, run_id: str, shard_id: str, result_digest: str) -> dict[str, Any]:
        """分片可重试，但同一分片标识只计数一次。"""
        run = self.runs.get(run_id)
        if run is None:
            raise DomainError(f"分析运行 {run_id} 不存在")
        shard = run["shards"].get(shard_id)
        if shard is None:
            raise DomainError(f"分片 {shard_id} 不属于运行 {run_id}")
        _require_text(result_digest, "分片结果摘要")
        shard["attempts"] += 1
        if shard["completed"]:
            return {"counted": False, "run_completed": run["status"] == "completed"}
        shard["completed"] = True
        shard["result_digest"] = result_digest
        self._emit(
            "SHARD_COMPLETED",
            "analysis_run",
            run_id,
            f"分片 {shard_id} 完成（第 {shard['attempts']} 次尝试）",
            {"shard_id": shard_id, "result_digest": result_digest, "attempt": shard["attempts"]},
        )
        if all(item["completed"] for item in run["shards"].values()):
            run["status"] = "completed"
            run["ranking_output"] = f"{run_id}-ranking"
            self._emit(
                "ANALYSIS_COMPLETED",
                "analysis_run",
                run_id,
                f"分析运行 {run_id} 完成",
                {"ranking_output": run["ranking_output"]},
            )
        return {"counted": True, "run_completed": run["status"] == "completed"}

    # ------------------------------------------------------------------
    # 跨物种对应关系与人工复核
    # ------------------------------------------------------------------

    def assert_correspondence(
        self,
        correspondence_id: str,
        run_id: str,
        left_species: str,
        right_species: str,
        left_gene: str,
        right_gene: str,
        statement: str,
    ) -> dict[str, Any]:
        """登记跨物种对应关系，快照其所依据的具体输入与软件配置。"""
        _require_text(correspondence_id, "对应关系标识")
        _require_text(left_species, "左侧物种")
        _require_text(right_species, "右侧物种")
        _require_text(left_gene, "左侧基因")
        _require_text(right_gene, "右侧基因")
        _require_text(statement, "对应关系陈述")
        run = self.runs.get(run_id)
        if run is None:
            raise DomainError(f"分析运行 {run_id} 不存在")
        if run["status"] != "completed":
            raise DomainError(f"分析运行 {run_id} 尚未完成")
        if run["stale"]:
            raise DomainError(f"分析运行 {run_id} 的输入已被撤回，结论待重建")
        observed = {
            self.batches[batch_id]["source_id"] for batch_id in run["batch_ids"]
        }
        species_seen = {self.sources[source_id]["species"] for source_id in observed}
        for species in (left_species, right_species):
            if species not in species_seen:
                raise DomainError(f"物种 {species} 不在运行 {run_id} 的输入范围内")
        inputs_snapshot = {
            "batches": [
                {"batch_id": batch_id, "fingerprint": self.batches[batch_id]["fingerprint"]}
                for batch_id in run["batch_ids"]
            ],
            "annotation_id": run["annotation_id"],
            "filter_profile_id": run["filter_profile_id"],
        }
        record = {
            "correspondence_id": correspondence_id,
            "run_id": run_id,
            "left_species": left_species,
            "right_species": right_species,
            "left_gene": left_gene,
            "right_gene": right_gene,
            "statement": statement,
            "inputs": inputs_snapshot,
            "software": dict(run["software"]),
        }
        fingerprint = _fingerprint(record)
        existing = self.correspondences.get(correspondence_id)
        if existing is not None:
            if existing["fingerprint"] != fingerprint:
                raise ConflictError(f"对应关系 {correspondence_id} 已登记且内容不同，必须更换标识以隔离")
            return existing
        record["fingerprint"] = fingerprint
        self.correspondences[correspondence_id] = record
        self._emit(
            "CORRESPONDENCE_ASSERTED",
            "correspondence",
            correspondence_id,
            f"登记跨物种对应关系 {left_species}:{left_gene} ↔ {right_species}:{right_gene}",
            record,
        )
        return record

    def approve(self, target_id: str, reviewer: str, note: str = "") -> dict[str, Any]:
        """人工复核通过；审批人留痕。"""
        _require_text(reviewer, "审批人")
        known = (
            target_id in self.correspondences
            or target_id in self.runs
            or target_id in self.claims
        )
        if not known:
            raise DomainError(f"复核对象 {target_id} 不存在")
        aggregate_type = (
            "correspondence"
            if target_id in self.correspondences
            else "analysis_run"
            if target_id in self.runs
            else "research_claim"
        )
        record = {"target_id": target_id, "reviewer": reviewer, "note": note}
        self.approvals.setdefault(target_id, []).append(record)
        self._emit(
            "REVIEW_APPROVED",
            aggregate_type,
            target_id,
            f"{reviewer} 复核通过 {target_id}",
            record,
        )
        return record

    # ------------------------------------------------------------------
    # 质量复核撤回
    # ------------------------------------------------------------------

    def revoke_batch(self, batch_id: str, reviewer: str, reason: str) -> dict[str, Any]:
        """撤回批次：只重建受影响的排名与图表，已发布结论保留并标注限制。"""
        _require_text(reviewer, "审批人")
        _require_text(reason, "撤回原因")
        batch = self.batches.get(batch_id)
        if batch is None:
            raise DomainError(f"批次 {batch_id} 未登记")
        if batch["revoked"]:
            raise DomainError(f"批次 {batch_id} 已被撤回")
        batch["revoked"] = True
        self._emit(
            "REVIEW_REVOKED",
            "specimen_batch",
            batch_id,
            f"质量复核撤回批次 {batch_id}",
            {"reviewer": reviewer, "reason": reason},
        )
        rebuilt: list[str] = []
        for run in self.runs.values():
            if batch_id in run["batch_ids"] and not run["stale"]:
                run["stale"] = True
                rebuilt.append(run["run_id"])
                self._emit(
                    "RANKING_REBUILT",
                    "analysis_run",
                    run["run_id"],
                    f"批次 {batch_id} 撤回，重建运行 {run['run_id']} 的排名与图表",
                    {"revoked_batch": batch_id, "reason": reason},
                )
        limited: list[str] = []
        for claim in self.claims.values():
            affected = any(
                self.runs[self.correspondences[cid]["run_id"]]["stale"]
                for cid in claim["correspondence_ids"]
            )
            if affected and claim["claim_id"] not in limited:
                limitation = {
                    "revoked_batch": batch_id,
                    "reason": reason,
                    "reviewer": reviewer,
                    "limited_at": _now(),
                }
                self.limitations.setdefault(claim["claim_id"], []).append(limitation)
                limited.append(claim["claim_id"])
                self._emit(
                    "CLAIM_LIMITED",
                    "research_claim",
                    claim["claim_id"],
                    f"结论 {claim['claim_id']} 保留并标注限制",
                    limitation,
                )
        return {"rebuilt_runs": rebuilt, "limited_claims": limited}

    # ------------------------------------------------------------------
    # 结论发布与资产
    # ------------------------------------------------------------------

    def release_claim(
        self,
        claim_id: str,
        correspondence_ids: Sequence[str],
        statement: str,
        uncertainty: str,
        claim_kind: str = "correlation",
        scope: str = "population",
        extra: Mapping[str, Any] | None = None,
    ) -> dict[str, Any]:
        """发布公开结论；相关性结论不得改写成个体诊断。"""
        _require_text(claim_id, "结论标识")
        _require_text(statement, "结论陈述")
        _require_text(uncertainty, "不确定性说明")
        if claim_id in self.claims:
            raise ConflictError(f"结论 {claim_id} 已发布")
        if not correspondence_ids:
            raise DomainError("结论必须引用至少一条跨物种对应关系")
        for correspondence_id in correspondence_ids:
            correspondence = self.correspondences.get(correspondence_id)
            if correspondence is None:
                raise DomainError(f"对应关系 {correspondence_id} 未登记")
            if not self.approvals.get(correspondence_id):
                raise DomainError(f"对应关系 {correspondence_id} 尚未通过人工复核")
            run = self.runs[correspondence["run_id"]]
            if run["stale"]:
                raise DomainError(f"对应关系 {correspondence_id} 依据的运行已被撤回，需先重建")
        if claim_kind == "correlation":
            if scope != "population":
                raise DomainError("相关性结论只能发布群体层面的陈述")
            disallowed = _DIAGNOSTIC_KEYS & set((extra or {}).keys())
            if disallowed:
                raise DomainError(
                    f"相关性结论不得包含个体诊断字段：{sorted(disallowed)}"
                )
        record = {
            "claim_id": claim_id,
            "correspondence_ids": list(correspondence_ids),
            "statement": statement,
            "uncertainty": uncertainty,
            "claim_kind": claim_kind,
            "scope": scope,
            "extra": dict(extra or {}),
            "released_at": _now(),
        }
        self.claims[claim_id] = record
        self._emit(
            "CLAIM_RELEASED",
            "research_claim",
            claim_id,
            f"发布公开结论 {claim_id}",
            record,
        )
        return record

    def publish_assets(
        self,
        claim_id: str,
        assets: Sequence[Mapping[str, str]],
    ) -> dict[str, Any]:
        """按资产标识幂等发布；中途重启只补齐缺失资产。"""
        if claim_id not in self.claims:
            raise DomainError(f"结论 {claim_id} 尚未发布")
        published = self.assets.setdefault(claim_id, {})
        added: list[str] = []
        skipped: list[str] = []
        for asset in assets:
            asset_id = _require_text(asset.get("asset_id"), "资产标识")
            kind = _require_text(asset.get("kind"), "资产类型")
            digest = _require_text(asset.get("content_digest"), "资产内容摘要")
            if asset_id in published:
                skipped.append(asset_id)
                continue
            record = {"asset_id": asset_id, "kind": kind, "content_digest": digest}
            published[asset_id] = record
            added.append(asset_id)
            self._emit(
                "ASSET_PUBLISHED",
                "publication_asset",
                asset_id,
                f"发布资产 {asset_id}（{kind}）",
                {"claim_id": claim_id, **record},
            )
        return {"published": added, "skipped": skipped}

    def missing_assets(self, claim_id: str, required: Sequence[str]) -> list[str]:
        """列出结论尚未发布的必需资产，供重启后补齐。"""
        published = self.assets.get(claim_id, {})
        return [asset_id for asset_id in required if asset_id not in published]

    # ------------------------------------------------------------------
    # 角色视图与追溯
    # ------------------------------------------------------------------

    def view_claim(self, claim_id: str, role: str) -> dict[str, Any]:
        """按角色裁剪结论字段：公众 < 审稿人 < 研究者。"""
        if role not in _ROLES:
            raise DomainError(f"未知角色 {role}")
        claim = self.claims.get(claim_id)
        if claim is None:
            raise DomainError(f"结论 {claim_id} 不存在")
        if role == "researcher":
            return self.trace_claim(claim_id)
        view = {field: claim.get(field) for field in _PUBLIC_FIELDS}
        view["limitations"] = list(self.limitations.get(claim_id, []))
        if role == "reviewer":
            view["correspondences"] = [
                {
                    "correspondence_id": cid,
                    "statement": self.correspondences[cid]["statement"],
                    "inputs": self.correspondences[cid]["inputs"],
                    "software": self.correspondences[cid]["software"],
                }
                for cid in claim["correspondence_ids"]
            ]
            view["approvals"] = [
                approval
                for cid in claim["correspondence_ids"]
                for approval in self.approvals.get(cid, [])
            ]
        return view

    def trace_claim(self, claim_id: str) -> dict[str, Any]:
        """从结论追溯到原始文件、版本、审批人与不确定性说明。"""
        claim = self.claims.get(claim_id)
        if claim is None:
            raise DomainError(f"结论 {claim_id} 不存在")
        correspondences = []
        for cid in claim["correspondence_ids"]:
            correspondence = self.correspondences[cid]
            run = self.runs[correspondence["run_id"]]
            annotation = self.annotations[run["annotation_id"]]
            profile = self.filter_profiles[run["filter_profile_id"]]
            batches = []
            for batch_id in run["batch_ids"]:
                batch = self.batches[batch_id]
                source = self.sources[batch["source_id"]]
                batches.append(
                    {
                        "batch_id": batch_id,
                        "fingerprint": batch["fingerprint"],
                        "files": list(batch["files"]),
                        "revoked": batch["revoked"],
                        "source": {
                            "source_id": source["source_id"],
                            "species": source["species"],
                            "cell_source": source["cell_source"],
                            "provider_lab": source["provider_lab"],
                            "authorization": dict(source["authorization"]),
                        },
                    }
                )
            correspondences.append(
                {
                    "correspondence_id": cid,
                    "statement": correspondence["statement"],
                    "run": {
                        "run_id": run["run_id"],
                        "run_key": run["run_key"],
                        "software": dict(run["software"]),
                        "stale": run["stale"],
                    },
                    "annotation": {
                        "annotation_id": annotation["annotation_id"],
                        "database": annotation["database"],
                        "version": annotation["version"],
                    },
                    "filter_profile": {
                        "profile_id": profile["profile_id"],
                        "params": dict(profile["params"]),
                    },
                    "batches": batches,
                    "approvals": list(self.approvals.get(cid, [])),
                }
            )
        return {
            **{field: claim[field] for field in _PUBLIC_FIELDS if field in claim},
            "correspondence_ids": list(claim["correspondence_ids"]),
            "correspondences": correspondences,
            "limitations": list(self.limitations.get(claim_id, [])),
            "assets": list(self.assets.get(claim_id, {}).values()),
        }
