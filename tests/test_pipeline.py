from __future__ import annotations

import json
import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
from cross_species_research.contracts import validate_event
from cross_species_research.pipeline import ConflictError, DomainError, Pipeline

FILE_A = {"path": "s3://seq/human/run1.fq.gz", "sha256": "a" * 64}
FILE_B = {"path": "s3://seq/mouse/run1.fq.gz", "sha256": "b" * 64}
SOFTWARE = {"name": "crossmap", "version": "2.3.1", "image_digest": "sha256:" + "c" * 64}


def build_pipeline() -> Pipeline:
    pipe = Pipeline()
    pipe.register_source(
        "src-human",
        species="人",
        cell_source="外周血单核细胞",
        provider_lab="甲实验室",
        authorization={"granted_by": "伦理委员会", "scope": ["跨物种比较", "论文发表"]},
    )
    pipe.register_source(
        "src-mouse",
        species="小鼠",
        cell_source="脾细胞",
        provider_lab="乙实验室",
        authorization={"granted_by": "伦理委员会", "scope": ["跨物种比较"]},
    )
    pipe.register_batch("batch-human", "src-human", [FILE_A])
    pipe.register_batch("batch-mouse", "src-mouse", [FILE_B])
    pipe.lock_annotation("anno-2026q3", "OrthoDB", "12.1", ["免疫应答", "细胞周期"])
    pipe.define_filter_profile("filter-default", {"min_cells": 200, "max_mt": 0.15})
    return pipe


def run_analysis(pipe: Pipeline) -> dict:
    run = pipe.request_analysis(
        ["batch-human", "batch-mouse"], "anno-2026q3", "filter-default", SOFTWARE, shard_count=2
    )
    for shard_id in run["shards"]:
        pipe.complete_shard(run["run_id"], shard_id, result_digest="d" * 64)
    return run


def release_claim(pipe: Pipeline) -> dict:
    run_analysis(pipe)
    pipe.assert_correspondence(
        "corr-1", "run-0001", "人", "小鼠", "GENE-A", "Gene-a", "免疫应答功能对应"
    )
    pipe.approve("corr-1", reviewer="复核员甲", note="人工核对通过")
    return pipe.release_claim(
        "claim-1",
        ["corr-1"],
        statement="人与小鼠的免疫应答细胞群存在功能对应",
        uncertainty="基于两个批次，注释版本 12.1，置信有限",
    )


class RegistrationTests(unittest.TestCase):
    def test_duplicate_batch_submission_is_reused(self) -> None:
        pipe = build_pipeline()
        again = pipe.register_batch("batch-human", "src-human", [FILE_A])
        self.assertEqual(pipe.batches["batch-human"], again)
        self.assertEqual(1, len(pipe._events_of("batch-human", "BATCH_REGISTERED")))

    def test_same_id_with_different_files_is_isolated(self) -> None:
        pipe = build_pipeline()
        changed = {"path": FILE_A["path"], "sha256": "f" * 64}
        with self.assertRaises(ConflictError):
            pipe.register_batch("batch-human", "src-human", [changed])
        self.assertEqual("a" * 64, pipe.batches["batch-human"]["files"][0]["sha256"])

    def test_same_filter_id_with_different_params_is_isolated(self) -> None:
        pipe = build_pipeline()
        with self.assertRaises(ConflictError):
            pipe.define_filter_profile("filter-default", {"min_cells": 100})

    def test_batch_requires_registered_source_and_checksums(self) -> None:
        pipe = build_pipeline()
        with self.assertRaises(DomainError):
            pipe.register_batch("batch-x", "src-missing", [FILE_A])
        with self.assertRaises(DomainError):
            pipe.register_batch("batch-y", "src-human", [{"path": "p", "sha256": "bad"}])


class AnalysisRunTests(unittest.TestCase):
    def test_resubmission_reuses_run(self) -> None:
        pipe = build_pipeline()
        first = pipe.request_analysis(
            ["batch-human", "batch-mouse"], "anno-2026q3", "filter-default", SOFTWARE, 2
        )
        second = pipe.request_analysis(
            ["batch-mouse", "batch-human"], "anno-2026q3", "filter-default", SOFTWARE, 2
        )
        self.assertEqual(first["run_id"], second["run_id"])
        self.assertEqual(1, len(pipe._events_of(first["run_id"], "ANALYSIS_STARTED")))

    def test_different_params_or_software_are_isolated(self) -> None:
        pipe = build_pipeline()
        pipe.define_filter_profile("filter-strict", {"min_cells": 500})
        base = pipe.request_analysis(
            ["batch-human", "batch-mouse"], "anno-2026q3", "filter-default", SOFTWARE, 1
        )
        other_filter = pipe.request_analysis(
            ["batch-human", "batch-mouse"], "anno-2026q3", "filter-strict", SOFTWARE, 1
        )
        other_software = pipe.request_analysis(
            ["batch-human", "batch-mouse"],
            "anno-2026q3",
            "filter-default",
            {"name": "crossmap", "version": "2.4.0"},
            1,
        )
        self.assertEqual(3, len({base["run_id"], other_filter["run_id"], other_software["run_id"]}))

    def test_shard_retry_counts_once(self) -> None:
        pipe = build_pipeline()
        run = pipe.request_analysis(
            ["batch-human", "batch-mouse"], "anno-2026q3", "filter-default", SOFTWARE, 2
        )
        shard_id = next(iter(run["shards"]))
        first = pipe.complete_shard(run["run_id"], shard_id, "d" * 64)
        retry = pipe.complete_shard(run["run_id"], shard_id, "e" * 64)
        self.assertTrue(first["counted"])
        self.assertFalse(retry["counted"])
        self.assertFalse(retry["run_completed"])
        self.assertEqual(1, len(pipe._events_of(run["run_id"], "SHARD_COMPLETED")))
        for other in run["shards"]:
            if other != shard_id:
                pipe.complete_shard(run["run_id"], other, "d" * 64)
        self.assertEqual("completed", pipe.runs[run["run_id"]]["status"])
        self.assertEqual(2, run["shards"][shard_id]["attempts"])

    def test_correspondence_snapshots_inputs_and_software(self) -> None:
        pipe = build_pipeline()
        run_analysis(pipe)
        corr = pipe.assert_correspondence(
            "corr-1", "run-0001", "人", "小鼠", "GENE-A", "Gene-a", "功能对应"
        )
        self.assertEqual(SOFTWARE, corr["software"])
        self.assertEqual(
            {"batch-human", "batch-mouse"},
            {item["batch_id"] for item in corr["inputs"]["batches"]},
        )
        self.assertEqual("anno-2026q3", corr["inputs"]["annotation_id"])

    def test_correspondence_rejects_species_outside_run(self) -> None:
        pipe = build_pipeline()
        run_analysis(pipe)
        with self.assertRaises(DomainError):
            pipe.assert_correspondence(
                "corr-2", "run-0001", "斑马鱼", "小鼠", "G1", "G2", "超范围"
            )


class RevocationTests(unittest.TestCase):
    def test_revocation_rebuilds_only_affected_runs(self) -> None:
        pipe = build_pipeline()
        pipe.register_batch(
            "batch-human-2",
            "src-human",
            [{"path": "s3://seq/human/run2.fq.gz", "sha256": "1" * 64}],
        )
        affected = run_analysis(pipe)
        unaffected = pipe.request_analysis(
            ["batch-human-2", "batch-mouse"], "anno-2026q3", "filter-default", SOFTWARE, 1
        )
        pipe.complete_shard(unaffected["run_id"], next(iter(unaffected["shards"])), "d" * 64)
        result = pipe.revoke_batch("batch-human", reviewer="质控员", reason="批次污染")
        self.assertEqual([affected["run_id"]], result["rebuilt_runs"])
        self.assertTrue(pipe.runs[affected["run_id"]]["stale"])
        self.assertFalse(pipe.runs[unaffected["run_id"]]["stale"])
        self.assertEqual(1, len(pipe._events_of(affected["run_id"], "RANKING_REBUILT")))
        self.assertEqual(0, len(pipe._events_of(unaffected["run_id"], "RANKING_REBUILT")))

    def test_released_claim_is_retained_with_limitation(self) -> None:
        pipe = build_pipeline()
        release_claim(pipe)
        result = pipe.revoke_batch("batch-human", reviewer="质控员", reason="批次污染")
        self.assertEqual(["claim-1"], result["limited_claims"])
        self.assertIn("claim-1", pipe.claims)
        view = pipe.view_claim("claim-1", "public")
        self.assertEqual("批次污染", view["limitations"][0]["reason"])
        self.assertEqual(1, len(pipe._events_of("claim-1", "CLAIM_LIMITED")))

    def test_revoked_inputs_block_new_claims_and_reruns(self) -> None:
        pipe = build_pipeline()
        release_claim(pipe)
        pipe.revoke_batch("batch-human", reviewer="质控员", reason="批次污染")
        with self.assertRaises(DomainError):
            pipe.release_claim("claim-2", ["corr-1"], "陈述", "不确定性")
        with self.assertRaises(DomainError):
            pipe.request_analysis(
                ["batch-human", "batch-mouse"], "anno-2026q3", "filter-default", SOFTWARE, 1
            )


class ClaimGuardTests(unittest.TestCase):
    def test_claim_requires_approval_and_uncertainty(self) -> None:
        pipe = build_pipeline()
        run_analysis(pipe)
        pipe.assert_correspondence(
            "corr-1", "run-0001", "人", "小鼠", "GENE-A", "Gene-a", "功能对应"
        )
        with self.assertRaises(DomainError):
            pipe.release_claim("claim-1", ["corr-1"], "陈述", "不确定性")
        pipe.approve("corr-1", reviewer="复核员甲")
        with self.assertRaises(DomainError):
            pipe.release_claim("claim-1", ["corr-1"], "陈述", "")

    def test_correlation_never_becomes_individual_diagnosis(self) -> None:
        pipe = build_pipeline()
        run_analysis(pipe)
        pipe.assert_correspondence(
            "corr-1", "run-0001", "人", "小鼠", "GENE-A", "Gene-a", "功能对应"
        )
        pipe.approve("corr-1", reviewer="复核员甲")
        with self.assertRaises(DomainError):
            pipe.release_claim(
                "claim-1", ["corr-1"], "陈述", "不确定性", extra={"diagnosis": "某患者"}
            )
        with self.assertRaises(DomainError):
            pipe.release_claim("claim-1", ["corr-1"], "陈述", "不确定性", scope="individual")


class PublicationTests(unittest.TestCase):
    def test_restart_only_fills_missing_assets(self) -> None:
        pipe = build_pipeline()
        release_claim(pipe)
        first = pipe.publish_assets(
            "claim-1",
            [
                {"asset_id": "ranking-table", "kind": "table", "content_digest": "1" * 64},
                {"asset_id": "similarity-chart", "kind": "chart", "content_digest": "2" * 64},
            ],
        )
        self.assertEqual(["ranking-table", "similarity-chart"], first["published"])
        restarted = pipe.publish_assets(
            "claim-1",
            [
                {"asset_id": "similarity-chart", "kind": "chart", "content_digest": "2" * 64},
                {"asset_id": "methods-note", "kind": "note", "content_digest": "3" * 64},
            ],
        )
        self.assertEqual(["methods-note"], restarted["published"])
        self.assertEqual(["similarity-chart"], restarted["skipped"])
        self.assertEqual([], pipe.missing_assets("claim-1", ["ranking-table", "similarity-chart", "methods-note"]))
        self.assertEqual(3, len([e for e in pipe.events if e["event_type"] == "ASSET_PUBLISHED"]))


class VisibilityAndTraceTests(unittest.TestCase):
    def test_roles_see_different_fields(self) -> None:
        pipe = build_pipeline()
        release_claim(pipe)
        public = pipe.view_claim("claim-1", "public")
        self.assertIn("uncertainty", public)
        self.assertNotIn("correspondences", public)
        self.assertNotIn("approvals", public)
        reviewer = pipe.view_claim("claim-1", "reviewer")
        self.assertEqual("复核员甲", reviewer["approvals"][0]["reviewer"])
        self.assertNotIn("files", json.dumps(reviewer, ensure_ascii=False))
        researcher = pipe.view_claim("claim-1", "researcher")
        files = researcher["correspondences"][0]["batches"][0]["files"]
        self.assertEqual(FILE_A, files[0])
        with self.assertRaises(DomainError):
            pipe.view_claim("claim-1", "anonymous")

    def test_claim_traces_back_to_files_versions_approvers_and_uncertainty(self) -> None:
        pipe = build_pipeline()
        release_claim(pipe)
        trace = pipe.trace_claim("claim-1")
        corr = trace["correspondences"][0]
        self.assertEqual(FILE_A["sha256"], corr["batches"][0]["files"][0]["sha256"])
        self.assertEqual("12.1", corr["annotation"]["version"])
        self.assertEqual({"min_cells": 200, "max_mt": 0.15}, corr["filter_profile"]["params"])
        self.assertEqual("2.3.1", corr["run"]["software"]["version"])
        self.assertEqual("复核员甲", corr["approvals"][0]["reviewer"])
        self.assertEqual("基于两个批次，注释版本 12.1，置信有限", trace["uncertainty"])


class EventContractTests(unittest.TestCase):
    def test_all_pipeline_events_satisfy_contract(self) -> None:
        schema = json.loads((ROOT / "contracts" / "domain.schema.json").read_text(encoding="utf-8"))
        pipe = build_pipeline()
        release_claim(pipe)
        pipe.publish_assets(
            "claim-1", [{"asset_id": "ranking-table", "kind": "table", "content_digest": "1" * 64}]
        )
        pipe.revoke_batch("batch-human", reviewer="质控员", reason="批次污染")
        self.assertTrue(pipe.events)
        for event in pipe.events:
            self.assertEqual([], validate_event(event, schema), event["event_id"])
        versions = {}
        for event in pipe.events:
            key = (event["aggregate_type"], event["aggregate_id"])
            versions[key] = versions.get(key, 0) + 1
            self.assertEqual(versions[key], event["version"])


if __name__ == "__main__":
    unittest.main()
