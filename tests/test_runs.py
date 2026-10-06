"""运行提交指纹复用、同标识隔离与分片幂等测试。"""

from __future__ import annotations

import unittest

from helpers import (
    FILTERS,
    MODEL,
    SOFTWARE,
    standard_pipeline,
)

from cross_species_research.pipeline import PipelineError


class RunFingerprintTests(unittest.TestCase):
    def _submit(self, pipeline, *, label="microglia-match", request_id="req-a", **overrides):
        params = dict(
            label=label,
            batch_ids=["B-HUMAN", "B-ZFISH"],
            annotation_id="ANN-HOMOLOG-2026_09",
            filter_params=FILTERS,
            software=SOFTWARE,
            model_config=MODEL,
            submitted_by="analyst.zhao",
            request_id=request_id,
        )
        params.update(overrides)
        return pipeline.submit_run(**params)

    def test_identical_batch_reuses_run_by_fingerprint(self) -> None:
        p = standard_pipeline()
        first = self._submit(p, request_id="req-a")
        second = self._submit(p, request_id="req-b")  # 不同 request_id 也应识别
        self.assertFalse(first["reused"])
        self.assertTrue(second["reused"])
        self.assertEqual("fingerprint", second["reason"])
        self.assertEqual(first["run_id"], second["run_id"])
        # 复用不产生第二条 ANALYSIS_STARTED 事件。
        started = [e for e in p.store.stream("analysis_run") if e["event_type"] == "ANALYSIS_STARTED"]
        self.assertEqual(1, len(started))

    def test_repeated_request_id_reuses_even_if_caller_changes_label(self) -> None:
        p = standard_pipeline()
        first = self._submit(p, label="label-a", request_id="req-dup")
        second = self._submit(p, label="label-b", request_id="req-dup")
        self.assertEqual(first["run_id"], second["run_id"])
        self.assertTrue(second["reused"])

    def test_same_request_id_with_different_content_is_conflict(self) -> None:
        p = standard_pipeline()
        self._submit(p, request_id="req-conflict")
        with self.assertRaisesRegex(PipelineError, "idempotency_conflict"):
            self._submit(
                p,
                request_id="req-conflict",
                filter_params={**FILTERS, "min_genes": 500},
            )

    def test_same_label_but_different_filters_is_isolated(self) -> None:
        p = standard_pipeline()
        first = self._submit(p, label="ranking", request_id="req-1")
        second = self._submit(
            p, label="ranking", request_id="req-2",
            filter_params={**FILTERS, "max_mito_pct": 5.0},
        )
        self.assertNotEqual(first["run_id"], second["run_id"])
        self.assertEqual("ranking", first["run_id"])
        self.assertTrue(second["run_id"].startswith("ranking~iso"))
        self.assertNotEqual(first["run_fingerprint"], second["run_fingerprint"])
        self.assertEqual(2, len(p.runs))
        # 两个运行互不覆盖，规格各自保留。
        self.assertEqual(10.0, p.runs[first["run_id"]].spec["filter_params"]["max_mito_pct"])
        self.assertEqual(5.0, p.runs[second["run_id"]].spec["filter_params"]["max_mito_pct"])

    def test_same_label_but_different_annotation_version_is_isolated(self) -> None:
        p = standard_pipeline()
        p.lock_annotation(
            annotation_id="ANN-2026_10",
            source="HomoloGene+CellOntology",
            version="2026-10",
            ontology="CL:0000129",
            locked_by="annotator.chen",
        )
        first = self._submit(p, label="ranking", request_id="req-1")
        second = self._submit(
            p, label="ranking", request_id="req-2",
            annotation_id="ANN-2026_10",
        )
        self.assertNotEqual(first["run_id"], second["run_id"])
        self.assertTrue(second["run_id"].startswith("ranking~iso"))

    def test_same_label_but_dependent_file_changes_is_isolated(self) -> None:
        # 重新登记同标识批次但文件不同（新批次号），运行指纹必须变化。
        p = standard_pipeline()
        first = self._submit(p, batch_ids=["B-HUMAN", "B-ZFISH"], request_id="req-1")
        from helpers import file_entry, register_verified
        register_verified(
            p, "B-ZFISH-V2", "Danio rerio", "脑组织小胶质样细胞",
            [file_entry("z-rna-001", "zebrafish-updated-content-v2", "zf_mgl_v2.h5")],
        )
        second = self._submit(
            p, label="microglia-match", request_id="req-2",
            batch_ids=["B-HUMAN", "B-ZFISH-V2"],
        )
        self.assertNotEqual(first["run_fingerprint"], second["run_fingerprint"])
        self.assertTrue(second["run_id"].startswith("microglia-match~iso"))

    def test_software_config_change_changes_fingerprint(self) -> None:
        p = standard_pipeline()
        first = self._submit(p, request_id="req-1")
        changed = [
            {**s, "config": {**s["config"], "seed": 99}} if s["name"] == "orthomap" else s
            for s in SOFTWARE
        ]
        second = self._submit(p, request_id="req-2", software=changed)
        self.assertNotEqual(first["run_fingerprint"], second["run_fingerprint"])


class ShardTests(unittest.TestCase):
    def _run_with_one_shard(self):
        from helpers import standard_pipeline, submit_completed_run
        p = standard_pipeline()
        p.submit_run(
            label="microglia-match",
            batch_ids=["B-HUMAN", "B-ZFISH"],
            annotation_id="ANN-HOMOLOG-2026_09",
            filter_params=FILTERS,
            software=SOFTWARE,
            model_config=MODEL,
            submitted_by="analyst.zhao",
            request_id="req-run",
        )
        return p

    def test_shard_retry_is_idempotent_and_not_double_counted(self) -> None:
        p = self._run_with_one_shard()
        payload = {"rows": list(range(500))}
        first = p.commit_shard(
            "microglia-match", shard_id="s1", item_count=500, payload=payload,
            committed_by="w", request_id="req-s1",
        )
        # 网络重试：同 shard_id、同内容，不同 request_id 也必须识别为重复。
        retry = p.commit_shard(
            "microglia-match", shard_id="s1", item_count=500, payload=payload,
            committed_by="w", request_id="req-s1-retry",
        )
        self.assertFalse(first["reused"])
        self.assertTrue(retry["reused"])
        p.commit_shard(
            "microglia-match", shard_id="s2", item_count=300,
            payload={"rows": list(range(300))}, committed_by="w", request_id="req-s2",
        )
        p.complete_run(
            "microglia-match",
            outputs=[{"ref": "o1", "kind": "ranking", "checksum": "sha256:" + "a" * 64}],
        )
        run = p.runs["microglia-match"]
        self.assertEqual(2, len(run.shards))
        self.assertEqual(800, run.total_items)  # 不是 1300
        committed = [e for e in p.store.stream("analysis_run", "microglia-match")
                     if e["event_type"] == "SHARD_COMMITTED"]
        self.assertEqual(2, len(committed))

    def test_same_shard_id_with_different_payload_is_conflict(self) -> None:
        p = self._run_with_one_shard()
        p.commit_shard(
            "microglia-match", shard_id="s1", item_count=500,
            payload={"v": 1}, committed_by="w", request_id="req-s1",
        )
        with self.assertRaisesRegex(PipelineError, "shard_conflict"):
            p.commit_shard(
                "microglia-match", shard_id="s1", item_count=500,
                payload={"v": 2}, committed_by="w", request_id="req-s1b",
            )

    def test_completing_without_shards_is_rejected(self) -> None:
        p = self._run_with_one_shard()
        with self.assertRaisesRegex(PipelineError, "no_shards"):
            p.complete_run(
                "microglia-match",
                outputs=[{"ref": "o1", "kind": "ranking"}],
            )

    def test_full_happy_path_completes(self) -> None:
        from helpers import submit_completed_run
        p = standard_pipeline()
        run_id = submit_completed_run(p)
        self.assertEqual("completed", p.runs[run_id].status)
        self.assertEqual(2000, p.runs[run_id].total_items)


if __name__ == "__main__":
    unittest.main()
