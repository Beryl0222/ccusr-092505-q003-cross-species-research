"""复核撤回、选择性重建与历史保留测试。"""

from __future__ import annotations

import unittest

from helpers import (
    FILTERS,
    MODEL,
    SOFTWARE,
    approve,
    file_entry,
    register_verified,
    release_claim,
    standard_pipeline,
    submit_completed_run,
)

from cross_species_research.pipeline import PipelineError


class RevocationTests(unittest.TestCase):
    def _two_runs_and_claim(self):
        """run A 消费两个批次；run B 只消费人批次；结论引用 run A。"""
        p = standard_pipeline()
        run_a = submit_completed_run(p, run_label="microglia-match", request_id="req-a")
        run_b = submit_completed_run(
            p, run_label="human-only-ranking", batch_ids=["B-HUMAN"],
            request_id="req-b",
        )
        approve(p, run_a)
        approve(p, run_b, reviewer="reviewer.ma")
        release_claim(p, run_id=run_a)
        return p, run_a, run_b

    def test_batch_revocation_only_invalidates_affected_runs(self) -> None:
        p, run_a, run_b = self._two_runs_and_claim()
        p.revoke_batch("B-ZFISH", revoked_by="qc.lead", reason="发现样本标签错误")
        self.assertIsNotNone(p.runs[run_a].invalidated)
        self.assertEqual("upstream_batch_revoked", p.runs[run_a].invalidated["cause"])
        # 未消费该批次的运行保持有效。
        self.assertIsNone(p.runs[run_b].invalidated)
        self.assertEqual("revoked", p.batches["B-ZFISH"].status)
        # 受影响结论只被标注限制，历史事件与资产记录保留。
        claim = p.claims["CLAIM-MGL-2026-01"]
        self.assertIsNotNone(claim.flag)
        self.assertTrue(claim.flag["history_retained"])
        self.assertIn("B-ZFISH", claim.flag["detail"]["batch_id"])
        self.assertIn(run_a, claim.flag["detail"]["affected_runs"])

    def test_paper_history_retains_assets_and_events_after_revocation(self) -> None:
        p, run_a, _ = self._two_runs_and_claim()
        from helpers import ASSETS
        p.publish_assets("CLAIM-MGL-2026-01", ASSETS)
        before = p.store.export()
        p.revoke_batch("B-ZFISH", revoked_by="qc.lead", reason="样本污染")
        after = p.store.export()
        self.assertEqual(before, after[: len(before)])  # 只追加，不改写历史
        self.assertEqual(2, len(p.claims["CLAIM-MGL-2026-01"].assets))

    def test_rebuild_creates_new_run_with_replacement_batch(self) -> None:
        p, run_a, run_b = self._two_runs_and_claim()
        p.revoke_batch("B-ZFISH", revoked_by="qc.lead", reason="样本标签错误")
        register_verified(
            p, "B-ZFISH-FIX", "Danio rerio", "脑组织小胶质样细胞",
            [file_entry("z-rna-101", "zebrafish-relabeled-clean", "zf_clean_r1.h5")],
        )
        result = p.rerun_invalidated(
            old_run_id=run_a,
            submitted_by="analyst.zhao",
            request_id="req-rebuild-1",
            batch_replacements={"B-ZFISH": "B-ZFISH-FIX"},
        )
        new_id = result["run_id"]
        self.assertNotEqual(run_a, new_id)
        self.assertIn("rebuild", new_id)
        self.assertEqual(["B-HUMAN", "B-ZFISH-FIX"], p.runs[new_id].batch_ids)
        # 旧运行仍在（历史保留），新运行可独立完成与审批。
        self.assertIsNotNone(p.runs[run_a].invalidated)
        p.commit_shard(
            new_id, shard_id="s1", item_count=10, payload={"rows": list(range(10))},
            committed_by="w", request_id="req-rb-s1",
        )
        p.complete_run(
            new_id,
            outputs=[
                {"ref": "ranking.csv", "kind": "ranking"},
                {"ref": "correspondences.json", "kind": "correspondence"},
            ],
        )
        approve(p, new_id)
        # 未受影响的运行不需要重建。
        self.assertIsNone(p.runs[run_b].invalidated)

    def test_cannot_commit_shards_to_invalidated_run(self) -> None:
        p, run_a, _ = self._two_runs_and_claim()
        p.revoke_batch("B-ZFISH", revoked_by="qc.lead", reason="x")
        with self.assertRaisesRegex(PipelineError, "run_invalidated"):
            p.commit_shard(
                run_a, shard_id="s9", item_count=1, payload={"x": 1},
                committed_by="w", request_id="req-x",
            )

    def test_run_level_revocation_flags_claim_but_leaves_batch_usable(self) -> None:
        p, run_a, run_b = self._two_runs_and_claim()
        p.revoke_run(run_a, revoked_by="reviewer.sun", reason="复核发现参数选择错误")
        self.assertEqual("verified", p.batches["B-ZFISH"].status)
        self.assertIsNone(p.runs[run_b].invalidated)
        self.assertIsNotNone(p.claims["CLAIM-MGL-2026-01"].flag)

    def test_double_revocation_is_rejected(self) -> None:
        p, run_a, _ = self._two_runs_and_claim()
        p.revoke_batch("B-ZFISH", revoked_by="qc.lead", reason="x")
        with self.assertRaisesRegex(PipelineError, "batch_revoked"):
            p.revoke_batch("B-ZFISH", revoked_by="qc.lead", reason="y")

    def test_restart_rebuilds_state_from_event_log(self) -> None:
        from cross_species_research.events import EventStore
        from cross_species_research.pipeline import EvidencePipeline
        from helpers import load_schema
        p, run_a, run_b = self._two_runs_and_claim()
        p.revoke_batch("B-ZFISH", revoked_by="qc.lead", reason="标签错误")
        events = p.store.export()

        restarted = EvidencePipeline(EventStore(events), schema=load_schema())
        self.assertIsNotNone(restarted.runs[run_a].invalidated)
        self.assertIsNone(restarted.runs[run_b].invalidated)
        self.assertIsNotNone(restarted.claims["CLAIM-MGL-2026-01"].flag)
        # 幂等表不持久化时，同指纹提交仍能被读模型识别并复用。
        again = restarted.submit_run(
            label="microglia-match",
            batch_ids=["B-HUMAN"],
            annotation_id="ANN-HOMOLOG-2026_09",
            filter_params=FILTERS,
            software=SOFTWARE,
            model_config=MODEL,
            submitted_by="analyst.zhao",
            request_id="req-after-restart",
        )
        self.assertEqual(run_b, again["run_id"])
        self.assertTrue(again["reused"])


if __name__ == "__main__":
    unittest.main()
