"""结论发布门禁、资产幂等与重启补齐测试。"""

from __future__ import annotations

import hashlib
import os
import tempfile
import unittest

from helpers import (
    ASSETS,
    approve,
    release_claim,
    standard_pipeline,
    submit_completed_run,
)

from cross_species_research.pipeline import PipelineError


class ReleaseGateTests(unittest.TestCase):
    def _ready(self):
        p = standard_pipeline()
        run_id = submit_completed_run(p)
        return p, run_id

    def test_release_requires_approval(self) -> None:
        p, run_id = self._ready()
        with self.assertRaisesRegex(PipelineError, "evidence_not_approved"):
            release_claim(p, run_id=run_id)

    def test_release_requires_uncertainty_statement(self) -> None:
        p, run_id = self._ready()
        approve(p, run_id)
        with self.assertRaisesRegex(PipelineError, "uncertainty_required"):
            p.release_claim(
                claim_id="C1", title="t", statement="s", run_ids=[run_id],
                uncertainty={"level": "weird"},
                released_by="pi.zhou", visibility="public",
            )

    def test_release_requires_complete_run(self) -> None:
        p = standard_pipeline()
        p.submit_run(
            label="microglia-match",
            batch_ids=["B-HUMAN", "B-ZFISH"],
            annotation_id="ANN-HOMOLOG-2026_09",
            filter_params={"min_genes": 200, "max_mito_pct": 10.0, "min_cells": 3,
                           "doublet_threshold": 0.25},
            software=[
                {"name": "scanpy", "version": "1.10.2", "config": {"resolution": 0.8}},
                {"name": "orthomap", "version": "0.4.1", "config": {"seed": 7}},
            ],
            model_config={"name": "m", "version": "1"},
            submitted_by="a", request_id="req",
        )
        with self.assertRaisesRegex(PipelineError, "evidence_not_completed"):
            release_claim(p, run_id="microglia-match")

    def test_public_release_requires_public_consent(self) -> None:
        from helpers import human_batch, register_verified
        p = standard_pipeline(second_batch=False)
        # 人批次已默认 public；用一个禁止公开发布的批次替换语义。
        register_verified(
            p, "B-PRIVATE", "Homo sapiens", "私有捐献小胶质细胞",
            [human_batch()[1][0]], public=False,
        )
        result = p.submit_run(
            label="private-run", batch_ids=["B-PRIVATE"],
            annotation_id="ANN-HOMOLOG-2026_09",
            filter_params={"min_genes": 200},
            software=[{"name": "scanpy", "version": "1.10", "config": {}}],
            model_config={"name": "m"},
            submitted_by="a", request_id="req-priv",
        )
        rid = result["run_id"]
        p.commit_shard(rid, shard_id="s1", item_count=5, payload={"x": 1},
                       committed_by="w", request_id="req-ps")
        p.complete_run(rid, outputs=[{"ref": "o", "kind": "ranking"}])
        approve(p, rid)
        with self.assertRaisesRegex(PipelineError, "public_release_not_consented"):
            release_claim(p, claim_id="C-PRIV", run_id=rid, visibility="public")
        # 研究者可见范围可以发布。
        event = release_claim(
            p, claim_id="C-PRIV", run_id=rid, visibility="researcher",
            with_correspondence=False,
        )
        self.assertEqual("CLAIM_RELEASED", event["event_type"])

    def test_correspondence_must_cite_run_output_and_software(self) -> None:
        p, run_id = self._ready()
        approve(p, run_id)
        bad_link = {
            "correspondence_id": "C-BAD",
            "functional_description": "x",
            "link": {
                "left": {"run_id": run_id, "output_ref": "ranking.csv",
                         "software_cited": ["scanpy@1.10.2"]},
                "right": {"run_id": run_id, "output_ref": "ranking.csv"},
            },
        }
        with self.assertRaisesRegex(PipelineError, "software_citation_required"):
            p.release_claim(
                claim_id="C1", title="t", statement="s", run_ids=[run_id],
                correspondences=[bad_link],
                uncertainty={"level": "low", "statement": "ok"},
                released_by="pi.zhou",
            )

    def test_correspondence_citing_unknown_output_is_rejected(self) -> None:
        p, run_id = self._ready()
        approve(p, run_id)
        bad = {
            "correspondence_id": "C-BAD",
            "functional_description": "x",
            "link": {
                "left": {"run_id": run_id, "output_ref": "ranking.csv",
                         "software_cited": ["scanpy@1.10.2"]},
                "right": {"run_id": run_id, "output_ref": "ghost.png",
                          "software_cited": ["scanpy@1.10.2"]},
            },
        }
        with self.assertRaisesRegex(PipelineError, "citation_output_missing"):
            p.release_claim(
                claim_id="C1", title="t", statement="s", run_ids=[run_id],
                correspondences=[bad],
                uncertainty={"level": "low", "statement": "ok"},
                released_by="pi.zhou",
            )

    def test_release_carries_diagnostics_prohibition(self) -> None:
        p, run_id = self._ready()
        approve(p, run_id)
        event = release_claim(p, run_id=run_id)
        self.assertFalse(event["individual_diagnosis_allowed"])
        self.assertIn("个体诊断", event["diagnostics_disclaimer"])


class AssetPublishTests(unittest.TestCase):
    def _published(self, root: str | None = None):
        p = standard_pipeline(asset_root=root)
        run_id = submit_completed_run(p)
        approve(p, run_id)
        release_claim(p, run_id=run_id)
        return p, p.publish_assets("CLAIM-MGL-2026-01", ASSETS)

    def test_assets_are_published_with_checksums_and_sources(self) -> None:
        p, result = self._published()
        self.assertEqual([], result["reused"])
        self.assertEqual(2, len(p.claims["CLAIM-MGL-2026-01"].assets))
        for asset in p.claims["CLAIM-MGL-2026-01"].assets.values():
            self.assertTrue(asset["checksum"].startswith("sha256:"))
            self.assertEqual("microglia-match", asset["source_run_id"])

    def test_republish_only_reuses_existing_assets(self) -> None:
        p, _ = self._published()
        again = p.publish_assets("CLAIM-MGL-2026-01", ASSETS)
        self.assertEqual([], again["materialized"])
        self.assertEqual({"fig1-similarity", "tab1-correspondence"}, set(again["reused"]))
        events = [e for e in p.store.stream("research_claim", "CLAIM-MGL-2026-01")
                  if e["event_type"] == "CLAIM_ASSET_PUBLISHED"]
        self.assertEqual(2, len(events))

    def test_restart_backfills_only_missing_asset_files(self) -> None:
        from cross_species_research.events import EventStore
        from cross_species_research.pipeline import EvidencePipeline
        from helpers import load_schema
        with tempfile.TemporaryDirectory() as root:
            p, _ = self._published(root=root)
            fig_path = os.path.join(root, "CLAIM-MGL-2026-01", "fig1-similarity.figure.json")
            tab_path = os.path.join(root, "CLAIM-MGL-2026-01", "tab1-correspondence.table.json")
            self.assertTrue(os.path.exists(fig_path))
            os.remove(fig_path)  # 模拟发布中途宕机丢失一个文件。
            events = p.store.export()

            restarted = EvidencePipeline(
                EventStore(events), schema=load_schema(), asset_root=root,
            )
            result = restarted.publish_assets("CLAIM-MGL-2026-01", ASSETS)
            self.assertEqual(["fig1-similarity"], result["materialized"])
            self.assertEqual(["tab1-correspondence"], result["reused"])
            self.assertTrue(os.path.exists(fig_path))
            self.assertTrue(os.path.exists(tab_path))

    def test_existing_asset_with_wrong_checksum_is_not_overwritten(self) -> None:
        with tempfile.TemporaryDirectory() as root:
            p, _ = self._published(root=root)
            fig_path = os.path.join(root, "CLAIM-MGL-2026-01", "fig1-similarity.figure.json")
            with open(fig_path, "wb") as handle:
                handle.write(b"tampered")
            with self.assertRaisesRegex(PipelineError, "asset_corrupted"):
                p.publish_assets("CLAIM-MGL-2026-01", ASSETS)

    def test_assets_must_cite_existing_output(self) -> None:
        p = standard_pipeline()
        run_id = submit_completed_run(p)
        approve(p, run_id)
        release_claim(p, run_id=run_id)
        bad = [{"asset_id": "a1", "kind": "figure",
                "source_run_id": run_id, "source_output_ref": "missing.png"}]
        with self.assertRaisesRegex(PipelineError, "asset_source_missing"):
            p.publish_assets("CLAIM-MGL-2026-01", bad)

    def test_flagged_claim_blocks_new_assets(self) -> None:
        p = standard_pipeline()
        run_id = submit_completed_run(p)
        approve(p, run_id)
        release_claim(p, run_id=run_id)
        p.revoke_batch("B-ZFISH", revoked_by="qc.lead", reason="污染")
        with self.assertRaisesRegex(PipelineError, "claim_flagged"):
            p.publish_assets("CLAIM-MGL-2026-01", ASSETS)

    def test_superseded_claim_is_flagged_but_history_retained(self) -> None:
        from helpers import UNCERTAINTY
        p = standard_pipeline()
        run_v1 = submit_completed_run(p, run_label="match", request_id="req-v1")
        approve(p, run_v1)
        release_claim(p, claim_id="CLAIM-OLD", run_id=run_v1)
        # 用新阈值产生隔离运行，再发布替代结论。
        result = p.submit_run(
            label="match", batch_ids=["B-HUMAN", "B-ZFISH"],
            annotation_id="ANN-HOMOLOG-2026_09",
            filter_params={"min_genes": 300, "max_mito_pct": 5.0, "min_cells": 3,
                           "doublet_threshold": 0.25},
            software=[
                {"name": "scanpy", "version": "1.10.2", "config": {"n_top_genes": 2000,
                                                                    "resolution": 0.8}},
                {"name": "orthomap", "version": "0.4.1",
                 "config": {"aligner": "embedding-cosine", "seed": 7}},
            ],
            model_config={"name": "cross-species-embedder", "version": "3.1", "seed": 7},
            submitted_by="analyst.zhao", request_id="req-v2",
        )
        run_v2 = result["run_id"]
        p.commit_shard(run_v2, shard_id="s1", item_count=10, payload={"x": 1},
                       committed_by="w", request_id="req-v2-s1")
        p.complete_run(run_v2, outputs=[{"ref": "ranking.csv", "kind": "ranking"}])
        approve(p, run_v2, reviewer="reviewer.ma")
        p.release_claim(
            claim_id="CLAIM-NEW", title="修订结论", statement="s",
            run_ids=[run_v2], uncertainty=dict(UNCERTAINTY),
            released_by="pi.zhou", visibility="researcher",
            supersedes=["CLAIM-OLD"],
        )
        old = p.claims["CLAIM-OLD"]
        self.assertEqual("CLAIM-NEW", old.superseded_by)
        self.assertTrue(old.flag["history_retained"])


if __name__ == "__main__":
    unittest.main()
