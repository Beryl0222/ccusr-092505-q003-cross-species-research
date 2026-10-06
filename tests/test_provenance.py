"""分角色字段范围与结论溯源链测试。"""

from __future__ import annotations

import unittest

from helpers import (
    ASSETS,
    approve,
    release_claim,
    standard_pipeline,
    submit_completed_run,
)

from cross_species_research.pipeline import PipelineError
from cross_species_research.provenance import project_claim, trace_claim


def _full_scenario(*, visibility: str = "public"):
    p = standard_pipeline()
    run_id = submit_completed_run(p)
    approve(p, run_id)
    release_claim(p, run_id=run_id, visibility=visibility)
    p.publish_assets("CLAIM-MGL-2026-01", ASSETS)
    return p, run_id


class RoleViewTests(unittest.TestCase):
    def test_public_view_hides_sensitive_fields(self) -> None:
        p, run_id = _full_scenario()
        view = project_claim(p, "CLAIM-MGL-2026-01", "public")
        self.assertNotIn("runs", view)
        self.assertNotIn("batches", view)
        self.assertNotIn("filter_params", json_dumps(view))
        self.assertNotIn("restrictions", json_dumps(view))
        self.assertNotIn("approved_by", view)  # 仅给数量，不暴露姓名
        self.assertEqual(1, view["peer_review"]["reviewer_count"])
        # 诊断禁令对公众显式呈现。
        self.assertFalse(view["individual_diagnosis_allowed"])
        self.assertIn("个体诊断", view["diagnostics_disclaimer"])
        # 对应关系公开版只呈现功能与物种，不暴露批次/软件细节。
        link_view = view["correspondences"][0]
        self.assertEqual(["Danio rerio"], link_view["species"]["right"])
        self.assertNotIn("link", link_view)
        # 资产仍可凭校验和核验。
        self.assertEqual("sha256:", view["assets"][0]["checksum"][:7])

    def test_researcher_view_contains_reproducibility_fields(self) -> None:
        p, run_id = _full_scenario(visibility="researcher")
        view = project_claim(p, "CLAIM-MGL-2026-01", "researcher")
        run = view["runs"][run_id]
        self.assertEqual(200, run["spec"]["filter_params"]["min_genes"])
        self.assertEqual("2026-09", run["spec"]["annotation"]["version"])
        self.assertIn("scanpy", {s["name"] for s in run["spec"]["software"]})
        self.assertEqual("reviewer.sun", view["approved_by"][0])
        # 对应关系引用具体运行、产出与软件配置。
        link = view["correspondences"][0]["link"]
        self.assertEqual(run_id, link["left"]["run_id"])
        self.assertEqual("ranking.csv", link["right"]["output_ref"])
        # 研究者不看到审计细节。
        self.assertNotIn("source_events", view)
        self.assertNotIn("restrictions", json_dumps(view["batches"]))

    def test_reviewer_view_contains_audit_fields(self) -> None:
        p, run_id = _full_scenario(visibility="researcher")
        view = project_claim(p, "CLAIM-MGL-2026-01", "reviewer")
        self.assertIn("source_events", view)
        self.assertIn("verification", view["batches"]["B-HUMAN"])
        self.assertTrue(
            all(f["match"] for f in view["batches"]["B-HUMAN"]["verification"]["files"])
        )
        self.assertIn("checklist", view["runs"][run_id]["approval"])

    def test_public_cannot_view_researcher_only_claim(self) -> None:
        p, _ = _full_scenario(visibility="reviewer")
        with self.assertRaisesRegex(PipelineError, "access_denied"):
            project_claim(p, "CLAIM-MGL-2026-01", "public")
        # reviewer 角色可看。
        self.assertEqual("CLAIM-MGL-2026-01",
                         project_claim(p, "CLAIM-MGL-2026-01", "reviewer")["claim_id"])

    def test_flagged_claim_shows_restriction_to_all_permitted_roles(self) -> None:
        p, run_id = _full_scenario()
        p.revoke_batch("B-ZFISH", revoked_by="qc.lead", reason="标签错误")
        public_view = project_claim(p, "CLAIM-MGL-2026-01", "public")
        self.assertTrue(public_view["restriction"]["restricted"])
        self.assertTrue(public_view["restriction"]["history_retained"])
        reviewer_view = project_claim(p, "CLAIM-MGL-2026-01", "reviewer")
        self.assertIn("detail", reviewer_view["restriction"])


class TraceTests(unittest.TestCase):
    def test_trace_reaches_raw_files_annotation_reviewer_and_uncertainty(self) -> None:
        p, run_id = _full_scenario()
        trace = trace_claim(p, "CLAIM-MGL-2026-01")
        claim = trace["claim"]
        self.assertEqual("medium", claim["uncertainty"]["level"])
        self.assertIn("排名", claim["uncertainty"]["statement"])
        self.assertEqual("pi.zhou", claim["released_by"])
        # 审批人
        review_steps = [s for s in trace["evidence_chain"] if s["step"] == "manual_review"]
        self.assertEqual("reviewer.sun", review_steps[0]["reviewer"])
        # 原始文件与注释版本
        raw_steps = [s for s in trace["evidence_chain"] if s["step"] == "raw_files"]
        all_files = [f for s in raw_steps for f in s["files"]]
        self.assertTrue(all(f["verified_match"] for f in all_files))
        self.assertEqual(3, len(all_files))  # 人 2 个 + 鱼 1 个
        self.assertEqual("2026-09", trace["annotations"]["ANN-HOMOLOG-2026_09"]["version"])
        # 资产与对应关系在溯源中可查
        self.assertEqual(2, len(trace["assets"]))
        self.assertEqual("computed_correspondence", trace["correspondences"][0]["evidence_nature"])

    def test_trace_distinguishes_observed_from_computed(self) -> None:
        p, _ = _full_scenario()
        trace = trace_claim(p, "CLAIM-MGL-2026-01")
        natures = {step["evidence_nature"] for step in trace["evidence_chain"]
                   if "evidence_nature" in step}
        self.assertIn("observed_raw_file", natures)
        self.assertIn("manual_review", natures)
        self.assertIn("computed", natures)
        # 图例解释每类证据性质。
        self.assertIn("真实观测", trace["legend"]["observed_raw_file"])
        self.assertIn("计算推断", trace["legend"]["computed"])

    def test_trace_marks_invalidated_run_after_revocation(self) -> None:
        p, _ = _full_scenario()
        p.revoke_batch("B-ZFISH", revoked_by="qc.lead", reason="污染")
        trace = trace_claim(p, "CLAIM-MGL-2026-01")
        self.assertIsNotNone(trace["claim"]["restriction"])
        run_steps = [s for s in trace["evidence_chain"] if s["step"] == "run"]
        self.assertFalse(run_steps[0]["current"])

    def test_correspondence_endpoints_are_grounded_in_trace(self) -> None:
        p, run_id = _full_scenario()
        trace = trace_claim(p, "CLAIM-MGL-2026-01")
        link = trace["correspondences"][0]["link"]
        for side in ("left", "right"):
            endpoint = link[side]
            self.assertEqual(run_id, endpoint["run_id"])
            self.assertTrue(endpoint["software_cited"])
            # 引用的产出确实存在于该运行
            refs = {o["ref"] for o in trace["runs"][run_id]["outputs"]}
            self.assertIn(endpoint["output_ref"], refs)


def json_dumps(value) -> str:
    import json
    return json.dumps(value, ensure_ascii=False, default=str)


if __name__ == "__main__":
    unittest.main()
