"""登记、文件校验、授权与注释版本的门禁测试。"""

from __future__ import annotations

import unittest

from helpers import (
    FILTERS,
    MODEL,
    SOFTWARE,
    consent,
    file_entry,
    file_hashes,
    human_batch,
    new_pipeline,
    register_verified,
    sha256_text,
)

from cross_species_research.pipeline import PipelineError, RESEARCH_PURPOSE


class RegistrationTests(unittest.TestCase):
    def test_registers_species_source_consent_and_file_checksums(self) -> None:
        p = new_pipeline()
        bid, files = human_batch()
        register_verified(p, bid, "Homo sapiens", "成年大脑小胶质细胞", files)
        batch = p.batches[bid]
        self.assertEqual("Homo sapiens", batch.species)
        self.assertEqual("成年大脑小胶质细胞", batch.cell_source)
        self.assertEqual("verified", batch.status)
        self.assertEqual(2, len(batch.files))
        self.assertTrue(all(v["match"] for v in batch.verification.values()))
        self.assertIn(RESEARCH_PURPOSE, batch.consent.allowed_purposes)

    def test_duplicate_batch_id_is_rejected(self) -> None:
        p = new_pipeline()
        bid, files = human_batch()
        register_verified(p, bid, "Homo sapiens", "小胶质细胞", files)
        with self.assertRaisesRegex(PipelineError, "batch_exists"):
            p.register_batch(
                batch_id=bid,
                species="other",
                cell_source="other",
                collection_lab="lab-x",
                files=files,
                consent=consent(),
                registered_by="curator.li",
            )

    def test_bad_checksum_format_is_rejected(self) -> None:
        p = new_pipeline()
        bad = [{"file_id": "f1", "name": "a.h5", "size": 1, "sha256": "not-a-hash"}]
        with self.assertRaisesRegex(PipelineError, "bad_checksum"):
            p.register_batch(
                batch_id="B-BAD", species="S", cell_source="C",
                collection_lab="l", files=bad, consent=consent(),
                registered_by="curator.li",
            )

    def test_checksum_mismatch_blocks_verification_and_records_no_event(self) -> None:
        p = new_pipeline()
        bid, files = human_batch()
        p.register_batch(
            batch_id=bid, species="Homo sapiens", cell_source="小胶质细胞",
            collection_lab="lab-h", files=files, consent=consent(),
            registered_by="curator.li",
        )
        observed = file_hashes(files)
        observed["h-rna-001"] = sha256_text("tampered-content")
        with self.assertRaisesRegex(PipelineError, "checksum_mismatch"):
            p.verify_files(bid, observed, verified_by="qc.wang")
        # 校验失败不改变状态，不得产生 FILES_VERIFIED 事实。
        self.assertEqual("registered", p.batches[bid].status)
        events = p.store.stream("specimen_batch", bid)
        self.assertEqual(["BATCH_REGISTERED"], [e["event_type"] for e in events])

    def test_file_set_mismatch_reports_missing_and_extra(self) -> None:
        p = new_pipeline()
        bid, files = human_batch()
        p.register_batch(
            batch_id=bid, species="Homo sapiens", cell_source="小胶质细胞",
            collection_lab="lab-h", files=files, consent=consent(),
            registered_by="curator.li",
        )
        with self.assertRaisesRegex(PipelineError, "file_set_mismatch") as ctx:
            p.verify_files(bid, {"h-rna-001": files[0]["sha256"]}, verified_by="qc.wang")
        self.assertIn("h-rna-002", ctx.exception.message)

    def test_analysis_requires_verified_files(self) -> None:
        p = new_pipeline()
        bid, files = human_batch()
        p.register_batch(
            batch_id=bid, species="Homo sapiens", cell_source="小胶质细胞",
            collection_lab="lab-h", files=files, consent=consent(),
            registered_by="curator.li",
        )
        p.lock_annotation(
            annotation_id="A1", source="Ont", version="v1",
            ontology="CL", locked_by="annotator.chen",
        )
        with self.assertRaisesRegex(PipelineError, "files_unverified"):
            p.submit_run(
                label="r", batch_ids=[bid], annotation_id="A1",
                filter_params=FILTERS, software=SOFTWARE, model_config=MODEL,
                submitted_by="a", request_id="req-1",
            )

    def test_consent_scope_gates_analysis(self) -> None:
        p = new_pipeline()
        bid, files = human_batch()
        register_verified(
            p, bid, "Homo sapiens", "小胶质细胞", files,
            purposes=["unrelated_purpose"],
        )
        p.lock_annotation(
            annotation_id="A1", source="Ont", version="v1",
            ontology="CL", locked_by="annotator.chen",
        )
        with self.assertRaisesRegex(PipelineError, "consent_scope_denied"):
            p.submit_run(
                label="r", batch_ids=[bid], annotation_id="A1",
                filter_params=FILTERS, software=SOFTWARE, model_config=MODEL,
                submitted_by="a", request_id="req-1",
            )

    def test_individual_diagnosis_consent_is_refused(self) -> None:
        p = new_pipeline()
        _, files = human_batch()
        forbidden = consent()
        forbidden["individual_diagnosis"] = True
        with self.assertRaisesRegex(PipelineError, "diagnostic_use_prohibited"):
            p.register_batch(
                batch_id="B-DX", species="Homo sapiens", cell_source="小胶质细胞",
                collection_lab="lab-h", files=files, consent=forbidden,
                registered_by="curator.li",
            )


if __name__ == "__main__":
    unittest.main()
