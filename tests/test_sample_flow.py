"""生成的联调事件流必须逐事件满足领域契约。"""

from __future__ import annotations

import json
import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from cross_species_research.contracts import validate_event


class SampleFlowContractTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.schema = json.loads(
            (ROOT / "contracts" / "domain.schema.json").read_text(encoding="utf-8")
        )
        cls.flow = json.loads((ROOT / "data" / "sample_flow.json").read_text(encoding="utf-8"))

    def test_flow_is_nonempty_event_list(self) -> None:
        self.assertIsInstance(self.flow, list)
        self.assertGreater(len(self.flow), 10)

    def test_every_event_satisfies_contract(self) -> None:
        for event in self.flow:
            issues = validate_event(event, self.schema)
            self.assertEqual([], [(i.field, i.code) for i in issues], msg=event.get("event_id"))

    def test_versions_are_gapless_per_aggregate(self) -> None:
        seen: dict[tuple[str, str], int] = {}
        for event in self.flow:
            key = (event["aggregate_type"], event["aggregate_id"])
            expected = seen.get(key, 0) + 1
            self.assertEqual(expected, event["version"], msg=event["event_id"])
            seen[key] = event["version"]

    def test_event_ids_are_unique(self) -> None:
        ids = [e["event_id"] for e in self.flow]
        self.assertEqual(len(ids), len(set(ids)))

    def test_flow_covers_core_lifecycle(self) -> None:
        types = {e["event_type"] for e in self.flow}
        for required in (
            "BATCH_REGISTERED",
            "FILES_VERIFIED",
            "ANNOTATION_LOCKED",
            "ANALYSIS_STARTED",
            "SHARD_COMMITTED",
            "ANALYSIS_COMPLETED",
            "REVIEW_APPROVED",
            "REVIEW_REVOKED",
            "RUN_INVALIDATED",
            "CLAIM_RELEASED",
            "CLAIM_ASSET_PUBLISHED",
            "CLAIM_FLAGGED",
        ):
            self.assertIn(required, types)


if __name__ == "__main__":
    unittest.main()
