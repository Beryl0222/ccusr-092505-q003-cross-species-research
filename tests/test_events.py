"""事件存储与指纹的直接单元测试。"""

from __future__ import annotations

import unittest

from cross_species_research.events import (
    ConcurrencyError,
    DuplicateEventError,
    DuplicateRequestError,
    EventStore,
    canonical_json,
    content_fingerprint,
)


def _event(event_id: str, aggregate_type: str = "analysis_run",
           aggregate_id: str = "r1", version: int = 1) -> dict:
    return {
        "event_id": event_id,
        "event_type": "ANALYSIS_STARTED",
        "aggregate_type": aggregate_type,
        "aggregate_id": aggregate_id,
        "occurred_at": "2026-10-06T09:00:00+08:00",
        "version": version,
        "summary": "x",
    }


class EventStoreTests(unittest.TestCase):
    def test_versions_must_be_gapless_per_aggregate(self) -> None:
        store = EventStore()
        store.append(_event("e1"))
        with self.assertRaises(ConcurrencyError):
            store.append(_event("e3", version=3))
        store.append(_event("e2", version=2))
        self.assertEqual(2, store.version_of("analysis_run", "r1"))

    def test_expected_version_optimistic_concurrency(self) -> None:
        store = EventStore()
        store.append(_event("e1"), expected_version=0)
        with self.assertRaises(ConcurrencyError):
            store.append(_event("e2", version=2), expected_version=0)

    def test_duplicate_event_id_is_rejected(self) -> None:
        store = EventStore()
        store.append(_event("e1"))
        with self.assertRaises(DuplicateEventError):
            store.append(_event("e1", version=2))

    def test_export_and_reload_roundtrip(self) -> None:
        store = EventStore()
        store.append(_event("e1"))
        store.append(_event("e2", aggregate_id="r2"))
        store.append(_event("e3", version=2))
        reloaded = EventStore(store.export())
        self.assertEqual(
            [e["event_id"] for e in store.stream()],
            [e["event_id"] for e in reloaded.stream()],
        )
        self.assertEqual(2, reloaded.version_of("analysis_run", "r1"))

    def test_request_idempotency_scopes_are_separate(self) -> None:
        store = EventStore()
        store.remember_request("run_submit", "req-1", {"run_id": "r1"})
        # 同一 request_id 在另一作用域可独立使用。
        store.remember_request("shard_commit", "req-1", {"shard_id": "s1"})
        self.assertEqual({"run_id": "r1"}, store.recalled_request("run_submit", "req-1"))
        self.assertIsNone(store.recalled_request("run_submit", "missing"))
        with self.assertRaises(DuplicateRequestError):
            store.remember_request("run_submit", "req-1", {"run_id": "r2"})


class FingerprintTests(unittest.TestCase):
    def test_key_order_does_not_change_fingerprint(self) -> None:
        a = {"b": 1, "a": [1, 2, {"c": 3}]}
        b = {"a": [1, 2, {"c": 3}], "b": 1}
        self.assertEqual(canonical_json(a), canonical_json(b))
        self.assertEqual(content_fingerprint(a), content_fingerprint(b))

    def test_content_change_changes_fingerprint(self) -> None:
        self.assertNotEqual(
            content_fingerprint({"threshold": 5}),
            content_fingerprint({"threshold": 6}),
        )
        self.assertTrue(content_fingerprint({}).startswith("sha256:"))


if __name__ == "__main__":
    unittest.main()
