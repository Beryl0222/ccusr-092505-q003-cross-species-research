"""事件存储与内容指纹工具（仅依赖标准库）。

事件按追加顺序保存，同一聚合的版本号严格递增；请求幂等表保证
同一 request_id 的重复提交直接复用首次结果，而不是再产生事件。
"""

from __future__ import annotations

import copy
import hashlib
import json
from collections.abc import Iterable, Iterator
from typing import Any


class EventStoreError(Exception):
    """事件存储层错误基类。"""


class DuplicateEventError(EventStoreError):
    """event_id 已存在。"""


class ConcurrencyError(EventStoreError):
    """聚合版本与预期不一致（标识相同而内容已分叉）。"""


class DuplicateRequestError(EventStoreError):
    """request_id 已被使用但绑定了不同的幂等范畴。"""


def canonical_json(value: Any) -> str:
    """跨进程稳定的规范 JSON：键排序、无空白、非 ASCII 原样保留。"""
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        default=_json_default,
    )


def _json_default(value: Any) -> Any:  # pragma: no cover - 防御性
    if isinstance(value, (set, frozenset)):
        return sorted(value, key=canonical_json)
    raise TypeError(f"无法规范化类型: {type(value)!r}")


def content_fingerprint(value: Any) -> str:
    """对规范化内容计算 sha256 指纹，前缀 sha256:。"""
    digest = hashlib.sha256(canonical_json(value).encode("utf-8")).hexdigest()
    return f"sha256:{digest}"


class EventStore:
    """内存事件日志；可整体导出/导入以便持久化与重启恢复。"""

    def __init__(self, events: Iterable[dict[str, Any]] | None = None) -> None:
        self._events: list[dict[str, Any]] = []
        self._versions: dict[tuple[str, str], int] = {}
        self._event_ids: set[str] = set()
        self._requests: dict[tuple[str, str], Any] = {}
        if events:
            for event in events:
                self.append(event)

    def append(
        self,
        event: dict[str, Any],
        *,
        expected_version: int | None = None,
    ) -> dict[str, Any]:
        aggregate_type = event["aggregate_type"]
        aggregate_id = event["aggregate_id"]
        key = (aggregate_type, aggregate_id)
        current = self._versions.get(key, 0)
        if expected_version is not None and expected_version != current:
            raise ConcurrencyError(
                f"聚合 {aggregate_id} 版本 {current} 与预期 {expected_version} 不一致"
            )
        if event["version"] != current + 1:
            raise ConcurrencyError(
                f"聚合 {aggregate_id} 的事件版本必须为 {current + 1}，实际 {event['version']}"
            )
        event_id = event["event_id"]
        if event_id in self._event_ids:
            raise DuplicateEventError(f"事件 {event_id} 已存在")
        stored = copy.deepcopy(event)
        self._events.append(stored)
        self._versions[key] = stored["version"]
        self._event_ids.add(event_id)
        return copy.deepcopy(stored)

    def stream(
        self,
        aggregate_type: str | None = None,
        aggregate_id: str | None = None,
    ) -> list[dict[str, Any]]:
        result = self._events
        if aggregate_type is not None:
            result = [e for e in result if e["aggregate_type"] == aggregate_type]
        if aggregate_id is not None:
            result = [e for e in result if e["aggregate_id"] == aggregate_id]
        return copy.deepcopy(result)

    def all_events(self) -> Iterator[dict[str, Any]]:
        for event in self._events:
            yield copy.deepcopy(event)

    def version_of(self, aggregate_type: str, aggregate_id: str) -> int:
        return self._versions.get((aggregate_type, aggregate_id), 0)

    def exists(self, event_id: str) -> bool:
        return event_id in self._event_ids

    # -- 请求幂等 --------------------------------------------------------

    def remember_request(self, scope: str, request_id: str, result: Any) -> Any:
        """登记幂等请求结果。

        scope 区分使用场景（如 "run_submit" 与 "shard_commit"），
        同一 request_id 在不同 scope 复用属于调用方错误。
        """
        key = (scope, request_id)
        if key in self._requests:
            raise DuplicateRequestError(f"请求 {request_id} 在 {scope} 中重复登记")
        self._requests[key] = copy.deepcopy(result)
        return copy.deepcopy(result)

    def recalled_request(self, scope: str, request_id: str) -> Any | None:
        if (scope, request_id) not in self._requests:
            return None
        return copy.deepcopy(self._requests[(scope, request_id)])

    # -- 持久化 ----------------------------------------------------------

    def export(self) -> list[dict[str, Any]]:
        return copy.deepcopy(self._events)
