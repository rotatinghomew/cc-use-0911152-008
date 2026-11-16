"""异常事件、观测可信度与路线事实。"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Iterable, Mapping

from corridor_twin.events.logistics_event import (
    ContractError,
    bounded,
    finite_number,
    stable_hash,
)

INCIDENT_KINDS = (
    "port_congestion",
    "cold_chain",
    "customs_hold",
    "route_closure",
    "weather",
    "capacity",
)
INCIDENT_STATUSES = ("open", "confirmed", "resolved", "withdrawn")

SOURCE_RELIABILITY = {
    "port_authority": 0.95,
    "terminal_system": 0.92,
    "customs_system": 0.9,
    "sensor": 0.88,
    "carrier": 0.78,
    "forwarder": 0.7,
    "manual": 0.6,
    "local-fixture": 0.7,
}


def normalized_key(value: Any) -> str:
    text = " ".join(str(value if value is not None else "").strip().split())
    return text.casefold()


def normalized_node(value: Any) -> str:
    text = normalized_key(value)
    text = re.sub(r"[^0-9a-z一-鿿._:-]+", "-", text).strip("-")
    return text


def parse_time(value: Any) -> datetime:
    text = str(value or "").strip()
    if not text:
        raise ContractError("事件时间不能为空")
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError as exc:
        raise ContractError(f"无效的事件时间: {value!r}") from exc
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def format_time(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat()


def _optional_identifier(value: Any, field_name: str) -> str:
    text = normalized_key(value)
    if not text:
        return ""
    text = re.sub(r"[^0-9a-z._:-]+", "-", text).strip("-")
    if not text:
        raise ContractError(f"{field_name} 无法规范为标识")
    return text


def route_segments(value: Any) -> tuple[str, ...]:
    if isinstance(value, (list, tuple)):
        parts = [normalized_node(part) for part in value]
    else:
        text = normalized_key(value).replace("_", "-").replace("/", "-").replace(">", "-")
        text = re.sub(r"\s*-\s*", "-", text)
        parts = [normalized_node(part) for part in re.split(r"[-\s,]+", text)]
    return tuple(part for part in parts if part)


def route_edges(segments: Iterable[str]) -> frozenset[str]:
    parts = tuple(segments)
    return frozenset(f"{left}>{right}" for left, right in zip(parts, parts[1:]))


@dataclass(frozen=True)
class Uncertainty:
    reason: str
    weight: float
    kind: str = "evidence"

    def __post_init__(self) -> None:
        object.__setattr__(self, "weight", bounded(self.weight))
        if not self.reason.strip():
            raise ContractError("不确定性说明不能为空")

    def as_dict(self) -> dict[str, Any]:
        return {"reason": self.reason, "weight": self.weight, "kind": self.kind}

    def __str__(self) -> str:
        return self.reason


@dataclass(frozen=True)
class IncidentEvent:
    event_id: str
    kind: str
    node: str
    occurred_at: datetime
    reported_at: datetime
    source: str = "local-fixture"
    route_code: str = ""
    shipment_id: str = ""
    credibility: float = 0.7
    severity: int = 3
    status: str = "open"
    supersedes: str = ""
    payload: dict[str, Any] = field(default_factory=dict)
    revision: int = 1

    def __post_init__(self) -> None:
        object.__setattr__(self, "event_id", _optional_identifier(self.event_id, "event_id"))
        if not self.event_id:
            raise ContractError("event_id 不能为空")
        kind = normalized_key(self.kind).replace(" ", "_")
        if kind not in INCIDENT_KINDS:
            raise ContractError(f"不支持的事件类型: {self.kind!r}")
        object.__setattr__(self, "kind", kind)
        node = normalized_node(self.node)
        if not node:
            raise ContractError("事件必须带有节点/口岸")
        object.__setattr__(self, "node", node)
        if not isinstance(self.occurred_at, datetime) or not isinstance(self.reported_at, datetime):
            raise ContractError("事件时间必须是 datetime")
        if self.reported_at < self.occurred_at:
            raise ContractError("报告时间不能早于发生时间")
        if self.status not in INCIDENT_STATUSES:
            raise ContractError(f"不支持的事件状态: {self.status}")
        if isinstance(self.severity, bool) or not isinstance(self.severity, int) or not 1 <= self.severity <= 5:
            raise ContractError("severity 必须为 1-5 的整数")
        if isinstance(self.revision, bool) or not isinstance(self.revision, int) or self.revision < 1:
            raise ContractError("revision 必须为正整数")
        object.__setattr__(self, "credibility", bounded(self.credibility))
        object.__setattr__(self, "route_code", normalized_key(self.route_code))
        object.__setattr__(self, "shipment_id", _optional_identifier(self.shipment_id, "shipment_id"))
        object.__setattr__(self, "supersedes", _optional_identifier(self.supersedes, "supersedes"))
        object.__setattr__(self, "source", normalized_key(self.source) or "unknown")
        object.__setattr__(self, "payload", dict(self.payload))

    @classmethod
    def create(cls, value: "IncidentEvent | Mapping[str, Any]") -> "IncidentEvent":
        if isinstance(value, cls):
            return value
        if not isinstance(value, Mapping):
            raise ContractError("事件必须为对象")
        occurred = parse_time(value.get("occurred_at") or value.get("observed_at"))
        reported = parse_time(value.get("reported_at") or value.get("received_at") or format_time(occurred))
        source = normalized_key(value.get("source") or "manual") or "manual"
        raw_credibility = value.get("credibility")
        credibility = (SOURCE_RELIABILITY.get(source, 0.7) if raw_credibility in (None, "")
                       else finite_number(raw_credibility))
        return cls(
            event_id=str(value.get("event_id") or value.get("id") or ""),
            kind=str(value.get("kind") or value.get("event_type") or ""),
            node=str(value.get("node") or value.get("location") or ""),
            occurred_at=occurred,
            reported_at=reported,
            source=source,
            route_code=str(value.get("route_code") or ""),
            shipment_id=str(value.get("shipment_id") or ""),
            credibility=credibility,
            severity=int(value.get("severity") or 3),
            status=str(value.get("status") or "open"),
            supersedes=str(value.get("supersedes") or value.get("corrects") or ""),
            payload=dict(value.get("payload") or {}),
            revision=int(value.get("revision") or 1),
        )

    @property
    def fingerprint(self) -> str:
        return stable_hash({
            "event_id": self.event_id,
            "kind": self.kind,
            "node": self.node,
            "occurred_at": format_time(self.occurred_at),
            "reported_at": format_time(self.reported_at),
            "source": self.source,
            "route_code": self.route_code,
            "shipment_id": self.shipment_id,
            "credibility": self.credibility,
            "severity": self.severity,
            "status": self.status,
            "supersedes": self.supersedes,
            "payload": self.payload,
            "revision": self.revision,
        })

    @property
    def lateness_hours(self) -> float:
        return max(0.0, (self.reported_at - self.occurred_at).total_seconds() / 3600.0)

    def segments(self) -> tuple[str, ...]:
        value = self.payload.get("segments") or self.payload.get("route_after") or self.route_code
        return route_segments(value)

    def edges(self) -> frozenset[str]:
        return route_edges(self.segments())

    def route_changed_from(self, other: "IncidentEvent") -> bool:
        left, right = self.segments(), other.segments()
        if not left or not right:
            return False
        return left != right and self.route_code != other.route_code

    def effective_credibility(self, corroborators: int = 0) -> float:
        decay = 1.0 / (1.0 + self.lateness_hours / 36.0)
        base = self.credibility * (0.6 + 0.4 * decay)
        return bounded(base + 0.04 * min(max(0, corroborators), 3))

    def uncertainty(self, corroborators: int = 0) -> list[Uncertainty]:
        rows: list[Uncertainty] = []
        if self.credibility < 0.55:
            rows.append(Uncertainty(f"来源可信度 {self.credibility:.2f} 偏低", 1.0 - self.credibility, "source"))
        if self.lateness_hours >= 6.0:
            rows.append(Uncertainty(f"事件迟到 {self.lateness_hours:.1f} 小时才报告", min(1.0, self.lateness_hours / 48.0), "latency"))
        if corroborators == 0:
            rows.append(Uncertainty("尚无独立来源交叉印证", 0.25, "corroboration"))
        if len(self.segments()) < 2:
            rows.append(Uncertainty("缺少完整路线段，只能按节点判断影响", 0.2, "route"))
        return rows

    def as_dict(self) -> dict[str, Any]:
        return {
            "event_id": self.event_id,
            "kind": self.kind,
            "node": self.node,
            "occurred_at": format_time(self.occurred_at),
            "reported_at": format_time(self.reported_at),
            "source": self.source,
            "route_code": self.route_code,
            "shipment_id": self.shipment_id,
            "credibility": self.credibility,
            "severity": self.severity,
            "status": self.status,
            "supersedes": self.supersedes,
            "payload": self.payload,
            "revision": self.revision,
            "fingerprint": self.fingerprint,
        }


def event_ordering(event: IncidentEvent) -> tuple[str, str]:
    return format_time(event.occurred_at), event.event_id


def superseded_map(events: Iterable[IncidentEvent]) -> dict[str, str]:
    """返回 被替代事件 -> 替代事件。"""
    superseded: dict[str, str] = {}
    known = {event.event_id for event in events}
    for event in events:
        if event.supersedes and event.supersedes != event.event_id:
            superseded[event.supersedes] = event.event_id
            if event.supersedes not in known:
                # 由 analyze 负责把“更正指向未知事件”转成不确定性。
                pass
    return superseded


def cargo_signature(event: IncidentEvent) -> tuple[str, ...]:
    values = []
    for key in ("cargo_class", "cargo", "product", "consignee", "temperature_regime"):
        text = normalized_key(event.payload.get(key))
        if text:
            values.append(text)
    return tuple(sorted(set(values)))


def route_overlap(left: IncidentEvent, right: IncidentEvent) -> tuple[float, bool]:
    """返回路段 Jaccard 与路线信息是否可用。"""
    left_edges, right_edges = left.edges(), right.edges()
    if not left_edges or not right_edges:
        return 0.0, False
    union = left_edges | right_edges
    return len(left_edges & right_edges) / len(union), True
