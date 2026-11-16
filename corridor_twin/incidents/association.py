"""影响链识别：路线、发生顺序、观测可信度与更正/迟到处理。"""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Iterable, Mapping

from corridor_twin.events.logistics_event import ContractError, bounded, stable_hash
from corridor_twin.incidents.model import (
    IncidentEvent,
    Uncertainty,
    cargo_signature,
    format_time,
    normalized_key,
    route_overlap,
    route_segments,
    superseded_map,
)

KIND_LABELS = {
    "port_congestion": "口岸拥堵",
    "cold_chain": "冷链异常",
    "customs_hold": "海关查验",
    "route_closure": "路段封闭",
    "weather": "天气影响",
    "capacity": "运力不足",
}
CAUSAL_KINDS = frozenset({"port_congestion", "customs_hold", "route_closure", "weather", "capacity"})
OWNER_ROLES = {
    "port_congestion": "口岸拥堵协调席",
    "cold_chain": "冷链保障席",
    "customs_hold": "关务合规席",
    "route_closure": "路网调度席",
    "weather": "气象与安全席",
    "capacity": "运力计划席",
}
MERGE_RELATIONS = frozenset({"same_incident", "upstream_cause"})
DEFAULT_MERGE_WINDOW_HOURS = 24.0
DEFAULT_LINK_THRESHOLD = 0.52
DEFAULT_MAX_CAUSE_GAP_HOURS = 72.0


def category(kind: str) -> str:
    return KIND_LABELS.get(kind, kind)


def _hours(left: datetime, right: datetime) -> float:
    return abs((right - left).total_seconds()) / 3600.0


def _dedupe_events(events: Iterable[IncidentEvent]) -> dict[str, IncidentEvent]:
    by_id: dict[str, IncidentEvent] = {}
    for event in events:
        current = by_id.get(event.event_id)
        if current is None:
            by_id[event.event_id] = event
            continue
        if current.fingerprint == event.fingerprint:
            continue
        if event.revision > current.revision:
            by_id[event.event_id] = event
        elif event.revision == current.revision:
            raise ContractError(f"事件 {event.event_id} 在相同修订版出现冲突内容")
    return by_id


def _duplicate_observations(events: Iterable[IncidentEvent]) -> dict[str, str]:
    groups: dict[str, list[IncidentEvent]] = defaultdict(list)
    for event in events:
        key = stable_hash({
            "kind": event.kind,
            "node": event.node,
            "occurred_at": format_time(event.occurred_at),
            "route_code": event.route_code,
            "shipment_id": event.shipment_id,
            "source": event.source,
            "severity": event.severity,
            "payload": event.payload,
        })
        groups[key].append(event)
    duplicates: dict[str, str] = {}
    for group in groups.values():
        if len(group) < 2:
            continue
        # 更正事件优先作为保留项，避免重复导入把“更正后的事件”降级成副本。
        canonical = min(group, key=lambda item: (0 if item.supersedes else 1,
                                                 format_time(item.reported_at), item.event_id))
        for event in group:
            if event.event_id != canonical.event_id:
                duplicates[event.event_id] = canonical.event_id
    return duplicates


def _corroboration(events: list[IncidentEvent]) -> dict[str, int]:
    groups: dict[tuple[str, str], set[str]] = defaultdict(set)
    for event in events:
        groups[(event.kind, event.node)].add(event.source)
    result: dict[str, int] = {}
    for event in events:
        result[event.event_id] = max(0, len(groups[(event.kind, event.node)]) - 1)
    return result


def _link_strength(earlier: IncidentEvent, later: IncidentEvent) -> tuple[float, bool, list[str]]:
    reasons: list[str] = []
    if earlier.node == later.node:
        return 0.7, True, [f"两者位于同一节点 {earlier.node}"]
    overlap, route_available = route_overlap(earlier, later)
    if overlap >= 0.25:
        return bounded(0.6 + overlap * 0.4), True, [
            f"路线路段重合 {overlap:.0%}（{','.join(sorted(earlier.edges() & later.edges()))}）"
        ]
    if earlier.node in later.segments():
        return 0.8, True, [f"上游节点 {earlier.node} 出现在后续路线中"]
    if later.node in earlier.segments():
        reasons.append(f"后续节点 {later.node} 已在上游路线中")
        return 0.55, True, reasons
    if route_available:
        reasons.append("两者路线没有重合路段")
    else:
        reasons.append("缺少完整路线，无法按路段判断传播")
    return 0.0, False, reasons


def _declared_cause(event: IncidentEvent) -> str:
    for key in ("cause_event_id", "caused_by", "caused_by_event_id"):
        value = event.payload.get(key)
        if value:
            return normalized_key(value)
    return ""


def _pair_association(
    left: IncidentEvent,
    right: IncidentEvent,
    corroboration: Mapping[str, int],
    *,
    merge_window_hours: float,
    link_threshold: float,
    max_cause_gap_hours: float,
) -> "Association | None":
    same_kind = left.kind == right.kind
    same_node = left.node == right.node
    gap = _hours(left.occurred_at, right.occurred_at)
    cred = (left.effective_credibility(corroboration.get(left.event_id, 0))
            + right.effective_credibility(corroboration.get(right.event_id, 0))) / 2.0
    severity = (left.severity + right.severity) / 10.0
    route_score, route_available = route_overlap(left, right)
    if same_node and not route_available:
        route_score = 0.5
    cargo_left, cargo_right = cargo_signature(left), cargo_signature(right)
    cargo_mismatch = bool(cargo_left and cargo_right and not set(cargo_left) & set(cargo_right))
    uncertainties: list[Uncertainty] = []
    reasons: list[str] = []
    route_changed = (left.route_code and right.route_code and left.route_code != right.route_code
                     and route_available and route_score < 0.25)

    earlier, later = ((left, right) if (left.occurred_at, left.event_id) <= (right.occurred_at, right.event_id)
                      else (right, left))
    declared = ""
    if _declared_cause(later) == earlier.event_id:
        declared = earlier.event_id

    if same_kind and same_node and gap <= merge_window_hours:
        if left.kind == "cold_chain" and cargo_mismatch:
            score = bounded(0.42 + 0.18 * cred)
            uncertainties.append(Uncertainty("同一节点同一时段的冷链异常涉及不同货类，不能直接并案", 0.45, "cargo"))
            return Association(left.event_id, right.event_id, "co_incident", score, (
                category(left.kind), f"同为 {category(left.kind)}，节点 {left.node} 相同且间隔 {gap:.1f} 小时，但货类不同",
            ), tuple(uncertainties), route_score)
        time_score = max(0.0, 1.0 - gap / max(1.0, merge_window_hours))
        score = bounded(0.30 + 0.25 * time_score + 0.20 * cred + 0.15 * route_score + 0.10 * severity)
        reasons.extend([
            f"类型与节点相同（{category(left.kind)} @ {left.node}），发生间隔 {gap:.1f} 小时",
            f"观测可信度加权 {cred:.2f}",
            "同一口岸同类报告按单一口岸事件并案，不逐票重复建单",
        ])
        if route_changed:
            reasons.append("路线描述已变化，影响范围按最新路线重新界定，不据此拆分同一口岸事件")
            uncertainties.append(Uncertainty("路线描述变化，受影响路段需按较新事件重新核对", 0.2, "route"))
        if left.lateness_hours >= 6 or right.lateness_hours >= 6:
            uncertainties.append(Uncertainty("至少一条报告迟到超过 6 小时，现场状态可能已经变化", 0.2, "latency"))
        if cred < 0.5:
            uncertainties.append(Uncertainty("观测可信度不足，并案结论需交叉印证", 0.35, "source"))
        return Association(left.event_id, right.event_id, "same_incident", score,
                           tuple(reasons), tuple(uncertainties), route_score)

    link, linked, link_reasons = _link_strength(earlier, later)
    if gap > max_cause_gap_hours:
        return Association(earlier.event_id, later.event_id, "independent", 0.0,
                           (f"发生间隔 {gap:.1f} 小时，超过因果观察窗口 {max_cause_gap_hours:.0f} 小时",),
                           (Uncertainty("时间间隔过远，不能把两件事当作同一条影响链", 0.4, "time"),), link)
    if same_kind and same_node:
        # 同一口岸同类报告间隔超出并案窗口，说明拥堵已中断过，按两次独立事件处理，避免无限续链。
        return Association(earlier.event_id, later.event_id, "independent", 0.0,
                           (f"同一口岸同类报告间隔 {gap:.1f} 小时，超过并案窗口 {merge_window_hours:.0f} 小时，按两次独立事件处理",),
                           (Uncertainty("间隔超过并案窗口，原事件可能已恢复，不能顺延并案", 0.35, "time"),), link)
    if not linked and not declared:
        return None

    time_score = max(0.0, 1.0 - gap / max(1.0, max_cause_gap_hours))
    score = bounded(0.30 * time_score + 0.30 * link + 0.25 * cred + 0.15 * severity)
    reasons.extend(link_reasons)
    reasons.append(f"发生顺序：{category(earlier.kind)} @ {earlier.node} 早于 {category(later.kind)} @ {later.node}（{gap:.1f} 小时）")
    reasons.append(f"观测可信度加权 {cred:.2f}")
    if declared:
        score = bounded(score + 0.15)
        reasons.append(f"后续事件声明由 {declared} 引起，作为显式因果证据")
    shares_shipment = bool(earlier.shipment_id and earlier.shipment_id == later.shipment_id)
    if shares_shipment:
        score = bounded(score + 0.10)
        reasons.append(f"两次观测指向同一货票 {later.shipment_id}，上下游关系可核验")
    if cargo_mismatch:
        uncertainties.append(Uncertainty("货类不一致，可能不是同一影响链", 0.35, "cargo"))
        reasons.append("货类不一致，降低因果置信度")
    if route_changed:
        uncertainties.append(Uncertainty("路线在两次观测之间发生变化，传播路径需要复核", 0.25, "route"))
    if cred < 0.5:
        uncertainties.append(Uncertainty("观测可信度不足，因果判断需人工确认", 0.35, "source"))

    if later.kind == "cold_chain" and earlier.kind != "cold_chain" and not declared:
        # 冷链异常常被误并入同一口岸拥堵；没有显式因果声明时保持独立，交由冷链席核验。
        reasons.append("没有显式因果证据，冷链异常不并入口岸事件，保留独立负责人")
        uncertainties.append(Uncertainty("冷链异常与口岸事件可能只是同期发生，需独立核验温度与责任段", 0.4, "separation"))
        return Association(earlier.event_id, later.event_id, "co_incident", score,
                           tuple(reasons), tuple(uncertainties), link)
    if later.kind == "cold_chain" and earlier.kind != "cold_chain" and declared and shares_shipment:
        reasons.append("同一货票上冷链异常声明由上游事件引起，保留显式因果并标注需核验责任段")
        uncertainties.append(Uncertainty("即使同一货票，也需核验温控责任在口岸等待还是运输环节", 0.3, "attribution"))
    if score >= link_threshold and earlier.kind in CAUSAL_KINDS:
        reasons.append("满足强度阈值，按上游原因建立影响链")
        return Association(earlier.event_id, later.event_id, "upstream_cause", score,
                           tuple(reasons), tuple(uncertainties), link)
    reasons.append("证据强度不足或方向不明确，暂列为同期共发而非因果")
    uncertainties.append(Uncertainty("仅能确认同期相关，不能确认谁导致谁", 0.3, "direction"))
    return Association(earlier.event_id, later.event_id, "co_incident", score,
                       tuple(reasons), tuple(uncertainties), link)


@dataclass(frozen=True)
class Association:
    left: str
    right: str
    relation: str
    score: float
    reasons: tuple[str, ...]
    uncertainties: tuple[Uncertainty, ...]
    route_signal: float = 0.0

    def __post_init__(self) -> None:
        if self.relation not in {"same_incident", "upstream_cause", "co_incident", "independent"}:
            raise ContractError("不支持的关联类型")
        object.__setattr__(self, "score", bounded(self.score))

    def as_dict(self) -> dict[str, Any]:
        return {
            "left": self.left,
            "right": self.right,
            "relation": self.relation,
            "score": self.score,
            "reasons": list(self.reasons),
            "uncertainties": [item.as_dict() for item in self.uncertainties],
            "route_signal": self.route_signal,
        }


@dataclass(frozen=True)
class Exposure:
    shipment_id: str
    basis: str
    event_ids: tuple[str, ...]
    commitments: tuple[dict[str, Any], ...] = ()
    status: str = "active"
    impact: str = "in_scope"
    action: str = ""

    def as_dict(self) -> dict[str, Any]:
        return {
            "shipment_id": self.shipment_id,
            "basis": self.basis,
            "event_ids": list(self.event_ids),
            "commitments": [dict(item) for item in self.commitments],
            "status": self.status,
            "impact": self.impact,
            "action": self.action,
        }


@dataclass(frozen=True)
class Chain:
    chain_id: str
    root_event: str
    root_kind: str
    node: str
    events: tuple[str, ...]
    relations: tuple[dict[str, Any], ...]
    exposures: tuple[Exposure, ...]
    owner_role: str
    rationale: tuple[str, ...]
    uncertainties: tuple[Uncertainty, ...]
    confidence: float
    status: str

    @property
    def tickets(self) -> tuple[str, ...]:
        return tuple(item.shipment_id for item in self.exposures if item.status != "withdrawn")

    def as_dict(self) -> dict[str, Any]:
        return {
            "chain_id": self.chain_id,
            "root_event": self.root_event,
            "root_kind": self.root_kind,
            "node": self.node,
            "events": list(self.events),
            "relations": [dict(item) for item in self.relations],
            "exposures": [item.as_dict() for item in self.exposures],
            "tickets": list(self.tickets),
            "owner_role": self.owner_role,
            "rationale": list(self.rationale),
            "uncertainties": [item.as_dict() for item in self.uncertainties],
            "confidence": self.confidence,
            "status": self.status,
            "impact_scope": {
                "shipments": [item.shipment_id for item in self.exposures],
                "active_shipments": list(self.tickets),
                "commitments": [dict(item) for item in self.exposures for item in item.commitments],
            },
        }


@dataclass(frozen=True)
class Analysis:
    events: tuple[IncidentEvent, ...]
    associations: tuple[Association, ...]
    chains: tuple[Chain, ...]
    superseded: dict[str, str]
    duplicates: dict[str, str]
    withdrawn_events: tuple[str, ...]
    global_uncertainties: tuple[Uncertainty, ...]
    settings: dict[str, Any] = field(default_factory=dict)
    withdrawn_tickets: tuple[str, ...] = ()

    def as_dict(self) -> dict[str, Any]:
        return {
            "events": [event.as_dict() for event in self.events],
            "associations": [item.as_dict() for item in self.associations],
            "chains": [item.as_dict() for item in self.chains],
            "superseded": dict(self.superseded),
            "duplicates": dict(self.duplicates),
            "withdrawn_events": list(self.withdrawn_events),
            "withdrawn_tickets": list(self.withdrawn_tickets),
            "uncertainties": [item.as_dict() for item in self.global_uncertainties],
            "settings": dict(self.settings),
        }

    def chain_for_event(self, event_id: str) -> Chain | None:
        for chain in self.chains:
            if event_id in chain.events:
                return chain
        return None

    @property
    def open_chains(self) -> tuple[Chain, ...]:
        return tuple(chain for chain in self.chains if chain.status != "resolved")


def _record_view(item: Any) -> tuple[str, Mapping[str, Any]]:
    if isinstance(item, Mapping):
        payload = item.get("payload") or item
        return str(item.get("record_id") or payload.get("shipment_id") or ""), payload
    return str(getattr(item, "record_id", "")), getattr(item, "payload", {}) or {}


def _commitment_index(commitments: Iterable[Any]) -> dict[str, list[dict[str, Any]]]:
    index: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for item in commitments:
        row = dict(item) if isinstance(item, Mapping) else dict(getattr(item, "__dict__", {}))
        shipment = normalized_key(row.get("shipment_id"))
        if shipment:
            index[shipment].append(row)
    return index


def _shipment_routes(records: Iterable[Any]) -> dict[str, tuple[str, ...]]:
    routes: dict[str, tuple[str, ...]] = {}
    for record in records:
        _, payload = _record_view(record)
        shipment = normalized_key(payload.get("shipment_id"))
        route = payload.get("route_code") or payload.get("segments") or ""
        if not shipment:
            continue
        parts = route_segments(route)
        if parts:
            routes[shipment] = parts
    return routes


def _shipment_cargo(records: Iterable[Any]) -> dict[str, tuple[str, ...]]:
    cargo: dict[str, tuple[str, ...]] = {}
    for record in records:
        _, payload = _record_view(record)
        shipment = normalized_key(payload.get("shipment_id"))
        if not shipment:
            continue
        values = tuple(sorted({normalized_key(payload.get(key)) for key in ("cargo_class", "cargo")
                               if normalized_key(payload.get(key))}))
        if values:
            cargo[shipment] = values
    return cargo


def _exposure_impact(commitment: Mapping[str, Any], nodes: set[str],
                     route: tuple[str, ...], observed: bool) -> tuple[str, str]:
    status = normalized_key(commitment.get("status"))
    if status in {"completed", "cancelled"}:
        return "closed", "运输承诺已结束，仅保留影响记录"
    if not route:
        return ("in_scope" if observed else "monitoring",
                "缺少货票路线：须人工确认是否经过事件节点后再调整承诺")
    intersects = bool(nodes & set(route))
    if not intersects and not observed:
        return "monitoring", "路线未经过事件节点，保持观察"
    if status == "dispatched":
        return ("in_transit" if intersects else "in_scope",
                "在途货票：评估改线、补货与客户承诺更新" if intersects
                else "在途货票已直接关联事件，需核实是否绕行")
    if status == "reserved":
        return ("route_impacted" if intersects else "in_scope",
                "尚未发运：重新确认路线/时间窗；必要时取消后重新预订" if intersects
                else "尚未发运且已直接关联事件，需核对路线")
    return ("in_scope" if intersects else "monitoring", "核对运输承诺是否受到影响")


def _exposures_for(
    members: list[IncidentEvent],
    shipments: dict[str, tuple[str, ...]],
    cargo: Mapping[str, tuple[str, ...]],
    commitments: Mapping[str, list[dict[str, Any]]],
    withdrawals: Mapping[str, str],
) -> tuple[Exposure, ...]:
    observed: dict[str, set[str]] = defaultdict(set)
    matched_nodes: dict[str, set[str]] = defaultdict(set)
    edges: set[str] = set()
    for event in members:
        edges.update(event.edges())
        if event.shipment_id:
            observed[event.shipment_id].add(event.event_id)
            matched_nodes[event.shipment_id].add(event.node)
    inferred: dict[str, set[str]] = defaultdict(set)
    event_cargo = {event.event_id: set(cargo_signature(event)) for event in members}
    for shipment, route in shipments.items():
        if shipment in observed:
            continue
        shipment_cargo = set(cargo.get(shipment, ()))
        route_edges = {f"{left}>{right}" for left, right in zip(route, route[1:])}
        for event in members:
            # 冷链事件只在货类吻合时才推断受影响货票，避免把所有过境货票都算进去。
            if event.kind == "cold_chain" and event_cargo[event.event_id]:
                if not shipment_cargo & event_cargo[event.event_id]:
                    continue
            if event.node in route or (edges and event.edges() & route_edges):
                inferred[shipment].add(event.event_id)
                if event.node in route:
                    matched_nodes[shipment].add(event.node)
    exposures: list[Exposure] = []
    for shipment in sorted(set(observed) | set(inferred)):
        basis = "observed" if shipment in observed else "route_inferred"
        event_ids = tuple(sorted(observed.get(shipment) or inferred.get(shipment) or ()))
        nodes = matched_nodes.get(shipment) or {members[0].node}
        rows: list[dict[str, Any]] = []
        impact = "in_scope"
        action = "核对货票与事件的关系"
        for commitment in commitments.get(shipment, []):
            route = shipments.get(shipment, ())
            row_impact, row_action = _exposure_impact(
                commitment, set(nodes), route, basis == "observed")
            row = dict(commitment)
            row["impact"] = row_impact
            row["action"] = row_action
            rows.append(row)
            if row_impact in {"route_impacted", "in_transit"}:
                impact, action = row_impact, row_action
            elif impact == "in_scope" and row_impact != "closed":
                impact, action = row_impact, row_action
        withdrawn = shipment in withdrawals
        exposures.append(Exposure(
            shipment_id=shipment,
            basis=basis,
            event_ids=event_ids,
            commitments=tuple(rows),
            status="withdrawn" if withdrawn else "active",
            impact="withdrawn" if withdrawn else impact,
            action=("该票运输已撤销；事件仍按其他货票与口岸范围跟踪" if withdrawn else action),
        ))
    return tuple(exposures)


def analyze(
    events: Iterable[IncidentEvent | Mapping[str, Any]],
    *,
    records: Iterable[Any] = (),
    commitments: Iterable[Any] = (),
    withdrawals: Mapping[str, str] | None = None,
    merge_window_hours: float = DEFAULT_MERGE_WINDOW_HOURS,
    link_threshold: float = DEFAULT_LINK_THRESHOLD,
    max_cause_gap_hours: float = DEFAULT_MAX_CAUSE_GAP_HOURS,
) -> Analysis:
    if merge_window_hours <= 0 or max_cause_gap_hours <= 0:
        raise ContractError("关联时间窗必须为正数")
    if not 0.0 <= link_threshold <= 1.0:
        raise ContractError("关联阈值必须在 0-1 之间")
    by_id = _dedupe_events([IncidentEvent.create(event) for event in events])
    superseded = superseded_map(by_id.values())
    # 只接受指向已知事件、且更正事件自身仍然有效的替代关系。
    superseded = {
        old: new for old, new in superseded.items()
        if old in by_id and by_id[new].status != "withdrawn"
    }
    duplicates = _duplicate_observations(by_id.values())
    withdrawn = tuple(sorted(event_id for event_id, event in by_id.items() if event.status == "withdrawn"))
    active = [event for event in by_id.values()
              if event.event_id not in superseded
              and event.event_id not in duplicates
              and event.status != "withdrawn"]
    for canonical_id in sorted(set(duplicates.values())):
        # 保留作为“原件”的那条如果被更正替代，则不再出现在有效集合中；
        # 选择它在重复组里最早报告的一条继续参与分析，避免异常整体丢失。
        if canonical_id in {event.event_id for event in active} or canonical_id not in by_id:
            continue
        group = [event for event, target in duplicates.items() if target == canonical_id]
        candidates = [by_id[event_id] for event_id in group if by_id[event_id].status != "withdrawn"]
        if not candidates:
            continue
        candidate = min(candidates, key=lambda item: (format_time(item.reported_at), item.event_id))
        active.append(candidate)
        duplicates.pop(candidate.event_id, None)
    active.sort(key=lambda event: (format_time(event.occurred_at), event.event_id))
    active_ids = {event.event_id for event in active}
    global_uncertainties: list[Uncertainty] = []
    for event in by_id.values():
        if event.supersedes and event.supersedes not in by_id:
            global_uncertainties.append(Uncertainty(
                f"事件 {event.event_id} 声明更正 {event.supersedes}，但原事件不存在，无法判断拆分范围",
                0.3, "correction"))
        if event.lateness_hours >= 6.0:
            global_uncertainties.append(Uncertainty(
                f"事件 {event.event_id} 迟到 {event.lateness_hours:.1f} 小时报告，迟到信息可能改变原有并案",
                min(0.4, event.lateness_hours / 72.0), "latency"))
        if event.event_id in superseded:
            newer = by_id[superseded[event.event_id]]
            if (newer.node, newer.kind) != (event.node, event.kind):
                global_uncertainties.append(Uncertainty(
                    f"事件 {event.event_id} 被 {newer.event_id} 更正，节点/类型由 {event.node}/{category(event.kind)} "
                    f"改为 {newer.node}/{category(newer.kind)}，原关联可能拆开",
                    0.35, "correction"))
            elif newer.route_code != event.route_code:
                global_uncertainties.append(Uncertainty(
                    f"事件 {event.event_id} 被 {newer.event_id} 更正，路线变化可能改变受影响货票范围",
                    0.25, "correction"))
    for duplicate, canonical in sorted(duplicates.items()):
        global_uncertainties.append(Uncertainty(
            f"事件 {duplicate} 与 {canonical} 为同一观测的重复导入，已合并为一条工作线索",
            0.05, "duplicate"))

    corroboration = _corroboration(active)
    associations: list[Association] = []
    for index, left in enumerate(active):
        for right in active[index + 1:]:
            association = _pair_association(
                left, right, corroboration,
                merge_window_hours=merge_window_hours,
                link_threshold=link_threshold,
                max_cause_gap_hours=max_cause_gap_hours,
            )
            if association is not None:
                associations.append(association)
    associations.sort(key=lambda item: (item.left, item.right))

    parent = {event_id: event_id for event_id in active_ids}

    def find(value: str) -> str:
        while parent[value] != value:
            parent[value] = parent[parent[value]]
            value = parent[value]
        return value

    def union(left: str, right: str) -> None:
        root_left, root_right = find(left), find(right)
        if root_left != root_right:
            parent[root_right] = root_left

    for association in associations:
        if association.relation in MERGE_RELATIONS:
            union(association.left, association.right)
    components: dict[str, list[str]] = defaultdict(list)
    for event_id in sorted(active_ids):
        components[find(event_id)].append(event_id)

    shipment_routes = _shipment_routes(records)
    shipment_cargo = _shipment_cargo(records)
    commitment_index = _commitment_index(commitments)
    withdrawal_map = dict(withdrawals or {})
    by_event = {event.event_id: event for event in active}
    chains: list[Chain] = []
    for members in components.values():
        member_events = sorted((by_event[event_id] for event_id in members),
                               key=lambda event: (format_time(event.occurred_at), event.event_id))
        root = member_events[0]
        member_ids = tuple(event.event_id for event in member_events)
        internal = [
            association for association in associations
            if association.left in member_ids and association.right in member_ids
            and association.relation in MERGE_RELATIONS
        ]
        rationale: list[str] = [
            f"根因事件 {root.event_id}：{category(root.kind)} @ {root.node}，发生于 {format_time(root.occurred_at)}"
        ]
        for association in internal:
            left, right = by_id[association.left], by_id[association.right]
            rationale.append(
                f"{left.event_id} -> {right.event_id} 判为{'同一事件' if association.relation == 'same_incident' else '上游原因'}："
                + "；".join(association.reasons)
            )
        uncertainties: list[Uncertainty] = []
        for association in internal:
            uncertainties.extend(association.uncertainties)
        for event in member_events:
            uncertainties.extend(event.uncertainty(corroboration.get(event.event_id, 0)))
        seen: set[str] = set()
        unique_uncertainties: list[Uncertainty] = []
        for item in uncertainties:
            key = f"{item.kind}:{item.reason}"
            if key not in seen:
                seen.add(key)
                unique_uncertainties.append(item)
        if len(member_events) == 1:
            rationale.append("当前仅有单一有效事件，影响范围按观测货票与路线推断，缺失独立印证")
        if any(event.kind == "cold_chain" for event in member_events) and len(member_events) > 1:
            rationale.append("冷链异常只有在路线、顺序与可信度同时支持时才并入上游事件，否则保留为独立链")
        exposures = _exposures_for(member_events, shipment_routes, shipment_cargo,
                                   commitment_index, withdrawal_map)
        if exposures:
            observed = [item.shipment_id for item in exposures if item.basis == "observed"]
            inferred = [item.shipment_id for item in exposures if item.basis == "route_inferred"]
            rationale.append(
                f"影响货票：直接观测 {len(observed)} 票，按路线推断 {len(inferred)} 票；"
                f"运输承诺 {sum(len(item.commitments) for item in exposures)} 条"
            )
        else:
            uncertainties.append(Uncertainty("尚未关联到任何货票或运输承诺，影响范围为空", 0.3, "scope"))
        confidence = (sum(item.score for item in internal) / len(internal)) if internal else (
            root.effective_credibility(corroboration.get(root.event_id, 0)) * 0.85)
        status = "resolved" if all(event.status == "resolved" for event in member_events) else "open"
        chains.append(Chain(
            chain_id=stable_hash({"events": sorted(member_ids), "root": root.event_id})[:24],
            root_event=root.event_id,
            root_kind=root.kind,
            node=root.node,
            events=member_ids,
            relations=tuple(item.as_dict() for item in internal),
            exposures=exposures,
            owner_role=OWNER_ROLES.get(root.kind, "控制塔值班席"),
            rationale=tuple(rationale),
            uncertainties=tuple(unique_uncertainties),
            confidence=bounded(confidence),
            status=status,
        ))
    chains.sort(key=lambda chain: (format_time(by_id[chain.root_event].occurred_at), chain.chain_id))
    # 同期共发/独立关联中的冷链事件明确给出“不并案”的理由，避免被误并入拥堵处置。
    independent_notes: list[str] = []
    for association in associations:
        if association.relation in {"co_incident", "independent"}:
            left, right = by_id[association.left], by_id[association.right]
            if left.kind == "cold_chain" or right.kind == "cold_chain":
                independent_notes.append(
                    f"{left.event_id}/{right.event_id} 未并入同一影响链："
                    + "；".join(association.reasons[-2:])
                )
    if independent_notes:
        global_uncertainties.append(Uncertainty(
            "存在未并案的冷链关联，需分别指定负责人：" + " | ".join(independent_notes),
            0.25, "separation"))
    # 未进入任何影响链的有效事件（例如被更正或重复）仍要在结果中可追踪。
    linked_ids = {member for chain in chains for member in chain.events}
    for event_id in sorted(active_ids - linked_ids):
        global_uncertainties.append(Uncertainty(
            f"事件 {event_id} 在本轮分析中未进入任何影响链，需人工确认是否遗漏", 0.3, "unlinked"))
    return Analysis(
        events=tuple(sorted(active, key=lambda event: (format_time(event.occurred_at), event.event_id))),
        associations=tuple(associations),
        chains=tuple(chains),
        superseded=dict(superseded),
        duplicates=dict(duplicates),
        withdrawn_events=withdrawn,
        global_uncertainties=tuple(global_uncertainties),
        settings={"merge_window_hours": merge_window_hours, "link_threshold": link_threshold,
                  "max_cause_gap_hours": max_cause_gap_hours},
        withdrawn_tickets=tuple(sorted(withdrawal_map)),
    )
