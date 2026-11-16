"""异常处置台账：工作项、分派、确认依据与拆分/更正留痕。"""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any, Iterable, Mapping

from corridor_twin.events.logistics_event import ContractError, canonical_json, stable_hash, utc_now
from corridor_twin.incidents.association import (
    DEFAULT_LINK_THRESHOLD,
    DEFAULT_MAX_CAUSE_GAP_HOURS,
    DEFAULT_MERGE_WINDOW_HOURS,
    Analysis,
    analyze,
    category,
)
from corridor_twin.incidents.model import IncidentEvent, format_time, normalized_key

SCHEMA = """
CREATE TABLE IF NOT EXISTS incident_events(
    event_id TEXT PRIMARY KEY,
    revision INTEGER NOT NULL,
    kind TEXT NOT NULL,
    node TEXT NOT NULL,
    status TEXT NOT NULL,
    fingerprint TEXT NOT NULL,
    occurred_at TEXT NOT NULL,
    reported_at TEXT NOT NULL,
    payload_json TEXT NOT NULL,
    updated_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS incident_work(
    work_id TEXT PRIMARY KEY,
    chain_id TEXT NOT NULL,
    root_kind TEXT NOT NULL,
    node TEXT NOT NULL,
    owner_role TEXT NOT NULL,
    status TEXT NOT NULL,
    revision INTEGER NOT NULL,
    event_ids_json TEXT NOT NULL,
    ticket_ids_json TEXT NOT NULL,
    basis_hash TEXT NOT NULL,
    basis_json TEXT NOT NULL,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS incident_work_history(
    work_id TEXT NOT NULL,
    revision INTEGER NOT NULL,
    change TEXT NOT NULL,
    payload_json TEXT NOT NULL,
    recorded_at TEXT NOT NULL,
    PRIMARY KEY(work_id, revision)
);
CREATE TABLE IF NOT EXISTS incident_dispositions(
    disposition_id TEXT PRIMARY KEY,
    work_id TEXT NOT NULL,
    work_revision INTEGER NOT NULL,
    basis_hash TEXT NOT NULL,
    owner_role TEXT NOT NULL,
    action TEXT NOT NULL,
    note TEXT NOT NULL,
    actor TEXT NOT NULL,
    snapshot_json TEXT NOT NULL,
    confirmed_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS incident_ticket_withdrawals(
    shipment_id TEXT PRIMARY KEY,
    order_id TEXT NOT NULL DEFAULT '',
    reason TEXT NOT NULL,
    withdrawn_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_incident_work_chain ON incident_work(chain_id, status);
CREATE INDEX IF NOT EXISTS idx_incident_dispositions_work ON incident_dispositions(work_id, confirmed_at);
"""

WORK_STATUSES = ("open", "awaiting_confirmation", "confirmed", "resolved", "superseded")

@dataclass(frozen=True)
class WorkItem:
    work_id: str
    chain_id: str
    root_kind: str
    node: str
    owner_role: str
    status: str
    revision: int
    event_ids: tuple[str, ...]
    tickets: tuple[str, ...]
    basis_hash: str
    basis: dict[str, Any]
    created_at: str
    updated_at: str

    def as_dict(self) -> dict[str, Any]:
        return {
            "work_id": self.work_id,
            "chain_id": self.chain_id,
            "root_kind": self.root_kind,
            "root_label": category(self.root_kind),
            "node": self.node,
            "owner_role": self.owner_role,
            "status": self.status,
            "revision": self.revision,
            "event_ids": list(self.event_ids),
            "tickets": list(self.tickets),
            "basis_hash": self.basis_hash,
            "created_at": self.created_at,
            "updated_at": self.updated_at,
        }


@dataclass(frozen=True)
class Disposition:
    disposition_id: str
    work_id: str
    work_revision: int
    basis_hash: str
    owner_role: str
    action: str
    note: str
    actor: str
    snapshot: dict[str, Any]
    confirmed_at: str

    def as_dict(self) -> dict[str, Any]:
        return {
            "disposition_id": self.disposition_id,
            "work_id": self.work_id,
            "work_revision": self.work_revision,
            "basis_hash": self.basis_hash,
            "owner_role": self.owner_role,
            "action": self.action,
            "note": self.note,
            "actor": self.actor,
            "snapshot": self.snapshot,
            "confirmed_at": self.confirmed_at,
        }


def _basis(chain: Any) -> dict[str, Any]:
    return {
        "chain_id": chain.chain_id,
        "root_event": chain.root_event,
        "root_kind": chain.root_kind,
        "node": chain.node,
        "events": list(chain.events),
        "relations": [dict(item) for item in chain.relations],
        "exposures": [item.as_dict() for item in chain.exposures],
        "rationale": list(chain.rationale),
        "uncertainties": [item.as_dict() for item in chain.uncertainties],
        "confidence": chain.confidence,
        "status": chain.status,
    }


def _basis_hash(basis: Mapping[str, Any]) -> str:
    return stable_hash(basis)


class IncidentBook:
    def __init__(self, repository):
        self.repository = repository
        with repository.connect() as connection:
            connection.executescript(SCHEMA)

    # ------------------------------------------------------------------ events
    def ingest(self, event: IncidentEvent | Mapping[str, Any]) -> IncidentEvent:
        record = IncidentEvent.create(event)
        with self.repository.transaction() as connection:
            row = connection.execute(
                "SELECT revision,fingerprint,payload_json FROM incident_events WHERE event_id=?",
                (record.event_id,),
            ).fetchone()
            if row is not None:
                if row["fingerprint"] == record.fingerprint:
                    return record
                if record.revision < row["revision"]:
                    raise ContractError("事件修订版落后于已存版本")
                if record.revision == row["revision"]:
                    raise ContractError("相同修订版的事件内容冲突")
            connection.execute(
                "INSERT INTO incident_events(event_id,revision,kind,node,status,fingerprint,occurred_at,reported_at,payload_json,updated_at) "
                "VALUES(?,?,?,?,?,?,?,?,?,?) ON CONFLICT(event_id) DO UPDATE SET "
                "revision=excluded.revision,kind=excluded.kind,node=excluded.node,status=excluded.status,"
                "fingerprint=excluded.fingerprint,occurred_at=excluded.occurred_at,reported_at=excluded.reported_at,"
                "payload_json=excluded.payload_json,updated_at=excluded.updated_at",
                (record.event_id, record.revision, record.kind, record.node, record.status, record.fingerprint,
                 format_time(record.occurred_at), format_time(record.reported_at),
                 canonical_json(record.as_dict()), utc_now()),
            )
            self.repository.append_event(connection, "incident.ingested", f"incident:{record.event_id}",
                                         record.revision,
                                         {"fingerprint": record.fingerprint, "status": record.status,
                                          "kind": record.kind, "node": record.node})
        return record

    def ingest_many(self, events: Iterable[IncidentEvent | Mapping[str, Any]]) -> list[IncidentEvent]:
        return [self.ingest(event) for event in events]

    def events(self) -> list[IncidentEvent]:
        with self.repository.connect() as connection:
            rows = connection.execute("SELECT payload_json FROM incident_events").fetchall()
        events = []
        for row in rows:
            payload = json.loads(row["payload_json"])
            payload.pop("fingerprint", None)
            events.append(IncidentEvent.create(payload))
        return sorted(events, key=lambda event: (format_time(event.occurred_at), event.event_id))

    def withdraw_ticket(self, shipment_id: str, reason: str, order_id: str = "") -> dict[str, Any]:
        shipment = normalized_key(shipment_id)
        if not shipment:
            raise ContractError("撤销货票必须指明 shipment_id")
        if not str(reason).strip():
            raise ContractError("撤销货票必须写明理由")
        with self.repository.transaction() as connection:
            connection.execute(
                "INSERT INTO incident_ticket_withdrawals(shipment_id,order_id,reason,withdrawn_at) VALUES(?,?,?,?) "
                "ON CONFLICT(shipment_id) DO UPDATE SET order_id=excluded.order_id,reason=excluded.reason,"
                "withdrawn_at=excluded.withdrawn_at",
                (shipment, order_id or "", str(reason).strip(), utc_now()),
            )
            self.repository.append_event(connection, "incident.ticket_withdrawn", f"shipment:{shipment}", 1,
                                         {"reason": str(reason).strip(), "order_id": order_id or ""})
        return {"shipment_id": shipment, "withdrawn": True, "reason": str(reason).strip()}

    def withdraw_ticket_and_reconcile(self, shipment_id: str, reason: str,
                                      order_id: str = "") -> dict[str, Any]:
        """撤销一票运输但保留仍影响其他货票的事件与工作项。

        运输承诺本身的取消由 TransportBook 完成；这里只记录撤销事实，
        并重新界定哪些事件/工作项仍因其他货票而继续有效。
        """
        shipment = normalized_key(shipment_id)
        before = {chain.chain_id: chain for chain in self.analyze().chains}
        withdrawal = self.withdraw_ticket(shipment, reason, order_id)
        result = self.reconcile()
        analysis = result["analysis"]
        after = {chain.chain_id: chain for chain in analysis.chains}
        surviving: list[dict[str, Any]] = []
        for chain_id, chain in before.items():
            if shipment in chain.tickets and chain_id in after:
                surviving.append({
                    "chain_id": chain_id,
                    "root_event": chain.root_event,
                    "remaining_tickets": list(after[chain_id].tickets),
                    "owner_role": after[chain_id].owner_role,
                })
        return {**withdrawal, "surviving_chains": surviving, **{
            key: result[key] for key in ("work", "dispatch", "updated", "split", "reopened")
        }}

    def withdrawals(self) -> dict[str, str]:
        with self.repository.connect() as connection:
            rows = connection.execute("SELECT shipment_id,reason FROM incident_ticket_withdrawals").fetchall()
        return {row["shipment_id"]: row["reason"] for row in rows}

    # --------------------------------------------------------------- analysis
    def _records(self) -> list[Any]:
        return self.repository.list_records()

    def _commitments(self) -> list[dict[str, Any]]:
        return self.repository.transport_orders()

    def analyze(self, *, merge_window_hours: float = DEFAULT_MERGE_WINDOW_HOURS,
                link_threshold: float = DEFAULT_LINK_THRESHOLD,
                max_cause_gap_hours: float = DEFAULT_MAX_CAUSE_GAP_HOURS) -> Analysis:
        return analyze(
            self.events(),
            records=self._records(),
            commitments=self._commitments(),
            withdrawals=self.withdrawals(),
            merge_window_hours=merge_window_hours,
            link_threshold=link_threshold,
            max_cause_gap_hours=max_cause_gap_hours,
        )

    # ------------------------------------------------------------------ work
    def _load_work(self) -> list[WorkItem]:
        with self.repository.connect() as connection:
            rows = connection.execute("SELECT * FROM incident_work").fetchall()
        return [
            WorkItem(
                work_id=row["work_id"], chain_id=row["chain_id"], root_kind=row["root_kind"],
                node=row["node"], owner_role=row["owner_role"], status=row["status"],
                revision=row["revision"], event_ids=tuple(json.loads(row["event_ids_json"])),
                tickets=tuple(json.loads(row["ticket_ids_json"])), basis_hash=row["basis_hash"],
                basis=json.loads(row["basis_json"]), created_at=row["created_at"], updated_at=row["updated_at"],
            )
            for row in rows
        ]

    def _save_work(self, work: WorkItem, change: str, detail: Mapping[str, Any]) -> None:
        with self.repository.transaction() as connection:
            connection.execute(
                "INSERT INTO incident_work(work_id,chain_id,root_kind,node,owner_role,status,revision,"
                "event_ids_json,ticket_ids_json,basis_hash,basis_json,created_at,updated_at) "
                "VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?) ON CONFLICT(work_id) DO UPDATE SET "
                "chain_id=excluded.chain_id,root_kind=excluded.root_kind,node=excluded.node,"
                "owner_role=excluded.owner_role,status=excluded.status,revision=excluded.revision,"
                "event_ids_json=excluded.event_ids_json,ticket_ids_json=excluded.ticket_ids_json,"
                "basis_hash=excluded.basis_hash,basis_json=excluded.basis_json,updated_at=excluded.updated_at",
                (work.work_id, work.chain_id, work.root_kind, work.node, work.owner_role, work.status,
                 work.revision, canonical_json(list(work.event_ids)), canonical_json(list(work.tickets)),
                 work.basis_hash, canonical_json(work.basis), work.created_at, work.updated_at),
            )
            connection.execute(
                "INSERT OR IGNORE INTO incident_work_history(work_id,revision,change,payload_json,recorded_at) "
                "VALUES(?,?,?,?,?)",
                (work.work_id, work.revision, change,
                 canonical_json({"change": change, "detail": dict(detail), "basis_hash": work.basis_hash,
                                 "owner_role": work.owner_role, "tickets": list(work.tickets),
                                 "event_ids": list(work.event_ids), "status": work.status}),
                 utc_now()),
            )
            self.repository.append_event(connection, "incident.work_changed", f"incident-work:{work.work_id}",
                                         work.revision,
                                         {"change": change, "owner_role": work.owner_role,
                                          "tickets": list(work.tickets), "status": work.status,
                                          "basis_hash": work.basis_hash})

    def reconcile(self) -> dict[str, Any]:
        analysis = self.analyze()
        existing = {work.work_id: work for work in self._load_work()}
        matched: set[str] = set()
        created: list[str] = []
        updated: list[str] = []
        split: list[str] = []
        reopened: list[str] = []
        reconciled: dict[str, WorkItem] = dict(existing)

        for chain in analysis.chains:
            basis = _basis(chain)
            basis_hash = _basis_hash(basis)
            tickets = tuple(chain.tickets)
            candidate = None
            best_overlap = 0
            for work in existing.values():
                if work.work_id in matched or work.status == "superseded":
                    continue
                direct = set(work.event_ids) & set(chain.events)
                corrected = {
                    analysis.superseded[event_id] for event_id in work.event_ids
                    if analysis.superseded.get(event_id) in set(chain.events)
                }
                overlap = max(len(direct), len(corrected))
                if overlap and overlap > best_overlap:
                    candidate, best_overlap = work, overlap
                elif overlap and candidate is None:
                    candidate, best_overlap = work, overlap
            if candidate is None:
                # 同一口岸的持续拥堵按事件重叠判断；没有重叠则视为新事件，不按节点永久并案。
                work_id = stable_hash({"chain": chain.chain_id, "root": chain.root_event})[:24]
                work = WorkItem(work_id, chain.chain_id, chain.root_kind, chain.node, chain.owner_role,
                                "open", 1, chain.events, tickets, basis_hash, basis, utc_now(), utc_now())
                self._save_work(work, "created", {
                    "reason": "首次识别影响链并分派",
                    "root_event": chain.root_event,
                    "tickets": list(tickets),
                    "uncertainties": [item.reason for item in chain.uncertainties],
                })
                reconciled[work_id] = work
                created.append(work_id)
                continue
            matched.add(candidate.work_id)
            previous_events = set(candidate.event_ids)
            current_events = set(chain.events)
            previous_tickets = set(candidate.tickets)
            # 影响链没有推断出任何货票时，保留人工改派写入的货票，避免下一次对账把责任范围清空。
            current_tickets = set(tickets) if tickets else set(candidate.tickets)
            tickets = tuple(sorted(current_tickets))
            removed_events = previous_events - current_events
            added_events = current_events - previous_events
            basis_changed = basis_hash != candidate.basis_hash
            # 依据未变时保留人工调整过的负责人；依据变化才回到按事件类型派生的负责人角色。
            owner_role = candidate.owner_role if not basis_changed else chain.owner_role
            change_parts: list[str] = []
            if added_events:
                change_parts.append("新增事件（迟到报告或补充观测）")
            if removed_events:
                change_parts.append("事件被更正/撤销，原关联拆开")
            if previous_tickets != current_tickets:
                change_parts.append("受影响货票变化")
            change = "unchanged"
            if removed_events:
                change = "split"
            elif added_events or previous_tickets != current_tickets:
                change = "reassigned"
            elif basis_changed:
                change = "rebased"

            confirmed = bool(self.dispositions(candidate.work_id))
            status = candidate.status
            if change in {"split", "reassigned", "rebased"}:
                if confirmed:
                    status = "awaiting_confirmation"
                    reopened.append(candidate.work_id)
                elif candidate.status in {"open", "awaiting_confirmation"}:
                    status = "open"
            if change == "split":
                split.append(candidate.work_id)
            revision = candidate.revision + (1 if change != "unchanged" else 0)
            work = WorkItem(
                candidate.work_id, chain.chain_id, chain.root_kind, chain.node, owner_role,
                status, revision, chain.events, tickets, basis_hash, basis,
                candidate.created_at, utc_now(),
            )
            if change != "unchanged":
                self._save_work(work, change, {
                    "reason": "；".join(change_parts),
                    "removed_events": sorted(removed_events),
                    "added_events": sorted(added_events),
                    "previous_tickets": sorted(previous_tickets),
                    "tickets": sorted(current_tickets),
                    "previous_owner_role": candidate.owner_role,
                    "owner_role": owner_role,
                    "uncertainties": [item.reason for item in chain.uncertainties],
                })
                updated.append(candidate.work_id)
            reconciled[candidate.work_id] = work

        for work_id, work in existing.items():
            if work_id in matched:
                continue
            if work.status in {"resolved", "superseded"}:
                continue
            # 原工作项已没有直接对应的影响链：不能静默关闭，必须留下去向与未完成事项。
            replacement = None
            for chain in analysis.chains:
                direct = set(work.event_ids) & set(chain.events)
                corrected = {
                    analysis.superseded[event_id] for event_id in work.event_ids
                    if analysis.superseded.get(event_id) in set(chain.events)
                }
                if direct or corrected or (set(work.tickets) & set(chain.tickets)):
                    replacement = chain
                    break
            if replacement is not None:
                surviving = next((item for item in reconciled.values()
                                  if item.chain_id == replacement.chain_id), None)
                detail = {
                    "reason": "原影响链被更正/拆分后并入其他影响链，工作项不再单独保留",
                    "previous_events": list(work.event_ids),
                    "merged_into_chain": replacement.chain_id,
                    "merged_into_work": surviving.work_id if surviving else "",
                    "inherited_owner_role": replacement.owner_role,
                    "unresolved_events": [event_id for event_id in work.event_ids
                                          if event_id in set(replacement.events)],
                    "tickets": list(replacement.tickets),
                }
                absorbed = WorkItem(
                    work.work_id, replacement.chain_id, replacement.root_kind, replacement.node,
                    replacement.owner_role, "superseded", work.revision + 1, work.event_ids,
                    tuple(replacement.tickets), _basis_hash(_basis(replacement)), _basis(replacement),
                    work.created_at, utc_now(),
                )
                self._save_work(absorbed, "merged", detail)
                updated.append(work_id)
                continue
            detail = {
                "reason": "原影响链在当前证据下不再成立（可能因更正、撤销或重复导入）；未解决事件仍保留在本工作项",
                "previous_events": list(work.event_ids),
                "remaining_events": [event_id for event_id in work.event_ids
                                     if any(event.event_id == event_id and event.status != "withdrawn"
                                            for event in analysis.events)],
                "tickets": list(work.tickets),
                "owner_role": work.owner_role,
            }
            updated_work = WorkItem(
                work.work_id, work.chain_id, work.root_kind, work.node, work.owner_role,
                "awaiting_confirmation", work.revision + 1, work.event_ids, work.tickets,
                work.basis_hash, work.basis, work.created_at, utc_now(),
            )
            self._save_work(updated_work, "needs_reassignment", detail)
            updated.append(work_id)

        works = self._load_work()
        return {
            "analysis": analysis,
            "work": works,
            "created": created,
            "updated": updated,
            "split": split,
            "reopened": reopened,
            "dispatch": self.dispatch_board(analysis=analysis, works=works),
        }

    def dispositions(self, work_id: str | None = None) -> list[Disposition]:
        query = "SELECT * FROM incident_dispositions"
        params: tuple[Any, ...] = ()
        if work_id:
            query += " WHERE work_id=?"
            params = (work_id,)
        query += " ORDER BY confirmed_at, disposition_id"
        with self.repository.connect() as connection:
            rows = connection.execute(query, params).fetchall()
        return [
            Disposition(
                disposition_id=row["disposition_id"], work_id=row["work_id"],
                work_revision=row["work_revision"], basis_hash=row["basis_hash"],
                owner_role=row["owner_role"], action=row["action"], note=row["note"],
                actor=row["actor"], snapshot=json.loads(row["snapshot_json"]),
                confirmed_at=row["confirmed_at"],
            )
            for row in rows
        ]

    def assign(self, work_id: str, owner_role: str, tickets: Iterable[str],
               actor: str, note: str = "") -> WorkItem:
        """人工重新明确未完成工作的负责人与货票范围（例如拆分后新链暂无已知货票）。"""
        work = next((item for item in self._load_work() if item.work_id == work_id), None)
        if work is None:
            raise ContractError("工作项不存在")
        if work.status in {"resolved", "superseded"}:
            raise ContractError("已结束的工作项不能再改派")
        if not owner_role.strip() or not actor.strip():
            raise ContractError("改派需记录负责人与操作人")
        scope = tuple(sorted({normalized_key(ticket) for ticket in tickets if normalized_key(ticket)}))
        if not scope:
            raise ContractError("改派必须明确至少一票受影响货票")
        assigned = WorkItem(
            work.work_id, work.chain_id, work.root_kind, work.node, owner_role.strip(),
            "awaiting_confirmation" if work.status == "confirmed" else "open",
            work.revision + 1, work.event_ids, scope, work.basis_hash, work.basis,
            work.created_at, utc_now(),
        )
        self._save_work(assigned, "assigned", {
            "reason": note.strip() or "人工重新明确负责人与货票",
            "actor": actor.strip(),
            "previous_owner_role": work.owner_role,
            "owner_role": owner_role.strip(),
            "previous_tickets": list(work.tickets),
            "tickets": list(scope),
        })
        return assigned

    def confirm(self, work_id: str, owner_role: str, action: str, actor: str, note: str = "") -> Disposition:
        work = next((item for item in self._load_work() if item.work_id == work_id), None)
        if work is None:
            raise ContractError("工作项不存在")
        if work.status in {"resolved", "superseded"}:
            raise ContractError("已结束的工作项不能再确认处置")
        if not owner_role.strip() or not action.strip() or not actor.strip():
            raise ContractError("确认处置需写明负责人、处置动作与确认人")
        if not work.tickets:
            raise ContractError("工作项没有受影响货票，不能确认处置范围")
        analysis = self.analyze()
        chain = None
        for event_id in work.event_ids:
            chain = analysis.chain_for_event(event_id)
            if chain is not None:
                break
        if chain is None:
            raise ContractError("工作项依据的事件已不构成影响链，须先重新对账分派")
        basis = _basis(chain)
        current_basis_hash = _basis_hash(basis)
        if current_basis_hash != work.basis_hash:
            raise ContractError("影响链依据已变化，须先重新对账后再确认处置")
        snapshot = {
            "basis": basis,
            "basis_hash": current_basis_hash,
            "work_revision": work.revision,
            "owner_role": owner_role,
            "tickets": list(work.tickets),
            "event_ids": list(work.event_ids),
            "confirmed_at": utc_now(),
        }
        disposition_id = stable_hash({"work": work_id, "revision": work.revision,
                                      "basis_hash": current_basis_hash, "actor": actor,
                                      "action": action, "note": note})[:32]
        with self.repository.transaction() as connection:
            connection.execute(
                "INSERT OR IGNORE INTO incident_dispositions(disposition_id,work_id,work_revision,basis_hash,"
                "owner_role,action,note,actor,snapshot_json,confirmed_at) VALUES(?,?,?,?,?,?,?,?,?,?)",
                (disposition_id, work_id, work.revision, current_basis_hash, owner_role.strip(),
                 action.strip(), note.strip(), actor.strip(), canonical_json(snapshot), snapshot["confirmed_at"]),
            )
            connection.execute(
                "UPDATE incident_work SET status='confirmed',owner_role=?,updated_at=? WHERE work_id=?",
                (owner_role.strip(), utc_now(), work_id),
            )
            self.repository.append_event(connection, "incident.disposition_confirmed", f"incident-work:{work_id}",
                                         work.revision,
                                         {"disposition_id": disposition_id, "basis_hash": current_basis_hash,
                                          "owner_role": owner_role.strip(), "tickets": list(work.tickets)})
        return Disposition(disposition_id, work_id, work.revision, current_basis_hash, owner_role.strip(),
                           action.strip(), note.strip(), actor.strip(), snapshot, snapshot["confirmed_at"])

    def resolve(self, work_id: str, actor: str, note: str = "") -> WorkItem:
        work = next((item for item in self._load_work() if item.work_id == work_id), None)
        if work is None:
            raise ContractError("工作项不存在")
        if not actor.strip():
            raise ContractError("结束工作项需记录操作人")
        analysis = self.analyze()
        chains = {chain.chain_id: chain for chain in analysis.chains}
        chain = chains.get(work.chain_id)
        if chain is None:
            raise ContractError("工作项对应的影响链已不存在，需先重新分派")
        if any(item.status != "resolved" for item in analysis.events if item.event_id in work.event_ids):
            raise ContractError("仍有未解决事件，不能关闭该工作项")
        if any(chain.status != "resolved" for chain in analysis.chains
               if set(chain.events) & set(work.event_ids)):
            raise ContractError("影响链仍未解决，不能关闭该工作项")
        resolved = WorkItem(
            work.work_id, work.chain_id, work.root_kind, work.node, work.owner_role,
            "resolved", work.revision + 1, work.event_ids, work.tickets, work.basis_hash,
            work.basis, work.created_at, utc_now(),
        )
        self._save_work(resolved, "resolved", {"actor": actor.strip(), "note": note.strip(),
                                               "tickets": list(work.tickets)})
        return resolved

    def _historical_tickets(self, work_id: str) -> set[str]:
        with self.repository.connect() as connection:
            rows = connection.execute(
                "SELECT payload_json FROM incident_work_history WHERE work_id=? ORDER BY revision",
                (work_id,)).fetchall()
        tickets: set[str] = set()
        for row in rows:
            detail = json.loads(row["payload_json"]).get("detail", {})
            for key in ("tickets", "previous_tickets"):
                tickets.update(str(item) for item in detail.get(key) or [])
        return tickets

    def dispatch_board(self, *, analysis: Analysis | None = None,
                       works: Iterable[WorkItem] | None = None) -> list[dict[str, Any]]:
        analysis = analysis or self.analyze()
        works = list(works) if works is not None else self._load_work()
        chain_by_id = {chain.chain_id: chain for chain in analysis.chains}
        board: list[dict[str, Any]] = []
        for work in works:
            if work.status in {"resolved", "superseded"}:
                continue
            chain = chain_by_id.get(work.chain_id)
            active_tickets = [ticket for ticket in work.tickets
                              if ticket not in analysis.withdrawn_tickets]
            withdrawn_tickets = sorted((set(work.tickets) | self._historical_tickets(work.work_id))
                                       & set(analysis.withdrawn_tickets))
            board.append({
                "work_id": work.work_id,
                "owner_role": work.owner_role,
                "status": work.status,
                "root_kind": work.root_kind,
                "root_label": category(work.root_kind),
                "node": work.node,
                "event_ids": list(work.event_ids),
                "tickets": active_tickets,
                "withdrawn_tickets": withdrawn_tickets,
                "commitment_impacts": ([item.as_dict() for item in chain.exposures] if chain else []),
                "uncertainties": ([item.reason for item in chain.uncertainties] if chain else []),
                "requires_confirmation": work.status in {"open", "awaiting_confirmation"},
                "last_confirmed": (self.dispositions(work.work_id)[-1].as_dict()
                                   if self.dispositions(work.work_id) else None),
            })
        return board

    def inspect(self) -> dict[str, Any]:
        analysis = self.analyze()
        return {
            "events": [event.as_dict() for event in analysis.events],
            "associations": [item.as_dict() for item in analysis.associations],
            "chains": [chain.as_dict() for chain in analysis.chains],
            "superseded": dict(analysis.superseded),
            "duplicates": dict(analysis.duplicates),
            "withdrawn_events": list(analysis.withdrawn_events),
            "uncertainties": [item.as_dict() for item in analysis.global_uncertainties],
            "work": [work.as_dict() for work in self._load_work()],
            "dispatch": self.dispatch_board(analysis=analysis),
            "dispositions": [item.as_dict() for item in self.dispositions()],
        }
