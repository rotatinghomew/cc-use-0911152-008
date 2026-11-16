from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Mapping

from corridor_twin.events.logistics_event import ContractError, Decision, DomainRecord, records_from_fixture, stable_hash
from corridor_twin.state.twin_repository import Repository
from corridor_twin.dispatch.decision_flow import advance_for_outcome, decide
from corridor_twin.visibility.control_tower import export_bundle, verify_bundle
from corridor_twin.network.capacity_replay import derive_context
from corridor_twin.dispatch.transport_book import TransportBook
from corridor_twin.incidents.book import IncidentBook
from corridor_twin.incidents.model import IncidentEvent


class Application:
    def __init__(self, database: str | Path, context: Mapping[str, Any] | None = None):
        self.repository = Repository(database)
        self.transport_book = TransportBook(self.repository)
        self.incidents = IncidentBook(self.repository)
        self.context = dict(context or {})

    def ingest(self, record: DomainRecord) -> DomainRecord:
        current = self.repository.get_record(record.record_id)
        if current is None:
            self.repository.save_record(record)
            return record
        if current.fingerprint == record.fingerprint:
            return current
        replacement = DomainRecord(record.record_id, record.payload, current.state,
                                   current.version + 1, record.occurred_at, record.source)
        self.repository.save_record(replacement, expected_version=current.version)
        return replacement

    def process(self, record: DomainRecord) -> tuple[DomainRecord, Decision]:
        stored = self.ingest(record)
        context = dict(self.context)
        context.update(derive_context(stored, self.repository))
        decision = decide(stored, context)
        self.repository.save_decision(decision)
        advanced = advance_for_outcome(stored, decision)
        if advanced.fingerprint != stored.fingerprint:
            self.repository.save_record(advanced, expected_version=stored.version)
        return advanced, decision

    def run_fixture(self, fixture: Mapping[str, Any], output: str | Path) -> dict[str, Any]:
        records = records_from_fixture(fixture)
        fixture_hash = stable_hash(fixture)
        run_id = self.repository.begin_run(fixture_hash, len(records))
        completed = self.repository.completed_run(run_id)
        if completed is not None:
            return completed
        decisions: dict[str, Decision] = {}
        final_records: list[DomainRecord] = []
        for record in records:
            final_record, decision = self.process(record)
            final_records.append(final_record)
            decisions[record.record_id] = decision
        snapshot = self.repository.snapshot()
        if snapshot["chain_errors"]:
            raise ContractError("audit chain verification failed")
        incident_result = None
        if fixture.get("incidents"):
            self.incidents.ingest_many(fixture["incidents"])
            reconciled = self.incidents.reconcile()
            incident_result = {
                "chains": [chain.as_dict() for chain in reconciled["analysis"].chains],
                "work": [work.as_dict() for work in reconciled["work"]],
                "dispatch": reconciled["dispatch"],
                "created": reconciled["created"],
                "updated": reconciled["updated"],
                "split": reconciled["split"],
                "reopened": reconciled["reopened"],
                "superseded": dict(reconciled["analysis"].superseded),
                "duplicates": dict(reconciled["analysis"].duplicates),
                "withdrawn_events": list(reconciled["analysis"].withdrawn_events),
                "withdrawn_tickets": list(reconciled["analysis"].withdrawn_tickets),
                "uncertainties": [item.as_dict()
                                  for item in reconciled["analysis"].global_uncertainties],
            }
            snapshot = self.repository.snapshot()
        manifest = export_bundle(output, final_records, decisions, snapshot,
                                 self.repository.event_rows(), incident_result)
        result = {"run_id": run_id, "fixture_hash": fixture_hash, "records": len(final_records),
                  "decisions": len(decisions), "manifest": manifest, "snapshot": snapshot}
        if incident_result is not None:
            result["incidents"] = incident_result
        self.repository.finish_run(run_id, result)
        return result

    def inspect(self) -> dict[str, Any]:
        return {"snapshot": self.repository.snapshot(), "records": [record.__dict__ for record in self.repository.list_records()],
                "decisions": self.repository.decision_rows(), "transport": self.transport_book.inspect(),
                "incidents": self.incidents.inspect()}

    def withdraw_ticket(self, shipment_id: str, reason: str) -> dict[str, Any]:
        """撤销一票运输：只取消未发运承诺，不关闭仍影响其他货票的事件。"""
        orders = [row for row in self.repository.transport_orders()
                  if row["shipment_id"] == shipment_id and row["status"] in {"reserved", "dispatched"}]
        if any(row["status"] == "dispatched" for row in orders):
            raise ContractError("已发运承诺不可撤销或改写")
        for row in orders:
            self.transport_book.transition(row["order_id"], "cancelled")
        return self.incidents.withdraw_ticket_and_reconcile(shipment_id, reason)


def load_fixture(path: str | Path) -> dict[str, Any]:
    try:
        value = json.loads(Path(path).read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ContractError(f"cannot read fixture: {exc}") from exc
    if not isinstance(value, dict):
        raise ContractError("fixture root must be an object")
    return value


def run(fixture_path: str | Path, database: str | Path, output: str | Path,
        context: Mapping[str, Any] | None = None) -> dict[str, Any]:
    return Application(database, context).run_fixture(load_fixture(fixture_path), output)


def verify(database: str | Path, output: str | Path) -> dict[str, Any]:
    repository = Repository(database)
    chain_errors = repository.verify_chain()
    bundle_errors = verify_bundle(output)
    return {"ok": not chain_errors and not bundle_errors, "chain_errors": chain_errors,
            "bundle_errors": bundle_errors, "snapshot": repository.snapshot()}


def ingest_incidents(database: str | Path, events: list[IncidentEvent | Mapping[str, Any]]) -> dict[str, Any]:
    application = Application(database)
    application.incidents.ingest_many(events)
    return analyze_incidents(database)


def analyze_incidents(database: str | Path) -> dict[str, Any]:
    application = Application(database)
    result = application.incidents.reconcile()
    analysis = result["analysis"]
    payload = {
        "analysis": {key: value for key, value in analysis.as_dict().items() if key != "events"}
        | {"event_count": len(analysis.events)},
        "work": [work.as_dict() for work in result["work"]],
        "dispatch": result["dispatch"],
        "created": result["created"],
        "updated": result["updated"],
        "split": result["split"],
        "reopened": result["reopened"],
    }
    return {"ok": True, **payload}
