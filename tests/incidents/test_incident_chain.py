import json
import os
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path

from corridor_twin.events.logistics_event import ContractError, DomainRecord
from corridor_twin.incidents.association import analyze
from corridor_twin.incidents.model import IncidentEvent
from corridor_twin.twin_service import Application

BASE = datetime(2026, 11, 16, tzinfo=timezone.utc)


def at(hours: float, minutes: float = 0) -> str:
    return (BASE + timedelta(hours=hours, minutes=minutes)).isoformat()


def event(event_id, kind, node, hour, *, route="a-b-c", source="port_authority", supersedes="",
          status="open", severity=4, reported=None, shipment="", payload=None, revision=1, credibility=None):
    body = {"segments": route.split("-")}
    body.update(payload or {})
    value = {
        "event_id": event_id,
        "kind": kind,
        "node": node,
        "occurred_at": at(hour),
        "reported_at": at(hour) if reported is None else reported,
        "source": source,
        "route_code": route,
        "severity": severity,
        "status": status,
        "supersedes": supersedes,
        "shipment_id": shipment,
        "payload": body,
        "revision": revision,
    }
    if credibility is not None:
        value["credibility"] = credibility
    return value


class AssociationTests(unittest.TestCase):
    def setUp(self):
        self.records = [
            {"record_id": "s1", "payload": {"shipment_id": "s1", "route_code": "a-b-c", "cargo_class": "cold"}},
            {"record_id": "s2", "payload": {"shipment_id": "s2", "route_code": "b-c-d", "cargo_class": "dry"}},
            {"record_id": "s3", "payload": {"shipment_id": "s3", "route_code": "x-y-z", "cargo_class": "dry"}},
        ]
        self.commitments = [
            {"order_id": "o1", "shipment_id": "s1", "status": "dispatched"},
            {"order_id": "o2", "shipment_id": "s2", "status": "reserved"},
        ]

    def test_same_port_reports_merge_into_one_chain_and_do_not_repeat_per_ticket(self):
        analysis = analyze(
            [event("port-1", "port_congestion", "b", 1),
             event("port-2", "port_congestion", "b", 2, source="carrier")],
            records=self.records, commitments=self.commitments,
        )
        self.assertEqual(len(analysis.chains), 1)
        chain = analysis.chains[0]
        self.assertEqual(chain.events, ("port-1", "port-2"))
        self.assertEqual(chain.tickets, ("s1", "s2"))
        self.assertTrue(any("同一口岸同类报告" in reason for reason in chain.rationale))
        associations = [item for item in analysis.associations if item.relation == "same_incident"]
        self.assertEqual(len(associations), 1)
        self.assertGreater(associations[0].score, 0.5)

    def test_cold_chain_is_not_swallowed_by_port_event_without_causal_evidence(self):
        analysis = analyze(
            [event("port-1", "port_congestion", "b", 1),
             event("cold-1", "cold_chain", "b", 2, source="sensor", payload={"cargo_class": "cold"})],
            records=self.records, commitments=self.commitments,
        )
        roots = {chain.root_event for chain in analysis.chains}
        self.assertEqual(roots, {"port-1", "cold-1"})
        notes = [item.reason for item in analysis.global_uncertainties]
        self.assertTrue(any("未并入同一影响链" in note for note in notes))
        cold_chain = next(chain for chain in analysis.chains if chain.root_event == "cold-1")
        self.assertEqual(cold_chain.owner_role, "冷链保障席")
        self.assertIn("冷链", "".join(cold_chain.rationale))

    def test_declared_cause_is_the_only_way_cold_chain_joins_port_chain(self):
        analysis = analyze(
            [event("port-1", "port_congestion", "b", 1),
             event("cold-1", "cold_chain", "b", 2, source="sensor",
                   payload={"cargo_class": "cold", "cause_event_id": "port-1"})],
            records=self.records, commitments=self.commitments,
        )
        self.assertEqual(len(analysis.chains), 1)
        self.assertEqual(analysis.chains[0].events, ("port-1", "cold-1"))
        association = next(item for item in analysis.associations if item.relation == "upstream_cause")
        self.assertTrue(any("显式因果证据" in reason for reason in association.reasons))

    def test_route_overlap_and_order_are_recorded_as_chain_reasons(self):
        analysis = analyze(
            [event("port-1", "port_congestion", "b", 1, route="a-b-c"),
             event("customs-1", "customs_hold", "c", 4, route="b-c-d", source="customs_system")],
            records=self.records, commitments=self.commitments,
        )
        chain = next(chain for chain in analysis.chains if chain.root_event == "port-1")
        joined = " ".join(chain.rationale)
        self.assertIn("路线路段重合", joined)
        self.assertIn("发生顺序", joined)
        self.assertIn("观测可信度", joined)

    def test_low_credibility_carries_uncertainty_into_dispatch_evidence(self):
        analysis = analyze(
            [event("port-1", "port_congestion", "b", 1, source="manual", credibility=0.2)],
            records=self.records,
        )
        chain = analysis.chains[0]
        self.assertTrue(chain.uncertainties)
        self.assertLess(chain.confidence, 0.5)
        self.assertTrue(any("可信度" in item.reason for item in chain.uncertainties))

    def test_late_report_is_flagged_and_late_correction_splits_existing_association(self):
        analysis = analyze(
            [event("port-1", "port_congestion", "b", 1),
             event("port-2", "port_congestion", "b", 2, source="carrier"),
             event("port-fix", "port_congestion", "x", 2, route="x-y-z", supersedes="port-2",
                   reported=at(50))],
            records=self.records,
        )
        self.assertEqual(analysis.superseded, {"port-2": "port-fix"})
        self.assertEqual({chain.root_event for chain in analysis.chains}, {"port-1", "port-fix"})
        notes = " ".join(item.reason for item in analysis.global_uncertainties)
        self.assertIn("更正", notes)
        self.assertIn("迟到", notes)

    def test_duplicate_import_collapses_observation_without_creating_extra_chain(self):
        first = event("port-1", "port_congestion", "b", 1)
        duplicate = dict(first, event_id="port-copy")
        analysis = analyze([first, duplicate], records=self.records)
        self.assertEqual(analysis.duplicates, {"port-copy": "port-1"})
        self.assertEqual(len(analysis.chains), 1)
        self.assertEqual(analysis.chains[0].events, ("port-1",))
        self.assertFalse(any("未进入任何影响链" in item.reason for item in analysis.global_uncertainties))

    def test_cold_chain_exposure_requires_cargo_match(self):
        analysis = analyze(
            [event("cold-1", "cold_chain", "b", 1, source="sensor", payload={"cargo_class": "cold"})],
            records=self.records,
        )
        chain = analysis.chains[0]
        self.assertEqual(chain.tickets, ("s1",))
        self.assertNotIn("s2", chain.tickets)

    def test_commitment_impact_reports_route_and_status(self):
        analysis = analyze(
            [event("port-1", "port_congestion", "b", 1)],
            records=self.records, commitments=self.commitments,
        )
        exposures = {item.shipment_id: item for item in analysis.chains[0].exposures}
        self.assertEqual(exposures["s1"].impact, "in_transit")
        self.assertEqual(exposures["s2"].impact, "route_impacted")
        self.assertIn("改线", exposures["s1"].action)
        self.assertIn("重新预订", exposures["s2"].action)


class IncidentBookTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.app = Application(os.path.join(self.temp.name, "state.sqlite3"))
        for shipment, route, cargo in (("s1", "a-b-c", "cold"), ("s2", "b-c-d", "dry")):
            self.app.ingest(DomainRecord(shipment, {
                "shipment_id": shipment, "route_code": route, "cargo_class": cargo,
                "eta_min": 60, "capacity_units": 10, "priority": 3,
            }))
        book = self.app.transport_book
        for edge, start, end in (("ab", "a", "b"), ("bc", "b", "c"), ("cd", "c", "d")):
            book.capacity(edge, "w", start, end, 5)
        book.reserve("o1", "s1", "w", ["ab", "bc"], 1, {"ab": 1, "bc": 1})
        book.reserve("o2", "s2", "w", ["bc", "cd"], 1, {"bc": 1, "cd": 1})
        book.transition("o1", "dispatched")
        self.book = self.app.incidents

    def test_reconcile_creates_separate_owners_for_congestion_and_cold_chain(self):
        self.book.ingest_many([
            event("port-1", "port_congestion", "b", 1),
            event("cold-1", "cold_chain", "b", 2, source="sensor", payload={"cargo_class": "cold"}),
        ])
        result = self.book.reconcile()
        owners = {work.root_kind: work.owner_role for work in result["work"]}
        self.assertEqual(owners["port_congestion"], "口岸拥堵协调席")
        self.assertEqual(owners["cold_chain"], "冷链保障席")
        self.assertTrue(all(work.tickets == ("s1",) for work in result["work"]
                            if work.root_kind == "cold_chain"))
        port_work = next(work for work in result["work"] if work.root_kind == "port_congestion")
        self.assertEqual(port_work.tickets, ("s1", "s2"))

    def test_confirmed_disposition_keeps_basis_and_blocks_stale_confirmation(self):
        self.book.ingest(event("port-1", "port_congestion", "b", 1))
        result = self.book.reconcile()
        work = result["work"][0]
        disposition = self.book.confirm(work.work_id, work.owner_role, "统一口岸处置", "duty-1", "初始依据")
        self.assertEqual(disposition.basis_hash, work.basis_hash)
        self.assertEqual(disposition.snapshot["work_revision"], 1)
        self.assertIn("rationale", disposition.snapshot["basis"])
        replayed = self.book.confirm(work.work_id, work.owner_role, "统一口岸处置", "duty-1", "初始依据")
        self.assertEqual(replayed.disposition_id, disposition.disposition_id)
        self.book.ingest(event("port-2", "port_congestion", "b", 2, source="carrier"))
        with self.assertRaisesRegex(ContractError, "重新对账"):
            self.book.confirm(work.work_id, work.owner_role, "旧依据", "duty-2")
        self.assertEqual(self.book.dispositions(work.work_id)[0].basis_hash, work.basis_hash)

    def test_duplicate_import_does_not_create_new_work_or_lose_open_ticket(self):
        self.book.ingest(event("port-1", "port_congestion", "b", 1))
        first = self.book.reconcile()
        self.book.ingest(dict(event("port-1", "port_congestion", "b", 1)))
        second = self.book.reconcile()
        self.assertEqual(second["created"], [])
        self.assertEqual(len(second["work"]), len(first["work"]))
        board = second["dispatch"]
        self.assertEqual(board[0]["tickets"], ["s1", "s2"])
        self.assertTrue(board[0]["requires_confirmation"])

    def test_late_bridge_event_reopens_confirmed_work_and_reassigns_tickets(self):
        self.book.ingest_many([
            event("port-1", "port_congestion", "b", 1, route="a-b-c"),
            event("customs-1", "customs_hold", "d", 2, route="c-d-e", source="customs_system"),
        ])
        result = self.book.reconcile()
        port_work = next(work for work in result["work"] if work.root_kind == "port_congestion")
        self.book.confirm(port_work.work_id, port_work.owner_role, "按票分别处理", "duty-1")
        self.book.ingest(event("bridge", "port_congestion", "c", 1.5, route="a-b-c-d-e", reported=at(40)))
        merged = self.book.reconcile()
        self.assertTrue(merged["reopened"])
        merged_work = next(work for work in merged["work"] if work.status == "awaiting_confirmation")
        self.assertEqual(set(merged_work.tickets), {"s1", "s2"})
        self.assertEqual(merged_work.owner_role, "口岸拥堵协调席")
        history = self.book.dispositions(port_work.work_id)
        self.assertEqual(len(history), 1)
        self.assertNotEqual(history[0].work_revision, merged_work.revision)

    def test_withdrawing_one_ticket_does_not_close_event_for_other_tickets(self):
        self.book.ingest(event("port-1", "port_congestion", "b", 1))
        result = self.book.reconcile()
        work = result["work"][0]
        self.assertEqual(set(work.tickets), {"s1", "s2"})
        withdrawal = self.book.withdraw_ticket_and_reconcile("s1", "客户撤销")
        self.assertTrue(withdrawal["surviving_chains"])
        remaining = next(item for item in withdrawal["dispatch"] if item["work_id"] == work.work_id)
        self.assertEqual(remaining["tickets"], ["s2"])
        self.assertIn("s1", remaining["withdrawn_tickets"])
        self.book.ingest(event("port-1", "port_congestion", "b", 1, status="open", revision=2,
                               payload={"segments": ["a", "b", "c"], "note": "still congested"}))
        with self.assertRaisesRegex(ContractError, "未解决"):
            self.book.resolve(work.work_id, "duty-1", "仅撤销一票")
        self.assertTrue(any(item.event_id == "port-1" for item in self.book.events()))

    def test_correction_splits_work_and_new_work_needs_owner_and_tickets(self):
        self.book.ingest_many([
            event("port-1", "port_congestion", "b", 1),
            event("port-2", "port_congestion", "b", 2, source="carrier"),
        ])
        split_source = self.book.reconcile()
        self.assertEqual(len(split_source["work"]), 1)
        self.book.confirm(split_source["work"][0].work_id, "口岸拥堵协调席", "并案处置", "duty-1")
        self.book.ingest(event("port-fix", "port_congestion", "x", 2, route="x-y-z", supersedes="port-2"))
        result = self.book.reconcile()
        self.assertTrue(result["split"])
        by_node = {work.node: work for work in result["work"] if work.status != "superseded"}
        self.assertIn("b", by_node)
        self.assertIn("x", by_node)
        self.assertEqual(by_node["b"].status, "awaiting_confirmation")
        self.assertEqual(by_node["x"].status, "open")
        self.assertEqual(by_node["x"].owner_role, "口岸拥堵协调席")
        self.assertEqual(by_node["x"].tickets, ())
        with self.assertRaisesRegex(ContractError, "没有受影响货票"):
            self.book.confirm(by_node["x"].work_id, "口岸拥堵协调席", "直接关闭", "duty-2")
        assigned = self.book.assign(by_node["x"].work_id, "路网调度席", ["s2"], "duty-1", "更正后改由路网席跟踪")
        self.assertEqual(assigned.owner_role, "路网调度席")
        self.assertEqual(assigned.tickets, ("s2",))
        self.assertEqual(assigned.revision, by_node["x"].revision + 1)
        stable = self.book.reconcile()
        kept = next(work for work in stable["work"] if work.work_id == assigned.work_id)
        self.assertEqual(kept.owner_role, "路网调度席")
        self.assertEqual(kept.tickets, ("s2",))
        history = self.app.repository.connect().execute(
            "SELECT change FROM incident_work_history WHERE work_id=? ORDER BY revision",
            (by_node["b"].work_id,)).fetchall()
        self.assertIn("split", [row[0] for row in history])

    def test_resolve_requires_all_events_resolved_then_closes_work(self):
        self.book.ingest(event("port-1", "port_congestion", "b", 1))
        work = self.book.reconcile()["work"][0]
        with self.assertRaisesRegex(ContractError, "未解决"):
            self.book.resolve(work.work_id, "duty-1")
        self.book.ingest(event("port-1", "port_congestion", "b", 1, status="resolved", revision=2))
        resolved = self.book.resolve(work.work_id, "duty-1", "口岸恢复")
        self.assertEqual(resolved.status, "resolved")
        board = self.book.dispatch_board()
        self.assertNotIn(work.work_id, [item["work_id"] for item in board])

    def test_inspect_exposes_uncertainties_for_control_tower(self):
        self.book.ingest(event("port-1", "port_congestion", "b", 1, source="manual", credibility=0.3))
        self.book.reconcile()
        payload = self.app.inspect()["incidents"]
        self.assertTrue(payload["work"])
        self.assertTrue(payload["dispatch"])
        self.assertTrue(payload["uncertainties"] or payload["chains"][0]["uncertainties"])
        self.assertIn("associations", payload)


class IncidentFixtureRunTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.fixture = Path(__file__).resolve().parents[2] / "fixtures" / "incident_chain.json"

    def test_acceptance_fixture_exports_incident_bundle_and_verifies(self):
        from corridor_twin.twin_service import run, verify
        result = run(self.fixture, self.root / "db.sqlite3", self.root / "out")
        self.assertIn("incidents", result)
        self.assertTrue((self.root / "out" / "incidents.json").is_file())
        manifest = json.loads((self.root / "out" / "manifest.json").read_text(encoding="utf-8"))
        self.assertIn("incidents.json", manifest["files"])
        self.assertTrue(verify(self.root / "db.sqlite3", self.root / "out")["ok"])
        incidents = result["incidents"]
        self.assertEqual(incidents["duplicates"], {"port-hekou-02-dup": "port-hekou-02"})
        roots = {chain["root_event"]: chain for chain in incidents["chains"]}
        self.assertIn("port-hekou-01", roots)
        self.assertEqual(roots["port-hekou-01"]["owner_role"], "口岸拥堵协调席")
        self.assertIn("cold-s1-01", roots)
        self.assertEqual(roots["cold-s1-01"]["owner_role"], "冷链保障席")
        self.assertNotEqual(roots["port-hekou-01"]["tickets"], roots["cold-s1-01"]["tickets"])
        self.assertTrue(roots["port-hekou-01"]["rationale"])

    def test_rerun_is_idempotent_for_incident_work(self):
        from corridor_twin.twin_service import analyze_incidents, run
        first = run(self.fixture, self.root / "db.sqlite3", self.root / "out")
        database = str(self.root / "db.sqlite3")
        replayed = analyze_incidents(database)
        again = analyze_incidents(database)
        self.assertEqual(first["incidents"]["work"], replayed["work"])
        self.assertEqual(again["created"], [])
        self.assertEqual(again["updated"], [])
        self.assertEqual(len(again["work"]), len(first["incidents"]["work"]))
        self.assertEqual(again["analysis"]["duplicates"], {"port-hekou-02-dup": "port-hekou-02"})


if __name__ == "__main__":
    unittest.main()
