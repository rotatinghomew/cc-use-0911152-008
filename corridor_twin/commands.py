from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Sequence

from corridor_twin.events.logistics_event import ContractError
from corridor_twin.twin_service import Application, analyze_incidents, ingest_incidents, run, verify


def parser() -> argparse.ArgumentParser:
    root = argparse.ArgumentParser(prog='corridor_twin', description='中国—东盟跨境智慧物流走廊数字孪生')
    commands = root.add_subparsers(dest="command", required=True)
    execute = commands.add_parser("run")
    execute.add_argument("--fixture", required=True)
    execute.add_argument("--database", required=True)
    execute.add_argument("--output", required=True)
    check = commands.add_parser("verify")
    check.add_argument("--database", required=True)
    check.add_argument("--output", required=True)
    inspect = commands.add_parser("inspect")
    inspect.add_argument("--database", required=True)
    incidents = commands.add_parser("incidents")
    incidents.add_argument("--database", required=True)
    incidents.add_argument("--fixture", required=False,
                           help="事件 JSON（对象或数组）；提供后先导入再重新对账")
    return root


def _load_incident_fixture(path: str) -> list:
    try:
        value = json.loads(Path(path).read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ContractError(f"cannot read incident fixture: {exc}") from exc
    if isinstance(value, dict):
        for key in ("incidents", "events"):
            if key in value:
                value = value[key]
                break
        else:
            value = [value]
    if not isinstance(value, list) or not value:
        raise ContractError("incident fixture must be a non-empty list or object with incidents")
    return value


def main(argv: Sequence[str] | None = None) -> int:
    args = parser().parse_args(argv)
    if args.command == "run":
        result = run(args.fixture, args.database, args.output)
    elif args.command == "verify":
        result = verify(args.database, args.output)
    elif args.command == "incidents":
        if args.fixture:
            result = ingest_incidents(args.database, _load_incident_fixture(args.fixture))
        else:
            result = analyze_incidents(args.database)
    else:
        result = Application(args.database).inspect()
    print(json.dumps(result, ensure_ascii=False, sort_keys=True))
    return 0 if result.get("ok", True) else 1


if __name__ == "__main__":
    raise SystemExit(main())
