from __future__ import annotations

import argparse
import json
from dataclasses import asdict
from pathlib import Path

from .contracts import MetricRequest, canonical


def load(path: Path):
    return json.loads(path.read_text())


def main():
    parser = argparse.ArgumentParser(description="Bookshop governed-data companion")
    parser.add_argument("--work", type=Path, default=Path("work"))
    commands = parser.add_subparsers(dest="command", required=True)
    commands.add_parser("seed")
    commands.add_parser("versions")
    q = commands.add_parser("query")
    q.add_argument("--principal", required=True)
    q.add_argument("--metric", default="net_revenue")
    q.add_argument("--start", default="2026-09-01")
    q.add_argument("--end", default="2026-10-01")
    q.add_argument("--currency", default="USD")
    q.add_argument("--snapshot", type=int)
    q = commands.add_parser("sql")
    q.add_argument("--principal", required=True)
    q.add_argument("query")
    for name in ("retain", "replay"):
        q = commands.add_parser(name)
        q.add_argument("receipt", type=Path)
        if name == "replay":
            q.add_argument("--principal", required=True)
    q = commands.add_parser("acknowledge")
    q.add_argument("run_id")
    q = commands.add_parser("export-traces")
    q.add_argument("output", type=Path)
    q = commands.add_parser("export-tools")
    q.add_argument("output", type=Path)
    q = commands.add_parser("ingest-traces")
    q.add_argument("input", type=Path)
    for name in ("ingest-evaluations", "ingest-feedback", "ingest-tools"):
        q = commands.add_parser(name)
        q.add_argument("input", type=Path)
    q = commands.add_parser("index-sync")
    q.add_argument("image", type=Path)
    q = commands.add_parser("search")
    q.add_argument("text")
    q.add_argument("--principal", required=True)
    q = commands.add_parser("search-current")
    q.add_argument("text")
    q.add_argument("--principal", required=True)
    q = commands.add_parser("assess")
    q.add_argument("metrics", type=Path)
    q = commands.add_parser("plan")
    q.add_argument("metrics", type=Path)
    q.add_argument("--max-bytes", type=int, required=True)
    q.add_argument("--candidate-bytes", type=int, required=True)
    q.add_argument("--writers-quiesced", action="store_true")
    q = commands.add_parser("review-plan")
    q.add_argument("proposal", type=Path)
    q.add_argument("--reviewer", required=True)
    q.add_argument("--decision", choices=("accept","reject"), required=True)
    q.add_argument("--reason-code", required=True)
    q = commands.add_parser("incident")
    q.add_argument("packet", type=Path)
    q = commands.add_parser("evaluate")
    q.add_argument("attempts", type=Path)
    q.add_argument("--expected-case", action="append", required=True)
    q = commands.add_parser("propose")
    q.add_argument("--proposer", required=True)
    q.add_argument("--order-id", required=True, type=int)
    q.add_argument("--refund-cents", required=True, type=int)
    for name in ("stage", "validate", "approve", "publish", "show-change"):
        q = commands.add_parser(name)
        q.add_argument("change_id")
        if name == "approve":
            q.add_argument("--reviewer", required=True)
    args = parser.parse_args()
    result = None
    if args.command == "versions":
        from importlib.metadata import version, PackageNotFoundError
        result = {}
        for name in ("iceberg-bookshop-agents", "pyiceberg", "duckdb", "pyarrow", "sqlglot", "mcp", "claude-agent-sdk", "pyspark"):
            try: result[name] = version(name)
            except PackageNotFoundError: result[name] = None
    elif args.command == "seed":
        from .fixture import seed
        result = seed(args.work)
    elif args.command in ("query", "sql"):
        from .gateway import Gateway
        gateway = Gateway(args.work, args.principal)
        result = asdict(gateway.query(args.query) if args.command == "sql" else gateway.metric(MetricRequest(args.metric,args.start,args.end,args.currency),args.snapshot))
    elif args.command in ("retain", "replay"):
        from .provenance import retain, replay
        receipt = load(args.receipt)
        result = {"tag": retain(args.work,receipt)} if args.command == "retain" else replay(args.work,args.principal,receipt)
    elif args.command in ("acknowledge", "export-traces", "export-tools"):
        from .runstore import RunStore
        store = RunStore(args.work / "runs.sqlite")
        try:
            if args.command == "acknowledge":
                store.acknowledge(args.run_id)
                result = {"run_id":args.run_id,"delivered":True}
            elif args.command == "export-tools":
                store.export_tools(args.output)
                result = {"output":str(args.output)}
            else:
                store.export(args.output)
                result = {"output":str(args.output)}
        finally: store.close()
    elif args.command == "ingest-traces":
        from .trace_lake import ingest
        result = ingest(args.work,args.input)
    elif args.command in ("ingest-evaluations", "ingest-feedback", "ingest-tools"):
        from .judgments import ingest_judgments
        kind = "tool_calls" if args.command == "ingest-tools" else args.command.removeprefix("ingest-")
        result = ingest_judgments(args.work,args.input,kind)
    elif args.command == "search-current":
        from .retrieval import search_current
        result = search_current(args.work,args.principal,args.text)
    elif args.command in ("index-sync", "search"):
        from .retrieval import Index
        from .contracts import principal
        index = Index(args.work / "retrieval.sqlite")
        try:
            result = index.sync(**load(args.image)) if args.command == "index-sync" else index.search(args.text,principal(args.principal).tenants)
        finally: index.close()
    elif args.command == "review-plan":
        from .planning import review
        result=review(args.work,load(args.proposal),reviewer=args.reviewer,decision=args.decision,reason_code=args.reason_code)
    elif args.command in ("assess", "plan", "incident"):
        from .operations import assess, plan, incident_packet
        if args.command == "assess": result = assess(load(args.metrics))
        elif args.command == "plan": result = plan(load(args.metrics),max_rewrite_bytes=args.max_bytes,candidate_bytes=args.candidate_bytes,concurrent_writers=not args.writers_quiesced)
        else: result = incident_packet(**load(args.packet))
    elif args.command == "evaluate":
        from .evaluation import release_decision
        result = release_decision(load(args.attempts),expected_case_ids=set(args.expected_case))
    else:
        from .workflow import Workflow, validate_change
        from .catalog import catalog, table_name
        ledger = Workflow(args.work / "approvals.sqlite")
        spark = None
        try:
            if args.command == "propose":
                table = catalog(args.work).load_table(table_name())
                result = ledger.propose(table=table_name(),table_uuid=str(table.metadata.table_uuid),base_snapshot=table.current_snapshot().snapshot_id,
                                        proposer=args.proposer,order_id=args.order_id,refund_cents=args.refund_cents)
            elif args.command == "approve":
                ledger.approve(args.change_id,args.reviewer)
                result = ledger.get(args.change_id)
            elif args.command == "show-change": result = ledger.get(args.change_id)
            else:
                from .sparkops import session, SparkBridge
                spark = session()
                bridge = SparkBridge(spark,args.work)
                if args.command == "stage": ledger.stage_with_bridge(args.change_id,bridge)
                elif args.command == "validate":
                    ledger.validate_with_bridge(args.change_id,bridge)
                elif args.command == "publish": ledger.publish(args.change_id,bridge)
                result = ledger.get(args.change_id)
        finally:
            ledger.close()
            if spark is not None: spark.stop()
    print(canonical(result))


if __name__ == "__main__":
    main()
