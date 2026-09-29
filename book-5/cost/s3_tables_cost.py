#!/usr/bin/env python3
"""Monthly cost estimate for one S3 Tables workload, from published prices.

Standard library only. Nothing here calls AWS: prices come from a dated JSON
file whose every figure carries the URL it was read from, and the workload is
a second JSON file you write. The same two inputs always print the same
report, to the cent, because every figure is a Decimal and the output order is
fixed.

    python3 s3_tables_cost.py --prices prices/us-east-1-2026-09-28.json \
                              --workload workloads/bookshop.json

This is arithmetic on list prices. It is not a quote, it ignores the Free
Tier, taxes, data transfer and the engines' own charges (Athena, EMR, Glue,
Redshift), and it has never been compared with a real bill.
"""

from __future__ import annotations

import argparse
import json
import sys
from decimal import ROUND_HALF_UP, Decimal
from pathlib import Path

CENT = Decimal("0.01")
THOUSAND = Decimal(1000)


def D(value) -> Decimal:
    """Decimal from JSON without passing through float."""
    return Decimal(str(value))


def load(path: Path) -> dict:
    return json.loads(path.read_text(), parse_float=Decimal, parse_int=Decimal)


def price(prices: dict, key: str) -> Decimal:
    entry = prices["dimensions"].get(key)
    if entry is None:
        sys.exit(f"price file has no dimension {key!r}")
    if entry.get("usd") is None:
        sys.exit(
            f"price for {key!r} is unconfirmed in {prices['_path']} "
            f"({entry.get('note', 'no note')}); fill it in before using it"
        )
    return D(entry["usd"])


def tiered_storage(prices: dict, gb: Decimal) -> Decimal:
    """Storage priced in cumulative tiers: [{"up_to_gb": n|null, "usd": p}]."""
    tiers = prices["dimensions"]["storage_gb_month"]["tiers"]
    remaining, floor, total = gb, Decimal(0), Decimal(0)
    for tier in tiers:
        if tier.get("usd") is None:
            sys.exit("a storage tier price is unconfirmed; fill it in first")
        ceiling = tier.get("up_to_gb")
        band = remaining if ceiling is None else min(remaining, D(ceiling) - floor)
        if band <= 0:
            break
        total += band * D(tier["usd"])
        remaining -= band
        floor = D(ceiling) if ceiling is not None else floor
    return total


def estimate(prices: dict, w: dict) -> list[tuple[str, str, Decimal]]:
    """Return (line, basis, usd) rows for one month."""
    rows: list[tuple[str, str, Decimal]] = []

    gb = D(w["stored_gb"])
    rows.append(("storage", f"{gb} GB-month", tiered_storage(prices, gb)))

    objects = D(w["objects_stored"])
    rows.append((
        "object monitoring",
        f"{objects} objects",
        objects / THOUSAND * price(prices, "monitoring_per_1000_objects"),
    ))

    puts = D(w["put_requests"])
    gets = D(w["get_requests"])
    rows.append(("PUT-class requests", f"{puts} requests",
                 puts / THOUSAND * price(prices, "put_per_1000_requests")))
    rows.append(("GET-class requests", f"{gets} requests",
                 gets / THOUSAND * price(prices, "get_per_1000_requests")))

    for job in w.get("compaction", []):
        strategy = job["strategy"]  # binpack | sort | zorder; "auto" bills as whichever it picks
        n = D(job["objects_processed"])
        g = D(job["gb_processed"])
        rows.append((
            f"compaction ({strategy}), objects",
            f"{n} objects",
            n / THOUSAND * price(prices, f"compaction_{strategy}_per_1000_objects"),
        ))
        rows.append((
            f"compaction ({strategy}), bytes",
            f"{g} GB",
            g * price(prices, f"compaction_{strategy}_per_gb"),
        ))

    if w.get("replicated_table_updates"):
        u = D(w["replicated_table_updates"])
        rows.append((
            "replication, table updates",
            f"{u} updates",
            u / THOUSAND * price(prices, "replication_per_1000_table_updates"),
        ))

    if w.get("metadata_journal_updates"):
        m = D(w["metadata_journal_updates"])
        rows.append((
            "S3 Metadata journal updates",
            f"{m} updates",
            m / Decimal(1_000_000) * price(prices, "metadata_journal_per_million_updates"),
        ))

    return rows


def render(prices: dict, w: dict, rows) -> str:
    out = [
        f"S3 Tables monthly estimate: {w['name']}",
        f"Region {prices['region']}, list prices read {prices['fetched']} "
        f"from {prices['_path']}",
        "Arithmetic only: not run against an AWS bill.",
        "",
        f"{'line':<34}{'basis':<26}{'USD':>12}",
        "-" * 72,
    ]
    total = Decimal(0)
    for line, basis, usd in rows:
        total += usd
        out.append(f"{line:<34}{basis:<26}{usd.quantize(CENT, ROUND_HALF_UP):>12}")
    out += ["-" * 72, f"{'total':<60}{total.quantize(CENT, ROUND_HALF_UP):>12}"]
    return "\n".join(out)


def main(argv=None) -> int:
    here = Path(__file__).resolve().parent
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--prices", type=Path,
                    default=here / "prices" / "us-east-1-2026-09-28.json")
    ap.add_argument("--workload", type=Path,
                    default=here / "workloads" / "bookshop.json")
    args = ap.parse_args(argv)

    prices = load(args.prices)
    prices["_path"] = args.prices.name
    workload = load(args.workload)
    print(render(prices, workload, estimate(prices, workload)))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
