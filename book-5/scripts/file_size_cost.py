from pathlib import Path
"""What file size does to the per-object lines of an S3 Tables bill.

Prices: the companion's us-east-1 file, read 2026-09-28. Arithmetic only.
"""
import json
from decimal import Decimal as D
P = json.load(open(Path(__file__).resolve().parent.parent / "cost/prices/us-east-1-2026-09-28.json"),
              parse_float=D)["dimensions"]
monitor = P["monitoring_per_1000_objects"]["usd"]
compact = P["compaction_binpack_per_1000_objects"]["usd"]
table_mb = 2048 * 1024          # a 2 TB table
print(f"{'file size':>10} {'objects':>9} {'monitoring/month':>17} {'compact all once':>17}")
for mb in (8, 32, 128, 512):
    n = table_mb // mb
    print(f"{str(mb)+' MB':>10} {n:>9,} {'$'+str((D(n)/1000*monitor).quantize(D('0.01'))):>17} "
          f"{'$'+str((D(n)/1000*compact).quantize(D('0.01'))):>17}")
