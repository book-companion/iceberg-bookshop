"""Chapter 18: the cost model, eight lines, populated from the book's measurements.

The counts are the platform's (chapter 2's request footprints, chapter 8's compaction,
chapter 9's load-table payload, chapter 11's commit rate). The prices are inputs: the
defaults are one large object store's published list prices at the time of writing,
labelled as such, and you should replace them with yours.

  python cost/cost_model.py [--checkpoint-s 5] [--tables 200] [--engines 40] [--queries-per-hour 500]
"""
import sys

def arg(name, default):
    return type(default)(sys.argv[sys.argv.index(name) + 1]) if name in sys.argv else default

# --- inputs: rates the platform chooses
checkpoint_s = arg("--checkpoint-s", 5)          # chapter 11
streaming_tables = arg("--tables", 200)
engines = arg("--engines", 40)                   # engines and dashboards loading each table
loads_per_min = arg("--loads-per-min", 1)
customer_queries_per_hour = arg("--queries-per-hour", 500)
snapshots_retained = arg("--snapshots", 600)     # chapter 9: 0.93 KB of metadata per retained snapshot

# --- inputs: prices the store publishes (list prices, not a measured bill)
put_per_1000 = arg("--put-price", 0.005)
get_per_1000 = arg("--get-price", 0.0004)
gb_month = arg("--storage-price", 0.023)
transfer_gb = arg("--transfer-price", 0.02)

# --- measurements from the book
commit_requests, commit_puts = 7, 3              # ch2: one commit = 1 PUT data, 2 PUT metadata, 2 GET metadata, 2 HEAD
compaction_puts, compaction_mb, compaction_s = 30, 8.5, 1.0     # ch8, on the 150-file streaming-shaped table
gets_saved_per_customer_query = 450 - 100        # ch8
load_kb_per_snapshot = 0.93                      # ch9

commits_day = 86400 / checkpoint_s
print("== requests: the commit is the unit")
for interval in (checkpoint_s, 30, 300):
    c = 86400 / interval
    print(f"   checkpoint {interval:>4}s  commits/day {c:>9,.0f}  requests/day {c*commit_requests:>10,.0f}  PUTs/day {c*commit_puts:>9,.0f}  ~${(c*commit_puts*put_per_1000 + c*(commit_requests-commit_puts)*get_per_1000)/1000:.2f}/day/table")

print("== requests: what compaction buys back")
cost = compaction_puts * put_per_1000 / 1000
saved_per_query = gets_saved_per_customer_query * get_per_1000 / 1000
print(f"   one rewrite: {compaction_puts} PUTs, {compaction_mb} MB, {compaction_s}s  ~${cost:.5f};  each customer query after it saves {gets_saved_per_customer_query} GETs  ~${saved_per_query:.5f}")
print(f"   payback: {cost/saved_per_query:.0f} customer queries; at {customer_queries_per_hour}/hour that is {cost/saved_per_query/customer_queries_per_hour*60:.0f} minutes")

print("== storage: what the table does not count")
print(f"   metadata copies: {snapshots_retained} retained snapshots x {load_kb_per_snapshot} KB per copy, one copy per commit  ->  {snapshots_retained*snapshots_retained*load_kb_per_snapshot/2/1024:,.0f} MB on the store (quadratic) until delete-after-commit")
print(f"   unreferenced data: the pre-merge rows of every copy-on-write update, until expiry + orphan cleanup (ch15: 2x the table's rows)")
print(f"   holds: every tag pins a full copy of the table through each later compaction (ch10)")

print("== catalog service: the payload per load")
load_kb = snapshots_retained * load_kb_per_snapshot
egress_gb_day = engines * loads_per_min * 1440 * load_kb / 1024 / 1024
print(f"   {engines} engines x {loads_per_min}/min x {load_kb:,.0f} KB  =  {egress_gb_day:,.1f} GB/day of catalog egress for one table at {snapshots_retained} snapshots  (~${egress_gb_day*transfer_gb:.2f}/day if priced as transfer)")
print(f"   the same table at 10 snapshots: {engines*loads_per_min*1440*10*load_kb_per_snapshot/1024/1024:,.2f} GB/day")

print("== the fleet")
print(f"   {streaming_tables} streaming tables at {checkpoint_s}s: {streaming_tables*commits_day*commit_requests:,.0f} requests/day, {streaming_tables*commits_day:,.0f} files/day to compact, ~${streaming_tables*(commits_day*commit_puts*put_per_1000 + commits_day*(commit_requests-commit_puts)*get_per_1000)/1000:,.2f}/day in write requests alone")
print("\nEvery line above is owned by whoever can change its rate: the writers own the checkpoint interval, the platform owns retention and compaction, the consumers own the query rate.")
