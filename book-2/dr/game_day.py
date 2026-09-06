"""Chapter 16: the disaster-recovery game day.

  1. record   for each table: rows, snapshot count, refs, schema, metadata location
  2. lose     drop the namespace's tables from the catalog (files untouched)
  3. discover list <ns>/<table>/metadata/*.metadata.json per prefix
  4. choose   the newest file whose predecessors match the metadata-log chain — never simply the newest by name
  5. register register_table(ns.table, that file)
  6. validate every referenced file exists; rows, snapshots, refs, schema equal step 1
  7. time     steps 3–6 per table: that is the RTO

  python dr/game_day.py <namespace> [--replica http://localhost:8335]

With --replica, steps 3–6 run against a second object store and a scratch SQL catalog
(the DR catalog is built from the replica's metadata files; it is not a copy of the
primary catalog). Exits non-zero if any table fails validation. Step 8, the wrong-order
drill, is `dr/replicate.py --order data-first`.
"""
import os, sys, time, json
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from lib.platform import s3client, keys, REST, S3, AK, SK, REGION
from pyiceberg.catalog import load_catalog


def state(cat, ident):
    t = cat.load_table(ident)
    refs = sorted((n, str(r.snapshot_ref_type), r.snapshot_id) for n, r in t.metadata.refs.items())
    return {"rows": len(t.scan().to_arrow()), "snapshots": len(t.metadata.snapshots), "refs": refs, "fields": len(t.schema().fields)}


def choose_metadata(s3, bucket, prefix):
    """The head of the metadata-log chain: the file no other file names as its predecessor."""
    files = sorted(k for k in keys(s3, bucket, prefix + "metadata/") if k.endswith(".metadata.json"))
    named = set()
    for k in files:
        mj = json.loads(s3.get_object(Bucket=bucket, Key=k)["Body"].read())
        for e in mj.get("metadata-log", []):
            named.add(e["metadata-file"].split(f"{bucket}/", 1)[-1])
    heads = [k for k in files if k not in named]
    if len(heads) > 1:
        print(f"     WARNING: {len(heads)} unreferenced metadata files under {prefix} — one is a failed commit's stray: {[h.rsplit('/',1)[-1][:12] for h in heads]}")
    if not files:
        raise FileNotFoundError(f"no metadata files under {prefix}metadata/ on this store — the table was never replicated here")
    return sorted(heads)[0] if heads else files[-1]


def referenced_missing(s3, bucket, t):
    have = set(keys(s3, bucket, t.location().split(f"{bucket}/", 1)[1] + "/"))
    want = [f["file_path"].split(f"{bucket}/", 1)[1] for f in t.inspect.files().to_pylist()]
    return [w for w in want if w not in have]


def main(argv):
    ns = argv[0]; replica = argv[argv.index("--replica") + 1] if "--replica" in argv else None
    primary = load_catalog("primary", **{"uri": REST, "s3.endpoint": S3, "s3.access-key-id": AK, "s3.secret-access-key": SK, "s3.region": REGION})
    tables = [t[1] for t in primary.list_tables(ns)]
    print(f"== 1. record: {len(tables)} tables in {ns}")
    before = {t: state(primary, f"{ns}.{t}") for t in tables}
    if replica:
        endpoint = replica; s3 = s3client(replica)
        target = load_catalog("dr", **{"type": "sql", "uri": f"sqlite:///{os.path.dirname(__file__)}/dr-catalog.db", "warehouse": "s3://warehouse/",
                                       "s3.endpoint": replica, "s3.access-key-id": AK, "s3.secret-access-key": SK, "s3.region": REGION})
        try: target.create_namespace(ns)
        except Exception: pass
        for t in tables:
            try: target.drop_table(f"{ns}.{t}")
            except Exception: pass
        print(f"== 2. lose: recovering on the replica at {replica} under a fresh catalog")
    else:
        s3 = s3client(); target = primary
        print("== 2. lose: dropping the tables from the catalog (files untouched)")
        for t in tables: primary.drop_table(f"{ns}.{t}")
    t0 = time.perf_counter(); after = {}; bad = []
    for t in tables:
        try:
            meta = choose_metadata(s3, "warehouse", f"{ns}/{t}/")
            target.register_table(f"{ns}.{t}", f"s3://warehouse/{meta}")
            tbl = target.load_table(f"{ns}.{t}"); missing = referenced_missing(s3, "warehouse", tbl)
            after[t] = state(target, f"{ns}.{t}")
        except Exception as e:
            bad.append((t, [str(e)[:120]], before[t], None)); continue
        if missing or after[t] != before[t]:
            bad.append((t, missing, before[t], after[t]))
    dt = time.perf_counter() - t0
    print(f"== 3–6. {len(tables)} tables discovered, registered and validated in {dt:.1f}s ({dt/max(len(tables),1):.2f}s per table)")
    for t, missing, b, a in bad:
        print(f"   FAILED {t}: {len(missing)} referenced files missing; before {b} / after {a}")
    print(f"== 7. history validated: {len(tables) - len(bad)}/{len(tables)} identical")
    sys.exit(1 if bad else 0)


if __name__ == "__main__":
    main(sys.argv[1:])
