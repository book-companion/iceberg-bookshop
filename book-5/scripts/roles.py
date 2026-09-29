import re
from pathlib import Path

src = Path("terraform/iam.tf").read_text()

def actions(local):
    body = re.search(local + r"\s*=\s*\[(.*?)\]", src, re.S).group(1)
    return set(re.findall(r'"(s3tables:\w+)"', body))

discover = actions("discover_actions")
reader = {"s3tables:ListTableBuckets"} | discover | actions("read_table_actions")
writer = reader | actions("write_table_actions")
maintenance = discover | actions("maintenance_bucket_actions") | actions("maintenance_table_actions")
admin = maintenance | actions("admin_bucket_actions") | actions("admin_table_actions")
roles = {"reader": reader, "writer": writer, "maintenance": maintenance, "admin": admin}

# What each operation needs, from the S3 User Guide's SQL-semantics page
# (s3-tables-sql.html) and the maintenance page's sort/z-order requirement.
ops = {
    "SELECT":             {"GetTableMetadataLocation", "GetTableData"},
    "INSERT / UPDATE":    {"GetTableMetadataLocation", "PutTableData", "UpdateTableMetadataLocation"},
    "DROP TABLE":         {"GetTableMetadataLocation", "PutTableData", "UpdateTableMetadataLocation", "DeleteTable"},
    "set sort compaction":{"PutTableMaintenanceConfiguration", "GetTableData"},
    "read job status":    {"GetTableMaintenanceJobStatus"},
    "delete the bucket":  {"DeleteTableBucket"},
}

for role, granted in roles.items():
    print(f"{role} ({len(granted)} actions)")
    for op, need in ops.items():
        missing = sorted(a for a in need if "s3tables:" + a not in granted)
        print(f"  {op:<20} {'yes' if not missing else 'missing ' + ', '.join(missing)}")
