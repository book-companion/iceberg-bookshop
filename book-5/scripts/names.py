import re
from urllib.parse import quote

# Naming rules as the S3 User Guide states them (s3-tables-buckets-naming.html).
BUCKET = re.compile(r"^[a-z0-9-]{3,63}$")   # 3-63: lowercase letters, digits, hyphens
TABLE = re.compile(r"^[a-z0-9_]{1,255}$")                    # lowercase, digits, underscores

def bucket_ok(name):
    return bool(BUCKET.match(name))

def namespace_ok(name):
    return bool(TABLE.match(name)) and not name.startswith("aws")

for name in ["bookshop-tables", "bookshop_tables", "bookshop.tables", "Bookshop-Tables"]:
    print(f"bucket    {name:<18} {'ok' if bucket_ok(name) else 'REJECTED'}")
for name in ["bookshop", "aws_sales", "book-shop"]:
    print(f"namespace {name:<18} {'ok' if namespace_ok(name) else 'REJECTED'}")
for name in ["orders", "order_lines", "order-lines", "Orders"]:
    print(f"table     {name:<18} {'ok' if TABLE.match(name) else 'REJECTED'}")

arn = "arn:aws:s3tables:us-east-1:111122223333:bucket/bookshop-tables"
print()
print("warehouse:", arn)
print("prefix:   ", quote(arn, safe=""))
