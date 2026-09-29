"""Lint PyIceberg REST catalog properties for the two AWS Iceberg endpoints.

Offline: reads property names from the installed PyIceberg and checks the
shapes AWS documents (sources C3-20..C3-22, C2-29, C2-31). No catalog is
constructed, because constructing one sends GET /v1/config to AWS.
"""
import re
from pyiceberg import __version__
from pyiceberg.catalog.rest import SIGV4, SIGV4_REGION, SIGV4_SERVICE

ENDPOINT = re.compile(r"^https://(s3tables|glue)\.([a-z0-9-]+)\.amazonaws\.com/iceberg$")
S3T_WAREHOUSE = re.compile(r"^arn:aws:s3tables:([a-z0-9-]+):\d{12}:bucket/[a-z0-9-]{3,63}$")
GLUE_WAREHOUSE = re.compile(r"^\d{12}:s3tablescatalog/[a-z0-9-]{3,63}$")

def lint(props):
    out = []
    m = ENDPOINT.match(props.get("uri", ""))
    if not m:
        return ["uri is neither AWS Iceberg endpoint"]
    service, region = m.groups()
    if props.get(SIGV4, "false").lower() != "true":
        out.append(f"{SIGV4} is not true: requests go unsigned")
    name = props.get(SIGV4_SERVICE)
    if name is None:
        out.append(f"{SIGV4_SERVICE} unset: PyIceberg {__version__} signs for 'execute-api'")
    elif name != service:
        out.append(f"{SIGV4_SERVICE}={name!r} but the endpoint is {service!r}")
    if props.get(SIGV4_REGION, region) != region:
        out.append(f"{SIGV4_REGION}={props[SIGV4_REGION]!r} but the endpoint is in {region!r}")
    wh = props.get("warehouse", "")
    shape = S3T_WAREHOUSE if service == "s3tables" else GLUE_WAREHOUSE
    if not shape.match(wh):
        out.append(f"warehouse {wh!r} is not the {service} shape")
    return out

ACCOUNT, BUCKET = "111122223333", "bookshop-tables"
configs = {
    "s3tables endpoint": {
        "type": "rest", "uri": "https://s3tables.us-east-1.amazonaws.com/iceberg",
        "warehouse": f"arn:aws:s3tables:us-east-1:{ACCOUNT}:bucket/{BUCKET}",
        SIGV4: "true", SIGV4_SERVICE: "s3tables", SIGV4_REGION: "us-east-1"},
    "glue endpoint": {
        "type": "rest", "uri": "https://glue.us-east-1.amazonaws.com/iceberg",
        "warehouse": f"{ACCOUNT}:s3tablescatalog/{BUCKET}",
        SIGV4: "true", SIGV4_SERVICE: "glue", SIGV4_REGION: "us-east-1"},
    "mixed paths": {
        "type": "rest", "uri": "https://glue.us-east-1.amazonaws.com/iceberg",
        "warehouse": f"arn:aws:s3tables:us-east-1:{ACCOUNT}:bucket/{BUCKET}",
        SIGV4: "true"},
}
for label, props in configs.items():
    problems = lint(props)
    print(f"{label:<20}", "ok" if not problems else f"{len(problems)} problem(s)")
    for p in problems:
        print("   -", p)
