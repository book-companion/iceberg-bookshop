# Book 5: S3 Tables

The companion to *S3 Tables: A Field Guide to Managed Apache Iceberg on AWS*.

**Unofficial.** This is not an AWS project, and AWS has not reviewed or endorsed it. Amazon S3 Tables and the other service names are used only to say what the code targets.

**Nothing here has been applied to a real AWS account.** The book was written from AWS's published documentation as it stood on 28 September 2026, without an AWS account. The Terraform has been formatted and validated, but it has never been planned or applied. The IAM policies have never been evaluated by IAM. The cost figures are arithmetic on list prices and have never been compared with a bill. Nothing in this folder calls AWS, reads credentials or needs them. Treat every file as a starting point that you test in your own account, not as something already tested.

## What is here

| Path | What it is | Runs offline |
|---|---|---|
| `terraform/` | One table bucket, one namespace (`bookshop`) and one Iceberg table (`orders`), with every maintenance setting written out. Four least-privilege IAM roles: reader, writer, maintenance and admin. | `terraform validate` only |
| `cost/s3_tables_cost.py` | A monthly cost estimate for an S3 Tables workload. Standard library only, with deterministic output. | yes |
| `cost/prices/us-east-1-2026-09-28.json` | The list prices it uses. Each figure is dated and carries the text of AWS's own price line. | — |
| `cost/workloads/bookshop.json` | An illustrative workload. The volumes are made up for the arithmetic, not measured. | — |
| `compatibility/s3-tables-compatibility-template.csv` | What AWS documents about each engine against S3 Tables, with a source URL and fact ID per row. The `last_run` and `last_run_result` columns are empty and are yours to fill. | — |

## Cost estimate

```bash
make cost
```

The estimate prints storage (tiered), object monitoring, PUT- and GET-class requests, and compaction. Compaction is priced separately for binpack and for sort or z-order, because AWS charges twice as much per GB processed for sort and z-order. Replication table updates and S3 Metadata journal updates are also priced when your workload sets them. The estimate leaves out the Free Tier, taxes, data transfer, a replica's own storage and requests, and every engine's charges (Athena, EMR, Glue, Redshift). If a price in the file is `null` (unconfirmed) and your workload uses it, the calculator refuses to guess and exits.

To estimate your own workload, copy `cost/workloads/bookshop.json` and replace every number:

```bash
python3 cost/s3_tables_cost.py --workload my-workload.json
```

**Prices go stale.** The price file is named for the day it was read. To refresh it, read the current figures from `https://aws.amazon.com/s3/pricing/`, or from AWS's public Price List file for the Region, and save them as a new dated file. Do not edit the old file.

## Terraform

Written against Terraform 1.5.7 and `hashicorp/aws` 6.66.0.

```bash
make tf-check      # fmt -check and validate; never plans or applies
```

To use it, copy `terraform/terraform.tfvars.example` to `terraform.tfvars` and put in your own principals. Read `iam.tf` before you apply anything. Some choices in it are deliberate:

- **The admin role cannot delete the table bucket.** Deleting the bucket is a break-glass action, so no role holds it as a standing right.
- **The maintenance role has `s3tables:GetTableData`.** AWS requires that permission to set the sort or z-order compaction strategy, and it also lets the role read table data.
- **Namespace actions are scoped to the table bucket.** S3 Tables has no namespace resource type, so these actions authorise against the bucket.
- **`ListTableBuckets` is granted on `*`.** It has no resource type to scope it to.
- **SSE-KMS needs a key-policy change this module does not make.** If you set `kms_key_arn`, the key policy must also let the service principal `maintenance.s3tables.amazonaws.com` call `kms:GenerateDataKey` and `kms:Decrypt`. AWS documents that without this, creating an SSE-KMS table fails. Changing the bucket's default encryption later applies only to tables created after the change.
- **A tag or branch stops snapshot management.** The `orders` table has snapshot management enabled. AWS documents that any user-defined Iceberg tag or branch on a table, or any `history.expire.*` table property, makes snapshot management fail for the whole table.

## Compatibility workbook

Each row records what AWS's documentation, or in a few marked rows an AWS blog post, said on 28 September 2026 about one engine and one capability. The `evidence` column is `documented`, `aws-blog` or `UNCONFIRMED`. A row is a claim until you run it. When you do, fill in `last_run` with the date and engine version, and put what happened in `last_run_result`.
