# Publishing RetailLake snapshots

Run after successful Silver and Gold stages:

```powershell
python scripts\publish_snapshots.py
python scripts\verify_published.py
```

The publisher uses AWS CLI profile retaillake, temporary credentials, account 037315459859, and the existing private Mumbai bucket. It checks the account and requires all four bucket Block Public Access controls to be enabled.

## Destination layout

- silver/runs/<silver_run_id>/orders/, order_items/, products/
- gold/runs/<gold_run_id>/product_dim/, item_facts/, category_sales/, monthly_sales/

These are Delta snapshots: Parquet files plus their _delta_log transaction files. Copying only the Parquet files would lose the Delta transaction metadata. Local Hadoop .crc sidecar files are excluded because they are local filesystem checksums, not Delta table data.

Only a complete, inactive local run is published. Files use conditional PutObject writes (If-None-Match: *) with SHA256 checksums, SSE-S3 encryption, and S3 Standard storage. Existing files are never replaced; on resume, their SHA256, sizes, and encryption must match. Unexpected destination objects cause publication to stop.

A completion_manifest.json containing S3 paths, schemas, row counts, artifact sizes, and SHA256 values is uploaded last. Consumers must require that manifest before treating a snapshot as complete. This is publisher-enforced immutability, not S3 Object Lock; another authorized tool could still alter the objects. No mutable S3 latest pointer is maintained.

## Verification

Each snapshot is downloaded to a fresh local verification directory. The publisher checks the complete inventory, byte sizes, and SHA256 values against the publication manifest. verify_published.py then reads the downloaded Delta tables, compares all row counts and schemas, and reconciles Gold fact/category/monthly sales with the 13,221,498.11 baseline.

Reports: outputs/publication_latest.json and outputs/publication_run.log. Delta readback log: outputs/published_readback.log.

S3 storage, PUT/LIST/HEAD/GET requests, and downloads can incur usage charges. No cloud compute, KMS keys, new buckets, or paid analytics services are created. Retaining many snapshots increases storage usage; choose retention rules before scheduling repeated runs.

Future production hardening: use a dedicated least-privilege pipeline role and enforce HTTPS in the bucket policy. Consider S3 access logging, CloudTrail data events, and CloudWatch metrics after estimating their added cost. These changes are deferred; the present upload uses TLS and temporary credentials, and restores the originally requested bucket-level Block Public Access settings.
