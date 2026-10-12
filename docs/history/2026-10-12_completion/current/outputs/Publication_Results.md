# S3 publication results — 12 October 2026

Status: verified.

Bucket: retaillake-dev-037315459859-mumbai (ap-south-1).

| Snapshot | Prefix | Objects |
| --- | --- | ---: |
| Silver | silver/runs/20261011T184927Z_eb5aca1b/ | 9 |
| Gold | gold/runs/20261011T185542Z_b894d4ac/ | 14 |

Total uploaded size including completion manifests: 31,640,494 bytes.

Each snapshot has a completion_manifest.json uploaded last. All objects were written using SSE-S3 and conditional writes, preserving existing matching objects when resuming. The manifests contain schemas, table counts, S3 paths, artifact inventories, byte sizes, and SHA256 checksums.

Verification downloaded both snapshots from S3 to a fresh local folder. All artifact bytes/checksums matched. Spark read back all seven Delta tables:

- Silver: orders 99,441; order_items 112,650; products 32,951.
- Gold: product_dim 32,951; item_facts 112,650; category_sales 74; monthly_sales 23.
- Fact, category, and monthly sales totals: 13,221,498.11, matching the prototype.

Security change: restored all four bucket Block Public Access settings to true after the live console/API state showed false. Bucket default encryption and uploaded objects use SSE-S3. No bucket policy exists. No AWS compute was launched.

Runtime issues: AWS CLI sandbox access failure interrupted publication; resumed in the normal Windows environment after verifying already-uploaded objects. Local Spark emitted temporary-directory cleanup errors after successful Delta readback. These do not change the verified publication result.

Reports and scripts: publication_latest.json, ../scripts/publish_snapshots.py, ../scripts/verify_published.py, and ../scripts/PUBLICATION_README.md.

Costs: additional S3 storage, requests, and downloads apply. Snapshot protection is implemented by the publisher, not S3 Object Lock. Dedicated least-privilege authentication, output retention, and abandoned Databricks setup cleanup remain pending.
