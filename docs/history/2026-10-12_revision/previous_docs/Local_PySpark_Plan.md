# RetailLake — local PySpark implementation plan

Selected: 11 October 2026. Implementation has not started.

## Architecture and purpose

Private Mumbai S3 Bronze → verified local input staging → local PySpark transformations → local Silver and Gold Delta tables → complete output snapshots in S3 → analytics.

S3 is the durable source and publication destination. The pipeline must download its inputs from S3 rather than silently reading the original Downloads folder. The laptop supplies compute; no AWS compute service is required. S3 storage, requests, and data transfer can still incur charges. The $5 monthly budget sends alerts and is not a hard cap.

Databricks work is retained as a prototype and transformation reference. Its private S3 connection never succeeded. Do not create a paid Databricks workspace or continue widening AWS permissions for that connection as part of this plan.

## Step-by-step implementation

1. Inspect Python, Java, laptop memory, and AWS CLI availability without reading credentials. Check current official compatibility requirements, then choose pinned PySpark and Delta versions in an isolated environment.
2. Back up RetailLake_01_Explore. Preserve the existing Databricks tables and charts.
3. Select supported temporary AWS authentication for project account 037315459859. Verify caller identity and define access limited to the RetailLake input and output prefixes. No long-lived access keys in code or files.
4. Download the three Bronze CSVs and ingestion_manifest.json from s3://retaillake-dev-037315459859-mumbai/bronze/olist/. Verify byte sizes and checksums against the manifest; stop on mismatches. Preserve raw inputs.
5. Port the existing PySpark transformations with explicit schemas, timestamp parsing, integer item numbers, and decimal price/freight values. Record rejected conversions and missing keys.
6. Save cleaned orders, order items, and products to local Silver Delta tables. Join items with orders and products while checking key uniqueness and unmatched references.
7. Create Gold item facts and product dimensions, then delivered-item category and monthly summaries. Define sales as item price excluding freight. Distinct order counts across categories are not additive.
8. Reconcile counts and totals with the prototype baseline. Include conversion, key, join, and sales checks in per-run reports.
9. Publish completed snapshots to separate run-specific Silver/Gold S3 prefixes. Use one writer, immutable published snapshots, and upload the completion manifest last. Do not copy actively changing Delta directories or advertise an incomplete run as complete.
10. Read back a published snapshot and verify its outputs. Add rerun safety, ingestion tracking, and key-based incremental updates as the next milestone.
11. Build final charts/dashboard, README, architecture diagram, operating instructions, and cost notes. Consider AI only after the pipeline is reproducible.

## Baseline to preserve

| Measure | Prototype result |
| --- | ---: |
| Orders | 99,441 |
| Order items | 112,650 |
| Products | 32,951 |
| Item rows after joins | 112,650 |
| Delivered-item sales, excluding freight | 13,221,498.11 |

These are previously reported/verified results, not a fresh local pipeline run. Original raw product column names include the source spelling `lenght`; document any renaming.

## Connection cleanup to track

- Unsaved RCP exception: cancel draft; do not apply it.
- Saved SCP exception in p-92jp4665: review removing the Databricks-specific Oregon self-assumption changes while retaining Mumbai access. The original archived JSON contains an invalid builderid:* entry, so do not blindly restore that file.
- Dedicated role RetailLakeDatabricksBronzeReadRole and credential retaillake_bronze_read: review removal after confirming no dependency.
- Pending automated external-location requests and possible file-event resources: inspect before cleanup. Provisioning and teardown failed; remaining infrastructure is not confirmed.

No cleanup has been performed by updating this plan. Review concrete permission changes before applying them.

## Local setup status
Selected project directory: C:\Users\HP5CD\Desktop\Aaryav\RetailLake.
User reported Python 3.13.15, Java 17.0.20.1, and JAVA_HOME set to C:\Program Files\Eclipse Adoptium\jdk-17.0.20.101-hotspot\. AWS CLI was not found in PowerShell. Virtual environment and packages are not yet confirmed installed.



## Bronze staging completed — 12 October 2026
Temporary CLI profile retaillake verified in account 037315459859. S3 inputs downloaded to data/bronze/olist/. scripts/verify_bronze.py calculated CRC64NVME from each downloaded CSV's bytes and counted CSV records excluding the header. All three checksums, byte sizes, and row counts match ingestion_manifest.json. Results saved in outputs/bronze_verification.json. Local Spark example succeeded per user output; Delta read/write and Silver/Gold processing remain pending.


## Silver runtime blocker
Prepared scripts/build_silver.py. First run downloaded Delta JARs but Spark startup failed due to missing winutils.exe/HADOOP_HOME. Packaged Hadoop version is 3.4.1. No completed Silver data or success manifest exists. WSL is not installed. Recommend a local Linux runtime through WSL rather than installing mismatched native Hadoop binaries. Windows feature installation and any restart require user coordination. The exported notebook is present. Details: outputs/silver_run.log.


## Selected runtime: WSL
On 12 October 2026 the user chose WSL for future runs. The Windows native dependency was installed and the pipeline completed before the stop request: Silver run 20261011T184927Z_eb5aca1b succeeded, with all three Delta tables read back. Report: outputs/silver_latest.json. Preserve this baseline. Next install Ubuntu through WSL, create a separate Linux virtual environment, install Java 17 and the pinned packages, and reuse the pipeline with Linux project paths. Do not reuse the Windows .venv. No WSL installation has been performed yet.


## Active runtime: Windows — 12 October 2026
The user chose to continue with the working Windows runtime. WSL migration is deferred. Project-local Hadoop 3.4.1 native components are configured by scripts/build_silver.py and reused by scripts/build_gold.py. scripts/build_gold.py reads only the last complete Silver snapshot and prepares immutable local Gold output snapshots. Previous WSL recommendation is historical, not an active prerequisite.


## Gold completed — 12 October 2026
Run 20261011T185542Z_b894d4ac produced verified product_dim (32,951 rows), item_facts (112,650 rows), category_sales (74 rows), and monthly_sales (23 rows). Delivered summaries represent 110,197 items and 96,478 distinct orders with items. Item-level, category, and monthly sales each total 13,221,498.11. Readback checks passed. outputs/gold_latest.json records paths, schemas, validation, and completion. Sorted summary CSVs are in the completed Gold run directory. Windows shutdown logged temporary JAR folder cleanup errors after output verification; no S3 publication has been performed. Windows remains the active runtime.


## S3 publication and readback completed — 12 October 2026
Published Silver (9 objects) and Gold (14 objects) snapshots under their run-specific prefixes in the existing Mumbai bucket, totaling 31,640,494 bytes including manifests. Completion manifests uploaded last with conditional writes; every artifact downloaded from S3 matches SHA256 and byte size. All seven downloaded Delta tables match expected row counts and schemas; fact/category/monthly totals all equal 13,221,498.11. outputs/publication_latest.json has status verified. All four bucket Block Public Access settings were restored to true after a live check found them false; no bucket policy exists. SSE-S3 is retained. No AWS compute created. Next: ingestion tracking and safe reruns, retention, dedicated least-privilege access, and final analytics.

