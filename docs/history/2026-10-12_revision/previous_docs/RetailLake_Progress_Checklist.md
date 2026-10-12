# RetailLake progression checklist

Updated: 12 October 2026

✅ Done · ⬜ Pending · ◻ Optional future work

## Current direction — Windows local PySpark with AWS S3

Decision: use local PySpark for the required pipeline. AWS S3 remains the source of truth and output storage. Databricks is a completed exploration prototype and optional reference. The connection investigation is paused; this choice does not establish that Free Edition cannot connect to S3.

- ✅ Inspected external-location and AWS IAM role credential setup forms.
- ✅ Prepared prefix-restricted read policy and staged trust policies locally.
- ✅ Obtained approval; created RetailLakeDatabricksBronzeReadRole with RetailLakeBronzeReadOnly inline policy.
- ✅ Created read-only Databricks credential retaillake_bronze_read and applied its generated external ID and self-trust in AWS.
- ✅ Attempted credential and external-location validation; access failed and no successful S3 read was confirmed.
- ✅ Saved the approved Oregon STS self-assumption SCP exception; subsequent validation still failed.
- ✅ Identified an organization RCP denying outside-organization access as a likely blocker; prepared an exception but did not save it.
- ✅ Chose the local route; further Databricks connection attempts are paused.
- ⬜ Cancel the unsaved RCP editor draft and confirm the original RCP remains applied.
- ⬜ Review and clean up unused Databricks credentials, pending setup requests, and the dedicated AWS role after confirming they are unused.
- ⬜ Review reversal of the saved Databricks-specific SCP exception; preserve the Mumbai region allowance and validate the revised policy before saving.
- ⬜ Confirm whether failed automatic file-event setup left any resources; teardown previously failed.

### Selected architecture — AWS storage with local PySpark processing

RetailLake remains an AWS-backed data lakehouse. S3 is the source of truth. The next implementation uses local PySpark and Delta Lake, reusing the transformations developed in Databricks. Databricks is an optional exploration workspace, not a required dependency.

Planned first pipeline: S3 Bronze → scripted, verified local staging → local PySpark Silver and Gold Delta tables → immutable output snapshots published to S3 → SQL analytics and reports.

This is local compute with AWS storage, not a deployed AWS compute pipeline. A staging download is part of the scripted pipeline; it must read S3 rather than the original Downloads ZIP. Start with one writer and publish complete snapshots with a completion manifest last. Do not treat copying changing Delta directories as a transaction or support concurrent writers. Direct Spark/Delta writes to S3 are a later enhancement requiring compatible storage and authentication configuration.

Use temporary AWS authentication. Check account compatibility before selecting CLI sign-in or Identity Center; do not create long-lived keys. Check laptop memory, Python, Java, and Spark/Delta compatibility before installation. Keep the three-file subset initially. S3 storage, requests, and downloads can incur charges; local execution avoids AWS compute charges.

Completed Databricks work below is exploration evidence. It does not mean equivalent Silver/Gold tables already exist in S3.

## 1. AWS account and cost controls

- ✅ Confirmed initial Free plan: $120 credits, 183 days remaining, $0 due.
- ✅ Deleted the earlier budget so you could recreate it yourself.
- ✅ Created a $5 recurring monthly cost budget.
- ✅ Added actual-cost alerts above $1, $2, and $5.
- ✅ Verified the alert email and confirmed it is active.
- ✅ Inspected project ownership and AccountFullAccessRole.
- ✅ Confirmed the role has AdministratorAccess and no permissions boundary.
- ✅ Upgraded to the Paid plan to proceed with Mumbai.
- ✅ Activated advanced features and management-account access.
- ✅ Identified the organization policy blocking Mumbai.
- ✅ Added ap-south-1 to its allowed-region list.
- ⬜ Review current credit expiry and Free Tier notification settings after upgrading.

## 2. Secure S3 storage

- ✅ Created retaillake-dev-037315459859-mumbai.
- ✅ Used Mumbai — ap-south-1.
- ✅ Selected a general-purpose bucket.
- ✅ Enabled Block all public access.
- ✅ Used Bucket owner enforced, with ACLs disabled.
- ✅ Enabled SSE-S3 encryption.
- ✅ Kept versioning and Object Lock disabled.
- ✅ Confirmed static website hosting is disabled.
- ✅ Created bronze/olist/.

## 3. Dataset ingestion and verification

- ✅ Downloaded the Olist dataset ZIP from Kaggle.
- ✅ Uploaded orders, order items, and products CSVs to S3.
- ✅ Confirmed the upload destination and file list.
- ✅ Checked local row counts: orders 99,441; order items 112,650; products 32,951.
- ✅ Compared all three S3 CRC64NVME checksums with the ZIP originals.
- ✅ Identified that the extracted local products CSV differed from its original.
- ✅ Created and uploaded ingestion_manifest.json.
- ✅ Added verified sizes and checksums to the manifest.
- ✅ Validated the updated manifest and uploaded it.
- ✅ Checked missing IDs, duplicate keys, and references between datasets.
- ✅ Found 775 orders without items and 610 products without categories.
- ✅ Created the initial data quality report.
- ✅ Prepared fresh source CSVs for Databricks.
- ⬜ Confirm the quality report upload and final documentation contents.

## 4. Databricks setup

- ✅ Confirmed Free Edition.
- ✅ Created the Python notebook RetailLake_01_Explore.
- ✅ Tested Spark with spark.range(5).show().
- ✅ Created schema workspace.retaillake.
- ✅ Created volume workspace.retaillake.raw_files.
- ✅ Uploaded the three CSVs to the volume.
- ✅ Corrected the orders CSV after detecting reformatted timestamps.
- ✅ Confirmed the corrected timestamp preview and all three row counts.

## 5. PySpark preparation

- ✅ Loaded the three CSVs into DataFrames.
- ✅ Inspected their schemas.
- ✅ Converted item numbers to integers.
- ✅ Converted price and freight to decimals.
- ✅ Converted purchase dates to timestamps.
- ✅ Checked required converted columns for missing values: zero.
- ✅ Joined items to orders using order_id.
- ✅ Joined items to products using product_id.
- ✅ Confirmed the joins preserved 112,650 item rows.
- ✅ Selected delivered orders for sales analysis.
- ✅ Labeled blank categories Unknown.
- ✅ Defined sales as item prices excluding freight.
- ✅ Ported and saved reusable prepared data as local Silver Delta tables in the new pipeline.

## 6. Sales summaries

- ✅ Calculated sales, item counts, and order counts by category.
- ✅ Reconciled category sales with item-level sales.
- ✅ Saved workspace.retaillake.category_sales as a Delta table.
- ✅ Read the saved category table back successfully.
- ✅ Calculated sales by purchase month.
- ✅ Reconciled monthly sales with item-level sales.
- ✅ Confirmed total delivered-item sales: 13,221,498.11.
- ✅ Saved workspace.retaillake.monthly_sales.
- ✅ Read back the saved monthly table; all displayed monthly results match the calculation.

## 7. Visual exploration and dashboard

- ⬜ Create a RetailLake dashboard.
- ✅ Created the monthly sales notebook chart; screenshot inspected.
- ⬜ Confirm the top-category notebook chart is saved and ordered by sales.
- ⬜ Build final category and monthly charts from pipeline outputs.
- ⬜ Add sales, delivered-order, and item-count indicators.
- ⬜ Explain metric definitions and dataset limitations.
- ⬜ Check dashboard totals against the saved tables.

## 8. Required next milestone — reproducible S3-backed pipeline

- ✅ Confirmed notebooks/RetailLake_01_Explore.ipynb is present as the Databricks backup and transformation reference.
- ⬜ Inspect local Python, Java, available memory, and AWS CLI without reading secrets.
- ⬜ Select and pin compatible PySpark and Delta versions in an isolated environment.
- ✅ Authenticated with temporary credentials using profile retaillake; verified account 037315459859 and AccountFullAccessRole.
- ⬜ Define least-privilege access to the RetailLake bucket and required prefixes.
- ✅ Downloaded all three Bronze CSVs, ingestion_manifest.json, and data_quality_report.txt from S3; local script verified CSV byte sizes, CRC64NVME checksums, and row counts against the manifest.
- ✅ Ported Bronze-to-Silver transformations into scripts/build_silver.py; Gold transformations remain pending.
- ✅ Created and read back local Silver Delta tables: orders 99,441; order_items 112,650; products 32,951. Windows remains the selected runtime.
- ✅ Created and read back Gold Delta tables: item_facts (112,650), product_dim (32,951), category_sales (74), and monthly_sales (23).
- ✅ Reconciled local Gold counts and sales: item-level, category, and monthly totals all equal 13,221,498.11; delivered items 110,197 and delivered orders with items 96,478.
- ✅ Published complete Silver and Gold snapshots to S3 under run-specific prefixes; uploaded completion manifests last, using conditional writes and SSE-S3.
- ✅ Downloaded the published snapshots; verified all file sizes/SHA256 checksums, all seven Delta table row counts/schemas, and reconciled sales totals at 13,221,498.11.
- ⬜ Add incremental ingestion tracking and key-based updates; label any simulated batches.
- ⬜ Verify rerunning the same batch produces no duplicates or changed totals.
- ⬜ Produce per-run logs, row counts, validation results, and timing metrics.

## 9. Repeatability, analytics, and project documentation

- ✅ Organized scripts/download_bronze.ps1, verify_bronze.py, build_silver.py, and build_gold.py with stage documentation.
- ✅ Added row-count, key, conversion, reference, sales reconciliation, and Delta readback checks to the executed pipeline stages.
- ✅ Selected immutable run-specific Silver/Gold snapshots, with completion manifest and latest-run pointer written only after success; incremental updates remain pending.
- ⬜ Verify a complete local pipeline rerun when implementing the validation milestone.
- ⬜ Retain the Databricks notebook backup as a reference (same export tracked in milestone 8).
- ⬜ Create an architecture diagram and project README.
- ⬜ Document setup, findings, costs, and operating instructions.

## 10. Optional AI milestone — after the core pipeline

- ◻ Choose whether an AI interface improves the completed project; Genie is not required.
- ◻ Check available features, usage limits, and costs before choosing a service.
- ◻ Configure plain-English questions over curated sales outputs.
- ◻ Add example questions and metric definitions.
- ◻ Compare AI answers with known SQL results.
- ◻ Build and evaluate a late-delivery prediction model.
- ◻ Explore monthly sales forecasting.

## 11. Optional deployment enhancements

- ◻ Replace local staging with direct Spark/Delta S3 access after compatibility checks.
- ◻ Deploy a short-lived AWS Glue job only after estimating cost and obtaining approval.
- ◻ Schedule pipeline runs after the manual pipeline is reliable.
- ◻ Add remote monitoring and failure notifications; per-run local logs are required above.

## Current position

The raw data is stored and verified in S3. The Databricks notebook is backed up, and the Windows local PySpark runtime has completed verified Silver Delta tables. Gold tables have been saved and verified. S3 output publication and readback verification are complete. Incremental processing, full pipeline rerun verification, and final analytics remain pending. Follow Local_PySpark_Plan.md.

The AWS budget sends alerts; it is not a spending cap. The account is now on the Paid plan. Advanced-feature activation removed project spend limits. The region-policy change applies across organization member accounts.

The Databricks workflow currently uses manually uploaded files rather than reading directly from S3. The checklist records progress from console checks, local calculations, and user-reported results; it is not a fresh audit of every resource.

## Local setup completed — 12 October 2026

- ✅ Selected project folder C:\Users\HP5CD\Desktop\Aaryav\RetailLake.
- ✅ Created .venv using Python 3.13.15 (user reported).
- ✅ Installed PySpark 4.0.1 and Delta Lake 4.0.1 (user installation output).
- ✅ Java 17 and JAVA_HOME configured (user reported).
- ✅ Ran first_spark.py successfully after using 127.0.0.1 for the driver hostname (user output).
- ✅ Installed AWS CLI 2.37.12 and verified S3 listing/access.
- ✅ Downloaded and verified S3 Bronze files; report: outputs/bronze_verification.json.
- ✅ Confirmed exported Databricks notebook is saved in notebooks/.
- ✅ Verified Delta writes and readback for all three Silver tables in the normal Windows environment with project-local Hadoop 3.4.1 components.

The local pipeline has produced verified Silver tables. Gold tables have been saved and verified. Current authentication uses the existing administrative role; least-privilege pipeline access remains pending.


## Silver implementation status — 12 October 2026

- ✅ Created scripts/build_silver.py with explicit source schemas, timestamp_ntz dates, integer quantities, decimal(12,2) money, key/reference checks, immutable run directories, and completion manifest written last.
- ✅ Downloaded Delta Lake Java dependencies from Maven Central into .cache/ivy/.
- ✅ Resolved the initial missing Windows Hadoop components and completed all three Silver Delta writes/readbacks. Earlier failed runs remain historical evidence.
- ✅ Selected a matching community Hadoop 3.4.1 Windows build and verified its published download checksum.


## Runtime direction changed — 12 October 2026

- ✅ Installed community Windows Hadoop 3.4.1 components in tools/hadoop-3.4.1; verified release SHA256. No system environment variables changed.
- ✅ Completed Silver run 20261011T184927Z_eb5aca1b before the switch request could stop it. All required fields, conversion checks, keys, references, row counts, and Delta readbacks passed. Findings: 775 orders without items; 610 products without categories.
- ✅ User considered WSL, then chose to continue with the now-working Windows setup.
- ◻ WSL installation is optional and deferred; Windows is the active runtime.
- ◻ If migrating to Linux later, create a separate Linux environment and configure Java 17.
- ◻ If migrating to Linux later, verify the same baseline before publishing Linux-generated snapshots.



## Gold completed — 12 October 2026

- ✅ Ran scripts/build_gold.py successfully in the normal Windows environment; run 20261011T185542Z_b894d4ac.
- ✅ Read back all four Gold Delta tables and checked summary rows and reconciled sales totals.
- ✅ Exported sorted category_sales.csv and monthly_sales.csv inside the completed Gold run folder.
- ✅ Documented fact grain, delivered-only sales, excluded freight, timestamp handling, and category order-count non-additivity in scripts/GOLD_README.md.
- ⬜ Address Windows Spark temporary JAR folder cleanup messages. They occurred after successful table writes/readbacks; completed outputs are preserved.

S3 publication and readback verification are complete. Next milestone: implement ingestion tracking and safe reruns, then final charts/dashboard. Optional AI follows the core pipeline.


## S3 publication completed — 12 October 2026

- ✅ Restored all four bucket Block Public Access settings: live check found them off; after restoration all four are true. SSE-S3 remains configured. No bucket policy currently exists.
- ✅ Published Silver run 20261011T184927Z_eb5aca1b at silver/runs/20261011T184927Z_eb5aca1b/ (9 objects).
- ✅ Published Gold run 20261011T185542Z_b894d4ac at gold/runs/20261011T185542Z_b894d4ac/ (14 objects).
- ✅ Published 23 objects totaling 31,640,494 bytes including completion manifests; used private S3 Standard/SSE-S3 and no AWS compute.
- ✅ Recovered from a local AWS CLI sandbox error by verifying existing object checksums and resuming outside the sandbox without replacement.
- ✅ Verified all downloaded artifacts and seven Delta tables; fact/category/monthly sales remain 13,221,498.11.
- ✅ Saved reusable publisher and readback scripts plus outputs/publication_latest.json.

Publisher-enforced snapshot immutability is not Object Lock. Storage, requests, and data transfer may incur charges. Least-privilege pipeline authentication, retention, and abandoned Databricks resource cleanup remain pending.

