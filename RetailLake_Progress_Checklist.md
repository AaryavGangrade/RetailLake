# RetailLake progression checklist

Updated: 12 October 2026. ✅ Completed and verified · ⏳ In progress · ⬜ Pending/blocked · ◻ Optional future enhancement.

## Current project
Private AWS S3 in Mumbai → downloaded and verified Bronze → Windows local PySpark/Delta Silver → Gold → immutable S3 snapshots → verified readback → offline retail dashboard.

AWS supplies storage. Your laptop runs Spark. Databricks is retained as an exploration prototype. No AWS compute service or hosted dashboard has been deployed. The three-file Olist subset is the completed core scope.

## 1. Account, storage and source data

- ✅ Created the $5 recurring monthly cost budget and actual-cost alerts above $1, $2 and $5; email verified historically and alert thresholds checked again on 12 October.
- ✅ Activated advanced account features and allowed Mumbai after investigating organization restrictions.
- ✅ Created private bucket `retaillake-dev-037315459859-mumbai` in `ap-south-1`.
- ✅ Configured owner-enforced ownership, ACLs disabled, SSE-S3, public-access protection; current Block Public Access and encryption checked again.
- ✅ Uploaded original orders, order-items and products CSVs to `bronze/olist/`.
- ✅ Created ingestion manifest and initial data-quality report.
- ✅ Verified row counts, byte sizes and CRC64NVME checksums against the original files.
- ✅ Identified and corrected an earlier CSV reformatting problem; the pipeline now reads the S3 originals.
- ⬜ Confirm current credit balance/expiry and Free Tier email preferences. Project-account Free Tier plan API returned Missing data; management-account review is required; the new login selected the project account again. The earlier $120/183-days report is historical.

## 2. Exploration and Windows setup

- ✅ Created Databricks notebook, schema and raw-files volume; explored schemas, typed columns, joins, category/monthly summaries and charts.
- ✅ Saved the notebook backup in `notebooks/RetailLake_01_Explore.ipynb`.
- ✅ Investigated private S3 integration: role, read-only credential, validation failures, region policies and likely RCP denial. A successful Databricks S3 connection was never verified.
- ✅ Selected local Windows processing; WSL and Databricks are optional.
- ✅ Installed Java 17, Python 3.13, PySpark 4.0.1, Delta 4.0.1 and AWS CLI; pinned Python dependencies.
- ✅ Fixed Spark's underscore-hostname problem using localhost binding.
- ✅ Added matching project-local Hadoop 3.4.1 Windows dependencies and documented community-binary provenance/hash.
- ✅ Confirmed laptop memory: 8,241,938,432 bytes (about 7.68 GiB); Spark uses local[2].
- ✅ Authenticated with temporary AWS sessions and verified account 037315459859.

## 3. Verified transformation pipeline

- ✅ Scripted S3 input download, manifest verification and SHA256 batch fingerprint.
- ✅ Silver Delta: 99,441 orders; 112,650 order items; 32,951 products.
- ✅ Checked conversion failures, required fields, duplicate keys and order/product references.
- ✅ Retained 775 orders without items and 610 products without categories; documented their analytical treatment.
- ✅ Gold Delta: product dimension, item facts, category sales and monthly sales.
- ✅ Confirmed all 112,650 fact rows survived joins; summaries include 110,197 delivered items and 96,478 delivered orders with items.
- ✅ Reconciled full-history delivered-item sales: R$13,221,498.11 in item-level, category and monthly outputs. Freight is excluded.
- ✅ Saved and read back all seven Delta tables with matching counts and schemas.
- ✅ Published Silver/Gold to unique S3 run prefixes using SHA256 checksums, SSE-S3 and conditional writes; completion manifests uploaded last.
- ✅ Downloaded published artifacts and checked every size/hash and all seven Delta tables.
- ✅ Completed a full end-to-end rebuild, dashboard refresh and completion recording in 230.39 seconds under the original profile.
- ✅ Completed the second full run through the scoped pipeline profile: 226.23 seconds. Verified actual encrypted writes, downloaded hashes and all seven Delta tables; dashboard/ledger refreshed. See `outputs/Completion_Status.json`.

## 4. Reruns, incremental demonstration and operations

- ✅ Normal same-batch run refreshes S3 Bronze then skips completed processing.
- ✅ Added explicit `--rebuild` for deliberate new snapshots; retained earlier completions in the ledger.
- ✅ Linked Silver, Gold and S3 completion manifests to the input batch ID; recording rejects mismatched inputs/snapshots.
- ✅ Added OS-held single-writer lock, atomic runner-report/ledger writes and unique per-run logs with durations and exit codes.
- ✅ Verified overlap rejection, lock release, intentional failure logging and JSON replacement.
- ✅ Ran separate, labeled incremental MERGE simulation: 2,000 inserts, 10 synthetic updates and 5 unchanged replays.
- ✅ Replayed the simulation: no duplicate keys or changed contents; documented that Delta still created another version and rewrote files.
- ✅ Created retention/recovery guidance; preserve Bronze, original/latest verified pairs, ledger and compact historical evidence.
- ✅ Added safe Windows temporary cleanup and removed 10 leftover Spark folders / 81,362,990 bytes after Java exited. Cleanup handles leftovers; the underlying shutdown warning may recur.
- ✅ Created the specifically approved `RetailLakeLocalPipelineRole` and `retaillake-pipeline` profile, trusted by the existing managed administrator role only.
- ✅ Limited it to Bronze reads and encrypted Silver/Gold snapshot writes/reads in this bucket; no delete, compute, IAM administration or other-bucket permissions.
- ✅ Verified role assumption, real Bronze download, completed-batch skip and existing publication reads; identity-policy simulation allows encrypted Gold writes and denies Bronze writes/deletes.
- ◻ Real changing-source ingestion, correction ordering, incremental Gold refresh and publication. No genuine update feed was supplied. Current pipeline intentionally enforces the static original manifest and baseline; the simulation is not production ingestion.
- ◻ Automatic scheduling and destructive snapshot expiry; review retention and costs before enabling these.

## 5. Final dashboard

- ✅ Built offline dashboard from the real verified Gold tables; simulated prices are excluded.
- ✅ Added indicators, monthly sales/orders/items, category rankings, filters, searchable/sortable/paginated table, CSV export and source/data notes.
- ✅ Set initial and Reset period to January 2017 through August 2018; kept earlier months selectable.
- ✅ Default-period totals: R$13,181,027.13; 96,211 delivered orders; 109,880 items.
- ✅ Fixed metric-dependent chart legend and stale browser data using snapshot-versioned asset links.
- ✅ Browser checks passed: defaults/reset, single-month view, category filter, all metric switches, empty search, pagination, Top 20 ranking, sort, notes and CSV export.
- ✅ Saved browser-check report and verification screenshot.
- ◻ Hosted dashboard or additional Databricks notebook chart polishing; neither is required for the local dashboard.

## 6. Documentation and outstanding cloud housekeeping

- ✅ Preserved previous checklist/plan/guide/code versions under `docs/history/2026-10-12_completion/`.
- ✅ Updated this checklist, the active plan, README, dashboard guide, revision guide and retention/recovery instructions.
- ✅ Recorded challenges, exact errors, fixes, evidence limits and new permission/verification results.
- ⬜ Review/cancel abandoned Databricks setup requests and read-only credential; confirm dependencies before deleting the dedicated role.
- ⬜ Review and reverse only the saved Databricks-specific SCP exception, preserving the Mumbai allowance and prior invalid-action fix.
- ⬜ Confirm the RCP exception remained unsaved and whether failed automatic file-event setup left resources. Project-account DescribePolicy calls were denied; management review is blocked until authenticated to account 209766594914.

## Optional later phases

- ◻ AI questions over curated metrics with cited numerical answers.
- ◻ Forecasting/late-delivery prediction with proper training/evaluation data.
- ◻ AWS Glue migration, direct Spark S3 access or cloud orchestration.
- ◻ More Olist source tables and richer customer/seller/delivery analytics.

## Study and evidence

Read [revision guide](docs/RetailLake_Revision_Guide.md), [active plan](Local_PySpark_Plan.md), [retention/recovery](docs/Retention_and_Recovery.md), and `outputs/Completion_Status.json` for final run IDs and blockers. Earlier dated documents are preserved as historical evidence; this checklist is the current status.

A budget sends alerts and is not a hard cap. Local compute avoids AWS compute charges; S3 storage, requests and downloads can still incur costs. Publication immutability is enforced by the script, not bucket Object Lock.
