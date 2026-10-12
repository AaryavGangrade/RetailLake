# RetailLake — active implementation plan

Updated: 12 October 2026. Runtime: Windows local PySpark. S3: retaillake-dev-037315459859-mumbai, ap-south-1.

## Current state

S3 Bronze → verified local staging → Silver Delta → Gold Delta → S3 snapshots → downloaded Delta readback is complete for the initial three-file batch. All seven tables and sales totals are verified. Databricks is retained as a prototype; WSL is deferred.

Detailed history, code explanations, troubleshooting, and evidence: [Revision guide](docs/RetailLake_Revision_Guide.md). Earlier versions of this plan are preserved in docs/history/2026-10-12_revision/previous_docs/; their pending statements describe earlier states.

## Completed

- Python/Java/venv/PySpark 4.0.1/Delta 4.0.1/AWS CLI setup, notebook export, temporary project account authentication.
- Hostname fix and matching project-local Windows Hadoop 3.4.1 components.
- S3 input download; original-manifest CRC64NVME, byte-size, and row-count checks.
- Silver: orders 99,441; items 112,650; products 32,951.
- Gold: product_dim 32,951; item_facts 112,650; category_sales 74; monthly_sales 23.
- Delivered summaries: 110,197 items, 96,478 orders with items, sales excluding freight 13,221,498.11.
- Publication: Silver 9 objects, Gold 14 objects; 31,640,494 bytes including manifests; all artifacts and seven Delta tables verified after S3 download.
- All four Block Public Access controls restored true after a live check found them false. SSE-S3 retained. No AWS compute created.

## Required next work

1. Implement input batch tracking and safe reruns; distinguish logical batch idempotency from creating more physical snapshots.
2. Add incremental key-based updates; label simulated batches from the static dataset.
3. Verify a complete end-to-end rerun and same-batch behavior.
4. Retain per-run logs and record timings; fix or safely clean Windows temporary JAR folders after processes exit.
5. Choose snapshot retention before repeated scheduling.
6. Configure dedicated least-privilege pipeline access; current profile uses AccountFullAccessRole.
7. Review current billing credits/expiry, Free Tier notifications, and usage after the upgrade.
8. Build final charts/dashboard from verified outputs and document metric limitations.
9. Review abandoned Databricks setup resources and saved organization-policy changes.

## Databricks cleanup state

- Dedicated role RetailLakeDatabricksBronzeReadRole and credential retaillake_bronze_read require dependency review before removal.
- Failed file-event teardown leaves resource cleanup unconfirmed.
- The scoped SCP exception p-92jp4665 was saved. Review reversing only Databricks-specific changes while preserving Mumbai; the original backup includes invalid builderid:* and must not be blindly reapplied.
- RCP p-027sbkih2r exception remained unsaved. Cancel/confirm draft state; do not apply it for this local pipeline.

No cleanup above has been performed by this documentation update.

## Optional enhancements

AI questions over curated metrics, late-delivery prediction, forecasting, direct Spark S3 access, WSL migration, scheduling, or paid AWS compute. Evaluate them after the core pipeline is repeatable.

## Operating boundaries

Use temporary credentials, one writer, inactive complete local snapshots, conditional S3 writes, and completion manifests uploaded last. Protection is implemented by the publisher, not Object Lock. S3 storage/requests/downloads can incur charges; the $5 budget is not a hard cap. Memory capacity remains unconfirmed because the CIM query was denied.

## Batch tracking milestone — 12 October 2026

User created and ran scripts/identify_batch.py, record_batch.py, and run_pipeline.py manually. Input batch SHA256 ID: ea23d41a00728ae8b14e7712bbc3b0f7305030bc26c6a7ce184c3e42ce002899. Bronze verification passed before completion was recorded in state/ingestion_ledger.json. Subsequent runner output was: Batch already completed. Processing skipped.

This verifies the completed-batch guard for the currently downloaded local inputs. It does not verify changed-batch processing, incremental updates, a full end-to-end second run, or automatic fresh S3 download. The current runner recalculates local fingerprints, checks the ledger, and skips before Silver/Gold/publication. Adding S3 refresh to the runner is next. Source/schema changes remain constrained by the current fixed baseline checks.

## Per-run logging completed — 12 October 2026

Updated scripts/run_pipeline.py to preserve a unique logs/<UTC timestamp>_<random suffix>.log and .json for each execution. The text log retains child commands, output, errors, and tracebacks. JSON records batch ID, started/finished UTC timestamps, total duration, per-step durations/exit codes, status, and error details where applicable. A running report is saved before processing and updated between steps. AWS CLI path discovery also supports a user-local install when the terminal's PATH is stale.

Executed run 20261011T193807Z_0ffded34: S3 refresh and Bronze checksum/size/row verification passed, fingerprint ea23d41a00728ae8b14e7712bbc3b0f7305030bc26c6a7ce184c3e42ce002899 matched the completed ledger entry, status was skipped, total duration 21.06 seconds, process exit code 0. No Silver/Gold rebuild or S3 output upload occurred. Logs: 20261011T193807Z_0ffded34.json and 20261011T193807Z_0ffded34.log in logs/.

This verifies retained logs/timings for the completed-batch skip path. New-batch completion, intentional failure reporting, crash recovery, concurrent-run protection, and a full second pipeline run have not been separately verified. Repeated runners still download S3 Bronze inputs before deciding to skip, so requests/downloads still apply. Current scripts remain single-writer; incremental processing is pending.

## Failure handling verified — 12 October 2026

Added `python scripts/run_pipeline.py --check-failure`. This opt-in branch runs a single local Python subprocess raising `RuntimeError: Intentional RetailLake failure check`, before any AWS download/fingerprint/Silver/Gold/publication/ledger stage. Default pipeline behavior remains the normal branch.

Executed run 20261011T194146Z_2b396279. Expected outcome occurred: child step failed with exit 1, runner recorded status failed and error/traceback, then exited 1. Only intentional_failure was recorded. The check itself passed; a failed runner status here is intentional, not a broken data pipeline.

Compared SHA256 inventories of all 117 existing files under data/, state/, and outputs/ before and after the check: unchanged, with no added/deleted files. No AWS step executed. Only logs/code/documentation/check evidence were written. Evidence: logs/failure_check_result.json, logs/20261011T194146Z_2b396279.json, and its .log. This checks subprocess failure propagation and logging; it does not prove recovery from crashes, interrupted uploads, concurrency, or every type of real AWS/Spark error. Incremental processing and full new-batch execution remain pending.

## Simulated incremental MERGE completed — 12 October 2026

Created and executed scripts/simulate_incremental.py separately from the main pipeline. Run 20261011T194436Z_83b87028 completed in 187.65 seconds. Source: verified Silver order_items. A deterministic composite-key sort seeded 110,650 rows, then a 2,015-row batch contained 2,000 held-out source rows simulating new arrivals, 10 synthetic price changes of +1.00, and 5 unchanged existing-row replays. This is a labeled local simulation, not real incremental retail activity or source corrections.

MERGE matched on order_id + order_item_id, updated matching rows only when non-key values differed using null-safe comparisons, inserted unmatched rows, and retained other target rows. Duplicate batch keys were rejected.

Verified first MERGE: 2,000 inserts, 10 updates, final 112,650 rows, zero duplicate keys, and full row-by-row equality against the expected simulated dataset. All-statuses item-price sum changed from 13,591,643.70 to 13,591,653.70 (synthetic +10.00). This is NOT the delivered-only Gold baseline of 13,221,498.11, which remained unchanged.

Replayed the exact batch through MERGE deliberately without using the main ledger skip. Verified zero inserts/updates and full equality of contents/counts/totals with version 1. Replay produced Delta version 2 and physically rewrote/copied 112,650 rows despite logical equality. Therefore data idempotency does not mean no I/O, no new transaction, or no runtime cost. The main batch ledger skip avoids that redundant work.

Compared before/after SHA256 inventories of 117 existing files under original Bronze/Silver/Gold/published-verification/state/outputs: unchanged. No AWS calls or S3 publication. Simulation artifacts reside only under data/simulations/incremental/20261011T194436Z_83b87028/ plus its report/log/documentation.

Evidence: outputs/incremental_simulation_latest.json, the run's simulation_report.json, retained logs/incremental_simulation_*.log, scripts/INCREMENTAL_README.md, and docs/history/2026-10-12_incremental/. The deterministic row_number window used one partition and logged a performance warning; suitable for this small demo, not a scalable partitioning strategy. Existing Windows temporary-JAR shutdown cleanup errors occurred after successful verification and exit 0.

This completes the isolated simulated key-based MERGE/replay milestone. It does NOT integrate changing source inputs into the main fixed-baseline full-snapshot pipeline. Real source batch semantics, correction ordering (older events must not overwrite newer corrections), multi-table dependencies, ledger transitions/recovery, Gold refresh, reconciliation baseline changes, publication, retention, deletions, schema changes, and concurrent writers remain separate work. The demo performs no deletions or schema evolution.

## Local dashboard created — 12 October 2026

Created dashboard/index.html, styles.css, app.js, data.js/data.json, and README.md, plus scripts/build_dashboard.py. It is an offline static dashboard with no external dependencies or cloud hosting. Includes delivered-item sales, distinct delivered orders with items, item count, sales per order, date/category filters, sales/orders/items trend, top category bars, searchable/sortable/paginated category table, selected-category CSV export, and metric/provenance notes.

Exporter read the actual verified Gold item_facts and checked all local artifacts against the published SHA256 manifest. It produced 23 monthly rows, 74 categories, and 1,273 category-month rows. Monthly/cube sales and item counts, monthly distinct order counts, and category cube totals match the Gold summaries. Baseline remains R$13,221,498.11, 96,478 delivered orders with items, and 110,197 items. No simulation data, AWS calls, S3 uploads, or compute deployment.

All-category order KPIs use monthly distinct counts; category counts are not summed across categories. Single-category counts can be summed across purchase months. Missing months are not filled with zeros. Currency is BRL; sales exclude freight and are not net revenue/profit. Data source and verification timestamp are shown in the notes.

A local preview attempt inside the sandbox could not be reached; restarted localhost-only outside the sandbox and confirmed HTTP 200. Codex browser handoff returned queued, not a confirmed visible page. The temporary preview was stopped after handoff; open the offline HTML directly. No browser interaction/visual tests were run. The exporter completed with the known post-run Windows temporary-JAR cleanup errors. These limitations are not presented as tested dashboard behavior.

Open from PowerShell: Start-Process .\dashboard\index.html. Rebuild data after a new verified publication with .\.venv\Scripts\python.exe scripts\build_dashboard.py. Keep dashboard assets together. Instructions and definitions are in dashboard/README.md. Real incremental integration, full pipeline rerun, operational cleanup/security/retention, and optional AWS Glue migration remain pending.
