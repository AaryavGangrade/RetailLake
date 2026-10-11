# RetailLake

Private Mumbai S3 storage, Windows local PySpark/Delta processing and an offline retail dashboard.

## Start here

- [Current progression checklist](RetailLake_Progress_Checklist.md)
- [Runbook and active plan](Local_PySpark_Plan.md)
- [Detailed revision/troubleshooting guide](docs/RetailLake_Revision_Guide.md)
- [Retention and recovery](docs/Retention_and_Recovery.md)
- [Final verification status](outputs/Completion_Status.json)
- [Completion evidence archive](docs/history/2026-10-12_completion/README.md)

## Completed core pipeline

S3 Bronze → verified local download → three Silver and four Gold Delta tables → immutable S3 snapshots → downloaded hash/schema/count verification → offline dashboard. The full pipeline passed again through the scoped `retaillake-pipeline` profile. Normal same-batch reruns skip completed processing; deliberate rebuilds preserve earlier snapshot history. Per-run reports/ledger writes are atomic and the runner has an OS-held single-writer lock.

Full-history delivered-item sales reconcile to R$13,221,498.11. The dashboard defaults to Jan 2017–Aug 2018 and initially shows R$13,181,027.13. Original 2016 data remains selectable. Metric switches, filtering, sorting, pagination, notes and CSV export were browser-verified.

## Run and view

From this project folder:

```powershell
.\.venv\Scripts\Activate.ps1
python scripts\run_pipeline.py
Start-Process .\dashboard\index.html
```

For a deliberate full rebuild use `python scripts\run_pipeline.py --rebuild`. This creates more S3 storage/requests. Keep the source `retaillake` temporary sign-in active; the scoped profile assumes the approved pipeline role automatically. See the runbook for authentication and recovery.

## Remaining boundaries

Management-account authentication and review are still needed for abandoned Databricks setup/org-policy cleanup and current credit/Free Tier email preferences. These blockers are saved in the status report. Databricks remains a historical prototype; its private S3 connection was never confirmed. Real update ingestion/correction ordering, incremental Gold refresh, scheduling, AI and AWS compute are future enhancements. No AWS compute or hosted dashboard was created.

The $5 budget is an alert, not a hard cap. S3 storage/requests/downloads may incur charges. Windows shutdown JAR cleanup warnings can recur; safe cleanup after Spark exits is documented and has been executed successfully. Logs are retained per runner; stage latest files can change, so frozen evidence is archived under docs/history/.
