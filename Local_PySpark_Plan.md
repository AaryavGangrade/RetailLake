# RetailLake — active plan and runbook

Updated 12 October 2026. Active folder: `C:\Users\HP5CD\Desktop\Aaryav\RetailLake`.

## Architecture and completed scope
S3 Bronze → verified local staging → local PySpark Silver/Gold Delta → conditional S3 publication → downloaded Delta verification → offline dashboard. Databricks is a preserved prototype. Windows remains the selected runtime. No paid AWS compute or AI service has been deployed.

The core initial-batch pipeline is complete. See `outputs/Completion_Status.json` for final verification IDs, durations and outstanding account cleanup. Full source counts: 99,441 orders, 112,650 order items, 32,951 products. Delivered-only sales: R$13,221,498.11. Dashboard defaults to Jan 2017–Aug 2018 and therefore initially shows R$13,181,027.13.

## Normal use

Open the project in your editor and activate its environment once per terminal:

```powershell
cd C:\Users\HP5CD\Desktop\Aaryav\RetailLake
.\.venv\Scripts\Activate.ps1
python scripts\run_pipeline.py
Start-Process .\dashboard\index.html
```

The normal run downloads and verifies Bronze, calculates the batch fingerprint, and skips completed batches. An uncompleted accepted batch runs Silver, Gold, publication, readback, dashboard export and completion recording. A live OS lock prevents another runner from overlapping. Do not launch stage scripts concurrently; they bypass the runner lock.

Default AWS profile is `retaillake-pipeline`, which assumes the approved scoped role through the temporary `retaillake` source session. When authentication expires:

```powershell
aws login --profile retaillake --region ap-south-1
aws sts get-caller-identity --profile retaillake-pipeline --region ap-south-1
```

Select project account 037315459859 during login. No access keys are needed. The source administrator session still has its existing privileges; the pipeline role does not remove those privileges. Administrative reviews use the source profile explicitly. An intentional temporary override uses `$env:RETAILLAKE_AWS_PROFILE`, which should normally be unset.

## Deliberate rebuild and dashboard-only refresh

```powershell
python scripts\run_pipeline.py --rebuild
python scripts\build_dashboard.py
```

Rebuild creates another immutable S3 snapshot pair even when data is unchanged, so use it for deliberate verification/recovery rather than routine scheduling. The second command only refreshes the dashboard from complete verified Gold; it does not call AWS. New dashboard asset URLs contain the Gold run ID to prevent stale browser data.

## Reliability, retention and recovery

Runner reports and ledger entries use atomic JSON replacement. Each runner has its own log/JSON file. The ledger records previous verified snapshot completions; Silver/Gold/publication manifests carry batch lineage. Failures stop subsequent stages. Completed publications are consumable only after their manifests exist.

Hard interruption can leave a running report or partial prefix; inspect logs/manifests and follow [retention/recovery guidance](docs/Retention_and_Recovery.md). No automatic destructive S3 expiry is configured. Keep Bronze and the original/latest verified snapshot pairs. Safe temporary cleanup is `powershell -NoProfile -File scripts\clean_spark_temp.ps1 -Apply` only after Spark exits.

## Remaining account housekeeping

Management-account review is needed for saved SCP/RCP changes and current credits/Free Tier notification preferences. Review abandoned Databricks credential/role/setup requests and possible failed file-event resources before deleting anything. Preserve Mumbai and avoid blindly restoring old policy backups containing invalid `builderid:*`. Every actual cleanup change must be described and reviewed before application.

## Future enhancements

The separate incremental MERGE exercise demonstrates keyed updates with synthetic changes. Real correction ordering and incremental dependent Gold refresh need a genuine update contract/feed and new manifest/baseline expectations. They are future scope, not an unfinished step in validating the static batch. AI, forecasting, additional datasets, scheduling, Glue migration and direct Spark S3 storage are optional next phases.

[Detailed revision guide](docs/RetailLake_Revision_Guide.md) explains every stage, error and evidence limit. [Checklist](RetailLake_Progress_Checklist.md) provides current completion markers. Earlier plan/checklist versions are preserved under `docs/history/`.
