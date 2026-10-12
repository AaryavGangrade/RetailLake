# Retention and recovery

## Selected retention policy
Keep the original Bronze inputs and manifest, the original verified Silver/Gold pair, the newest verified pair, all batch-ledger records, compact JSON/text logs, and documentation/history. The dashboard is regenerated from the newest verified Gold pair. Preserve notebooks as historical prototypes.

Before scheduling, review older complete snapshot pairs together. A Gold snapshot names its source Silver run: never remove only one half while another retained artifact depends on it. Keep at least two verified pairs. Incomplete prefixes are quarantined by the absence of a completion manifest, but still occupy storage.

There is deliberately no automatic S3 expiry or deletion. The bucket has versioning and Object Lock disabled, so deleting an output is not recoverable through version history. A concrete prefix inventory and explicit deletion approval are required. The pipeline role has no DeleteObject permission. Current storage inventory is in outputs/aws_completion_review.json or the completion evidence report.

## Windows temporary files
After Spark exits, run:

```powershell
powershell -NoProfile -File scripts\clean_spark_temp.ps1
powershell -NoProfile -File scripts\clean_spark_temp.ps1 -Apply
```

The first command reports candidates. The second removes only project `.cache/tmp/spark-*` directories, refuses reparse points, verifies absolute paths, and refuses cleanup while Java is active. This removes leftover JAR copies after Windows shutdown file-handle errors. It does not fix the underlying shutdown-hook warning, remove Ivy dependencies, or delete pipeline data.

## Reruns and failures

```powershell
python scripts\run_pipeline.py
python scripts\run_pipeline.py --rebuild
```

Normal reruns refresh and verify S3 Bronze, calculate its SHA256 fingerprint, then skip completed batches. Refresh still uses S3 requests/downloads. `--rebuild` deliberately creates another immutable snapshot pair for the same batch; do not schedule rebuild routinely. OS-held locks stop overlapping runner processes and release automatically when the process exits. Direct stage scripts remain single-writer tools and bypass this runner lock: do not run them concurrently.

Reports and the ledger use temporary-file writes followed by atomic replacement. This protects against torn JSON files, not against every machine/power failure. A hard kill can leave a report marked running and an incomplete local/S3 snapshot. Inspect its log, current publication status, and completion manifests. Never mark a failed batch complete manually. Rerun the full pipeline; failed snapshots remain for review. Resume publication of the same completed local snapshots with `publish_snapshots.py` followed by `verify_published.py`, `build_dashboard.py`, and `record_batch.py`, only after confirming Silver/Gold/current inputs refer to the same batch. The publisher verifies already-existing bytes before skipping them.

The original static Olist dataset is validated against fixed counts and delivered-sales totals. A genuinely changed source requires a reviewed new ingestion manifest and revised validation expectations. The separate incremental MERGE demo is simulated: production correction ordering, real-event ingestion and dependent incremental Gold refresh are future work. No real source updates were supplied.
