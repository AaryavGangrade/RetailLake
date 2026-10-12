# Pipeline execution logs

run_pipeline.py creates a unique pair per run:

- .log: step commands, subprocess output/errors, and exception traceback.
- .json: run/batch IDs, overall status, reason/error, UTC start/end times, total duration, and per-step timings/status/exit code.

Routine child output is redirected here, so the terminal can look quiet during downloads. On completion it prints status and both paths. A skipped run still downloads/verifies fresh S3 inputs before checking the ledger. Unique filenames preserve previous runs; inner stage outputs/*.log files can still be overwritten, but their output is also captured by the runner log.

Verified: completed-batch S3 refresh/skip path. New-batch, deliberate failure, interruption/crash, and concurrent execution paths are not separately verified. Use one runner at a time.

Failure-check option: python scripts/run_pipeline.py --check-failure. Expected runner status is failed and exit code 1; the deliberate check executes only a local failing subprocess. See failure_check_result.json for verified outcome and protected-file comparisons.

