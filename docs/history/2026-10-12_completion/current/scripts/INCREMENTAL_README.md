# Incremental MERGE simulation

Run separately from the verified production snapshots:

```powershell
python scripts\simulate_incremental.py
```

This is a local-only teaching example. Outputs are stored under data/simulations/incremental/<run_id>/. They are not published to S3 and must not be substituted for the verified Gold reports.

## Scenario

The verified order-items table is sorted by its composite key (order_id, order_item_id). The first 110,650 rows seed a separate Delta target. A 2,015-row batch contains:

- 2,000 held-out source rows, simulating newly arrived rows for this target.
- 10 existing rows with an intentionally synthetic price increase of 1.00 each.
- 5 existing rows replayed without changes.

No new real retail activity is claimed. The price updates are fabricated for demonstration, not actual corrections. Original Bronze/Silver/Gold files, publication reports, and the main ingestion ledger are protected by before/after SHA256 inventory comparisons.

## MERGE behavior

The match condition uses both order_id and order_item_id. Matching rows are updated only when a non-key field differs, using null-safe comparisons. Unmatched rows are inserted. Rows absent from the incoming batch are retained; the batch is not assumed to be a full replacement snapshot. Duplicate source keys are rejected before merging.

First expected result: 2,000 inserts, 10 updates, 112,650 final rows, no duplicate keys, and an all-statuses item-price sum exactly 10.00 above the original all-statuses sum. This sum is a simulation measure, not the delivered-only 13,221,498.11 Gold baseline.

The identical batch is then merged again deliberately, bypassing the main ledger skip guard to exercise Delta itself. Expected replay: zero inserted/updated rows and identical full table contents/counts/totals. Delta operation history and versioned readback record the evidence.

## Limits

This proves a deterministic local upsert/replay pattern. It does not integrate changing source files into the current fixed-baseline full pipeline. Real incremental handling still needs source batch definitions, ordering of corrections, multi-table dependencies, ledger state transitions, Gold refresh rules, new reconciliation baselines, and publication/retention decisions. There is no automatic source conflict resolution, deletion handling, schema evolution, concurrent writers, or crash recovery in this example.

A completed simulation_report.json and outputs/incremental_simulation_latest.json appear only after verification. Failed runs retain simulation_failure.json. Logs are preserved in logs/incremental_simulation_<timestamp>.log. Windows temporary JAR cleanup messages can still occur after successful processing.

## Verified outcome
Run 20261011T194436Z_83b87028 passed in 187.65 seconds. Initial 110,650; batch 2,015; first merge 2,000 inserts and 10 updates; final 112,650 rows; duplicates 0. Replay changed no rows/totals but created Delta version 2 and rewrote the target files. All-statuses simulation price sum was 13,591,653.70 (source 13,591,643.70 plus synthetic 10.00). The delivered Gold baseline remains untouched. Protected 117 original files unchanged. Single-partition Window warning and known Windows temporary JAR cleanup errors were logged; process exited 0.

