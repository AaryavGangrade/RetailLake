import json
import subprocess
import sys
from pathlib import Path

root = Path(__file__).resolve().parents[1]

# Confirm the inputs still match our original manifest.
subprocess.run(
    [sys.executable, str(root / "scripts" / "verify_bronze.py")],
    check=True,
)

batch = json.loads((root / "outputs" / "input_batch.json").read_text())
publication = json.loads(
    (root / "outputs" / "publication_latest.json").read_text()
)

if publication["status"] != "verified":
    raise ValueError("Record completion only after S3 verification.")

state = root / "state"
state.mkdir(exist_ok=True)
ledger_path = state / "ingestion_ledger.json"

ledger = json.loads(ledger_path.read_text()) if ledger_path.exists() else {}
batch_id = batch["batch_id"]

if batch_id in ledger:
    print("Batch already recorded; no duplicate entry added.")
else:
    ledger[batch_id] = {
        "status": "complete",
        "files": batch["files"],
        "snapshots": publication["publications"],
        "verified_at_utc": publication["verified_at_utc"],
    }
    ledger_path.write_text(json.dumps(ledger, indent=2), encoding="utf-8")
    print("Completed batch recorded.")