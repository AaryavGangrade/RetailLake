import json
import subprocess
import sys
from pathlib import Path

root = Path(__file__).resolve().parents[1]

def run(script):
    subprocess.run(
        [sys.executable, str(root / "scripts" / script)],
        check=True,
        cwd=root,
    )

subprocess.run(
    [
        "powershell.exe",
        "-NoProfile",
        "-File",
        str(root / "scripts" / "download_bronze.ps1"),
    ],
    check=True,
    cwd=root,
)

# Recalculate the fingerprint of the current local Bronze files.
run("identify_batch.py")
batch = json.loads((root / "outputs" / "input_batch.json").read_text())

ledger_path = root / "state" / "ingestion_ledger.json"
ledger = json.loads(ledger_path.read_text()) if ledger_path.exists() else {}

if ledger.get(batch["batch_id"], {}).get("status") == "complete":
    print("Batch already completed. Processing skipped.")
    sys.exit(0)

for script in [
    "build_silver.py",
    "build_gold.py",
    "publish_snapshots.py",
    "verify_published.py",
    "record_batch.py",
]:
    run(script)