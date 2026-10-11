import hashlib
import json
from pathlib import Path

root = Path(__file__).resolve().parents[1]
bronze = root / "data" / "bronze" / "olist"
manifest = json.loads(
    (bronze / "ingestion_manifest.json").read_text(encoding="utf-8-sig")
)

files = []
for entry in sorted(manifest["files"], key=lambda item: item["name"]):
    path = bronze / entry["name"]
    with path.open("rb") as stream:
        checksum = hashlib.file_digest(stream, "sha256").hexdigest()
    files.append({"name": path.name, "sha256": checksum})

batch_id = hashlib.sha256(
    json.dumps(files, sort_keys=True).encode()
).hexdigest()

result = {"batch_id": batch_id, "files": files}
(root / "outputs" / "input_batch.json").write_text(
    json.dumps(result, indent=2), encoding="utf-8"
)
print("Batch ID:", batch_id)