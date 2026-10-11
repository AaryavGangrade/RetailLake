"""Record only verified snapshots linked to the current input; preserve rebuild history."""
import hashlib
import json
import subprocess
import sys
from pathlib import Path
from runtime_state import atomic_json

root = Path(__file__).resolve().parents[1]
subprocess.run([sys.executable, str(root / 'scripts/verify_bronze.py')], check=True)
batch = json.loads((root / 'outputs/input_batch.json').read_text(encoding='utf-8-sig'))
publication = json.loads((root / 'outputs/publication_latest.json').read_text(encoding='utf-8-sig'))
if publication.get('status') != 'verified':
    raise ValueError('Record completion only after S3 verification.')
for entry in batch['files']:
    name = entry['name']
    if Path(name).name != name:
        raise ValueError('Invalid input filename')
    with (root / 'data/bronze/olist' / name).open('rb') as stream:
        actual = hashlib.file_digest(stream, 'sha256').hexdigest()
    if actual != entry['sha256']:
        raise ValueError('Input changed after its fingerprint: ' + name)
silver = json.loads((root / 'outputs/silver_latest.json').read_text(encoding='utf-8-sig'))
gold = json.loads((root / 'outputs/gold_latest.json').read_text(encoding='utf-8-sig'))
if silver.get('batch_id') != batch['batch_id'] or silver.get('input_files') != batch['files']:
    raise ValueError('Silver is not linked to the current input batch.')
if gold.get('batch_id') != batch['batch_id'] or gold['silver_run_id'] != silver['run_id']:
    raise ValueError('Gold is not linked to the current Silver batch.')
for layer, report in [('silver', silver), ('gold', gold)]:
    matches = [p for p in publication['publications'] if p['layer'] == layer and p['run_id'] == report['run_id']]
    if len(matches) != 1:
        raise ValueError('Verified publication does not match current ' + layer)
    manifest = json.loads((root / 'outputs' / (layer + '_publication_manifest.json')).read_text(encoding='utf-8-sig'))
    if manifest.get('batch_id') != batch['batch_id']:
        raise ValueError('Published manifest is not linked to this batch.')
ledger_path = root / 'state/ingestion_ledger.json'
ledger = json.loads(ledger_path.read_text(encoding='utf-8-sig')) if ledger_path.exists() else {}
previous = ledger.get(batch['batch_id'])
entry = {'status': 'complete', 'files': batch['files'], 'snapshots': publication['publications'],
         'verified_at_utc': publication['verified_at_utc']}
history = list(previous.get('previous_completions', [])) if previous else []
snapshot_ids = lambda value: sorted((p['layer'], p['run_id']) for p in value['snapshots'])
if previous and snapshot_ids(previous) != snapshot_ids(entry):
    history.append({key: value for key, value in previous.items() if key != 'previous_completions'})
entry['previous_completions'] = history
ledger[batch['batch_id']] = entry
atomic_json(ledger_path, ledger)
print('Verified batch recorded; previous snapshot completions retained:', len(history))
