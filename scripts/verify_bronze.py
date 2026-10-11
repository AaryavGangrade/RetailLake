import base64
import csv
import json
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
INPUT = ROOT / 'data' / 'bronze' / 'olist'
MASK = (1 << 64) - 1
POLY = 0x9A6C9329AC4BC9B5
TABLE = []
for index in range(256):
    value = index
    for _ in range(8):
        value = (value >> 1) ^ (POLY if value & 1 else 0)
    TABLE.append(value)

def crc64nvme(path):
    value = MASK
    with path.open('rb') as stream:
        while chunk := stream.read(1024 * 1024):
            for byte in chunk:
                value = TABLE[(value ^ byte) & 255] ^ (value >> 8)
    return base64.b64encode((value ^ MASK).to_bytes(8, 'big')).decode('ascii')

manifest = json.loads((INPUT / 'ingestion_manifest.json').read_text(encoding='utf-8-sig'))
results = []
for entry in manifest['files']:
    name = entry['name']
    if Path(name).name != name:
        raise ValueError('Manifest filenames must be simple filenames')
    if entry['checksum_algorithm'] != 'CRC64NVME' or entry['checksum_type'] != 'FULL_OBJECT':
        raise ValueError('Unsupported manifest checksum')
    path = INPUT / name
    size = path.stat().st_size
    checksum = crc64nvme(path)
    with path.open(encoding='utf-8-sig', newline='') as stream:
        reader = csv.reader(stream)
        header = next(reader)
        rows = sum(1 for _ in reader)
    passed = size == entry['size_bytes'] and rows == entry['data_rows'] and checksum == entry['checksum_base64']
    results.append({'name': name, 'size_bytes': size, 'data_rows': rows, 'checksum_algorithm': 'CRC64NVME', 'checksum_base64': checksum, 'passed': passed})
    print(f'{name}: rows={rows:,}, bytes={size:,}, checksum={checksum}, passed={passed}')
report = {'verified_at_utc': datetime.now(timezone.utc).isoformat(), 'source': 'S3 download', 'all_passed': all(result['passed'] for result in results), 'files': results}
output = ROOT / 'outputs' / 'bronze_verification.json'
output.parent.mkdir(parents=True, exist_ok=True)
output.write_text(json.dumps(report, indent=2) + '\n', encoding='utf-8')
if not report['all_passed']:
    raise SystemExit('Verification failed; stop processing')
print('All three files match the ingestion manifest.')
