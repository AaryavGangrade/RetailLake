"""Publish complete, immutable RetailLake snapshots through the authenticated AWS CLI."""
import base64
import hashlib
import json
import os
import shutil
import subprocess
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
BUCKET = 'retaillake-dev-037315459859-mumbai'
ACCOUNT = '037315459859'
PROFILE = 'retaillake'
REGION = 'ap-south-1'
AWS = shutil.which('aws') or str(Path.home() / 'AppData' / 'Local' / 'Programs' / 'Amazon' / 'AWSCLIV2' / 'aws.exe')
ENV = dict(os.environ, AWS_PAGER='', AWS_CLI_AUTO_PROMPT='off')

def aws(*arguments):
    command = [AWS, *arguments, '--profile', PROFILE, '--region', REGION, '--no-cli-pager']
    result = subprocess.run(command, capture_output=True, text=True, env=ENV)
    if result.returncode:
        raise RuntimeError(f'AWS {arguments[0]} {arguments[1]} failed: {result.stderr.strip()}')
    return result.stdout

def digest(path):
    with path.open('rb') as stream:
        return hashlib.file_digest(stream, 'sha256').digest()

def main():
    if json.loads(aws('sts', 'get-caller-identity'))['Account'] != ACCOUNT:
        raise ValueError('Wrong AWS account')
    public_block = json.loads(aws('s3api', 'get-public-access-block', '--bucket', BUCKET))['PublicAccessBlockConfiguration']
    if not all(public_block.values()):
        raise ValueError('All bucket Block Public Access controls must be enabled')
    publications = []
    for layer in ['silver', 'gold']:
        report = json.loads((ROOT / 'outputs' / f'{layer}_latest.json').read_text(encoding='utf-8-sig'))
        if report.get('status') != 'complete':
            raise ValueError(f'{layer} is incomplete')
        run_id = report['run_id']
        if Path(run_id).name != run_id:
            raise ValueError('Invalid run identifier')
        source = ROOT / 'data' / layer / 'runs' / run_id
        completion = json.loads((source / 'completion_manifest.json').read_text(encoding='utf-8'))
        if completion.get('status') != 'complete' or completion['run_id'] != run_id:
            raise ValueError('Source completion mismatch')
        prefix = f'{layer}/runs/{run_id}/'
        listing = json.loads(aws('s3api', 'list-objects-v2', '--bucket', BUCKET, '--prefix', prefix))
        existing_keys = {entry['Key'] for entry in listing.get('Contents', [])}
        files = []
        for path in sorted(source.rglob('*')):
            if not path.is_file() or path.name.endswith('.crc') or path.name == 'completion_manifest.json':
                continue
            if path.is_symlink():
                raise ValueError('Snapshot must not contain symlinks')
            checksum = digest(path)
            relative = path.relative_to(source).as_posix()
            files.append({'relative_path': relative, 'size_bytes': path.stat().st_size, 'sha256': checksum.hex()})
        tables = {name: {'rows': table['rows'], 'schema': table['schema'], 's3_uri': f's3://{BUCKET}/{prefix}{name}/'} for name, table in report['tables'].items()}
        expected_keys = {prefix + entry['relative_path'] for entry in files} | {prefix + 'completion_manifest.json'}
        if not existing_keys.issubset(expected_keys):
            raise ValueError('Unexpected objects already exist in the snapshot prefix')
        for entry in files:
            path = source / entry['relative_path']
            key = prefix + entry['relative_path']
            if key in existing_keys:
                head = json.loads(aws('s3api', 'head-object', '--bucket', BUCKET, '--key', key, '--checksum-mode', 'ENABLED', '--expected-bucket-owner', ACCOUNT))
                checksum_b64 = base64.b64encode(bytes.fromhex(entry['sha256'])).decode('ascii')
                if head['ContentLength'] != entry['size_bytes'] or head.get('ChecksumSHA256') != checksum_b64 or head.get('ServerSideEncryption') != 'AES256':
                    raise ValueError(f'Existing object differs: {key}')
                continue
            response = json.loads(aws('s3api', 'put-object', '--bucket', BUCKET, '--key', prefix + entry['relative_path'], '--body', str(path),
                '--if-none-match', '*', '--server-side-encryption', 'AES256', '--storage-class', 'STANDARD', '--expected-bucket-owner', ACCOUNT,
                '--checksum-algorithm', 'SHA256', '--checksum-sha256', base64.b64encode(bytes.fromhex(entry['sha256'])).decode('ascii')))
            if response.get('ServerSideEncryption') != 'AES256':
                raise ValueError('Upload did not confirm SSE-S3')
        # A snapshot is consumable only after this manifest has been uploaded last.
        manifest = {'status': 'complete', 'layer': layer, 'run_id': run_id, 'bucket': BUCKET, 'region': REGION, 'prefix': prefix,
            'published_at_utc': datetime.now(timezone.utc).isoformat(), 'tables': tables, 'files': files,
            'object_count_including_manifest': len(files) + 1, 'artifact_bytes': sum(entry['size_bytes'] for entry in files),
            'encryption': 'SSE-S3 / AES256', 'validation': report.get('validation', {}), 'metrics': report.get('metrics', {}),
            'source_silver_run_id': report.get('silver_run_id'), 'immutable_by_publisher': True,
            'note': 'Publisher uses conditional writes; bucket Object Lock is not enabled.'}
        manifest_path = ROOT / 'outputs' / f'{layer}_publication_manifest.json'
        manifest_path.write_text(json.dumps(manifest, indent=2) + '\n', encoding='utf-8')
        if prefix + 'completion_manifest.json' in existing_keys:
            remote_manifest_path = ROOT / 'outputs' / f'{layer}_existing_publication_manifest.json'
            aws('s3api', 'get-object', '--bucket', BUCKET, '--key', prefix + 'completion_manifest.json', '--expected-bucket-owner', ACCOUNT, str(remote_manifest_path))
            remote_manifest = json.loads(remote_manifest_path.read_text(encoding='utf-8'))
            for field in ['status', 'layer', 'run_id', 'prefix', 'tables', 'files', 'source_silver_run_id']:
                if remote_manifest.get(field) != manifest.get(field):
                    raise ValueError(f'Existing completion manifest differs: {field}')
            manifest = remote_manifest
            manifest_path.write_text(json.dumps(manifest, indent=2) + '\n', encoding='utf-8')
        else:
            aws('s3api', 'put-object', '--bucket', BUCKET, '--key', prefix + 'completion_manifest.json', '--body', str(manifest_path),
                '--if-none-match', '*', '--server-side-encryption', 'AES256', '--expected-bucket-owner', ACCOUNT,
                '--checksum-algorithm', 'SHA256', '--checksum-sha256', base64.b64encode(digest(manifest_path)).decode('ascii'))
        download = ROOT / 'data' / 'published_verification' / layer / run_id
        if download.exists():
            download = download.with_name(run_id + '_verify_' + datetime.now(timezone.utc).strftime('%H%M%S'))
        aws('s3', 'cp', f's3://{BUCKET}/{prefix}', str(download), '--recursive', '--no-progress', '--only-show-errors')
        downloaded_manifest = json.loads((download / 'completion_manifest.json').read_text(encoding='utf-8'))
        if downloaded_manifest != manifest:
            raise ValueError('Published completion manifest differs')
        expected_files = {entry['relative_path'] for entry in files} | {'completion_manifest.json'}
        actual_files = {path.relative_to(download).as_posix() for path in download.rglob('*') if path.is_file()}
        if actual_files != expected_files:
            raise ValueError('Published file inventory differs')
        for entry in files:
            downloaded = download / entry['relative_path']
            if downloaded.stat().st_size != entry['size_bytes'] or digest(downloaded).hex() != entry['sha256']:
                raise ValueError(f"Published checksum/size differs: {entry['relative_path']}")
        manifest_bytes = manifest_path.stat().st_size
        publications.append({'layer': layer, 'run_id': run_id, 's3_uri': f's3://{BUCKET}/{prefix}', 'verification_path': str(download),
            'object_count': len(files) + 1, 'uploaded_bytes_including_manifest': manifest['artifact_bytes'] + manifest_bytes, 'downloaded_checksums_passed': True})
        print(f'{layer}: published {len(files) + 1} objects; every downloaded file matches SHA256 and size', flush=True)
    result = {'status': 'files_verified', 'account': ACCOUNT, 'bucket': BUCKET, 'region': REGION, 'block_public_access': public_block, 'publications': publications}
    (ROOT / 'outputs' / 'publication_latest.json').write_text(json.dumps(result, indent=2) + '\n', encoding='utf-8')

if __name__ == '__main__':
    main()


