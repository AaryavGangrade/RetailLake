"""Refresh S3 inputs, skip completed batches, and retain one log/report per run."""
import json
import os
import subprocess
import sys
import traceback
from datetime import datetime, timezone
from pathlib import Path
from time import perf_counter
from uuid import uuid4

root = Path(__file__).resolve().parents[1]
logs = root / 'logs'
logs.mkdir(exist_ok=True)
run_id = datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ') + '_' + uuid4().hex[:8]
log_path = logs / f'{run_id}.log'
report_path = logs / f'{run_id}.json'
started = perf_counter()
report = {'run_id': run_id, 'started_at': datetime.now(timezone.utc).isoformat(),
          'status': 'running', 'log_file': str(log_path), 'steps': []}
exit_code = 0
# Pick up the user-local CLI even in terminals opened before its installation.
environment = dict(os.environ)
aws_bin = Path.home() / 'AppData' / 'Local' / 'Programs' / 'Amazon' / 'AWSCLIV2'
if os.name == 'nt' and (aws_bin / 'aws.exe').exists():
    environment['PATH'] = str(aws_bin) + os.pathsep + environment.get('PATH', '')

def save_report():
    report_path.write_text(json.dumps(report, indent=2) + '\n', encoding='utf-8')

save_report()
with log_path.open('w', encoding='utf-8') as log:
    def execute(command, name):
        step = {'name': name, 'status': 'running', 'started_at': datetime.now(timezone.utc).isoformat()}
        report['steps'].append(step)
        save_report()
        step_started = perf_counter()
        log.write(f'\nSTEP: {name}\nCOMMAND: {command}\n')
        log.flush()
        try:
            subprocess.run(command, check=True, cwd=root, env=environment,
                           stdout=log, stderr=subprocess.STDOUT)
            step['status'] = 'completed'
            step['exit_code'] = 0
        except subprocess.CalledProcessError as error:
            step['status'] = 'failed'
            step['exit_code'] = error.returncode
            raise
        except Exception:
            step['status'] = 'failed'
            raise
        finally:
            step['finished_at'] = datetime.now(timezone.utc).isoformat()
            step['duration_seconds'] = round(perf_counter() - step_started, 2)
            save_report()

    def run(script):
        execute([sys.executable, str(root / 'scripts' / script)], script)

    try:
        execute(['powershell.exe', '-NoProfile', '-File',
                 str(root / 'scripts' / 'download_bronze.ps1')], 'download_bronze.ps1')
        run('identify_batch.py')
        batch = json.loads((root / 'outputs' / 'input_batch.json').read_text(encoding='utf-8-sig'))
        report['batch_id'] = batch['batch_id']
        ledger_path = root / 'state' / 'ingestion_ledger.json'
        ledger = json.loads(ledger_path.read_text(encoding='utf-8-sig')) if ledger_path.exists() else {}
        if ledger.get(batch['batch_id'], {}).get('status') == 'complete':
            report['status'] = 'skipped'
            report['reason'] = 'Input batch already recorded as complete'
            log.write('\nBatch already completed. Processing skipped.\n')
        else:
            for script in ['build_silver.py', 'build_gold.py', 'publish_snapshots.py',
                           'verify_published.py', 'record_batch.py']:
                run(script)
            report['status'] = 'completed'
    except Exception as error:
        report['status'] = 'failed'
        report['error'] = str(error)
        traceback.print_exc(file=log)
        exit_code = 1
    finally:
        report['finished_at'] = datetime.now(timezone.utc).isoformat()
        report['duration_seconds'] = round(perf_counter() - started, 2)
        save_report()

print('Status:', report['status'])
print('Run report:', report_path)
print('Detailed log:', log_path)
sys.exit(exit_code)
