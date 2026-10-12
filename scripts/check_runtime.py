"""Local-only reliability checks; intentional child failures are expected, no AWS calls."""
import json
import subprocess
import sys
from pathlib import Path
from runtime_state import single_writer, atomic_json
root = Path(__file__).resolve().parents[1]
checks = {}
with single_writer(root / 'state/pipeline.lock'):
    child = subprocess.run([sys.executable, str(root / 'scripts/run_pipeline.py'), '--check-failure'], cwd=root, capture_output=True, text=True)
    checks['overlapping_runner_rejected'] = child.returncode == 1 and 'Status: failed' in child.stdout
    cleanup = subprocess.run(['powershell.exe', '-NoProfile', '-File', str(root / 'scripts/clean_spark_temp.ps1')], cwd=root, capture_output=True, text=True)
    checks['cleanup_refuses_active_runner'] = cleanup.returncode != 0 and 'state is in use' in cleanup.stderr
child = subprocess.run([sys.executable, str(root / 'scripts/run_pipeline.py'), '--check-failure'], cwd=root, capture_output=True, text=True)
checks['lock_released_and_failure_logged'] = child.returncode == 1 and 'Status: failed' in child.stdout
sample = root / '.cache/runtime_checks/atomic.json'
atomic_json(sample, {'version': 1})
atomic_json(sample, {'version': 2})
checks['atomic_json_replaced'] = json.loads(sample.read_text()) == {'version': 2} and not list(sample.parent.glob('*.tmp'))
for script in (root / 'scripts').glob('*.py'):
    compile(script.read_text(encoding='utf-8-sig'), str(script), 'exec')
checks['scripts_compile'] = True
atomic_json(root / 'outputs/runtime_checks.json', checks)
for name, passed in checks.items():
    print(name + ': ' + ('PASS' if passed else 'FAIL'))
raise SystemExit(0 if all(checks.values()) else 1)
