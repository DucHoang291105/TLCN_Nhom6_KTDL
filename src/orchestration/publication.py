from __future__ import annotations

import argparse
import contextlib
import hashlib
import json
import os
import subprocess
import uuid
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from pathlib import Path

from src.common.atomic_io import file_sha256, write_json_atomic

ROOT = Path(__file__).resolve().parents[2]
STATE = 'outputs/operation/publication.json'
LOCK = 'outputs/operation/catalog.lock'
REPORTS = ('snapshot_manifest', 'bronze_state_audit', 'silver_final_verification', 'gold_verification',
           'gold_listing_lifecycle_summary')


def now():
    return datetime.now(timezone.utc).isoformat()


@contextlib.contextmanager
def catalog_lock(root=ROOT):
    path = Path(root) / LOCK
    path.parent.mkdir(parents=True, exist_ok=True)
    try:
        path.mkdir()
    except FileExistsError as exc:
        raise RuntimeError('Another catalog operation holds catalog.lock; inspect active work before recovery') from exc
    token = uuid.uuid4().hex
    owner = path / 'owner.json'
    try:
        write_json_atomic(owner, {'token': token, 'pid': os.getpid(), 'at': now()})
        yield
    finally:

        if owner.exists() and json.loads(owner.read_text(encoding='utf-8')).get('token') == token:
            owner.unlink()
            path.rmdir()


def checksum(path, normalize=False):
    return file_sha256(path, normalize)


def identity(root=ROOT, workers=4):
    root = Path(root)
    if type(workers) is not int or not 1 <= workers <= 16:
        raise ValueError('Identity hash workers must be an integer between 1 and 16')
    paths = []
    for directory in ('src', 'config', 'dags', 'docker'):
        paths.extend(p for p in (root / directory).rglob('*') if p.is_file()
                     and '__pycache__' not in p.parts and p.suffix not in ('.pyc', '.pyo'))
    paths.extend(p for p in root.glob('docker-compose*.yml') if 'override' not in p.name)
    for directory in ('data/incoming/snapshots', 'data/incoming/historical'):
        for path in (root / directory).rglob('*.csv'):
            if directory.endswith('snapshots') and not (len(path.parent.name) == 8 and path.parent.name.isdigit()):
                continue
            paths.append(path)
    digest = hashlib.sha256()


    paths.sort(key=lambda p: p.relative_to(root).as_posix())

    def fingerprint(path):
        return path.relative_to(root).as_posix(), checksum(path, path.suffix != '.csv')


    with ThreadPoolExecutor(max_workers=workers) as pool:
        for relative, value in pool.map(fingerprint, paths):
            digest.update((relative + '\0' + value + '\n').encode())
    return digest.hexdigest()


def load(root=ROOT):
    path = Path(root) / STATE
    return json.loads(path.read_text(encoding='utf-8')) if path.exists() else {'status': 'UNVERIFIED', 'mode': 'write', 'completed': []}


def save(state, root=ROOT):
    state['updated_at'] = now()
    write_json_atomic(Path(root) / STATE, state)


def begin_job(app, owner, root=ROOT):
    from src.orchestration.pipeline import STAGES
    jobs = [path for stages in STAGES.values() for _, path in stages]
    state = load(root)
    if state.get('mode') == 'read':
        raise RuntimeError('Query window is open; switch environment to write before Spark')
    if state.get('status') == 'BUILDING' and state.get('owner') != owner:
        raise RuntimeError(f'Another pipeline owns this build: {state.get("owner")}')


    current = identity(root)
    if state.get('identity') != current:
        if state.get('status') == 'BUILDING':
            state.update(status='FAILED', error='Source/input changed between pipeline stages; start a fresh full build')
            save(state, root)
            raise RuntimeError(state['error'])
        state['completed'] = []
    if app in jobs:


        invalidated = {app} if app in {'src/silver/verify_silver.py', 'src/gold/verify_gold.py'} else set(jobs[jobs.index(app):])
        state['completed'] = [p for p in state.get('completed', []) if p not in invalidated]
    state.pop('error', None)
    state.update(status='BUILDING', mode='write', identity=current, owner=owner, active_job=app,
                 started_at=state.get('started_at') if state.get('status') == 'BUILDING' else now())
    save(state, root)
    return state


def finish_job(state, app, success, root=ROOT):
    if identity(root) != state['identity']:
        state.update(status='FAILED', error='Source or input bytes changed during Spark execution')
        save(state, root)
        raise RuntimeError(state['error'])
    if success:
        if app not in state['completed']:
            state['completed'].append(app)
        state.pop('active_job', None)
    else:
        state.update(status='FAILED', error=f'Spark failed at {app}')
    save(state, root)


def publish(root=ROOT, run_id=None):
    from src.orchestration.pipeline import STAGES
    state = load(root)
    required = {p for stages in STAGES.values() for _, p in stages}
    if state.get('status') != 'BUILDING' or not required <= set(state.get('completed', [])):
        raise RuntimeError('Cannot publish: pipeline is incomplete or failed; rerun missing stages')
    if state.get('identity') != identity(root):
        raise RuntimeError('Cannot publish: source/input changed after the build')
    reports = {}
    for name in REPORTS:
        path = Path(root) / f'docs/validation/{name}.json'
        report = json.loads(path.read_text(encoding='utf-8'))
        if report.get('status') != 'PASS':
            raise RuntimeError(f'Cannot publish: {name} is not PASS')
        reports[name] = checksum(path, normalize=True)
    state.update(status='PUBLISHED', published_at=now(), reports=reports, run_id=run_id or state.get('owner'))
    save(state, root)
    return state


def require_published(root=ROOT):
    state = load(root)
    if state.get('status') != 'PUBLISHED' or state.get('identity') != identity(root):
        raise RuntimeError('Read window blocked: no verified publication for current source/input bytes')
    for name, digest in state.get('reports', {}).items():
        if checksum(Path(root) / f'docs/validation/{name}.json', normalize=True) != digest:
            raise RuntimeError(f'Read window blocked: verification changed: {name}')
    if set(state.get('reports', {})) != set(REPORTS):
        raise RuntimeError('Read window blocked: publication has incomplete verification references')
    return state


def run_job(app, args=(), owner='manual', root=ROOT):
    from src.orchestration.pipeline import spark_command


    diagnostic = (app.startswith('src/quality/') or app.endswith('table_fingerprints.py')
                  or app == 'src/bronze/compare_snapshot_batches.py')
    with catalog_lock(root):
        state = None if diagnostic else begin_job(app, owner, root)
        if diagnostic and load(root).get('mode') == 'read':
            raise RuntimeError('Query window is open; switch environment to write before Spark')
        try:
            result = subprocess.run(spark_command(app, args), check=False)
            if state is not None:
                finish_job(state, app, result.returncode == 0, root)
            if result.returncode:
                return result.returncode
            if app == 'src/gold/verify_gold.py':
                publish(root, owner)
            return 0
        except BaseException:
            if state is not None:
                state.update(status='FAILED', error=f'Job interrupted or publication rejected: {app}')
                save(state, root)
            raise


def main():
    parser = argparse.ArgumentParser(description='Fail-closed publication gate shared by manual Spark jobs, Airflow and readers.\n\nIceberg commits individual tables, not the entire pipeline atomically. Readers\ntherefore remain closed between the first write and successful verification.\nThe same atomic directory lock works across the Windows/Docker bind mount.\n')
    parser.add_argument('app')
    parser.add_argument('args', nargs=argparse.REMAINDER)
    parser.add_argument('--owner', default=os.getenv('LAKEHOUSE_RUN_ID', 'manual'))
    args = parser.parse_args()
    raise SystemExit(run_job(args.app, args.args, args.owner))


if __name__ == '__main__':
    main()
