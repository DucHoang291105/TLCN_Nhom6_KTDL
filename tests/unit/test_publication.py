import json
from types import SimpleNamespace

import pytest

from src.orchestration import publication as gate
from src.orchestration.pipeline import STAGES


@pytest.fixture
def build(tmp_path, monkeypatch):
    monkeypatch.setattr(gate, 'identity', lambda root=tmp_path: 'same-bytes')
    folder = tmp_path / 'docs/validation'
    folder.mkdir(parents=True)
    for name in gate.REPORTS:
        (folder / f'{name}.json').write_text(json.dumps({'status': 'PASS'}))
    jobs = [p for stages in STAGES.values() for _, p in stages]
    state = {'status': 'BUILDING', 'mode': 'write', 'owner': 'run1', 'identity': 'same-bytes', 'completed': jobs}
    gate.save(state, tmp_path)
    return tmp_path, state, jobs


def test_lock_is_shared_and_cleans_up_after_exception(tmp_path):
    with pytest.raises(ValueError):
        with gate.catalog_lock(tmp_path):
            with pytest.raises(RuntimeError, match='Another catalog'):
                with gate.catalog_lock(tmp_path):
                    pytest.fail('Two catalog operations acquired the same lock')
            raise ValueError('interrupt')
    assert not (tmp_path / gate.LOCK).exists()


def test_unverified_and_incomplete_builds_block_readers(build):
    root, state, jobs = build
    with pytest.raises(RuntimeError, match='Read window blocked'):
        gate.require_published(root)
    state['completed'] = jobs[:-1]
    gate.save(state, root)
    with pytest.raises(RuntimeError, match='incomplete'):
        gate.publish(root)


def test_success_is_bound_to_reports_source_and_input(build, monkeypatch):
    root, state, jobs = build
    gate.publish(root, 'run1')
    assert gate.require_published(root)['run_id'] == 'run1'
    report = root / 'docs/validation/gold_verification.json'
    report.write_text(json.dumps({'status': 'FAIL'}))
    with pytest.raises(RuntimeError, match='verification changed'):
        gate.require_published(root)
    report.write_text(json.dumps({'status': 'PASS'}))
    monkeypatch.setattr(gate, 'identity', lambda root: 'new-data-or-source')
    with pytest.raises(RuntimeError, match='current source/input'):
        gate.require_published(root)


def test_partial_gold_failure_blocks_publication_and_requires_rebuild(build):
    root, _, jobs = build
    app = 'src/gold/build_price_model.py'
    state = gate.begin_job(app, 'run1', root)
    assert app not in state['completed'] and jobs[-1] not in state['completed']
    gate.finish_job(state, app, False, root)
    with pytest.raises(RuntimeError, match='Read window blocked'):
        gate.require_published(root)
    with pytest.raises(RuntimeError, match='incomplete or failed'):
        gate.publish(root)
    state = gate.begin_job(app, 'retry', root)
    assert 'error' not in state
    gate.finish_job(state, app, True, root)
    with pytest.raises(RuntimeError, match='incomplete'):
        gate.publish(root)


def test_read_window_and_other_pipeline_reject_manual_writer(build):
    root, state, jobs = build
    with pytest.raises(RuntimeError, match='Another pipeline'):
        gate.begin_job(jobs[0], 'manual', root)
    state['mode'] = 'read'
    gate.save(state, root)
    with pytest.raises(RuntimeError, match='Query window'):
        gate.begin_job(jobs[0], 'run1', root)


def test_read_only_verification_preserves_unchanged_downstream(build):
    root, state, jobs = build
    gate.publish(root, 'run1')
    state = gate.begin_job('src/silver/verify_silver.py', 'manual', root)
    assert 'src/gold/build_price_model.py' in state['completed']
    assert 'src/gold/verify_gold.py' in state['completed']
    assert 'src/silver/verify_silver.py' not in state['completed']
    gate.finish_job(state, 'src/silver/verify_silver.py', True, root)
    gate.publish(root, 'manual')
    assert gate.require_published(root)['status'] == 'PUBLISHED'


def test_changed_bytes_during_execution_fail_closed(build, monkeypatch):
    root, state, jobs = build
    monkeypatch.setattr(gate, 'identity', lambda root: 'input-mutated')
    with pytest.raises(RuntimeError, match='changed during Spark'):
        gate.finish_job(state, jobs[0], True, root)
    assert gate.load(root)['status'] == 'FAILED'


def test_killed_spark_exit_is_preserved_and_lock_released(build, monkeypatch):
    root, state, jobs = build
    monkeypatch.setattr(gate.subprocess, 'run', lambda *a, **kw: SimpleNamespace(returncode=137))
    assert gate.run_job(jobs[0], owner='run1', root=root) == 137
    assert gate.load(root)['status'] == 'FAILED'
    assert not (root / gate.LOCK).exists()


def test_real_identity_normalizes_source_but_keeps_csv_bytes(tmp_path):
    source = tmp_path / 'src/test.py'
    source.parent.mkdir()
    source.write_bytes(b'x = 1\n')
    raw = tmp_path / 'data/incoming/snapshots/guland/20261120/guland_raw.csv'
    raw.parent.mkdir(parents=True)
    raw.write_bytes(b'listing_id\n1\n')
    first = gate.identity(tmp_path)
    source.write_bytes(b'x = 1\r\n')
    assert gate.identity(tmp_path) == first
    raw.write_bytes(b'listing_id\r\n1\r\n')
    assert gate.identity(tmp_path) != first


def test_identity_uses_portable_path_sorting_not_native_windows_order(tmp_path):
    import hashlib
    upper = tmp_path / 'docker/fastapi/Dockerfile'
    lower = tmp_path / 'docker/fastapi/app/main.py'
    for path, body in ((upper, b'FROM fixed\n'), (lower, b'x=1\n')):
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(body)
    digest = hashlib.sha256()
    for path in (upper, lower):
        digest.update((path.relative_to(tmp_path).as_posix() + '\0' + gate.checksum(path, True) + '\n').encode())
    assert gate.identity(tmp_path) == digest.hexdigest()


@pytest.mark.parametrize('workers', [1, 4])
def test_parallel_identity_hashes_bytes_even_if_size_and_mtime_are_restored(tmp_path, workers):
    import os
    path = tmp_path / 'data/incoming/snapshots/guland/20261120/guland_raw.csv'
    path.parent.mkdir(parents=True)
    path.write_bytes(b'listing_id\n1\n')
    metadata = path.stat()
    before = gate.identity(tmp_path, workers=workers)
    path.write_bytes(b'listing_id\n2\n')
    os.utime(path, ns=(metadata.st_atime_ns, metadata.st_mtime_ns))
    assert path.stat().st_size == metadata.st_size
    assert gate.identity(tmp_path, workers=workers) != before


def test_parallel_hash_failure_is_not_suppressed(tmp_path, monkeypatch):
    path = tmp_path / 'src/probe.py'
    path.parent.mkdir()
    path.write_text('x=1')
    def denied(*args):
        raise OSError('unreadable source')
    monkeypatch.setattr(gate, 'checksum', denied)
    with pytest.raises(OSError, match='unreadable source'):
        gate.identity(tmp_path)


def test_source_change_between_stages_stops_before_next_spark_job(build, monkeypatch):
    root, state, jobs = build
    monkeypatch.setattr(gate, 'identity', lambda root: 'changed-between-stages')
    with pytest.raises(RuntimeError, match='between pipeline stages'):
        gate.begin_job(jobs[-1], 'run1', root)
    assert gate.load(root)['status'] == 'FAILED'
