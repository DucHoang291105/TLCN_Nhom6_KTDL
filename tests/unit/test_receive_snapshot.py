import pytest

from src.orchestration.receive_snapshot import receive


def sample(tmp_path):
    path = tmp_path / 'export.csv'
    path.write_text('listing_id,title\n1,test\n', encoding='utf-8')
    return path


def test_dry_run_then_atomic_copy_and_retry(tmp_path):
    path = sample(tmp_path)
    root = tmp_path / 'repo'
    assert receive(root, 'batdongsan', '20261113', path)['action'] == 'VALIDATED_NOT_COPIED'
    assert not root.exists()
    result = receive(root, 'batdongsan', '20261113', path, apply=True)
    from pathlib import Path
    assert Path(result['target']).read_bytes() == path.read_bytes()
    assert result['action'] == 'COPIED'
    assert receive(root, 'batdongsan', '20261113', path, apply=True)['action'] == 'REUSE_IDENTICAL'
    assert not list(root.rglob('*.tmp'))


def test_existing_csv_never_overwritten(tmp_path):
    path = sample(tmp_path)
    result = receive(tmp_path, 'guland', '20261113', path, apply=True)
    path.write_text('listing_id,title\n2,changed\n')
    with pytest.raises(ValueError, match='refusing overwrite'):
        receive(tmp_path, 'guland', '20261113', path, apply=True)
    from pathlib import Path
    assert Path(result['target']).read_text() == 'listing_id,title\n1,test\n'


@pytest.mark.parametrize('source,batch', [('other','20261113'), ('guland','20260230'),
                                        ('../x','20261113'), ('guland','20261118')])
def test_invalid_source_date_or_future_is_blocked(tmp_path, source, batch):
    with pytest.raises(ValueError):
        receive(tmp_path, source, batch, sample(tmp_path), today='20261117')


def test_copy_mutation_cannot_publish_bad_bytes(tmp_path, monkeypatch):
    from src.orchestration import receive_snapshot as module
    real = module.shutil.copyfileobj
    def corrupt(src, dst):
        real(src, dst)
        dst.write(b'bad')
    monkeypatch.setattr(module.shutil, 'copyfileobj', corrupt)
    with pytest.raises(ValueError, match='changed during copy'):
        receive(tmp_path, 'guland', '20261113', sample(tmp_path), apply=True)
    assert not list((tmp_path / 'data').rglob('*_raw.csv'))
    assert not list((tmp_path / 'data').rglob('*.tmp'))


def test_competing_writer_is_never_overwritten(tmp_path, monkeypatch):
    from src.orchestration import receive_snapshot as module
    from pathlib import Path
    def competing_link(src, dst):
        Path(dst).write_bytes(b'OTHER_WRITER')
        raise FileExistsError('competing writer')
    monkeypatch.setattr(module.os, 'link', competing_link)
    with pytest.raises(FileExistsError):
        receive(tmp_path, 'guland', '20261113', sample(tmp_path), apply=True)
    target = tmp_path / 'data/incoming/snapshots/guland/20261113/guland_raw.csv'
    assert target.read_bytes() == b'OTHER_WRITER'
    assert not list(target.parent.glob('*.tmp'))
