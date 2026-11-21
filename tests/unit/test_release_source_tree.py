from src.quality import defense_evidence as evidence


def test_source_hash_detects_deletion_and_is_stable_after_staging_it(tmp_path, monkeypatch):
    folder = tmp_path / 'src'
    folder.mkdir()
    old, new = folder / 'old.py', folder / 'new.py'
    old.write_text('x=1')
    new.write_text('x=2')
    monkeypatch.setattr(evidence.subprocess, 'check_output', lambda *a, **kw: b'src/old.py\0src/new.py\0')
    before = evidence.code_state(tmp_path)
    old.unlink()
    deleted = evidence.code_state(tmp_path)
    assert deleted != before
    monkeypatch.setattr(evidence.subprocess, 'check_output', lambda *a, **kw: b'src/new.py\0')
    staged = evidence.code_state(tmp_path)
    assert staged == deleted
