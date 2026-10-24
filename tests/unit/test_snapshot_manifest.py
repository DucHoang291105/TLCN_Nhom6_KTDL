import json
import pytest
from src.orchestration.snapshot_manifest import build_manifest, profile_csv
from src.common.atomic_io import write_json_atomic
from src.common.writer_lock import file_writer_lock


def prepare(root, text="listing_id,title\n1,example\n"):
    path = root / "data/incoming/snapshots/guland/20261023/guland_raw.csv"
    path.parent.mkdir(parents=True)
    path.write_text(text, encoding="utf-8")
    return path


def test_cached_profiles_still_hash_every_file(tmp_path):
    path = prepare(tmp_path)
    first = build_manifest(tmp_path, ["guland"], "20261023")
    second = build_manifest(tmp_path, ["guland"], "20261023")
    assert first["snapshots"] == second["snapshots"]
    assert second["verification"]["csv_profiles_reused"] == second["verification"]["sha256_files_checked"] == 1
    # Same size, even restored mtime, must not defeat the checksum.
    import os
    before = path.stat()
    path.write_text(path.read_text().replace("example", "changed"), encoding="utf-8")
    os.utime(path, ns=(before.st_atime_ns, before.st_mtime_ns))
    manifest = tmp_path / "docs/validation/snapshot_manifest.json"
    saved = manifest.read_bytes()
    with pytest.raises(ValueError, match="snapshot changed"):
        build_manifest(tmp_path, ["guland"], "20261023")
    assert manifest.read_bytes() == saved


def test_force_rescan_and_invalidated_parser(tmp_path):
    prepare(tmp_path)
    build_manifest(tmp_path, ["guland"], "20261023")
    assert build_manifest(tmp_path, ["guland"], "20261023", force_rescan=True)["verification"]["csv_profiles_parsed"] == 1
    manifest = tmp_path / "docs/validation/snapshot_manifest.json"
    old = json.loads(manifest.read_text())
    old["snapshots"][0]["parser_version"] = "old"
    write_json_atomic(manifest, old)
    assert build_manifest(tmp_path, ["guland"], "20261023")["verification"]["csv_profiles_parsed"] == 1


def test_profile_without_saved_digest_cannot_be_reused(tmp_path):
    prepare(tmp_path)
    first = build_manifest(tmp_path, ["guland"], "20261023")
    manifest = tmp_path / "docs/validation/snapshot_manifest.json"
    del first["snapshots"][0]["sha256"]
    first["snapshots"][0]["rows"] = 999
    write_json_atomic(manifest, first)
    result = build_manifest(tmp_path, ["guland"], "20261023")
    assert result["snapshot_rows"] == 1
    assert result["verification"]["csv_profiles_parsed"] == 1


@pytest.mark.parametrize("text", ["listing_id,title,title\n1,a,b\n", "listing_id,title\n1\n", "listing_id,title\n1,a,b\n", "listing_id,title\n"])
def test_malformed_csv_is_rejected(tmp_path, text):
    path = prepare(tmp_path, text)
    with pytest.raises(ValueError):
        profile_csv(path)


def test_atomic_write_failure_preserves_prior_file(tmp_path, monkeypatch):
    path = tmp_path / "result.json"
    write_json_atomic(path, {"value": "old"})
    def fail(*args):
        raise OSError("simulated replacement failure")
    monkeypatch.setattr("src.common.atomic_io.os.replace", fail)
    with pytest.raises(OSError):
        write_json_atomic(path, {"value": "new"})
    assert json.loads(path.read_text()) == {"value": "old"}
    assert list(tmp_path.iterdir()) == [path]


def test_competing_writer_rejected_and_lock_released_on_error(tmp_path):
    path = tmp_path / "writer.lock"
    with pytest.raises(ValueError):
        with file_writer_lock(path):
            with pytest.raises(RuntimeError, match="Another writer"):
                with file_writer_lock(path):
                    pytest.fail("Second writer acquired lock")
            raise ValueError("simulated writer failure")
    with file_writer_lock(path):
        pass


def test_public_atomic_artifact_mode_supports_cross_container_readers(tmp_path):
    import os
    import stat
    path = tmp_path / "public.json"
    write_json_atomic(path, {"status": "PASS"})
    assert json.loads(path.read_text())["status"] == "PASS"
    if os.name == "posix":
        assert stat.S_IMODE(path.stat().st_mode) == 0o644
    else:
        assert path.stat().st_mode & stat.S_IWRITE
