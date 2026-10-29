import json
import pytest
from src.orchestration.snapshot_manifest import build_manifest, profile_csv, profile_csv_with_checksum
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
@pytest.mark.parametrize("profile", [profile_csv, profile_csv_with_checksum])
def test_malformed_csv_is_rejected(tmp_path, text, profile):
    path = prepare(tmp_path, text)
    with pytest.raises(ValueError):
        profile(path)


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


def test_single_pass_profile_hashes_exact_bom_crlf_and_multiline_bytes(tmp_path):
    import hashlib
    from src.orchestration.snapshot_manifest import profile_csv_with_checksum
    path = tmp_path / "input.csv"
    content = b'\xef\xbb\xbflisting_id,title,_source_page\r\n1,"line 1\r\nline 2",3\r\n2,"a,b",7\r\n'
    path.write_bytes(content)
    profile, digest, size = profile_csv_with_checksum(path)
    assert profile == profile_csv(path) == {"rows": 2, "columns": 3, "max_source_page": 7}
    assert digest == hashlib.sha256(content).hexdigest()
    assert size == len(content)


def test_new_and_forced_profiles_do_not_reread_for_separate_checksums(tmp_path, monkeypatch):
    from src.orchestration import snapshot_manifest
    prepare(tmp_path)
    def redundant_read(*args):
        pytest.fail("A parsed file should be hashed in the same pass")
    monkeypatch.setattr(snapshot_manifest, "file_checksum", redundant_read)
    first = build_manifest(tmp_path, ["guland"], "20261023")
    forced = build_manifest(tmp_path, ["guland"], "20261023", force_rescan=True)
    assert first["snapshots"] == forced["snapshots"]
    assert forced["verification"]["csv_profiles_parsed"] == 1


def test_single_pass_rejects_input_changed_while_parsing(tmp_path, monkeypatch):
    from src.orchestration import snapshot_manifest
    path = prepare(tmp_path)
    original = snapshot_manifest._profile_stream
    def modified(handle, filename):
        result = original(handle, filename)
        with path.open("a", encoding="utf-8") as output:
            output.write("2,new row\n")
        return result
    monkeypatch.setattr(snapshot_manifest, "_profile_stream", modified)
    with pytest.raises(ValueError, match="changed while parsing"):
        snapshot_manifest.profile_csv_with_checksum(path)


def test_single_pass_rejects_lfs_pointer(tmp_path):
    from src.orchestration.snapshot_manifest import profile_csv_with_checksum
    path = tmp_path / "pointer.csv"
    path.write_text("version https://git-lfs.github.com/spec/v1\noid sha256:abc\nsize 10\n")
    with pytest.raises(RuntimeError, match="git lfs pull"):
        profile_csv_with_checksum(path)
