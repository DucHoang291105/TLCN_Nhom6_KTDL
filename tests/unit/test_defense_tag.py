import hashlib
import json

import pytest

from src.quality import tag_defense as release


def candidate(tmp_path, monkeypatch):
    folder = tmp_path / "docs/validation"
    folder.mkdir(parents=True)
    artifact = folder / "gold_verification.json"
    artifact.write_text('{"status":"PASS"}')
    code, data = {"sha256": "code"}, {"sha256": {"source.csv": "input"}}
    monkeypatch.setattr(release, "code_state", lambda _: code)
    monkeypatch.setattr(release, "inputs", lambda _: data)
    proof = {"status": "PASS", "checks": {"all_tables": True}, "code": code.copy(),
             "inputs": json.loads(json.dumps(data)),
             "evidence": {"gold_verification": {"sha256": hashlib.sha256(artifact.read_bytes()).hexdigest()}}}
    return proof, code, data, artifact


def test_exact_verified_candidate_is_accepted(tmp_path, monkeypatch):
    proof, *_ = candidate(tmp_path, monkeypatch)
    release.verified_candidate(tmp_path, proof)


def test_json_evidence_normalizes_only_line_endings(tmp_path, monkeypatch):
    proof, _, _, artifact = candidate(tmp_path, monkeypatch)
    artifact.write_bytes(b'{\n  "status": "PASS"\n}\n')
    reference = proof['evidence']['gold_verification']
    reference.update(sha256=release.evidence_checksum(artifact), hash_normalization='LF')
    artifact.write_bytes(artifact.read_bytes().replace(b'\n', b'\r\n'))
    release.verified_candidate(tmp_path, proof)
    artifact.write_bytes(artifact.read_bytes().replace(b'PASS', b'FAIL'))
    with pytest.raises(ValueError, match='Evidence changed'):
        release.verified_candidate(tmp_path, proof)


@pytest.mark.parametrize("change", ["code", "input", "evidence", "failed", "empty", "string_pass"])
def test_tag_rejects_changed_or_incomplete_verification(tmp_path, monkeypatch, change):
    proof, code, data, artifact = candidate(tmp_path, monkeypatch)
    if change == "code": code["sha256"] = "changed"
    elif change == "input": data["sha256"]["source.csv"] = "changed"
    elif change == "evidence": artifact.write_text('{"status":"FAIL"}')
    elif change == "failed": proof["status"] = "FAIL"
    elif change == "empty": proof["checks"] = {}
    else: proof["checks"]["all_tables"] = "PASS"
    with pytest.raises(ValueError):
        release.verified_candidate(tmp_path, proof)


@pytest.mark.parametrize("tag", ["v1.0-bao-ve", "v1.1-bao-ve"])
def test_both_planned_tags_still_require_clean_reviewed_worktree(monkeypatch, tag):
    monkeypatch.setattr(release, "git", lambda *args: " M source.py")
    with pytest.raises(ValueError, match="Commit the reviewed"):
        release.create_tag(tag)


@pytest.mark.parametrize("tag", ["v9.9", "main", "../unsafe", "v1.1-bao-ve --force"])
def test_unplanned_tags_rejected_before_git(monkeypatch, tag):
    def unexpected(*args):
        raise AssertionError("Git must not run")
    monkeypatch.setattr(release, "git", unexpected)
    with pytest.raises(ValueError, match="planned defense tag"):
        release.create_tag(tag)
