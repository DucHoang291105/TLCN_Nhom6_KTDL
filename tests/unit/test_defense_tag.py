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
