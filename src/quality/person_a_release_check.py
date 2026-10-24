"""Local release gates for A; uploading a branch and merging main are distinct gates."""
from __future__ import annotations
import hashlib
import json
import re
import subprocess
from datetime import datetime, timezone
from pathlib import Path
from xml.etree import ElementTree
from src.common.atomic_io import write_json_atomic

ROOT = Path(__file__).resolve().parents[2]
BASE = "a6909aed578f86cd5c7181e861ed55688c237012"


def git(*args):
    return subprocess.check_output(["git", *args], cwd=ROOT).decode("utf-8").strip()


def release_decision(checks, ci_current_commit=False, peer_review=False):
    local = bool(checks) and all(value is True for value in checks.values())
    return {"ready_to_push_branch": local,
            "ready_to_merge_main": local and ci_current_commit is True and peer_review is True,
            "ci_on_current_candidate": ci_current_commit is True, "peer_review_approved": peer_review is True}


def allowed_change(path):
    if path in {"README.md", ".gitignore"} or path.startswith(("docs/", "tests/", "src/orchestration/", "src/quality/")):
        return True
    return path in {"docker-compose.yml", "scripts/run_spark.ps1", "src/common/atomic_io.py", "src/common/writer_lock.py",
                    "src/common/table_fingerprints.py", "src/common/compare_rebuild_counts.py",
                    "src/silver/history.py", "src/silver/build_listing_history.py",
                    "src/gold/build_listing_lifecycle.py", "src/report/person_a_reviews.py"}


def secret_findings(path, text, local_secrets):
    findings = []
    for name, pattern in {
        "github_token": r"gh[pousr]_[A-Za-z0-9]{20,}",
        "aws_access_id": r"(?:AKIA|ASIA)[A-Z0-9]{16}",
        "private_key": r"-----BEGIN (?:RSA |EC |OPENSSH )?PRIVATE KEY-----",
    }.items():
        if re.search(pattern, text):
            findings.append({"file": path, "rule": name})
    for name, value in local_secrets.items():
        # A short/common local password may equal a service identifier. Do not
        # call 'connector.name=<service>' a credential leak. Still detect its
        # literal use as a password assignment or URI password.
        literal = re.search(r"(?i)(?:password|passwd|secret|token)[A-Za-z0-9_]*\s*[:=]\s*['\"]?" + re.escape(value) + r"(?:['\"\s]|$)", text) if value else None
        uri = (":"+value+"@") in text if value else False
        if (len(value) >= 12 and value in text) or literal or uri:
            findings.append({"file": path, "rule": "local_env_secret:"+name})
    return findings


def public_file_audit(manifest):
    paths = sorted(set(git("ls-files").splitlines()) | set(git("ls-files", "--others", "--exclude-standard").splitlines()))
    forbidden = [p for p in paths if Path(p).name.lower() in (".env", "tlcn_nhom6_codex_context_bronze_silver.txt") or
                 Path(p).name.lower().startswith("ke_hoach") or Path(p).suffix.lower() in (".pem", ".key")]
    secrets = {}
    env = ROOT / ".env"
    if env.exists():
        for line in env.read_text(encoding="utf-8-sig").splitlines():
            if "=" in line and not line.lstrip().startswith("#"):
                name, value = line.split("=", 1)
                if any(word in name.upper() for word in ("PASSWORD", "SECRET", "FERNET", "TOKEN")):
                    secrets[name.strip()] = value.strip().strip("\"'")
    extensions = {".py", ".ps1", ".md", ".txt", ".yml", ".yaml", ".toml", ".json", ".sql", ".properties"}
    findings, scanned = [], 0
    for path in paths:
        file = ROOT / path
        if file.is_file() and file.suffix.lower() in extensions and file.stat().st_size <= 2*1024*1024:
            scanned += 1
            findings.extend(secret_findings(path, file.read_text(encoding="utf-8-sig", errors="replace"), secrets))
    pointers = []
    for entry in manifest["snapshots"]:
        path = f"data/incoming/snapshots/{entry['source']}/{entry['batch_id']}/{entry['source']}_raw.csv"
        pointer = git("show", ":"+path)
        expected = "oid sha256:"+entry["sha256"]
        pointers.append(pointer.startswith("version https://git-lfs.github.com/spec/v1") and expected in pointer.splitlines())
    warnings = ["Weak/short local credential for "+name+"; rotate before any non-local deployment. Value redacted."
                for name,value in secrets.items() if len(value) < 12]
    return {"text_files_scanned": scanned, "forbidden_files": forbidden, "secret_findings": findings, "warnings": warnings,
            "snapshot_lfs_pointers_checked": len(pointers), "all_snapshot_pointers_match_manifest": all(pointers),
            "limitations": "Text scan and exact local .env secret matching, not a legal/PII audit. Existing raw CSV is unchanged and is not searched for personal data. No secret values or matching excerpts are emitted.",
            "status": "PASS" if not forbidden and not findings and all(pointers) else "FAIL"}


def read(name):
    return json.loads((ROOT / f"docs/validation/{name}.json").read_text(encoding="utf-8"))


def main():
    before, after = read("person_a_state_before"), read("person_a_state_after")
    comparisons = []
    for table in sorted(set(before["tables"]) | set(after["tables"])):
        a, b = before["tables"].get(table, {}), after["tables"].get(table, {})
        equal = all(a.get(key) == b.get(key) and a.get(key) is not None for key in ("rows", "distinct_keys", "content_hash", "hash_method"))
        comparisons.append({"table": table, "rows_before": a.get("rows"), "rows_after": b.get("rows"),
                            "excluded_build_columns": a.get("excluded_columns", []), "equal": equal})
    equivalence = {"fingerprint_version": before["fingerprint_version"], "tables_compared": len(comparisons),
                   "tables": comparisons, "status": "PASS" if comparisons and all(t["equal"] for t in comparisons) and
                   before["fingerprint_version"] == after["fingerprint_version"] == "sha256_multiset_v2" else "FAIL"}
    write_json_atomic(ROOT / "docs/validation/person_a_output_equivalence.json", equivalence)
    manifest, history, lifecycle = read("snapshot_manifest"), read("person_a_history_optimization"), read("person_a_lifecycle_optimization")
    run, silver, gold = read("person_a_airflow_run_final"), read("silver_final_verification"), read("gold_verification")
    memory = read("person_a_driver_memory")["after"]
    tests = ElementTree.parse(ROOT / "docs/validation/person_a_pytest.xml").getroot().find("testsuite")
    audit = public_file_audit(manifest)
    write_json_atomic(ROOT / "docs/validation/person_a_public_file_audit.json", audit)
    changed = sorted(set(git("diff", "--name-only", BASE).splitlines()) | set(git("ls-files", "--others", "--exclude-standard").splitlines()))
    unexpected = [path for path in changed if not allowed_change(path)]
    checks = {
        "unit_tests": tests is not None and int(tests.get("tests", 0)) > 0 and all(int(tests.get(k, 0)) == 0 for k in ("errors", "failures", "skipped")),
        "manifest_benchmark": read("person_a_manifest_benchmark")["status"] == "PASS",
        "history_benchmark_and_regressions": history["status"] == "PASS" and all(history["cache_fixtures"].values()),
        "lifecycle_benchmark_exact_content_and_schema": lifecycle["status"] == "PASS",
        "lifecycle_12_regressions": read("lifecycle_fixture_tests")["status"] == "PASS",
        "cross_container_atomic_io": read("person_a_shared_io_probe")["status"] == "PASS",
        "driver_headroom_no_oom": memory["memory_limit_bytes"] == 1536*1024*1024 and memory["events"]["oom_kill"] == 0,
        "persistent_ivy_cache": memory["ivy_named_volume"] is True and memory["ivy_cache_writable"] is True,
        "all_28_tables_unchanged": equivalence["status"] == "PASS" and equivalence["tables_compared"] == 28,
        "full_airflow_run": run["state"] == "success" and len(run["tasks"]) == 23 and all(t["state"] == "success" for t in run["tasks"]),
        "silver_30_checks": silver["status"] == "PASS" and len(silver["checks"]) >= 30 and all(silver["checks"].values()),
        "gold_92_checks": gold["status"] == "PASS" and len(gold["checks"]) >= 92 and all(gold["checks"].values()),
        "all_raw_files_rehashed": manifest["status"] == "PASS" and manifest["verification"]["sha256_files_checked"] == len(manifest["snapshots"]),
        "public_file_audit": audit["status"] == "PASS", "only_person_a_code_and_artifacts_changed": not unexpected,
    }
    digest = hashlib.sha256()
    for path in changed:
        if path == "docker-compose.yml" or path.startswith(("src/", "scripts/", "tests/", "dags/")):
            content = (ROOT/path).read_bytes().replace(b"\r\n", b"\n") if (ROOT/path).is_file() else b"<deleted>"
            digest.update(path.encode()+b"\0"+content)
    payload = {"generated_at": datetime.now(timezone.utc).isoformat(), "baseline_commit": BASE,
               "candidate_code_fingerprint": digest.hexdigest(), "head_at_verification": git("rev-parse", "HEAD"),
               "fingerprint_scope": "Changed source, scripts, tests, DAGs and docker-compose.yml relative to baseline; includes working-tree content. Not an artifact/docs hash or a replacement for exact-commit CI.",
               "branch": git("branch", "--show-current"), "checks": checks, "unexpected_changes": unexpected,
               "decision": release_decision(checks), "local_verification": "PASS" if all(checks.values()) else "FAIL",
               "github_status": "New optimization branch is not published. Previous CI/PR #2 does not certify this candidate. Exact pushed commit CI and peer review are required before main merge.",
               "status": "READY_TO_PUSH_BRANCH" if all(checks.values()) else "NOT_READY"}
    write_json_atomic(ROOT / "docs/validation/person_a_release_readiness.json", payload)
    print(json.dumps(payload, indent=2))
    if payload["local_verification"] != "PASS":
        raise SystemExit("Release gates failed; inspect person_a_release_readiness.json")


if __name__ == "__main__":
    main()
