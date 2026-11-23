from __future__ import annotations
import json
import re
import subprocess
from datetime import datetime, timezone
from pathlib import Path
from src.common.atomic_io import write_json_atomic

ROOT = Path(__file__).resolve().parents[2]


def git(*args):
    return subprocess.check_output(["git", *args], cwd=ROOT).decode("utf-8").strip()


def release_decision(checks, ci_current_commit=False, peer_review=False):
    local = bool(checks) and all(value is True for value in checks.values())
    return {"ready_to_push_branch": local,
            "ready_to_merge_main": local and ci_current_commit is True and peer_review is True,
            "ci_on_current_candidate": ci_current_commit is True, "peer_review_approved": peer_review is True}


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
    from src.quality.tag_defense import verified_candidate
    proof = read('defense_verification')
    error = None
    try:
        verified_candidate(ROOT, proof)
        verified = True
    except (OSError, ValueError) as exc:
        verified, error = False, str(exc)
    audit = public_file_audit(read('snapshot_manifest'))
    checks = {'current_technical_acceptance': verified, 'public_files': audit['status'] == 'PASS'}
    result = {'generated_at': datetime.now(timezone.utc).isoformat(), 'checks': checks,
              'status': 'PASS' if all(checks.values()) else 'FAIL', 'stale_reason': error,
              'run_id': proof['run_id'], 'decision': release_decision(checks),
              'note': 'Local readiness only. Exact pushed-commit CI and review are separate gates.', 'public_file_audit': audit}
    write_json_atomic(ROOT / 'docs/validation/release_readiness.json', result)
    print(json.dumps(result, indent=2))
    if result['status'] != 'PASS':
        raise SystemExit(2)


if __name__ == '__main__':
    main()
