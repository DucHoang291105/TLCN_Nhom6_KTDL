"""Create an immutable annotated defense tag only for a verified, CI-green commit."""
from __future__ import annotations

import argparse
import json
import subprocess
from pathlib import Path

from src.orchestration.snapshot_manifest import file_checksum
from src.quality.defense_evidence import ROOT, all_pass, code_state, inputs, read


def verified_candidate(root, evidence):
    if evidence.get("status") != "PASS" or not all_pass(evidence.get("checks", {})):
        raise ValueError("The complete acceptance suite must PASS before tagging")
    if code_state(root) != evidence["code"]:
        raise ValueError("Code/config changed after verification; rerun the acceptance suite")
    if inputs(root) != evidence["inputs"]:
        raise ValueError("Source data changed after verification; rerun the acceptance suite")
    for name, reference in evidence["evidence"].items():
        if file_checksum(Path(root) / "docs/validation" / f"{name}.json")[0] != reference["sha256"]:
            raise ValueError(f"Evidence changed after verification: {name}")


def git(*args):
    return subprocess.check_output(["git", *args], cwd=ROOT, text=True).strip()


def create_tag(tag, push=False):
    if tag != "v1.0-bao-ve":
        raise ValueError("This command finalizes only the planned v1.0-bao-ve tag")
    if git("status", "--porcelain"):
        raise ValueError("Commit the reviewed source, inputs and evidence before tagging")
    evidence = read(ROOT / "docs/validation/defense_verification.json")
    verified_candidate(ROOT, evidence)
    sha = git("rev-parse", "HEAD")
    tracked = json.loads(git("show", "HEAD:docs/validation/defense_verification.json"))
    if tracked != evidence:
        raise ValueError("The tagged commit must contain the exact verified evidence")
    result = subprocess.run(["gh", "run", "list", "--branch", git("branch", "--show-current"),
                             "--workflow", "tests.yml", "--limit", "30", "--json", "headSha,status,conclusion,url"],
                            cwd=ROOT, capture_output=True, text=True, check=True)
    ci = next((run for run in json.loads(result.stdout)
               if run["headSha"] == sha and run["status"] == "completed" and run["conclusion"] == "success"), None)
    if ci is None:
        raise ValueError("CI must pass on this exact commit; another commit's CI is insufficient")
    existing = subprocess.run(["git", "rev-parse", "--verify", f"refs/tags/{tag}^{{commit}}"],
                              cwd=ROOT, capture_output=True, text=True)
    if existing.returncode == 0:
        if existing.stdout.strip() != sha:
            raise ValueError("The tag already points elsewhere; never move a defense tag")
    else:
        message = (f"Pipeline and operation acceptance verified.\n\nBatch: {evidence['batch_id']}\n"
                   f"Run: {evidence['run_id']}\nCode SHA256: {evidence['code']['sha256']}\nCI: {ci['url']}\n"
                   "Scope: person A pipeline/operation; business-model acceptance and manual labels remain separate.\n")
        subprocess.run(["git", "tag", "-a", tag, "-m", message], cwd=ROOT, check=True)
    if push:
        subprocess.run(["git", "push", "origin", f"refs/tags/{tag}"], cwd=ROOT, check=True)
    print(json.dumps({"tag": tag, "commit": sha, "ci": ci["url"], "pushed": push}))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--tag", default="v1.0-bao-ve")
    parser.add_argument("--push", action="store_true")
    args = parser.parse_args()
    create_tag(args.tag, args.push)


if __name__ == "__main__":
    main()
