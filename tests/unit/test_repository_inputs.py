"""Required Bronze inputs must survive a Git checkout, including LFS-only CI."""
import ast
import subprocess
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]


def git(*args):
    return subprocess.run(["git", *args], cwd=ROOT, capture_output=True, text=True)


def test_all_configured_historical_inputs_are_versioned_with_lfs():
    # Parse constants without importing this Spark application or its secrets.
    module = ast.parse((ROOT / "src/bronze/ingest_bronze.py").read_text(encoding="utf-8-sig"))
    sources = next(ast.literal_eval(node.value) for node in module.body if isinstance(node, ast.Assign)
                   and any(isinstance(target, ast.Name) and target.id == "SOURCES" for target in node.targets))
    assert len(sources) == 6
    for source, spec in sources.items():
        path = f"data/incoming/historical/{source}/{spec['file']}"
        assert (ROOT / path).is_file(), f"Missing required historical input: {path}"
        assert git("ls-files", "--error-unmatch", "--", path).returncode == 0, f"Unversioned Bronze input: {path}"
        assert git("check-ignore", "--no-index", "-q", "--", path).returncode == 1, f"Required input is ignored: {path}"
        attributes = git("check-attr", "filter", "text", "--", path)
        assert attributes.returncode == 0
        assert f"{path}: filter: lfs" in attributes.stdout
        assert f"{path}: text: unset" in attributes.stdout


def test_private_files_and_unconsumed_archives_stay_ignored():
    for path in (".env", "docs/reviews/local_review.md", "outputs/validation/local.json",
                 "data/incoming/historical/mogi/debug.csv",
                 "data/incoming/snapshots/guland/20261023/guland_raw.jsonl",
                 "data/incoming/snapshots/nhadatvui/20261026_120000/nhadatvui_raw.csv"):
        assert git("check-ignore", "--no-index", "-q", "--", path).returncode == 0, path
