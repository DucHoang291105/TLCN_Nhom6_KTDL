import pytest
from src.orchestration.snapshot_manifest import build_manifest, confirm_inventory


def snapshot(root, batch, value="1"):
    path = root / "data/incoming/snapshots/guland" / batch / "guland_raw.csv"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("listing_id\n" + value + "\n", encoding="utf-8")
    return path


def test_complete_pinned_snapshot_set_is_accepted(tmp_path):
    snapshot(tmp_path, "20261027")
    manifest = build_manifest(tmp_path, ["guland"], "20261027")
    assert confirm_inventory(tmp_path, ["guland"], manifest) == 1


@pytest.mark.parametrize("change", ["added", "removed", "modified", "duplicate"])
def test_changed_inputs_cannot_start_bronze_writes(tmp_path, change):
    path = snapshot(tmp_path, "20261027")
    manifest = build_manifest(tmp_path, ["guland"], "20261027")
    if change == "added": snapshot(tmp_path, "20261030")
    elif change == "removed": path.unlink()
    elif change == "modified": path.write_text("listing_id\n2\n", encoding="utf-8")
    else: manifest["snapshots"].append(manifest["snapshots"][0])
    with pytest.raises((RuntimeError, FileNotFoundError)):
        confirm_inventory(tmp_path, ["guland"], manifest)
