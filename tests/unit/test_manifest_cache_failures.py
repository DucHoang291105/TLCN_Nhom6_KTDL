import json
import pytest
from src.orchestration.snapshot_manifest import build_manifest


def test_missing_previously_seen_snapshot_is_not_silently_forgotten(tmp_path):
    for batch in ("20261020", "20261023"):
        path = tmp_path / f"data/incoming/snapshots/guland/{batch}/guland_raw.csv"
        path.parent.mkdir(parents=True)
        path.write_text("listing_id\n1\n")
    build_manifest(tmp_path, ["guland"], "20261023")
    old = tmp_path / "data/incoming/snapshots/guland/20261020"
    (old / "guland_raw.csv").unlink()
    old.rmdir()
    with pytest.raises(ValueError, match="snapshots missing"):
        build_manifest(tmp_path, ["guland"], "20261023")


def test_new_batch_profiled_while_old_profile_reused(tmp_path):
    def create(batch):
        path = tmp_path / f"data/incoming/snapshots/guland/{batch}/guland_raw.csv"
        path.parent.mkdir(parents=True)
        path.write_text("listing_id\n1\n")
    create("20261020")
    build_manifest(tmp_path, ["guland"], "20261020")
    create("20261023")
    result = build_manifest(tmp_path, ["guland"], "20261023")
    assert result["verification"]["csv_profiles_reused"] == result["verification"]["csv_profiles_parsed"] == 1
    assert result["verification"]["sha256_files_checked"] == 2
