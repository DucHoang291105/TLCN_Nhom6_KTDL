import csv
import json
from datetime import date

import pytest

from src.silver import build_listing_core as core


@pytest.mark.parametrize("kind", ["csv", "json"])
@pytest.mark.parametrize("failure", ["serialization", "replace"])
def test_failed_silver_export_preserves_previous_bytes(tmp_path, monkeypatch, kind, failure):
    target = tmp_path / f"silver.{kind}"
    target.write_bytes(b"previous published result")

    class InvalidValue:
        def __str__(self):
            raise ValueError("cannot serialize")

    def fail_replace(*args):
        raise PermissionError("destination busy")

    if failure == "replace":
        monkeypatch.setattr(core.os, "replace", fail_replace)
    value = InvalidValue() if failure == "serialization" else "Nhà Việt Nam"
    with pytest.raises((ValueError, PermissionError)):
        if kind == "csv":
            core._write_csv_atomic(target, ["title"], [{"title": value}])
        else:
            core._write_json_atomic(target, {"title": value})
    assert target.read_bytes() == b"previous published result"
    assert sorted(p.name for p in tmp_path.iterdir()) == [target.name]


def test_silver_exports_keep_encoding_order_and_serialization(tmp_path):
    row = {"title": "Nhà, đất\nViệt Nam", "date": date(2026, 11, 20), "notes": [None, "2 phòng"]}
    fields = ["title", "date", "notes"]
    target = tmp_path / "silver.csv"
    core._write_csv_atomic(target, fields, [row])
    assert target.read_bytes().startswith(b"\xef\xbb\xbf")
    with target.open(encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        rows = list(reader)
        assert reader.fieldnames == fields
    assert rows == [{"title": row["title"], "date": "2026-11-20", "notes": '[null, "2 phòng"]'}]
    output = tmp_path / "summary.json"
    core._write_json_atomic(output, row)
    assert json.loads(output.read_text("utf-8")) == {**row, "date": "2026-11-20"}
    assert not list(tmp_path.glob("*.tmp"))


def test_local_silver_keeps_dedup_ranking_latest_record_and_quarantine(tmp_path):
    incoming = tmp_path / "incoming"
    fields = [
        "listing_id",
        "title",
        "price_text",
        "area_text",
        "_scraped_at",
        "_source_page",
        "category_id",
    ]
    base = {
        "listing_id": "1",
        "title": "Căn hộ 80 m²",
        "price_text": "2 tỷ",
        "area_text": "80 m²",
        "category_id": "324",
    }
    batches = {
        "20261117": [
            {**base, "_source_page": page, "_scraped_at": "2026-11-17T22:00:00+07:00"}
            for page in (2, 1, 3)
        ]
        + [{**base, "listing_id": "reject", "title": ""}],
        "20261120": [
            {
                **base,
                "price_text": "3 tỷ",
                "_source_page": 1,
                "_scraped_at": "2026-11-20T22:00:00+07:00",
            }
        ],
    }
    for batch, records in batches.items():
        folder = incoming / "batdongsan" / batch
        folder.mkdir(parents=True)
        with (folder / "batdongsan_raw.csv").open("w", encoding="utf-8", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=fields)
            writer.writeheader()
            writer.writerows(records)
    summary = core.build_local_silver(incoming, tmp_path / "silver")
    assert summary["input_rows"] == 5 and summary["duplicates_dropped"] == 2
    assert summary["observation_rows"] == 2 and summary["current_rows"] == 1
    assert summary["quarantine_rows"] == 1
    with open(
        summary["outputs"]["listing_observation"], encoding="utf-8-sig", newline=""
    ) as handle:
        observations = list(csv.DictReader(handle))
    assert [row["page_fetched"] for row in observations] == ["1", "1"]
    with open(
        summary["outputs"]["listings_current_27"], encoding="utf-8-sig", newline=""
    ) as handle:
        (current,) = csv.DictReader(handle)
    assert current["source_id"] == "batdongsan_1" and float(current["price"]) == 3_000_000_000
