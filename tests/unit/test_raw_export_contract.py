import csv
import json

import pytest

from src.ingestion.guland import guland_raw_crawler as guland
from src.ingestion.nhadatvui import nhadatvui_raw_crawler as nhadatvui


def test_guland_card_keeps_raw_fields_and_order(monkeypatch):
    monkeypatch.setattr(guland, "now_iso", lambda: "2026-11-20T22:00:00+07:00")
    html = """<div class="l-sdb-list__single" data-id="123">
      <div class="c-sdb-card__tle"><a href="/nha-123"> Nhà &amp; đất </a></div>
      <div class="c-sdb-card__img"><img src="/photo.jpg"></div>
      <div class="c-sdb-card__inf"><div class="sdb-inf-row">
        <span class="sdb-inf-data">2 tỷ</span><span class="sdb-inf-data">80 m²</span>
        <span class="sdb-inf-data">25 triệu/m²</span><span class="sdb-inf-data">5 m</span>
        <span class="sdb-inf-data">ignored fifth value</span>
      </div></div>
      <div class="data-type-adr"> Hồ Chí Minh </div><div class="data-type-adr"> </div>
      <div class="data-type-adr">Quận 1</div>
      <div class="c-sdb-card__spc"><a>2 phòng</a><a> </a><a>3 tầng</a></div>
      <a href="/map?lat=10.7&amp;lng=106.6">Bản đồ</a>
      <div class="profile-info__stl"><span>Hôm nay</span></div>
      <div class="profile-info__tle">Người bán</div>
      <div class="profile-info" data-phone="0901234567"></div>
      <div class="c-sdb-card__exc"> Mô tả\n nhà </div>
    </div>"""
    (record,) = guland.parse_page_html(html, 2)
    raw_html = record["raw_card_html"]
    expected = {
        "_source": "guland",
        "_source_page": 2,
        "_card_index": 1,
        "_scraped_at": "2026-11-20T22:00:00+07:00",
        "_crawl_url": f"{guland.BASE_URL}?page=2",
        "listing_id": "123",
        "title": "Nhà & đất",
        "listing_url": "https://guland.vn/nha-123",
        "image_url": "https://guland.vn/photo.jpg",
        "price_text": "2 tỷ",
        "area_text": "80 m²",
        "price_per_m2_text": "25 triệu/m²",
        "frontage_price_text": "5 m",
        "location_rows": ["Hồ Chí Minh", "Quận 1"],
        "lat": "10.7",
        "lon": "106.6",
        "posted_text": "Hôm nay",
        "features": ["2 phòng", "3 tầng"],
        "description": "Mô tả nhà",
        "seller_name": "Người bán",
        "seller_phone_raw": "0901234567",
        "raw_card_html": raw_html,
    }
    assert record == expected and list(record) == list(expected)
    assert "ignored fifth value" in raw_html


@pytest.mark.parametrize("html", ["", '<div class="l-sdb-list__single"></div>'])
def test_guland_missing_fields_stay_empty(html):
    records = guland.parse_page_html(html, 1)
    if not html:
        assert records == []
    else:
        (record,) = records
        assert record["features"] == record["location_rows"] == []
        assert all(
            record[key] is None for key in ("listing_id", "price_text", "area_text", "lat", "lon")
        )


@pytest.mark.parametrize("crawler", [guland, nhadatvui], ids=["guland", "nhadatvui"])
def test_raw_exports_append_duplicate_records_without_reordering(tmp_path, monkeypatch, crawler):
    monkeypatch.setattr(crawler, "JSONL_PATH", tmp_path / "raw.jsonl")
    monkeypatch.setattr(crawler, "CSV_PATH", tmp_path / "raw.csv")
    if crawler is guland:
        record = {"listing_id": "1", "title": "Nhà, đất\nViệt Nam", "features": ["2 phòng", None]}
        fields = ["listing_id", "title", "features"]
    else:
        record = {
            "_id": "1",
            "title": "Nhà, đất\nViệt Nam",
            "area": 0,
            "isVerified": False,
            "productId": {"name": "Nhà"},
            "attachments": [None, {"type": "image", "url": "/1.jpg"}],
            "details": {"Đuong_vao_(m)": "", "Duong_vao_(m)": 0},
            "content": "mô tả nguyên bản",
        }
        fields = [
            "_source",
            "_source_page",
            "_scraped_at",
            "_crawl_url",
            "listing_id",
            "title",
            "product_name",
            "product_slug",
            "unit",
            "area",
            "price",
            "min_price",
            "max_price",
            "lat",
            "lng",
            "province_code",
            "province_name",
            "ward_code",
            "ward_name",
            "street_code",
            "address",
            "is_verified",
            "is_sponsored",
            "is_negotiable",
            "status",
            "created_at_ms",
            "public_date_ms",
            "expired_date_ms",
            "rooms",
            "toilets",
            "floors",
            "frontage_m",
            "length_m",
            "road_width_m",
            "direction",
            "legal_document",
            "property_subtype",
            "image_count",
            "first_image_url",
            "has_content",
        ]
    crawler.append_jsonl([record])
    assert crawler.append_csv([record], False)
    first_jsonl, first_csv = crawler.JSONL_PATH.read_bytes(), crawler.CSV_PATH.read_bytes()
    crawler.append_jsonl([])
    assert crawler.append_csv([], True)
    assert crawler.JSONL_PATH.read_bytes() == first_jsonl
    assert crawler.CSV_PATH.read_bytes() == first_csv
    crawler.append_jsonl([record])
    assert crawler.append_csv([record], True)
    assert crawler.JSONL_PATH.read_bytes() == first_jsonl * 2
    assert first_jsonl.decode("utf-8").strip() == json.dumps(record, ensure_ascii=False)
    assert first_csv.startswith(b"\xef\xbb\xbf")
    with crawler.CSV_PATH.open(encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        rows = list(reader)
        assert reader.fieldnames == fields
    assert len(rows) == 2 and rows[0] == rows[1]
    assert rows[0]["title"] == "Nhà, đất\nViệt Nam"
    if crawler is nhadatvui:
        assert rows[0]["area"] == rows[0]["road_width_m"] == "0"
        assert rows[0]["is_verified"] == "False"
        assert rows[0]["image_count"] == "2" and rows[0]["first_image_url"] == "/1.jpg"
