from datetime import datetime

import pytest

from src.common.utils import (
    calculate_completeness_score, make_record_hash, make_source_id,
    normalize_coordinates, normalize_url, parse_area_m2, parse_rooms,
    parse_timestamp, parse_vietnamese_price,
)
from src.silver.build_listing_core import (
    transform_batdongsan_record, transform_guland_record, transform_nhadatvui_record,
)
from src.silver.build_location import haversine_km

@pytest.mark.parametrize("value,expected",[("3.5 tỷ",3_500_000_000.0),("850 triệu",850_000_000.0),("Thỏa thuận",None)])
def test_price(value,expected): assert parse_vietnamese_price(value)==expected

@pytest.mark.parametrize("value,expected",[("72,5 m2",72.5),("0 m2",None),(None,None)])
def test_area(value,expected): assert parse_area_m2(value)==expected

@pytest.mark.parametrize("value,expected",[("3 PN",3),("2 phòng ngủ",2),(None,None)])
def test_rooms(value,expected): assert parse_rooms(value)==expected

def test_source_id(): assert make_source_id("Guland","123")=="guland_123"
def test_coordinates(): assert normalize_coordinates(10.7,106.7)==(10.7,106.7,True)
def test_bad_coordinates(): assert normalize_coordinates(0,0)==(None,None,False)
def test_url_normalization(): assert normalize_url("HTTPS://Example.COM/a/")=="https://example.com/a"
def test_hash_ignores_technical_fields():
    a={"title":"A","price":1,"batch_id":"one"}; b={"title":"A","price":1,"batch_id":"two"}
    assert make_record_hash(a)==make_record_hash(b)
def test_completeness_range(): assert 0<=calculate_completeness_score({"title":"A"})<=1
def test_timestamp(): assert parse_timestamp("2026-10-03 12:00:00").year==2026

def test_guland_transformer():
    row=transform_guland_record({"_source":"guland","listing_id":"g1","title":"Nhà phố","price_text":"3 tỷ","area_text":"60 m2","listing_url":"https://guland.vn/g1","_scraped_at":"2026-10-03 10:00:00"})
    assert row["source_id"]=="guland_g1" and row["price"]==3_000_000_000.0

def test_batdongsan_transformer():
    row=transform_batdongsan_record({"_source":"batdongsan","listing_id":"b1","title":"Căn hộ","price_text":"2 tỷ","area_text":"50 m2","listing_url":"https://batdongsan.com.vn/b1","_scraped_at":"2026-10-03 10:00:00"})
    assert row["source_id"]=="batdongsan_b1"

def test_nhadatvui_transformer():
    row=transform_nhadatvui_record({"_source":"nhadatvui","listing_id":"n1","title":"Nhà đất","price":2_000_000_000,"area":80,"_scraped_at":"2026-10-03 10:00:00"})
    assert row["source_id"]=="nhadatvui_n1"

def test_haversine_zero(): assert haversine_km(10.0,106.0,10.0,106.0)==0.0


# --- Regression: category evidence in Silver Core (review 2026-10-09) -------
from src.silver.build_listing_core import (
    build_batdongsan_observation, compose_nhadatvui_address, map_batdongsan_category,
    map_guland_category, map_nhadatvui_category, _map_general_category,
)

def test_batdongsan_uses_taxonomy_not_endpoint():
    assert map_batdongsan_category({"category_id":"41","title":"Bán nhà 3 tầng 73m2"})==("house","Nhà ở","SOURCE_STRUCTURED")
    assert map_batdongsan_category({"category_id":"324"})[0]=="apartment"
    assert map_batdongsan_category({"listing_url":"https://batdongsan.com.vn/ban-nha-rieng-duong-x/pr1"})[0]=="house"

def test_batdongsan_unknown_category_is_not_apartment():
    assert map_batdongsan_category({"title":"Hẻm ô tô nhà đẹp 5 tầng"})==(None,None,"NONE")
    obs=build_batdongsan_observation({"listing_id":"1","title":"Hẻm ô tô nhà đẹp 5 tầng"},batch_id="20261002")
    assert obs["category_id"] is None and obs["category_evidence"]=="NONE" and "DQ07" in obs["dq_reasons"]

def test_general_category_ignores_project_names():
    assert _map_general_category("Căn hộ Đất Xanh 2PN")==("apartment","Căn hộ chung cư")
    assert _map_general_category("datxanh update")==(None,None)

def test_nhadatvui_slug_and_address():
    assert map_nhadatvui_category({"product_slug":"mua-ban-nha-rieng"})==("house","Nhà ở","SOURCE_STRUCTURED")
    assert compose_nhadatvui_address({"address":"Đại Hiệp, ","ward_name":"Xã Đại Lộc","province_name":"Thành phố Đà Nẵng"})=="Đại Hiệp, Xã Đại Lộc, Thành phố Đà Nẵng"

def test_guland_endpoint_default_is_marked():
    assert map_guland_category({"features":'["Hẻm xe hơi"]'})==("house","Nhà ở","ENDPOINT_CONTEXT")
    assert map_guland_category({"features":'["Nhà phố"]'})[2]=="SOURCE_LABEL"
