import pytest

from src.silver.build_location import build_location_record, resolve_province

HCM = (10.80, 106.71)

@pytest.mark.parametrize("address,district,title,expected,method",[
    # Street / ward / person names equal to (former) province names.
    ("Đường Điện Biên Phủ, Quận Bình Thạnh, TP.HCM",None,None,"Hồ Chí Minh",None),
    ("Khuông Việt, Phường Phú Thọ Hòa, Quận Tân Phú, TPHCM",None,None,"Hồ Chí Minh",None),
    ("Xa Lộ Hà Nội, Phường An Phú, Quận 2, TPHCM",None,None,"Hồ Chí Minh",None),
    ("Hồ Văn Huê, Phường 9, Quận Phú Nhuận",None,None,"Hồ Chí Minh",None),
    ("Hòa Bình, Phường Hiệp Tân, Quận Tân Phú",None,None,"Hồ Chí Minh",None),
    # A structured district outranks a province-like word in the title.
    (None,"Quận Tân Phú","Bán nhà Phú Thọ Hòa","Hồ Chí Minh","HCMC_DISTRICT"),
    (None,"Huyện Bình Chánh","Nhà Vĩnh Lộc A","Hồ Chí Minh","HCMC_DISTRICT"),
    # Structured province appended by Core (NhaDatVui) is an address component.
    ("454, Phường Long Trường, Thành phố Hồ Chí Minh",None,"Bán nhà Vinh","Hồ Chí Minh","ADDRESS_COMPONENT"),
    ("Bình Dương (Hồ Chí Minh mới)",None,None,"Hồ Chí Minh","ADDRESS_COMPONENT"),
])
def test_province_priority(address,district,title,expected,method):
    _, row, used = resolve_province(address,district,None,title,None,*HCM)
    assert row["province_name"]==expected
    if method: assert used==method

def test_accents_distinguish_vinh_and_vinh_loc():
    assert resolve_province(None,None,None,"Bán nhà Vĩnh Lộc")[1] is None
    assert resolve_province(None,None,None,"Bán nhà TP Vinh")[1]["province_name"]=="Nghệ An"

def test_free_text_must_agree_with_coordinates():
    # "Hà Nội" in a title of a listing whose coordinates are in HCMC is not used.
    assert resolve_province(None,None,None,"Bán nhà Hà Nội",None,*HCM)[1] is None
    assert resolve_province(None,None,None,"Bán nhà Hà Nội")[1]["province_name"]=="Hà Nội"

def test_conflict_keeps_flag_and_drops_distance():
    rec=build_location_record({"source_id":"x_1","address":"Phường Trúc Bạch, Thành phố Hà Nội","lat":HCM[0],"lon":HCM[1]})
    assert rec["province_name_model"]=="Hà Nội"
    assert "LQ05_PROVINCE_COORDINATE_CONFLICT" in rec["location_dq_reasons"]
    assert rec["distance_to_center_km"] is None

def test_reported_case_distance_is_local():
    rec=build_location_record({"source_id":"x_1","address":"Đường Điện Biên Phủ, Quận Bình Thạnh, TP.HCM","district_name":"Quận Bình Thạnh","lat":10.80,"lon":106.71})
    assert rec["province_name_model"]=="Hồ Chí Minh" and rec["distance_to_center_km"]<10
    assert rec["location_dq_reasons"]==[]
