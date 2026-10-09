import pytest

from src.silver.feature_rules import (
    FEATURE_COLUMNS, build_feature_record, feature_completeness, feature_status,
    fold_text, map_model_category,
)

def test_fold_text(): assert fold_text("Sổ Hồng, Hẻm-Xe Hơi!")=="so hong hem xe hoi"

@pytest.mark.parametrize("title",["Bán nhà sổ hồng riêng Q7","BAN NHA SO DO CHINH CHU","Nhà 3 tầng SHR, giá tốt","Căn hộ pháp lý rõ ràng","Nhà hẻm SĐ cầm tay"])
def test_legal_positive(title): assert feature_status("legal",title)=="POSITIVE"

@pytest.mark.parametrize("title",["Nhà chưa có sổ, giá rẻ","Đất giấy tay Củ Chi","Bán nhà vi bằng Thủ Đức"])
def test_legal_negative(title): assert feature_status("legal",title)=="NEGATIVE"

@pytest.mark.parametrize("title",["Căn hộ 2PN view sông","Bán nhà có sơ đồ thiết kế đẹp"])
def test_legal_not_mentioned(title): assert feature_status("legal",title) is None

@pytest.mark.parametrize("title",["Căn hộ full nội thất cao cấp","can ho day du noi that","Nhà mới NTCC ở ngay","Tặng toàn bộ nội thất"])
def test_furnished_positive(title): assert feature_status("furnished",title)=="POSITIVE"

@pytest.mark.parametrize("title",["Căn hộ bàn giao thô","Căn hộ không nội thất giá rẻ","Nhà sổ hồng nội thất"])
def test_furnished_not_positive(title): assert feature_status("furnished",title)!="POSITIVE"

def test_furnished_after_so_hong_not_negated(): assert feature_status("furnished","Sổ hồng full nội thất")=="POSITIVE"

@pytest.mark.parametrize("title",["Bán nhà mặt tiền Lê Lợi","Nhà MT kinh doanh Q10","nha mat pho hoan kiem","Nhà MTKD Tân Phú"])
def test_frontage_positive(title): assert feature_status("frontage",title)=="POSITIVE"

@pytest.mark.parametrize("title",["Nhà hẻm cách mặt tiền 20m","Nhà gần mặt tiền chợ","AMTHUC quán ăn"])
def test_frontage_not_positive(title): assert feature_status("frontage",title)!="POSITIVE"

@pytest.mark.parametrize("title",["Nhà 6 tầng thang máy","nha 5 tang co thang may","Nhà 7 tầng TM Q1"])
def test_elevator_positive(title): assert feature_status("elevator",title)=="POSITIVE"

@pytest.mark.parametrize("title",["Nhà không thang máy 4 tầng","Đất TM-DV 500m2","Trang HTML 5 tầng"])
def test_elevator_not_positive(title): assert feature_status("elevator",title)!="POSITIVE"

def test_elevator_negated(): assert feature_status("elevator","Nhà không thang máy")=="NEGATIVE"

@pytest.mark.parametrize("title",["Nhà hẻm xe hơi Q3","Nhà HXH Phú Nhuận","Ô tô vào nhà, sổ hồng","Nhà ngõ ô tô Cầu Giấy"])
def test_car_access_positive(title): assert feature_status("car_access",title)=="POSITIVE"

@pytest.mark.parametrize("title",["Nhà hẻm xe máy Q8","Nhà không ô tô vào được"])
def test_car_access_not_positive(title): assert feature_status("car_access",title)!="POSITIVE"

@pytest.mark.parametrize("category,expected",[
    ("Căn hộ chung cư","can_ho"),("Officetel","can_ho"),("Nhà ở","nha_pho"),("Nhà mặt phố","nha_pho"),
    ("Biệt thự","biet_thu"),("Shophouse","biet_thu"),("Nhà liền kề","biet_thu"),("Đất","dat"),("Đất nền dự án","dat"),
    ("Bất động sản thương mại","phong_tro_khac"),("Kho, nhà xưởng","phong_tro_khac"),("Phòng trọ","phong_tro_khac"),
])
def test_model_category(category,expected): assert map_model_category(category)==(expected,"CATEGORY_NAME")

def test_model_category_title_fallback(): assert map_model_category(None,"Bán căn hộ 2PN")==("can_ho","TITLE_FALLBACK")
@pytest.mark.parametrize("title,expected",[
    ("Bán CH 52m2 2PN Phan Văn Trị","can_ho"),("Bán gấp CC 3PN Empire City","can_ho"),("Bán căn 2PN 86m2 Hà Đô","can_ho"),
    ("Mặt tiền Hai Bà Trưng Q3 170 tỷ","nha_pho"),("Căn 1 trệt 1 lầu 1 tỷ 500","nha_pho"),
    ("Duy nhất còn 1 lô gần 300m2 SHR","dat"),("Bán CHDV 43 phòng 6 tầng","phong_tro_khac"),("Bán LK xẻ khe 98m2","biet_thu"),
])
def test_model_category_title_shorthand(title,expected): assert map_model_category("Bất động sản khác",title)==(expected,"TITLE_FALLBACK")

def test_shop_house_spacing(): assert map_model_category("Shop house")==("biet_thu","CATEGORY_NAME")
def test_room_category(): assert map_model_category("Phòng/Cho thuê")==("phong_tro_khac","CATEGORY_NAME")

def test_model_category_unmapped(): assert map_model_category(None,"Cần bán gấp")==("khong_ro","UNMAPPED")

def test_completeness_bounds():
    assert feature_completeness({})==0.0
    assert feature_completeness({k:True for k in ("price_known","area_known","rooms_known","location_known","legal_known")})==1.0

def test_build_feature_record():
    row=build_feature_record({"source_id":"guland_1","source":"guland","title":"Nhà mặt tiền sổ hồng","category_name":"Nhà ở","price":3e9,"area":60.0,"rooms":None,"has_coord":False,"province_name_model":"Hồ Chí Minh","dq_status":"WARN","is_rent":False})
    assert list(row)==FEATURE_COLUMNS
    assert row["model_category"]=="nha_pho" and row["title_has_frontage"] and row["title_has_legal"]
    assert not row["rooms_known"] and row["location_known"] and row["feature_completeness_score"]==0.8
