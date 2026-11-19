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
def test_model_category(category,expected): assert map_model_category(category)[:2]==(expected,"CATEGORY_NAME")

def test_model_category_title_fallback(): assert map_model_category(None,"Bán căn hộ 2PN")[:2]==("can_ho","TITLE_FALLBACK")
@pytest.mark.parametrize("title,expected",[
    ("Bán CH 52m2 2PN Phan Văn Trị","can_ho"),("Bán gấp CC 3PN Empire City","can_ho"),("Bán căn 2PN 86m2 Hà Đô","can_ho"),
    ("Mặt tiền Hai Bà Trưng Q3 170 tỷ","nha_pho"),("Căn 1 trệt 1 lầu 1 tỷ 500","nha_pho"),
    ("Duy nhất còn 1 lô gần 300m2 SHR","dat"),("Bán CHDV 43 phòng 6 tầng","phong_tro_khac"),("Bán LK xẻ khe 98m2","biet_thu"),
])
def test_model_category_title_shorthand(title,expected): assert map_model_category("Bất động sản khác",title)[:2]==(expected,"TITLE_FALLBACK")

def test_shop_house_spacing(): assert map_model_category("Shop house")[:2]==("biet_thu","CATEGORY_NAME")
def test_room_category(): assert map_model_category("Phòng/Cho thuê")[:2]==("phong_tro_khac","CATEGORY_NAME")

def test_model_category_unmapped(): assert map_model_category(None,"Cần bán gấp")[:2]==("khong_ro","UNMAPPED")

def test_completeness_bounds():
    assert feature_completeness({})==0.0
    assert feature_completeness({k:True for k in ("price_known","area_known","rooms_known","location_known","legal_known")})==1.0

def test_build_feature_record():
    row=build_feature_record({"source_id":"guland_1","source":"guland","title":"Nhà mặt tiền sổ hồng","category_name":"Nhà ở","price":3e9,"area":60.0,"rooms":None,"has_coord":False,"province_name_model":"Hồ Chí Minh","dq_status":"WARN","is_rent":False})
    assert list(row)==FEATURE_COLUMNS
    assert row["model_category"]=="nha_pho" and row["title_has_frontage"] and row["title_has_legal"]
    assert not row["rooms_known"] and row["location_known"] and row["feature_completeness_score"]==0.8

# --- Regression: review 2026-10-09 ------------------------------------------
from src.silver.feature_rules import category_from_title

@pytest.mark.parametrize("title,expected",[
    ("Hẻm ô tô Nguyễn Đình Chính, Phú Nhuận- nhà đẹp xây mới 5 tầng, thang máy 43,3m2","nha_pho"),
    ("Bán nhà 3 tầng 73m2. 78/60/7 Nguyễn Văn Khối, Gò Vấp","nha_pho"),
    ("Căn hộ Đất Xanh 2PN","can_ho"),            # project name must not mean land
    ("Bán nhà chính chủ CC, hẻm 4m, 3 tầng","nha_pho"),  # CC = chính chủ here
    ("Nhà phân lô 4 tầng 50m2","nha_pho"),       # "lô" alone is not land
    ("Bán lô đất 100m2 thổ cư","dat"),
])
def test_title_category_regressions(title,expected):
    assert map_model_category("Bất động sản khác",title)[:2]==(expected,"TITLE_FALLBACK")

def test_weak_signals_need_agreement():
    # "CC" (apartment?) and "lô" (land?) disagree: not enough evidence.
    assert category_from_title("Bán CC lô góc")==(None,None)
    assert map_model_category(None,"Bán CC lô góc")==("khong_ro","UNMAPPED",False)

def test_structured_label_yields_to_strong_title_of_other_family():
    # v5: the labelled sample showed the title right in most such conflicts.
    assert map_model_category("Căn hộ chung cư","Bán nhà 3 tầng 73m2 Gò Vấp","SOURCE_STRUCTURED")==("nha_pho","TITLE_OVER_LABEL",True)

def test_structured_label_kept_within_same_family():
    assert map_model_category("Biệt thự","Bán nhà 3 tầng khu biệt thự cũ","SOURCE_STRUCTURED")[:2]==("biet_thu","CATEGORY_NAME")

def test_endpoint_default_yields_to_strong_title():
    assert map_model_category("Nhà ở","Bán căn hộ Moonlight 2PN","ENDPOINT_CONTEXT")==("can_ho","TITLE_OVER_ENDPOINT",True)
    assert map_model_category("Nhà ở","Nhà 4 tầng hẻm xe hơi","ENDPOINT_CONTEXT")==("nha_pho","ENDPOINT_CONTEXT",False)

def test_weak_title_signal_never_overrides_label():
    assert map_model_category("Nhà ở","Bán CH Moonlight Park View","ENDPOINT_CONTEXT")==("nha_pho","ENDPOINT_CONTEXT",False)

@pytest.mark.parametrize("title",["Bán đất MT 5m dài 20m trong hẻm","Nhà MT 4,5m Q10","Nhà mặt tiền hẻm xe hơi","Nhà mặt tiền ngõ 3m"])
def test_frontage_width_or_alley_is_not_street_frontage(title): assert feature_status("frontage",title) is None

@pytest.mark.parametrize("title",["Nhà mặt tiền Lê Lợi 4x20m","Bán nhà 2 mặt tiền đường","Nhà mặt tiền 5m đường Nguyễn Trãi"])
def test_street_frontage_positive(title):
    # "mặt tiền 5m đường …" states width first, so it is conservatively not counted.
    expected=None if "5m" in title else "POSITIVE"
    assert feature_status("frontage",title)==expected

def test_negated_features_listed():
    row=build_feature_record({"source_id":"x_1","title":"Nhà không thang máy, chưa có sổ","category_name":"Nhà ở"})
    assert set(row["title_negated_features"])=={"elevator","legal"}
    assert row["title_has_elevator"] is False

@pytest.mark.parametrize("category,title",[
    ("Biệt thự","Bán nhà liền kề 4 tầng KĐT Văn Phú"),       # villa family title "nhà"
    ("Nhà ở","Nhà mặt tiền cho thuê làm văn phòng 5 tầng"),  # house used as office
    ("Shophouse","Bán nhà phố thương mại 5 tầng"),
])
def test_same_family_is_not_conflict(category,title):
    assert map_model_category(category,title,"SOURCE_STRUCTURED")[2] is False

def test_endpoint_default_kept_for_same_family():
    assert map_model_category("Nhà ở","Cho thuê mặt bằng kinh doanh 2 tầng","ENDPOINT_CONTEXT")==("nha_pho","ENDPOINT_CONTEXT",False)

def test_land_label_with_house_title_is_conflict():
    assert map_model_category("Đất","Bán nhà 3 tầng hẻm xe hơi","SOURCE_STRUCTURED")==("nha_pho","TITLE_OVER_LABEL",True)


@pytest.mark.parametrize("title,expected", [
    ("BÁN NHÀ 6 TẦNG – ĐƯỜNG 10M CÓ VỈA HÈ – GẦN CHUNG CƯ SCREC TRƯỜNG SA – 15 TỶ", "nha_pho"),
    ("ĐƯỜNG 10M CÓ VỈA HÈ – NGAY CHUNG CƯ SCREC TRƯỜNG SA – 6 TẦNG", "nha_pho"),
    ("Nhà phố đối diện căn hộ Masteri", "nha_pho"),
    ("Bán căn hộ chung cư 2PN ngay trung tâm", "can_ho"),   # proximity word after the type
])
def test_landmark_after_proximity_word_is_not_the_type(title, expected):
    assert category_from_title(title)[0] == expected


def test_endpoint_house_not_overridden_by_nearby_apartment():
    title = "BÁN NHÀ 6 TẦNG – GẦN CHUNG CƯ SCREC TRƯỜNG SA"
    assert map_model_category("Nhà ở", title, "ENDPOINT_CONTEXT") == ("nha_pho", "ENDPOINT_CONTEXT", False)


# --- v5: cases from the dev split of the labelled sample -----------------------
@pytest.mark.parametrize("title", [
    "Lô đất bán 6tỷ2 CÒN THƯƠNG LƯỢNG + THỔ CƯ TOÀN PHẦN",
    "Nhà phố LK 126m2 gần trường học sẵn sổ giá bán nhanh",
    "Masteri giá siêu tốt - bao pháp lý, cho vay 70%",
    "NHÀ PHỐ 2 TẦNG HOÀN CÔNG ĐỦ – HXH LÃ XUÂN OAI",
])
def test_v5_legal_mentions(title):
    assert feature_status("legal", title) == "POSITIVE"


def test_v5_land_status_negated():
    assert feature_status("legal", "Đất chưa thổ cư giá rẻ") == "NEGATIVE"


def test_v5_furnished_with_intensifier():
    assert feature_status("furnished", "penthouse 700m², nội thất siêu đẹp giá 36 tỷ") == "POSITIVE"


@pytest.mark.parametrize("title,expected", [
    ("Bán nhà khu Tên Lửa, góc 2MT đường Số 11A, DT 6 x 24.5m", "POSITIVE"),
    ("Bán cặp nhà khu Tên Lửa gần Aeon, MT đường Số 13B, DT 10 x 20m", "POSITIVE"),   # "gần" belongs to Aeon
    ("BÁN 232M2 ĐẤT MẶT TIỀN BỜ SÔNG SÀI GÒN", None),
    ("60m2 có dòng tiền cho thuê xưởng, đường oto thông mặt phố", None),
    ("Nhà hẻm 5m cách mặt tiền 20m", None),
])
def test_v5_frontage(title, expected):
    assert feature_status("frontage", title) == expected


@pytest.mark.parametrize("title,expected", [
    ("Vũ Tông Phan, Thanh Xuân - Oto - Thang Máy - 55m, 7 Tầng", "POSITIVE"),
    ("7 tầng - thang máy - gara ô tô - kinh doanh", "POSITIVE"),
    ("đất hẻm xa tải rẻ nhất quận gò vấp", "POSITIVE"),
    ("33,6m² x 4 tầng, Ngõ nông, Gần ôtô – Ở ngay", None),
    ("Nhà ngõ 2m không ô tô", "NEGATIVE"),
])
def test_v5_car_access(title, expected):
    assert feature_status("car_access", title) == expected


@pytest.mark.parametrize("label,title,expected", [
    ("Căn hộ chung cư", "Toà Nhà 32 CHDV Quận 1, 6 lầu, HĐ thuê 150tr", ("phong_tro_khac", "TITLE_SERVICE_BUILDING")),
    ("Nhà ở", "Bán tòa CCMN Nguyễn Thị Định 137m2, lô góc", ("phong_tro_khac", "TITLE_SERVICE_BUILDING")),
    ("Căn hộ", "Bán đất ngay chung cư C3 man thiện", ("dat", "TITLE_OVER_LABEL")),
    ("Đất", "Nhượng đất tổ 10 Lê Văn Lương, xã Phước Kiển, Huyện Nhà Bè", ("dat", "CATEGORY_NAME")),
    ("Đất", "Đất nền ngay nút xuống cao tốc Thủ Dầu Một", ("dat", "CATEGORY_NAME")),
])
def test_v5_category(label, title, expected):
    assert map_model_category(label, title, "SOURCE_STRUCTURED")[:2] == expected


def test_v5_avenue_is_not_a_lot():
    assert map_model_category("Bất động sản khác", 'The Sun Avenue – "Đại lộ Mặt Trời" Mai Chí Thọ')[0] == "khong_ro"


def test_v5_real_workshop_still_detected():
    assert map_model_category(None, "Bán kho xưởng 1000m2 Bình Tân")[0] == "phong_tro_khac"
