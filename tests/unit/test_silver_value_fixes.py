"""Regression tests for Silver value and district fixes (dashboard review 2026-10-24)."""

import pytest

from src.common.utils import apply_value_plausibility, area_from_title, parse_vietnamese_price
from src.silver.build_location import infer_district_model, normalize_district_field

HCM = "Hồ Chí Minh"


def fix(price_str, area, title="x", is_rent=False):
    return apply_value_plausibility(
        {"price": parse_vietnamese_price(price_str), "price_str": price_str, "area": area, "title": title, "is_rent": is_rent}
    )


# --- "tỷ" figures >= 1,000 seen in the crawls --------------------------------
@pytest.mark.parametrize("price_str,area,expected", [
    ("9.630 tỷ", 107.0, 9.63e9),          # dot used as decimal separator
    ("2.650 tỷ", 66.2, 2.65e9),
    ("5800 tỷ", 84.0, 5.8e9),             # millions labelled tỷ
    ("105000 tỷ", 426.2, 105e9),
    ("3150000000 tỷ", 150.0, 3.15e9),     # VND labelled tỷ
    ("2,424 tỷ", 1347.0, 2.424e9),        # comma is the Vietnamese decimal separator
    ("1.200 tỷ", 600000.0, 1200e9),       # 60 ha: the decimal reading is impossible, literal kept
])
def test_billion_label_rescaled_by_plausible_price_per_m2(price_str, area, expected):
    record, reasons = fix(price_str, area)
    assert record["price"] == pytest.approx(expected)
    assert "DQ14" in reasons
    assert record["price_per_m2"] == pytest.approx(expected / area)


def test_ordinary_prices_untouched():
    record, reasons = fix("12,5 tỷ", 80.0)
    assert record["price"] == pytest.approx(12.5e9) and reasons == []


# --- impossible prices --------------------------------------------------------
@pytest.mark.parametrize("price_str,area", [
    ("7.4 triệu", 54.0),     # title says 7,4 tỷ: unit wrong at the source
    ("7 nghìn", 36.0),
    ("850 tỷ", 120.0),       # 7 tỷ/m², no plausible reading
])
def test_impossible_sale_price_becomes_null(price_str, area):
    record, reasons = fix(price_str, area)
    assert record["price"] is None and record["price_per_m2"] is None
    assert "DQ16" in reasons
    assert record["price_str"] == price_str   # raw text kept for audit


def test_rent_price_not_checked_against_sale_bounds():
    record, reasons = fix("27 triệu", 213.0, is_rent=True)
    assert record["price"] == pytest.approx(27e6) and reasons == []


# --- impossible areas -----------------------------------------------------------
@pytest.mark.parametrize("title,expected", [
    ("CHÍNH CHỦ CẦN BÁN LÔ ĐẤT NÔNG NGHIỆP 1011,8m2 TẠI CHÂU THÀNH", 1011.8),
    ("Quỹ đất mặt tiền Nguyễn Thị Đành - Hóc Môn | 5.460,5 m2", 5460.5),
    ("NHÀ MỚI ĐẸP – 162M² – 4PN – THÁI HÒA", 162.0),
    ("4x20m Lề 3m Đường 10m TÂN HOÀ ĐÔNG", 80.0),
    ("Chỉ 2 tỷ xíu nhà mới rất to 4,1mx13m tân chánh hiệp", 53.3),
])
def test_area_from_title(title, expected):
    assert area_from_title(title) == pytest.approx(expected)


def test_tiny_area_replaced_from_title():
    record, reasons = fix("288 triệu", 1.01, "LÔ ĐẤT NÔNG NGHIỆP 1011,8m2")
    assert record["area"] == pytest.approx(1011.8) and reasons == ["DQ15"]


def test_tiny_area_without_title_area_becomes_null():
    record, reasons = fix("270 tỷ", 1.09, "BÁN KHU RESORT CỬA NGÕ THÀNH PHỐ")
    assert record["area"] is None and record["price_per_m2"] is None
    assert "DQ16" in reasons


# --- district names --------------------------------------------------------------
@pytest.mark.parametrize("raw,expected", [
    ("Bình Tân", "Quận Bình Tân"),            # luachonnhadat: no prefix
    ("Tân Phú", "Quận Tân Phú"),
    ("Bình Chánh", "Huyện Bình Chánh"),
    ("Bình Thạnh cũ)", "Quận Bình Thạnh"),     # fragment with "cũ"
    ("Phường Gò Vấp", "Quận Gò Vấp"),          # 2025 ward named after the old district
    ("Phường Thủ Đức", "Thành phố Thủ Đức"),
])
def test_bare_hcmc_district_names(raw, expected):
    assert normalize_district_field(raw, HCM)[0] == expected


@pytest.mark.parametrize("raw", ["Unknown", "Không xác định"])
def test_placeholders_are_not_districts(raw):
    assert normalize_district_field(raw, HCM) == (None, "CORE_PLACEHOLDER_DROPPED")
    assert normalize_district_field(raw, "Hà Nội")[0] is None


def test_text_fragment_dropped_then_inferred_from_address():
    fragment = "Tân Bình) phường Tân Hoà. TPHCM. - Diện tích sử dụng: 109 m2 - 2 tầng"
    assert normalize_district_field(fragment, HCM) == (None, "CORE_FRAGMENT_DROPPED")
    assert infer_district_model(fragment, None, "Bạch Đằng, Quận Tân Bình, TPHCM", None, None, HCM) == ("Quận Tân Bình", "TEXT_DISTRICT_RULE")


def test_other_provinces_keep_their_names():
    assert normalize_district_field("Quận Cầu Giấy", "Hà Nội") == ("Quận Cầu Giấy", "CORE_PRESERVED")
    assert normalize_district_field("Thị xã Thuận An", HCM) == ("Thị xã Thuận An", "CORE_PRESERVED")


def test_area_thousands_separator_repaired_from_title():
    record, reasons = fix("11 tỷ", 11.0, "Đất 2 mặt hẻm thôn hạ, đức trọng ( 11.000m2 )")
    assert record["area"] == pytest.approx(11000.0) and "DQ15" in reasons


def test_area_matching_title_untouched():
    record, reasons = fix("5 tỷ", 80.0, "Nhà 80m2 hẻm xe hơi")
    assert record["area"] == 80.0 and reasons == []


# --- price written inside a title (muaban puts the title in price_str) ---------
@pytest.mark.parametrize("text,expected", [
    ("Chính chủ bán gấp nhà Gò Vấp 52m2 giá 2tỷ360triệu đường Lê Đức Thọ.", 2.36e9),
    ("Bán gấp nhà DT 52,1m2 giá 2tỷ550 đường Lê Đức Thọ", 2.55e9),
    ("Chủ về quê cần bán gấp nhà 52m2 đường Lê Đức Thọ giá 2 TỶ 290 hẻm oto", 2.29e9),
    ("Tầng Cao CH Góc 53m2/2 tỷ 12 ,sổ bao Vat", 2.12e9),
    ("Bán nhà mặt tiền 68m2, 15,5 tỷ 4 lầu", 15.5e9),     # "4 lầu" is not part of the price
    ("7 tỷ 500 triệu", 7.5e9),
])
def test_price_is_the_number_before_the_unit(text, expected):
    assert parse_vietnamese_price(text) == pytest.approx(expected)


def test_area_in_title_does_not_trigger_billion_rescale():
    record, reasons = fix("Bán đất 1500m2 giá 5 tỷ", 1500.0)
    assert record["price"] == pytest.approx(5e9) and reasons == []


# --- historical exports: shipped price vs price_str -----------------------------
from src.common.utils import reconcile_shipped_price


def test_old_parser_defect_repaired_from_title():
    title = "Chủ về quê cần bán gấp nhà 52m2 đường Lê Đức Thọ giá 2 TỶ 290 hẻm oto"
    assert reconcile_shipped_price(52e9, title, 52.0) == pytest.approx(2.29e9)


def test_per_m2_price_str_keeps_shipped_total():
    assert reconcile_shipped_price(28.8e9, "180 triệu / m²", 160.0) == 28.8e9


def test_rent_per_month_keeps_shipped_value():
    assert reconcile_shipped_price(33_399_000, "33,399 Triệu/tháng", 50.0) == 33_399_000


def test_title_price_str_without_defect_keeps_shipped():
    assert reconcile_shipped_price(10e9, "Bán nhà 72m2 Quang Trung giá 5,68 tỷ hoặc 10 tỷ cả căn", 72.0) == 10e9


@pytest.mark.parametrize("text,expected", [
    ("11 triệu 500 nghìn", 11.5e6),
    ("2tr700", 2.7e6),
    ("33,399 triệu", 33.399e6),   # comma is a decimal separator
])
def test_million_forms(text, expected):
    assert parse_vietnamese_price(text) == pytest.approx(expected)
