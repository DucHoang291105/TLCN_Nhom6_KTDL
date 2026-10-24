"""Pure-Python normalization helpers shared by Silver jobs.

The functions in this module do not read or write Bronze/Silver storage. They
only convert individual raw values into the canonical representation defined
in ``docs/data/canonical_listing_schema.md``.
"""

from __future__ import annotations

import hashlib
import html
import json
import math
import re
import unicodedata
from datetime import datetime, timedelta, timezone
from typing import Any, Mapping, Sequence
from urllib.parse import urlsplit, urlunsplit


VIETNAM_TIMEZONE = timezone(timedelta(hours=7))

CANONICAL_COLUMNS = [
    "source",
    "source_group",
    "source_id",
    "ad_id",
    "title",
    "price",
    "price_str",
    "area",
    "rooms",
    "address",
    "ward",
    "district_id",
    "district_name",
    "category_id",
    "category_name",
    "lat",
    "lon",
    "image",
    "ad_url",
    "source_url",
    "posted_at",
    "scraped_at",
    "page_fetched",
    "price_m",
    "price_per_m2",
    "has_coord",
    "is_rent",
]

RECORD_HASH_FIELDS = [
    "title",
    "price",
    "area",
    "rooms",
    "address",
    "ward",
    "district_name",
    "category_id",
    "category_name",
    "lat",
    "lon",
    "is_rent",
]

DEFAULT_COMPLETENESS_FIELDS = [
    "title",
    "price",
    "area",
    "rooms",
    "address",
    "ward",
    "district_name",
    "category_name",
    "lat",
    "lon",
    "image",
    "ad_url",
    "posted_at",
]


def clean_text(value: Any) -> str | None:
    """Normalize Unicode and whitespace without changing business meaning."""

    if value is None:
        return None

    text = html.unescape(str(value))
    text = unicodedata.normalize("NFC", text)
    text = "".join(
        character
        for character in text
        if not unicodedata.category(character).startswith("C")
        or character in "\t\r\n"
    )
    text = re.sub(r"[\t\r\n]+", " ", text)
    text = re.sub(r"\s+", " ", text).strip()

    if not text or text.lower() in {"null", "none", "nan", "na"}:
        return None
    return text


def _parse_localized_number(value: Any) -> float | None:
    """Parse a number written with Vietnamese comma/dot separators."""

    if value is None or isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        number = float(value)
        return number if math.isfinite(number) else None

    text = clean_text(value)
    if text is None:
        return None

    match = re.search(r"-?\d[\d.,\s]*", text)
    if not match:
        return None

    number = re.sub(r"\s+", "", match.group())
    if "," in number and "." in number:
        if number.rfind(",") > number.rfind("."):
            number = number.replace(".", "").replace(",", ".")
        else:
            number = number.replace(",", "")
    elif "," in number:
        parts = number.split(",")
        number = ".".join(parts) if len(parts) == 2 and len(parts[1]) <= 2 else "".join(parts)
    elif "." in number:
        parts = number.split(".")
        number = ".".join(parts) if len(parts) == 2 and len(parts[1]) <= 2 else "".join(parts)

    try:
        parsed = float(number)
    except ValueError:
        return None
    return parsed if math.isfinite(parsed) else None


def parse_vietnamese_price(value: Any) -> float | None:
    """Convert a source price to VND; agreement/invalid prices become NULL."""

    if value is None:
        return None
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        price = float(value)
        return price if math.isfinite(price) and price > 0 else None

    text = clean_text(value)
    if text is None:
        return None

    lowered = text.lower()
    agreement_terms = (
        "thỏa thuận",
        "thoả thuận",
        "thoa thuan",
        "liên hệ",
        "lien he",
        "contact",
    )
    if any(term in lowered for term in agreement_terms):
        return None

    # Read the number written *before* the unit. Some sources put the whole
    # title in the price field ("Bán nhà 52m2 giá 2tỷ360triệu"), where the
    # first number in the text is the area, not the price.
    billion = _BILLION_PRICE.search(lowered)
    if billion:
        whole = _parse_localized_number(billion.group(1))
        if whole is None or whole <= 0:
            return None
        if whole >= 1_000:
            return _rescale_billion_label(billion.group(1), whole)
        rest = lowered[billion.end():]
        millions = _MILLION_AFTER_BILLION.match(rest)
        if millions:
            extra = _parse_localized_number(millions.group(1)) or 0.0
            return whole * 1_000_000_000 + extra * 1_000_000
        tail = _BILLION_TAIL.match(rest)
        if tail and float(whole).is_integer():
            # "2 tỷ 290", "2tỷ550", "2 tỷ 12": the digits continue the figure.
            digits = tail.group(1)
            return (whole + int(digits) / 10 ** len(digits)) * 1_000_000_000
        return whole * 1_000_000_000

    million = _MILLION_PRICE.search(lowered)
    if million:
        whole = _single_separator_decimal(million.group(1)) if "," in million.group(1) else None
        whole = whole if whole is not None else _parse_localized_number(million.group(1))
        if whole is None or whole <= 0:
            return None
        rest = lowered[million.end():]
        thousands = _THOUSAND_AFTER_MILLION.match(rest)
        if thousands:  # "11 triệu 500 nghìn"
            return whole * 1_000_000 + (_parse_localized_number(thousands.group(1)) or 0.0) * 1_000
        tail = _BILLION_TAIL.match(rest)
        if tail and float(whole).is_integer():  # "2tr700" = 2,7 triệu
            digits = tail.group(1)
            return (whole + int(digits) / 10 ** len(digits)) * 1_000_000
        return whole * 1_000_000

    unit = _THOUSAND_PRICE.search(lowered)
    if unit:
        number = _parse_localized_number(unit.group(1))
        price = number * 1_000 if number is not None else None
        return price if price is not None and math.isfinite(price) and price > 0 else None

    number = _parse_localized_number(lowered)
    if number is None or number <= 0:
        return None
    return number if math.isfinite(number) else None


_NOT_LETTER = r"(?![a-zà-ỹđ])"
_BILLION_PRICE = re.compile(rf"(\d[\d.,]*)\s*(?:tỷ|tỉ|ty){_NOT_LETTER}")
_MILLION_PRICE = re.compile(rf"(\d[\d.,]*)\s*(?:triệu|trieu|tr){_NOT_LETTER}")
_THOUSAND_PRICE = re.compile(rf"(\d[\d.,]*)\s*(?:nghìn|ngàn|nghin|ngan|k){_NOT_LETTER}")
_MILLION_AFTER_BILLION = re.compile(rf"\s*(\d[\d.,]*)\s*(?:triệu|trieu|tr){_NOT_LETTER}")
_THOUSAND_AFTER_MILLION = re.compile(rf"\s*(\d[\d.,]*)\s*(?:nghìn|ngàn|nghin|ngan|k){_NOT_LETTER}")
# 1-3 digits right after "tỷ" that are not a quantity of something else.
_BILLION_TAIL = re.compile(r"\s*(\d{1,3})(?![\d.,])(?!\s*(?:m|pn|phòng|tầng|tang|lầu|lau|%|x|wc|năm|nam|th))")


def _rescale_billion_label(text: str, number: float) -> float | None:
    """A "tỷ" figure of 1,000 or more is not a listing price.

    Seen in the crawls: "9.630 tỷ" (dot as decimal, 9,63 tỷ), "5800 tỷ"
    (millions labelled tỷ) and "3150000000 tỷ" (VND labelled tỷ).
    """

    decimal = _single_separator_decimal(text)
    if decimal is not None:
        return decimal * 1_000_000_000
    if number >= 1_000_000:
        return number
    return number * 1_000_000


def _single_separator_decimal(text: str) -> float | None:
    """ "2,424" or "9.630" read as a decimal (2.424 / 9.63), else None."""

    token = re.search(r"\d[\d.,]*", text)
    if token and re.fullmatch(r"\d{1,3}[.,]\d{3}", token.group()):
        return float(token.group().replace(",", "."))
    return None


def billion_label_readings(value: Any) -> list[float]:
    """Possible VND values of a "tỷ" figure >= 1,000, most likely first.

    A single separator followed by three digits ("2,424 tỷ", "9.630 tỷ") is
    read as a decimal first: listings at 1,000 tỷ or more are far rarer than a
    decimal written with three digits.
    """

    token = _billion_token(value)
    if token is None:
        return []
    number = _parse_localized_number(token)
    if number is None:
        return []
    decimal = _single_separator_decimal(token)
    readings = [decimal * 1_000_000_000] if decimal is not None else []
    readings += [number * 1_000_000_000, number * 1_000_000, number]
    return readings


def _billion_token(value: Any) -> str | None:
    """The figure written right before "tỷ", if any."""

    match = _BILLION_PRICE.search((clean_text(value) or "").lower())
    return match.group(1) if match else None


def price_unit_was_rescaled(value: Any) -> bool:
    """True when ``parse_vietnamese_price`` reinterpreted a "tỷ" figure >= 1,000."""

    token = _billion_token(value)
    number = _parse_localized_number(token) if token else None
    return number is not None and number >= 1_000


# Values no sale listing can have. They come from parsing errors (unit or
# separator mistakes, a width read as the area), not from real outliers,
# which DQ11 keeps and only flags.
MIN_PLAUSIBLE_AREA_M2 = 10.0
MIN_PLAUSIBLE_SALE_PRICE = 50_000_000
MIN_PLAUSIBLE_PRICE_PER_M2 = 20_000
MAX_PLAUSIBLE_PRICE_PER_M2 = 2_000_000_000
_TITLE_AREA = re.compile(r"(\d[\d.,]*)\s*(?:m2|m²|㎡|met vuong|mét vuông)", re.IGNORECASE)
_TITLE_DIMENSIONS = re.compile(r"(\d+(?:[.,]\d+)?)\s*m?\s*[x×*]\s*(\d+(?:[.,]\d+)?)\s*m?\b", re.IGNORECASE)


def area_from_title(title: Any) -> float | None:
    """Area stated in a title: "1011,8m2", "5.460,5 m2", "162M²" or "4x20m"."""

    text = clean_text(title)
    if text is None:
        return None
    for match in _TITLE_AREA.finditer(text):
        area = _parse_localized_number(match.group(1))
        if area is not None and area >= MIN_PLAUSIBLE_AREA_M2:
            return area
    for match in _TITLE_DIMENSIONS.finditer(text):
        width, length = _parse_localized_number(match.group(1)), _parse_localized_number(match.group(2))
        if width and length and 1 <= width <= 100 and 1 <= length <= 500 and width * length >= MIN_PLAUSIBLE_AREA_M2:
            return round(width * length, 2)
    return None


def apply_value_plausibility(record: Mapping[str, Any]) -> tuple[dict[str, Any], list[str]]:
    """Repair or drop impossible price/area values; return (record, DQ codes).

    DQ14  price label "tỷ" was rescaled by the parser (value kept, flagged).
    DQ15  area < 10 m² replaced by the area stated in the title.
    DQ16  area or price impossible and not repairable: set to NULL; the raw
          ``price_str`` stays for audit.
    Derived ``price_m`` and ``price_per_m2`` are recomputed.
    """

    out = dict(record)
    reasons: list[str] = []
    area = out.get("area")
    stated = area_from_title(out.get("title")) if area is not None else None
    if area is not None and area < MIN_PLAUSIBLE_AREA_M2:
        out["area"] = stated
        reasons.append("DQ15" if stated is not None else "DQ16")
    elif stated is not None and abs(stated - area * 1000) <= 0.02 * stated:
        # "11.000m2" in the title, 11 in the area field: thousands separator
        # read as a decimal point.
        out["area"] = stated
        reasons.append("DQ15")
    area = out.get("area")
    if out.get("price") is not None and price_unit_was_rescaled(out.get("price_str")):
        # With a known area, take the first reading of the "tỷ" figure that
        # gives a possible price per m²; otherwise keep the parser's default.
        if area:
            plausible = [
                value for value in billion_label_readings(out["price_str"])
                if MIN_PLAUSIBLE_PRICE_PER_M2 <= value / area <= MAX_PLAUSIBLE_PRICE_PER_M2
            ]
            out["price"] = plausible[0] if plausible else out["price"]
        reasons.append("DQ14")
    price, area = out.get("price"), out.get("area")
    if price is not None and not out.get("is_rent"):
        per_m2 = price / area if area else None
        if price < MIN_PLAUSIBLE_SALE_PRICE or (
            per_m2 is not None and not MIN_PLAUSIBLE_PRICE_PER_M2 <= per_m2 <= MAX_PLAUSIBLE_PRICE_PER_M2
        ):
            out["price"] = None
            if "DQ16" not in reasons:
                reasons.append("DQ16")
    price, area = out.get("price"), out.get("area")
    out["price_m"] = price / 1_000_000 if price is not None else None
    out["price_per_m2"] = price / area if price is not None and area else None
    return out, reasons


_PRICE_ONLY = re.compile(r"^\s*\d[\d.,]*\s*(?:tỷ|tỉ|triệu|trieu|tr|nghìn|ngàn|k)?(?:\s*\d[\d.,]*\s*(?:triệu|trieu|tr|nghìn|ngàn)?)?\s*$")
_PER_UNIT = re.compile(r"/\s*(?:m2|m²|m|tháng|thang|năm|nam)|/m")


def reconcile_shipped_price(price: Any, price_str: Any, area: Any) -> float | None:
    """Choose between a pre-computed price and the one parsed from price_str.

    Historical exports ship ``price`` computed by an older parser. price_str
    may be a plain price ("8,5 tỷ": re-parse), a per-m² or per-month price
    ("180 triệu / m²": keep the shipped total), or a whole title (muaban).
    For a title only the known defect of the old parser is repaired: it read
    the first number (the area) as the price, so ``price == area * 1e9``.
    """

    text = clean_text(price_str)
    shipped = float(price) if price is not None else None
    if text is None:
        return shipped
    lowered = text.lower()
    if _PER_UNIT.search(lowered):
        return shipped
    parsed = parse_vietnamese_price(text)
    if parsed is None:
        return shipped
    if _PRICE_ONLY.match(lowered) or shipped is None:
        return parsed
    if area and shipped and abs(shipped - float(area) * 1_000_000_000) < 1:
        return parsed
    return shipped


def parse_area_m2(value: Any) -> float | None:
    """Convert a source area to square metres."""

    area = _parse_localized_number(value)
    return area if area is not None and area > 0 else None


def parse_rooms(value: Any) -> int | None:
    """Extract a bedroom count only when the input is explicit."""

    if value is None or isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        number = float(value)
        if not math.isfinite(number) or number < 0 or not number.is_integer():
            return None
        return int(number)

    text = clean_text(value)
    if text is None:
        return None

    lowered = text.lower()
    for pattern in (
        r"\b(\d+)\s*pn\b",
        r"\b(\d+)\s*phòng\s*ngủ\b",
        r"\b(\d+)\s*phong\s*ngu\b",
        r"\b(\d+)\s*bedrooms?\b",
    ):
        match = re.search(pattern, lowered)
        if match:
            return int(match.group(1))

    return int(lowered) if re.fullmatch(r"\d+", lowered) else None


def parse_timestamp(value: Any) -> datetime | None:
    """Parse a timestamp and normalize it to UTC+07:00."""

    if value is None:
        return None

    if isinstance(value, datetime):
        parsed = value
    else:
        text = clean_text(value)
        if text is None:
            return None

        iso_text = text[:-1] + "+00:00" if text.endswith("Z") else text
        try:
            parsed = datetime.fromisoformat(iso_text)
        except ValueError:
            parsed = None
            for date_format in (
                "%Y-%m-%d %H:%M:%S",
                "%Y-%m-%d",
                "%d/%m/%Y %H:%M:%S",
                "%d/%m/%Y",
            ):
                try:
                    parsed = datetime.strptime(text, date_format)
                    break
                except ValueError:
                    continue
            if parsed is None:
                return None

    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=VIETNAM_TIMEZONE)
    return parsed.astimezone(VIETNAM_TIMEZONE)


def parse_epoch_milliseconds(value: Any) -> datetime | None:
    """Convert a Unix millisecond timestamp, as used by NhaDatVui."""

    try:
        milliseconds = int(float(value))
    except (TypeError, ValueError, OverflowError):
        return None
    if milliseconds <= 0:
        return None

    try:
        return datetime.fromtimestamp(milliseconds / 1000, tz=VIETNAM_TIMEZONE)
    except (ValueError, OverflowError, OSError):
        return None


def parse_relative_time(value: Any, scraped_at: Any) -> datetime | None:
    """Resolve a Vietnamese relative time against the crawl timestamp."""

    text = clean_text(value)
    observed_at = parse_timestamp(scraped_at)
    if text is None or observed_at is None:
        return None

    lowered = text.lower()
    for pattern, unit in (
        (r"(\d+)\s*(?:giây|giay)\s*trước", "seconds"),
        (r"(\d+)\s*(?:phút|phut)\s*trước", "minutes"),
        (r"(\d+)\s*(?:giờ|gio)\s*trước", "hours"),
        (r"(\d+)\s*(?:ngày|ngay)\s*trước", "days"),
    ):
        match = re.search(pattern, lowered)
        if match:
            return observed_at - timedelta(**{unit: int(match.group(1))})

    if "hôm qua" in lowered or "hom qua" in lowered:
        return observed_at - timedelta(days=1)
    return None


def normalize_coordinates(lat: Any, lon: Any) -> tuple[float | None, float | None, bool]:
    """Validate a complete latitude/longitude pair."""

    try:
        latitude = float(lat)
        longitude = float(lon)
    except (TypeError, ValueError):
        return None, None, False

    if not math.isfinite(latitude) or not math.isfinite(longitude):
        return None, None, False
    if latitude == 0 and longitude == 0:
        return None, None, False
    if not (-90 <= latitude <= 90) or not (-180 <= longitude <= 180):
        return None, None, False
    return latitude, longitude, True


def normalize_url(value: Any) -> str | None:
    """Normalize an absolute URL without dropping its query string."""

    text = clean_text(value)
    if text is None:
        return None

    try:
        parts = urlsplit(text)
    except ValueError:
        return None
    if parts.scheme.lower() not in {"http", "https"} or not parts.netloc:
        return None

    return urlunsplit(
        (
            parts.scheme.lower(),
            parts.netloc.lower(),
            parts.path.rstrip("/"),
            parts.query,
            "",
        )
    )


def make_source_id(source: Any, ad_id: Any) -> str | None:
    """Build the stable cross-source business key."""

    normalized_source = clean_text(source)
    normalized_ad_id = clean_text(ad_id)
    if normalized_source is None or normalized_ad_id is None:
        return None

    normalized_source = re.sub(r"\s+", "_", normalized_source.lower())
    return f"{normalized_source}_{normalized_ad_id}"


def _normalize_hash_value(field: str, value: Any) -> Any:
    if value is None:
        return None
    if field == "is_rent":
        return bool(value)
    if field in {"price", "area", "rooms", "lat", "lon"}:
        try:
            number = float(value)
        except (TypeError, ValueError):
            return None
        return round(number, 8) if math.isfinite(number) else None
    return clean_text(value)


def make_record_hash(record: Mapping[str, Any]) -> str:
    """Hash business fields; batch/page/observation metadata are excluded."""

    payload = {
        field: _normalize_hash_value(field, record.get(field))
        for field in RECORD_HASH_FIELDS
    }
    serialized = json.dumps(
        payload,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    )
    return hashlib.sha256(serialized.encode("utf-8")).hexdigest()


def calculate_completeness_score(
    record: Mapping[str, Any],
    fields: Sequence[str] = DEFAULT_COMPLETENESS_FIELDS,
) -> float:
    """Return the share of selected fields containing a meaningful value."""

    if not fields:
        return 0.0

    present = 0
    for field in fields:
        value = record.get(field)
        if value is None:
            continue
        if isinstance(value, str) and clean_text(value) is None:
            continue
        present += 1

    return round(present / len(fields), 6)
