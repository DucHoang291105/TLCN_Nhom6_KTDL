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

    billion_match = re.search(r"(\d[\d.,]*)\s*(?:tỷ|tỉ|ty)\b", lowered)
    million_match = re.search(r"(\d[\d.,]*)\s*(?:triệu|trieu|tr)\b", lowered)
    if billion_match and million_match:
        billions = _parse_localized_number(billion_match.group(1))
        millions = _parse_localized_number(million_match.group(1))
        if billions is not None and millions is not None:
            price = billions * 1_000_000_000 + millions * 1_000_000
            return price if price > 0 else None

    number = _parse_localized_number(lowered)
    if number is None or number <= 0:
        return None

    if re.search(r"\b(tỷ|tỉ|ty)\b", lowered):
        multiplier = 1_000_000_000
    elif re.search(r"\b(triệu|trieu|tr)\b", lowered):
        multiplier = 1_000_000
    elif re.search(r"\b(nghìn|ngàn|nghin|ngan|k)\b", lowered):
        multiplier = 1_000
    else:
        multiplier = 1

    price = number * multiplier
    return price if math.isfinite(price) and price > 0 else None


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
