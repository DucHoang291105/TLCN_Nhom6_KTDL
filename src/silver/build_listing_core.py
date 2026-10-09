"""Build canonical Silver Core records from source-specific Bronze rows.

Phase 1 implements and locally validates the Guland row transformer without
requiring Spark, Docker, or access to MinIO. The same pure transformation is
used as the behavioral contract when the Spark Bronze -> Silver job is added.

Run a read-only validation against a local Guland CSV:

    python -m src.silver.build_listing_core \
        --validate-guland-csv data/incoming/snapshots/guland/20261002/guland_raw.csv
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import re
import tempfile
import unicodedata
from collections import Counter
from datetime import date, datetime
from pathlib import Path
from typing import Any, Callable, Collection, Iterable, Mapping

from src.common.utils import (
    CANONICAL_COLUMNS,
    calculate_completeness_score,
    clean_text,
    make_record_hash,
    make_source_id,
    normalize_coordinates,
    normalize_url,
    parse_area_m2,
    parse_epoch_milliseconds,
    parse_relative_time,
    parse_rooms,
    parse_timestamp,
    parse_vietnamese_price,
)


OBSERVATION_METADATA_COLUMNS = [
    "batch_id",
    "snapshot_date",
    "bronze_ingested_at",
    "bronze_source_file",
    "bronze_path",
    "record_hash",
    "dq_status",
    "dq_reasons",
    "completeness_score",
    "category_evidence",
]

# Where ``category_name`` comes from, strongest first. Stored on
# listing_observation (not in the 27 canonical columns) so listing_feature can
# weigh a site taxonomy differently from an endpoint assumption.
CATEGORY_EVIDENCE_STRUCTURED = "SOURCE_STRUCTURED"  # site taxonomy id / slug
CATEGORY_EVIDENCE_LABEL = "SOURCE_LABEL"  # site text label or tag
CATEGORY_EVIDENCE_ENDPOINT = "ENDPOINT_CONTEXT"  # only the crawled endpoint
CATEGORY_EVIDENCE_NONE = "NONE"

GULAND_SOURCE_GROUP = "mua_ban_nha_mat_pho_mat_tien"
BATDONGSAN_SOURCE_GROUP = "ban_can_ho_chung_cu"
BATDONGSAN_POSTED_FIELDS = (
    "published_info_text",
    "posted_text",
    "published_at",
    "posted_at",
)
BATDONGSAN_LAT_FIELDS = ("lat", "latitude")
BATDONGSAN_LON_FIELDS = ("lon", "lng", "longitude")

# Batdongsan taxonomy. The crawler starts from /ban-can-ho-chung-cu, but the
# result pages also contain other property types (category_id and the listing
# URL prefix agree), so the endpoint is not evidence of an apartment.
BATDONGSAN_CATEGORY_IDS: dict[str, tuple[str, str]] = {
    "324": ("apartment", "Căn hộ chung cư"),
    "650": ("apartment", "Căn hộ chung cư"),  # căn hộ chung cư mini
    "41": ("house", "Nhà ở"),  # nhà riêng
    "163": ("house", "Nhà ở"),  # nhà mặt phố
    "325": ("villa", "Biệt thự"),  # nhà biệt thự, liền kề
    "575": ("shophouse", "Shophouse"),
    "562": ("condotel", "Condotel"),
    "45": ("commercial", "Bất động sản thương mại"),  # kho, nhà xưởng
    "44": ("farm_resort", "Trang trại, khu nghỉ dưỡng"),
    "48": ("other", "Bất động sản khác"),
}
# Listing URL prefixes, used only when category_id is missing.
BATDONGSAN_URL_PREFIXES: tuple[tuple[str, str], ...] = (
    ("ban-can-ho-chung-cu", "324"),
    ("ban-nha-rieng", "41"),
    ("ban-nha-mat-pho", "163"),
    ("ban-nha-biet-thu-lien-ke", "325"),
    ("ban-shophouse-nha-pho-thuong-mai", "575"),
    ("ban-condotel", "562"),
    ("ban-kho-nha-xuong", "45"),
    ("ban-trang-trai-khu-nghi-duong", "44"),
    ("ban-loai-bat-dong-san-khac", "48"),
)
NHADATVUI_PRODUCT_SLUGS: dict[str, tuple[str, str]] = {
    "mua-ban-can-ho-chung-cu": ("apartment", "Căn hộ chung cư"),
    "mua-ban-nha-rieng": ("house", "Nhà ở"),
    "mua-ban-nha-mat-pho": ("house", "Nhà ở"),
    "mua-ban-dat": ("land", "Đất"),
    "mua-ban-biet-thu": ("villa", "Biệt thự"),
    "mua-ban-shophouse": ("shophouse", "Shophouse"),
    "mua-ban-condotel": ("condotel", "Condotel"),
    "mua-ban-kho-nha-xuong": ("commercial", "Bất động sản thương mại"),
    "mua-ban-van-phong": ("commercial", "Bất động sản thương mại"),
    "mua-ban-khu-nghi-duong-trang-trai": ("farm_resort", "Trang trại, khu nghỉ dưỡng"),
    "mua-ban-bat-dong-san-khac": ("other", "Bất động sản khác"),
}

# Priority matters: a listing can contain both a broad house label and a more
# specific label such as Shophouse or Biệt thự.
GULAND_CATEGORY_RULES: tuple[tuple[str, str, set[str]], ...] = (
    (
        "land",
        "Đất",
        {"đất thổ cư", "đất ở", "có thổ cư", "full thổ cư"},
    ),
    ("officetel", "Officetel", {"offictel", "officetel"}),
    ("shophouse", "Shophouse", {"shophouse"}),
    (
        "villa",
        "Biệt thự",
        {"biệt thự", "nhà biệt thự", "nhà vườn"},
    ),
    (
        "house",
        "Nhà ở",
        {"nhà phố", "nhà nguyên căn", "nhà liền kề", "nhà cấp 4"},
    ),
)

# Matched per source *label* field (never on titles), on folded text with word
# boundaries, so "Đất" does not match inside a project name such as "Đất Xanh".
GENERAL_CATEGORY_RULES: tuple[tuple[str, str, tuple[str, ...]], ...] = (
    ("officetel", "Officetel", ("officetel", "offictel")),
    ("shophouse", "Shophouse", ("shophouse",)),
    ("villa", "Biệt thự", ("biệt thự", "biet thu", "villa")),
    (
        "apartment",
        "Căn hộ chung cư",
        ("căn hộ", "can ho", "chung cư", "chung cu", "apartment", "condotel"),
    ),
    (
        "commercial",
        "Bất động sản thương mại",
        (
            "văn phòng",
            "van phong",
            "mặt bằng",
            "mat bang",
            "kho nhà xưởng",
            "kho nha xuong",
            "commercial",
        ),
    ),
    # Land after the specific types: "Đất" also appears in names (Đất Xanh).
    ("land", "Đất", ("đất", "dat", "land")),
    (
        "house",
        "Nhà ở",
        (
            "nhà mặt phố",
            "nha mat pho",
            "nhà phố",
            "nha pho",
            "nhà riêng",
            "nha rieng",
            "nhà ở",
            "nha o",
        ),
    ),
)


def _parse_list(value: Any) -> list[str]:
    """Return a cleaned string list from JSON, list, tuple, or scalar input."""

    if value is None:
        return []

    if isinstance(value, (list, tuple, set)):
        raw_values: Iterable[Any] = value
    elif isinstance(value, str):
        text = value.strip()
        if not text:
            return []
        try:
            decoded = json.loads(text)
        except json.JSONDecodeError:
            decoded = [text]
        raw_values = decoded if isinstance(decoded, list) else [decoded]
    else:
        raw_values = [value]

    result: list[str] = []
    for item in raw_values:
        cleaned = clean_text(item)
        if cleaned is not None and cleaned not in result:
            result.append(cleaned)
    return result


def _parse_positive_int(value: Any) -> int | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        parsed = int(float(value))
    except (TypeError, ValueError, OverflowError):
        return None
    return parsed if parsed > 0 else None


def _extract_location_component(
    locations: Iterable[str],
    patterns: Iterable[str],
) -> str | None:
    # Prefer rows with more comma-separated administrative components. These
    # are normally the old/full address rows in Guland's location_rows.
    ordered = sorted(locations, key=lambda item: item.count(","), reverse=True)
    for location in ordered:
        for pattern in patterns:
            match = re.search(pattern, location, flags=re.IGNORECASE)
            if match:
                return clean_text(re.sub(r"\s*\((?:mới|cũ)\)\s*$", "", match.group(1), flags=re.IGNORECASE))
    return None


def _parse_guland_location(value: Any) -> tuple[str | None, str | None, str | None]:
    locations = _parse_list(value)
    if not locations:
        return None, None, None

    address = " | ".join(locations)
    ward = _extract_location_component(
        locations,
        (
            r"\b((?:Phường|Xã|Thị trấn)\s+[^,|]+)",
        ),
    )
    district = _extract_location_component(
        locations,
        (
            r"\b((?:Quận|Huyện|Thị xã)\s+[^,|]+)",
            r"\b((?:Thành phố|TP\.?)\s+Thủ Đức)\b",
        ),
    )
    return address, ward, district


def _guland_posted_at(posted_text: Any, scraped_at: datetime | None) -> datetime | None:
    if scraped_at is None:
        return None

    relative = parse_relative_time(posted_text, scraped_at)
    if relative is not None:
        return relative

    absolute = parse_timestamp(posted_text)
    if absolute is not None and absolute <= scraped_at:
        return absolute
    return None


def transform_guland_record(raw_record: Mapping[str, Any]) -> dict[str, Any]:
    """Map one Guland Bronze/raw row to the exact canonical 27-column schema."""

    source = clean_text(raw_record.get("_source")) or "guland"
    source = source.lower()
    ad_id = clean_text(raw_record.get("listing_id"))

    price_str = clean_text(raw_record.get("price_text"))
    price = parse_vietnamese_price(price_str)
    area = parse_area_m2(raw_record.get("area_text"))
    rooms = parse_rooms(raw_record.get("rooms"))
    if rooms is None:
        rooms = parse_rooms(raw_record.get("description"))

    address, ward, district_name = _parse_guland_location(
        raw_record.get("location_rows")
    )
    category_id, category_name, _ = map_guland_category(raw_record)
    lat, lon, has_coord = normalize_coordinates(
        raw_record.get("lat"),
        raw_record.get("lon"),
    )

    ad_url = normalize_url(raw_record.get("listing_url"))
    source_url = ad_url or normalize_url(raw_record.get("_crawl_url"))
    scraped_at = parse_timestamp(raw_record.get("_scraped_at"))
    posted_at = _guland_posted_at(raw_record.get("posted_text"), scraped_at)

    record = {
        "source": source,
        "source_group": GULAND_SOURCE_GROUP,
        "source_id": make_source_id(source, ad_id),
        "ad_id": ad_id,
        "title": clean_text(raw_record.get("title")),
        "price": price,
        "price_str": price_str,
        "area": area,
        "rooms": rooms,
        "address": address,
        "ward": ward,
        "district_id": None,  # Requires the canonical geography reference.
        "district_name": district_name,
        "category_id": category_id,
        "category_name": category_name,
        "lat": lat,
        "lon": lon,
        "image": normalize_url(raw_record.get("image_url")),
        "ad_url": ad_url,
        "source_url": source_url,
        "posted_at": posted_at,
        "scraped_at": scraped_at,
        "page_fetched": _parse_positive_int(raw_record.get("_source_page")),
        "price_m": price / 1_000_000 if price is not None else None,
        "price_per_m2": (
            price / area
            if price is not None and area is not None and area > 0
            else None
        ),
        "has_coord": has_coord,
        "is_rent": False,
    }

    # Re-project to guarantee exact order and to prevent raw fields leaking into
    # Silver Core.
    return {column: record.get(column) for column in CANONICAL_COLUMNS}


def _first_clean(*values: Any) -> str | None:
    for value in values:
        cleaned = clean_text(value)
        if cleaned is not None:
            return cleaned
    return None


def _first_url(value: Any) -> str | None:
    for item in _parse_list(value):
        normalized = normalize_url(item)
        if normalized is not None:
            return normalized
    return None


def _fold(value: Any) -> str:
    text = unicodedata.normalize("NFKD", str(value or ""))
    text = "".join(char for char in text if not unicodedata.combining(char))
    text = text.replace("đ", "d").replace("Đ", "D").casefold()
    return " ".join(re.sub(r"[^a-z0-9]+", " ", text).split())


_GENERAL_CATEGORY_INDEX = tuple(
    (
        category_id,
        category_name,
        tuple(re.compile(rf"(?:^| ){re.escape(_fold(p))}(?= |$)") for p in patterns),
    )
    for category_id, category_name, patterns in GENERAL_CATEGORY_RULES
)


def _map_general_category(*values: Any) -> tuple[str | None, str | None]:
    """Map source *label* fields, checked one field at a time in the given order.

    Fields are never concatenated, so a keyword in one field cannot override
    the label of another.
    """

    for value in values:
        for text in _parse_list(value):
            folded = _fold(text)
            for category_id, category_name, patterns in _GENERAL_CATEGORY_INDEX:
                if any(pattern.search(folded) for pattern in patterns):
                    return category_id, category_name
    return None, None


def map_batdongsan_category(raw_record: Mapping[str, Any]) -> tuple[str | None, str | None, str]:
    """Category from Batdongsan's taxonomy id, else its listing URL prefix."""

    category_key = clean_text(raw_record.get("category_id"))
    if category_key in BATDONGSAN_CATEGORY_IDS:
        return (*BATDONGSAN_CATEGORY_IDS[category_key], CATEGORY_EVIDENCE_STRUCTURED)
    url = clean_text(raw_record.get("listing_url")) or ""
    path = re.sub(r"^https?://[^/]+/", "", url)
    for prefix, key in BATDONGSAN_URL_PREFIXES:
        if path.startswith(prefix):
            return (*BATDONGSAN_CATEGORY_IDS[key], CATEGORY_EVIDENCE_STRUCTURED)
    # No default: the title is left to listing_feature, which records the method.
    return None, None, CATEGORY_EVIDENCE_NONE


def map_nhadatvui_category(raw_record: Mapping[str, Any]) -> tuple[str | None, str | None, str]:
    slug = (clean_text(raw_record.get("product_slug")) or "").casefold()
    if slug in NHADATVUI_PRODUCT_SLUGS:
        category_id, category_name = NHADATVUI_PRODUCT_SLUGS[slug]
        if category_id == "apartment" and _fold(raw_record.get("property_subtype")) == "officetel":
            category_id, category_name = "officetel", "Officetel"
        return category_id, category_name, CATEGORY_EVIDENCE_STRUCTURED
    category_id, category_name = _map_general_category(
        raw_record.get("product_name"), raw_record.get("property_subtype")
    )
    evidence = CATEGORY_EVIDENCE_LABEL if category_id else CATEGORY_EVIDENCE_NONE
    return category_id, category_name, evidence


def map_guland_category(raw_record: Mapping[str, Any]) -> tuple[str | None, str | None, str]:
    features = {feature.casefold() for feature in _parse_list(raw_record.get("features"))}
    for category_id, category_name, source_labels in GULAND_CATEGORY_RULES:
        if features.intersection(label.casefold() for label in source_labels):
            return category_id, category_name, CATEGORY_EVIDENCE_LABEL
    # Only the crawled endpoint (mua-ban-nha-mat-pho-mat-tien) says "house";
    # listing_feature lets clear title evidence override this weak default.
    return "house", "Nhà ở", CATEGORY_EVIDENCE_ENDPOINT


def compose_nhadatvui_address(raw_record: Mapping[str, Any]) -> str | None:
    """Raw address completed with the structured ward and province fields.

    Most NhaDatVui rows have an empty ``address``; without the structured
    ``province_name`` listing_location had to guess the province from the title.
    """

    raw = clean_text(raw_record.get("address")) or ""
    parts = [part.strip() for part in raw.split(",") if part.strip()]
    seen = {_fold(part) for part in parts}
    for field in ("ward_name", "province_name"):
        value = clean_text(raw_record.get(field))
        if value and _fold(value) not in seen:
            parts.append(value)
            seen.add(_fold(value))
    return ", ".join(parts) or None


def _parse_location_text(value: Any) -> tuple[str | None, str | None]:
    location = clean_text(value)
    if location is None:
        return None, None

    ward = _extract_location_component(
        [location],
        (r"\b((?:Phường|Xã|Thị trấn)\s+[^,|]+)",),
    )
    district = _extract_location_component(
        [location],
        (
            r"\b((?:Quận|Huyện|Thị xã)\s+[^,|]+)",
            r"\b((?:Thành phố|TP\.?)\s+Thủ Đức)\b",
        ),
    )
    return ward, district


def _safe_posted_at(value: Any, scraped_at: datetime | None) -> datetime | None:
    if scraped_at is None:
        return None

    relative = parse_relative_time(value, scraped_at)
    if relative is not None:
        return relative

    absolute = parse_timestamp(value)
    if absolute is not None and absolute <= scraped_at:
        return absolute
    return None


def transform_batdongsan_record(raw_record: Mapping[str, Any]) -> dict[str, Any]:
    """Map one Batdongsan Bronze/raw row to the canonical 27 columns."""

    source = (clean_text(raw_record.get("_source")) or "batdongsan").lower()
    ad_id = clean_text(raw_record.get("listing_id"))
    price_str = clean_text(raw_record.get("price_text"))
    price = parse_vietnamese_price(price_str)
    area = parse_area_m2(raw_record.get("area_text"))
    rooms = parse_rooms(
        _first_clean(
            raw_record.get("bedrooms_text"),
            raw_record.get("bedrooms_aria"),
        )
    )

    address = clean_text(raw_record.get("location_text"))
    ward, district_name = _parse_location_text(address)
    category_id, category_name, _ = map_batdongsan_category(raw_record)

    ad_url = normalize_url(raw_record.get("listing_url"))
    source_url = ad_url or normalize_url(raw_record.get("_crawl_url"))
    scraped_at = parse_timestamp(raw_record.get("_scraped_at"))
    posted_at = _safe_posted_at(
        _first_clean(
            *(raw_record.get(field) for field in BATDONGSAN_POSTED_FIELDS)
        ),
        scraped_at,
    )
    lat, lon, has_coord = normalize_coordinates(
        _first_clean(*(raw_record.get(field) for field in BATDONGSAN_LAT_FIELDS)),
        _first_clean(*(raw_record.get(field) for field in BATDONGSAN_LON_FIELDS)),
    )

    source_group = _first_clean(
        raw_record.get("intent"),
        raw_record.get("page_type"),
        raw_record.get("product_type"),
    ) or BATDONGSAN_SOURCE_GROUP

    record = {
        "source": source,
        "source_group": source_group,
        "source_id": make_source_id(source, ad_id),
        "ad_id": ad_id,
        "title": clean_text(raw_record.get("title")),
        "price": price,
        "price_str": price_str,
        "area": area,
        "rooms": rooms,
        "address": address,
        "ward": ward,
        "district_id": None,  # Requires the canonical geography reference.
        "district_name": district_name,
        "category_id": category_id,
        "category_name": category_name,
        "lat": lat,
        "lon": lon,
        "image": _first_url(raw_record.get("image_urls")),
        "ad_url": ad_url,
        "source_url": source_url,
        "posted_at": posted_at,
        "scraped_at": scraped_at,
        "page_fetched": _parse_positive_int(
            _first_clean(
                raw_record.get("_source_page"),
                raw_record.get("page_number_attr"),
            )
        ),
        "price_m": price / 1_000_000 if price is not None else None,
        "price_per_m2": (
            price / area
            if price is not None and area is not None and area > 0
            else None
        ),
        "has_coord": has_coord,
        "is_rent": False,
    }
    return {column: record.get(column) for column in CANONICAL_COLUMNS}


def transform_nhadatvui_record(raw_record: Mapping[str, Any]) -> dict[str, Any]:
    """Map one NhaDatVui Bronze/raw row to the canonical 27 columns."""

    source = (clean_text(raw_record.get("_source")) or "nhadatvui").lower()
    ad_id = clean_text(raw_record.get("listing_id"))
    price_source = raw_record.get("price")
    price = parse_vietnamese_price(price_source)
    area = parse_area_m2(raw_record.get("area"))
    rooms = parse_rooms(raw_record.get("rooms"))
    lat, lon, has_coord = normalize_coordinates(
        raw_record.get("lat"),
        raw_record.get("lng"),
    )

    source_group = _first_clean(
        raw_record.get("product_slug"),
        raw_record.get("product_name"),
    )
    category_id, category_name, _ = map_nhadatvui_category(raw_record)

    scraped_at = parse_timestamp(raw_record.get("_scraped_at"))
    posted_at = parse_epoch_milliseconds(raw_record.get("public_date_ms"))
    if posted_at is not None and scraped_at is not None and posted_at > scraped_at:
        posted_at = None

    raw_price_text = clean_text(price_source)
    record = {
        "source": source,
        "source_group": source_group,
        "source_id": make_source_id(source, ad_id),
        "ad_id": ad_id,
        "title": clean_text(raw_record.get("title")),
        "price": price,
        "price_str": raw_price_text,
        "area": area,
        "rooms": rooms,
        "address": compose_nhadatvui_address(raw_record),
        "ward": clean_text(raw_record.get("ward_name")),
        "district_id": None,  # Source CSV has no direct district field.
        "district_name": None,
        "category_id": category_id,
        "category_name": category_name,
        "lat": lat,
        "lon": lon,
        "image": normalize_url(raw_record.get("first_image_url")),
        "ad_url": None,  # Crawler CSV currently has no verified detail URL.
        "source_url": normalize_url(raw_record.get("_crawl_url")),
        "posted_at": posted_at,
        "scraped_at": scraped_at,
        "page_fetched": _parse_positive_int(raw_record.get("_source_page")),
        "price_m": price / 1_000_000 if price is not None else None,
        "price_per_m2": (
            price / area
            if price is not None and area is not None and area > 0
            else None
        ),
        "has_coord": has_coord,
        "is_rent": False,
    }
    return {column: record.get(column) for column in CANONICAL_COLUMNS}


def evaluate_core_data_quality(record: Mapping[str, Any]) -> tuple[str, list[str]]:
    """Apply the documented DQ rules that can be evaluated on Core fields."""

    reject_reasons: list[str] = []
    warn_reasons: list[str] = []

    if clean_text(record.get("source")) is None:
        reject_reasons.append("DQ01")
    if clean_text(record.get("ad_id")) is None:
        reject_reasons.append("DQ02")
    if clean_text(record.get("title")) is None:
        reject_reasons.append("DQ03")

    if record.get("price") is None:
        warn_reasons.append("DQ04")
    if record.get("area") is None:
        warn_reasons.append("DQ05")
    if record.get("is_rent") is None:
        warn_reasons.append("DQ06")
    if record.get("category_id") is None:
        warn_reasons.append("DQ07")
    if record.get("posted_at") is None:
        warn_reasons.append("DQ08")
    if not record.get("has_coord", False):
        warn_reasons.append("DQ09")

    reasons = reject_reasons + warn_reasons
    if reject_reasons:
        return "REJECT", reasons
    if warn_reasons:
        return "WARN", reasons
    return "PASS", []


def _parse_snapshot_date(batch_id: str | None) -> date | None:
    if batch_id is None or not re.fullmatch(r"\d{8}", batch_id):
        return None
    try:
        return datetime.strptime(batch_id, "%Y%m%d").date()
    except ValueError:
        return None


def _build_observation(
    core: Mapping[str, Any],
    raw_record: Mapping[str, Any],
    *,
    batch_id: str,
    default_source_file: str,
    bronze_path: str | None = None,
    source_unavailable_rules: Collection[str] = (),
    category_evidence: str = CATEGORY_EVIDENCE_NONE,
) -> dict[str, Any]:
    """Attach technical metadata to a canonical Core record."""

    dq_status, dq_reasons = evaluate_core_data_quality(core)
    unavailable = set(source_unavailable_rules)
    if unavailable:
        dq_reasons = [reason for reason in dq_reasons if reason not in unavailable]
        if dq_status != "REJECT" and not dq_reasons:
            dq_status = "PASS"

    snapshot_date = _parse_snapshot_date(batch_id)
    if snapshot_date is None:
        dq_status = "REJECT"
        dq_reasons = [*dq_reasons, "DQ13"]

    observation = {
        **core,
        "batch_id": batch_id,
        "snapshot_date": snapshot_date,
        "bronze_ingested_at": parse_timestamp(raw_record.get("_ingested_at")),
        "bronze_source_file": clean_text(
            raw_record.get("_source_file")
        ) or default_source_file,
        "bronze_path": clean_text(bronze_path),
        "record_hash": make_record_hash(core),
        "dq_status": dq_status,
        "dq_reasons": dq_reasons,
        "completeness_score": calculate_completeness_score(core),
        "category_evidence": category_evidence if core.get("category_name") else CATEGORY_EVIDENCE_NONE,
    }
    return observation


def build_guland_observation(
    raw_record: Mapping[str, Any],
    *,
    batch_id: str,
    bronze_path: str | None = None,
) -> dict[str, Any]:
    return _build_observation(
        transform_guland_record(raw_record),
        raw_record,
        batch_id=batch_id,
        default_source_file="guland_raw.csv",
        bronze_path=bronze_path,
        category_evidence=map_guland_category(raw_record)[2],
    )


def build_batdongsan_observation(
    raw_record: Mapping[str, Any],
    *,
    batch_id: str,
    bronze_path: str | None = None,
) -> dict[str, Any]:
    source_unavailable_rules: set[str] = set()
    if not any(field in raw_record for field in BATDONGSAN_POSTED_FIELDS):
        source_unavailable_rules.add("DQ08")
    has_lat_field = any(field in raw_record for field in BATDONGSAN_LAT_FIELDS)
    has_lon_field = any(field in raw_record for field in BATDONGSAN_LON_FIELDS)
    if not has_lat_field and not has_lon_field:
        source_unavailable_rules.add("DQ09")

    return _build_observation(
        transform_batdongsan_record(raw_record),
        raw_record,
        batch_id=batch_id,
        default_source_file="batdongsan_raw.csv",
        bronze_path=bronze_path,
        source_unavailable_rules=source_unavailable_rules,
        category_evidence=map_batdongsan_category(raw_record)[2],
    )


def build_nhadatvui_observation(
    raw_record: Mapping[str, Any],
    *,
    batch_id: str,
    bronze_path: str | None = None,
) -> dict[str, Any]:
    return _build_observation(
        transform_nhadatvui_record(raw_record),
        raw_record,
        batch_id=batch_id,
        default_source_file="nhadatvui_raw.csv",
        bronze_path=bronze_path,
        category_evidence=map_nhadatvui_category(raw_record)[2],
    )


def validate_guland_csv(
    csv_path: Path,
    *,
    limit: int | None = None,
) -> dict[str, Any]:
    """Read-only local validation used before Spark/MinIO integration."""

    if not csv_path.is_file():
        raise FileNotFoundError(f"Guland CSV not found: {csv_path}")

    batch_id = csv_path.parent.name
    total = 0
    status_counts: Counter[str] = Counter()
    reason_counts: Counter[str] = Counter()
    categories: Counter[str] = Counter()
    missing_columns: set[str] = set()

    with csv_path.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        required_raw = {
            "_source",
            "_source_page",
            "_scraped_at",
            "_crawl_url",
            "listing_id",
            "title",
            "price_text",
            "area_text",
            "location_rows",
            "features",
        }
        missing_columns = required_raw.difference(reader.fieldnames or [])
        if missing_columns:
            raise ValueError(
                "Guland CSV is missing required columns: "
                + ", ".join(sorted(missing_columns))
            )

        for raw in reader:
            observation = build_guland_observation(
                raw,
                batch_id=batch_id,
                bronze_path=str(csv_path),
            )
            if list(observation)[: len(CANONICAL_COLUMNS)] != CANONICAL_COLUMNS:
                raise AssertionError("Canonical column order changed")

            total += 1
            status_counts[observation["dq_status"]] += 1
            reason_counts.update(observation["dq_reasons"])
            categories[observation["category_name"] or "<NULL>"] += 1

            if limit is not None and total >= limit:
                break

    return {
        "path": str(csv_path),
        "batch_id": batch_id,
        "rows_validated": total,
        "status_counts": dict(status_counts),
        "dq_reason_counts": dict(reason_counts),
        "category_counts": dict(categories),
    }


ObservationBuilder = Callable[..., dict[str, Any]]

LOCAL_SOURCE_SPECS: dict[str, tuple[str, ObservationBuilder]] = {
    "batdongsan": ("batdongsan_raw.csv", build_batdongsan_observation),
    "guland": ("guland_raw.csv", build_guland_observation),
    "nhadatvui": ("nhadatvui_raw.csv", build_nhadatvui_observation),
}


def _is_lfs_pointer(path: Path) -> bool:
    with path.open("r", encoding="utf-8-sig", errors="replace") as handle:
        return handle.readline().strip() == "version https://git-lfs.github.com/spec/v1"


def _observation_rank(record: Mapping[str, Any]) -> tuple[float, float, float, str]:
    """Rank same-batch duplicates according to the documented grain rule."""

    scraped_at = record.get("scraped_at")
    scraped_rank = scraped_at.timestamp() if isinstance(scraped_at, datetime) else float("-inf")
    page = record.get("page_fetched")
    page_rank = -float(page) if isinstance(page, int) and page > 0 else float("-inf")
    return (
        float(record.get("completeness_score") or 0.0),
        scraped_rank,
        page_rank,
        str(record.get("record_hash") or ""),
    )


def _current_rank(record: Mapping[str, Any]) -> tuple[int, float, float, str]:
    snapshot_date = record.get("snapshot_date")
    snapshot_rank = snapshot_date.toordinal() if isinstance(snapshot_date, date) else -1
    scraped_at = record.get("scraped_at")
    scraped_rank = scraped_at.timestamp() if isinstance(scraped_at, datetime) else float("-inf")
    return (
        snapshot_rank,
        scraped_rank,
        float(record.get("completeness_score") or 0.0),
        str(record.get("record_hash") or ""),
    )


def _serialize_csv_value(value: Any) -> Any:
    if value is None:
        return ""
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, (list, tuple, set, dict)):
        return json.dumps(value, ensure_ascii=False, sort_keys=isinstance(value, dict))
    return value


def _write_csv_atomic(
    destination: Path,
    fieldnames: list[str],
    rows: Iterable[Mapping[str, Any]],
) -> None:
    destination.parent.mkdir(parents=True, exist_ok=True)
    temporary_path: Path | None = None
    try:
        with tempfile.NamedTemporaryFile(
            "w",
            encoding="utf-8-sig",
            newline="",
            dir=destination.parent,
            prefix=f".{destination.name}.",
            suffix=".tmp",
            delete=False,
        ) as handle:
            temporary_path = Path(handle.name)
            writer = csv.DictWriter(handle, fieldnames=fieldnames, extrasaction="ignore")
            writer.writeheader()
            for row in rows:
                writer.writerow(
                    {field: _serialize_csv_value(row.get(field)) for field in fieldnames}
                )
        os.replace(temporary_path, destination)
    finally:
        if temporary_path is not None and temporary_path.exists():
            temporary_path.unlink()


def _write_json_atomic(destination: Path, payload: Mapping[str, Any]) -> None:
    destination.parent.mkdir(parents=True, exist_ok=True)
    temporary_path: Path | None = None
    try:
        with tempfile.NamedTemporaryFile(
            "w",
            encoding="utf-8",
            dir=destination.parent,
            prefix=f".{destination.name}.",
            suffix=".tmp",
            delete=False,
        ) as handle:
            temporary_path = Path(handle.name)
            json.dump(payload, handle, ensure_ascii=False, indent=2, default=str)
            handle.write("\n")
        os.replace(temporary_path, destination)
    finally:
        if temporary_path is not None and temporary_path.exists():
            temporary_path.unlink()


def build_local_silver(
    input_root: Path,
    output_root: Path,
) -> dict[str, Any]:
    """Build reproducible local Silver CSVs from all available raw snapshots.

    This runner is intended for local validation and coursework handoff. The
    canonical transforms and ranking rules are shared with the future
    Spark/Iceberg job; the CSV output itself is not a replacement for Iceberg.
    """

    input_root = input_root.resolve()
    output_root = output_root.resolve()
    observations: list[dict[str, Any]] = []
    quarantine: list[dict[str, Any]] = []
    skipped_lfs_files: list[str] = []
    processed_files: list[str] = []
    input_rows = 0
    duplicates_dropped = 0

    for source, (filename, builder) in LOCAL_SOURCE_SPECS.items():
        source_root = input_root / source
        if not source_root.is_dir():
            continue

        for csv_path in sorted(source_root.glob(f"*/{filename}")):
            if _is_lfs_pointer(csv_path):
                skipped_lfs_files.append(str(csv_path))
                continue

            processed_files.append(str(csv_path))
            batch_id = csv_path.parent.name
            batch_winners: dict[tuple[str, str, str], dict[str, Any]] = {}

            with csv_path.open("r", encoding="utf-8-sig", newline="") as handle:
                for row_number, raw in enumerate(csv.DictReader(handle), start=2):
                    input_rows += 1
                    try:
                        observation = builder(
                            raw,
                            batch_id=batch_id,
                            bronze_path=str(csv_path),
                        )
                    except Exception as exc:
                        raise RuntimeError(
                            f"Cannot transform {csv_path} at CSV row {row_number}: {exc}"
                        ) from exc

                    if observation["dq_status"] == "REJECT":
                        quarantine.append(observation)
                        continue

                    dedup_key = (
                        str(observation["source"]),
                        str(observation["ad_id"]),
                        str(observation["batch_id"]),
                    )
                    previous = batch_winners.get(dedup_key)
                    if previous is None:
                        batch_winners[dedup_key] = observation
                    elif _observation_rank(observation) > _observation_rank(previous):
                        batch_winners[dedup_key] = observation
                        duplicates_dropped += 1
                    else:
                        duplicates_dropped += 1

            observations.extend(batch_winners.values())

    observations.sort(
        key=lambda row: (
            str(row.get("source") or ""),
            str(row.get("batch_id") or ""),
            str(row.get("ad_id") or ""),
        )
    )
    quarantine.sort(
        key=lambda row: (
            str(row.get("source") or ""),
            str(row.get("batch_id") or ""),
            str(row.get("ad_id") or ""),
        )
    )

    current_by_source_id: dict[str, dict[str, Any]] = {}
    for observation in observations:
        source_id = str(observation["source_id"])
        previous = current_by_source_id.get(source_id)
        if previous is None or _current_rank(observation) > _current_rank(previous):
            current_by_source_id[source_id] = observation

    current_rows = sorted(
        current_by_source_id.values(),
        key=lambda row: (str(row.get("source") or ""), str(row.get("source_id") or "")),
    )

    observation_columns = CANONICAL_COLUMNS + OBSERVATION_METADATA_COLUMNS
    observation_path = output_root / "listing_observation.csv"
    current_path = output_root / "listings_current_27.csv"
    quarantine_path = output_root / "listing_dq_quarantine.csv"
    summary_path = output_root / "build_summary.json"

    _write_csv_atomic(observation_path, observation_columns, observations)
    _write_csv_atomic(current_path, CANONICAL_COLUMNS, current_rows)
    _write_csv_atomic(quarantine_path, observation_columns, quarantine)

    status_counts = Counter(row["dq_status"] for row in observations)
    status_counts.update(row["dq_status"] for row in quarantine)
    reason_counts: Counter[str] = Counter()
    for row in [*observations, *quarantine]:
        reason_counts.update(row["dq_reasons"])

    summary: dict[str, Any] = {
        "input_root": str(input_root),
        "output_root": str(output_root),
        "processed_files": processed_files,
        "skipped_lfs_files": skipped_lfs_files,
        "input_rows": input_rows,
        "duplicates_dropped": duplicates_dropped,
        "observation_rows": len(observations),
        "current_rows": len(current_rows),
        "quarantine_rows": len(quarantine),
        "status_counts": dict(status_counts),
        "dq_reason_counts": dict(reason_counts),
        "outputs": {
            "listing_observation": str(observation_path),
            "listings_current_27": str(current_path),
            "listing_dq_quarantine": str(quarantine_path),
            "summary": str(summary_path),
        },
    }
    _write_json_atomic(summary_path, summary)
    return summary


def _build_argument_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Build/validate canonical Silver listing core records."
    )
    parser.add_argument(
        "--validate-guland-csv",
        type=Path,
        help="Read-only validation of a local Guland raw CSV.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Optional maximum rows for local validation.",
    )
    parser.add_argument(
        "--build-local-silver",
        action="store_true",
        help="Build deduplicated local Silver CSV outputs for all three sources.",
    )
    parser.add_argument(
        "--input-root",
        type=Path,
        default=Path("data/incoming/snapshots"),
        help="Root containing source/YYYYMMDD/*_raw.csv snapshots.",
    )
    parser.add_argument(
        "--output-root",
        type=Path,
        default=Path("outputs/silver"),
        help="Destination for local Silver CSVs and build summary.",
    )
    return parser


def main() -> None:
    args = _build_argument_parser().parse_args()
    if args.build_local_silver:
        result = build_local_silver(args.input_root, args.output_root)
        print(json.dumps(result, ensure_ascii=False, indent=2, default=str))
        return

    if args.validate_guland_csv is None:
        raise SystemExit(
            "Provide --validate-guland-csv or --build-local-silver."
        )

    result = validate_guland_csv(args.validate_guland_csv, limit=args.limit)
    print(json.dumps(result, ensure_ascii=False, indent=2, default=str))


if __name__ == "__main__":
    main()
