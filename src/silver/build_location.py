"""Build nationwide Silver location features from Silver Core on MinIO.

Province names are normalized against a versioned 34-province reference.
Distance is measured to the matched province's reference administrative
centre; no nationwide record defaults to Ho Chi Minh City.
"""

from __future__ import annotations

import csv
import hashlib
import json
import math
import os
import re
import sys
import unicodedata
from collections.abc import Iterator, Mapping
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

SILVER_CORE_ROOT = os.getenv(
    "SILVER_CORE_ROOT", "s3a://lakehouse-silver/real_estate/core"
).rstrip("/")
SILVER_LOCATION_ROOT = os.getenv(
    "SILVER_LOCATION_ROOT", "s3a://lakehouse-silver/real_estate/location"
).rstrip("/")
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
OUTPUT_PARTITIONS = max(1, int(os.getenv("SILVER_LOCATION_PARTITIONS", "8")))
PROVINCE_REFERENCE_PATH = PROJECT_ROOT / "config/vietnam_province_centers.csv"
PROVINCE_MODEL_VERSION = "vn_province_34_effective_2025_07_01_v1"
DISTRICT_MODEL_VERSION = "hcmc_analysis_legacy_district_v1"
CENTER_TYPE = "PROVINCIAL_ADMIN_REFERENCE"
MAX_NEAREST_CENTER_KM = 350.0

LOCATION_COLUMNS = [
    "source_id", "source", "address", "ward", "district_id",
    "district_name_raw", "district_name_model", "district_mapping_method",
    "district_model_version", "province_name_raw", "province_name_model",
    "province_mapping_method", "province_model_version", "lat", "lon",
    "has_coord", "location_precision", "center_name", "center_type",
    "center_lat", "center_lon", "distance_to_center_km",
    "location_dq_status", "location_dq_reasons", "location_hash",
]

HCMC_DISTRICTS = {
    "Thành phố Thủ Đức",
    *{f"Quận {number}" for number in (1, 3, 4, 5, 6, 7, 8, 10, 11, 12)},
    "Quận Bình Tân", "Quận Bình Thạnh", "Quận Gò Vấp",
    "Quận Phú Nhuận", "Quận Tân Bình", "Quận Tân Phú",
    "Huyện Bình Chánh", "Huyện Cần Giờ", "Huyện Củ Chi",
    "Huyện Hóc Môn", "Huyện Nhà Bè",
}
NUMBERED_DISTRICTS = {
    str(number): f"Quận {number}" for number in (1, 3, 4, 5, 6, 7, 8, 10, 11, 12)
}
DISTRICT_PATTERNS: list[tuple[re.Pattern[str], str]] = [
    (re.compile(r"\b(?:thanh pho|tp|quan|q)\s*(?:thu duc|2|9)\b"), "Thành phố Thủ Đức"),
    (re.compile(r"\bhuyen\s*binh chanh\b"), "Huyện Bình Chánh"),
    (re.compile(r"\bhuyen\s*can gio\b"), "Huyện Cần Giờ"),
    (re.compile(r"\bhuyen\s*cu chi\b"), "Huyện Củ Chi"),
    (re.compile(r"\bhuyen\s*hoc mon\b"), "Huyện Hóc Môn"),
    (re.compile(r"\bhuyen\s*nha be\b"), "Huyện Nhà Bè"),
    (re.compile(r"\b(?:quan|q)\s*binh thanh\b"), "Quận Bình Thạnh"),
    (re.compile(r"\b(?:quan|q)\s*binh tan\b"), "Quận Bình Tân"),
    (re.compile(r"\b(?:quan|q)\s*go vap\b"), "Quận Gò Vấp"),
    (re.compile(r"\b(?:quan|q)\s*phu nhuan\b"), "Quận Phú Nhuận"),
    (re.compile(r"\b(?:quan|q)\s*tan binh\b"), "Quận Tân Bình"),
    (re.compile(r"\b(?:quan|q)\s*tan phu\b"), "Quận Tân Phú"),
]
WARD_TO_HCMC_MODEL_DISTRICT: list[tuple[re.Pattern[str], str]] = [
    (re.compile(r"^phuong (sai gon|ben thanh|tan dinh|cau ong lanh)$"), "Quận 1"),
    (re.compile(r"^phuong (khanh hoi|xom chieu)$"), "Quận 4"),
    (re.compile(r"^phuong (cho lon|cho quan|an dong)$"), "Quận 5"),
    (re.compile(r"^phuong (binh tay|binh phu|phu lam|binh tien)$"), "Quận 6"),
    (re.compile(r"^phuong (binh dong|phu dinh)$"), "Quận 8"),
    (re.compile(r"^phuong dien hong$"), "Quận 10"),
    (re.compile(r"^phuong phu tho$"), "Quận 11"),
    (re.compile(r"^phuong (tay thanh|tan son nhi|phu tho hoa|phu thanh)$"), "Quận Tân Phú"),
    (re.compile(r"^phuong (bay hien|tan son hoa|tan hoa)$"), "Quận Tân Bình"),
    (re.compile(r"^phuong (hanh thong|an hoi|thong tay hoi|an nhon)$"), "Quận Gò Vấp"),
    (re.compile(r"^(xa|phuong) tan vinh loc$"), "Huyện Bình Chánh"),
    (
        re.compile(
            r"^phuong (an khanh|thu thiem|thao dien|cat lai|binh trung|"
            r"hiep binh|linh xuan|long binh|long phuoc|phuoc long|tam hiep|"
            r"truong thanh|thu duc)$"
        ),
        "Thành phố Thủ Đức",
    ),
]


def clean_optional_text(value: Any) -> str | None:
    if value is None:
        return None
    text = re.sub(r"\s+", " ", str(value)).strip()
    if not text or text.casefold() in {"na", "nan", "none", "null", "không rõ"}:
        return None
    return text


def match_text(value: Any) -> str:
    text = unicodedata.normalize("NFKD", clean_optional_text(value) or "")
    text = "".join(char for char in text if not unicodedata.combining(char))
    text = text.replace("đ", "d").replace("Đ", "D").casefold()
    return re.sub(r"\s+", " ", re.sub(r"[^a-z0-9]+", " ", text)).strip()


def valid_coordinate_pair(lat: Any, lon: Any) -> tuple[float | None, float | None]:
    try:
        lat_value, lon_value = float(lat), float(lon)
    except (TypeError, ValueError):
        return None, None
    if not (math.isfinite(lat_value) and math.isfinite(lon_value)):
        return None, None
    if not (-90 <= lat_value <= 90 and -180 <= lon_value <= 180):
        return None, None
    if lat_value == 0 and lon_value == 0:
        return None, None
    return lat_value, lon_value


def haversine_km(lat: float, lon: float, center_lat: float, center_lon: float) -> float:
    radius_km = 6371.0088
    lat1, lat2 = math.radians(lat), math.radians(center_lat)
    delta_lat = lat2 - lat1
    delta_lon = math.radians(center_lon - lon)
    a = (
        math.sin(delta_lat / 2) ** 2
        + math.cos(lat1) * math.cos(lat2) * math.sin(delta_lon / 2) ** 2
    )
    return radius_km * 2 * math.asin(min(1.0, math.sqrt(a)))


def load_province_reference(path: Path = PROVINCE_REFERENCE_PATH) -> list[dict[str, Any]]:
    if not path.exists():
        raise RuntimeError(f"Missing province reference: {path}")
    rows: list[dict[str, Any]] = []
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        for raw in csv.DictReader(handle):
            province = clean_optional_text(raw.get("province_name"))
            center = clean_optional_text(raw.get("center_name"))
            lat, lon = valid_coordinate_pair(raw.get("center_lat"), raw.get("center_lon"))
            if not province or not center or lat is None or lon is None:
                raise RuntimeError(f"Invalid province reference row: {raw}")
            aliases = [
                alias.strip() for alias in str(raw.get("aliases") or "").split("|")
                if alias.strip()
            ]
            if province not in aliases:
                aliases.append(province)
            rows.append(
                {
                    "province_name": province,
                    "center_name": center,
                    "center_lat": lat,
                    "center_lon": lon,
                    "aliases": aliases,
                }
            )
    if len(rows) != 34:
        raise RuntimeError(f"Province reference must contain 34 rows, found {len(rows)}")
    return rows


PROVINCE_REFERENCE = load_province_reference()
PROVINCE_ALIAS_INDEX = sorted(
    [
        (match_text(alias), alias, row)
        for row in PROVINCE_REFERENCE
        for alias in row["aliases"]
    ],
    key=lambda item: len(item[0]),
    reverse=True,
)


def _contains_alias(text: str, alias_key: str) -> bool:
    return bool(alias_key and re.search(rf"(?:^| ){re.escape(alias_key)}(?: |$)", text))


def province_from_text(*values: Any) -> tuple[str | None, dict[str, Any] | None]:
    text = " ".join(match_text(value) for value in values if value is not None)
    if not text:
        return None, None

    # Source strings such as "Bình Dương (Hồ Chí Minh mới)" must prefer the
    # explicitly marked new province over the legacy name appearing first.
    for alias_key, alias_raw, row in PROVINCE_ALIAS_INDEX:
        if alias_key and re.search(rf"(?:^| ){re.escape(alias_key)} moi(?: |$)", text):
            return alias_raw, row
    for alias_key, alias_raw, row in PROVINCE_ALIAS_INDEX:
        if _contains_alias(text, alias_key):
            return alias_raw, row
    return None, None


def nearest_province_center(lat: float, lon: float) -> tuple[dict[str, Any], float]:
    ranked = [
        (haversine_km(lat, lon, row["center_lat"], row["center_lon"]), row)
        for row in PROVINCE_REFERENCE
    ]
    distance, row = min(ranked, key=lambda item: item[0])
    return row, distance


def canonical_hcmc_district_from_text(text: str) -> str | None:
    normalized = match_text(text)
    numbered = re.search(
        r"\b(?:quan|q)\s*0?(1|2|3|4|5|6|7|8|9|10|11|12)\b", normalized
    )
    if numbered:
        number = numbered.group(1)
        return "Thành phố Thủ Đức" if number in {"2", "9"} else NUMBERED_DISTRICTS.get(number)
    for pattern, district in DISTRICT_PATTERNS:
        if pattern.search(normalized):
            return district
    return None


def infer_district_model(
    district_name: Any,
    ward: Any,
    address: Any,
    title: Any,
    source_url: Any,
    province_name_model: str | None,
) -> tuple[str | None, str]:
    current = clean_optional_text(district_name)
    if current:
        if province_name_model == "Hồ Chí Minh":
            canonical = canonical_hcmc_district_from_text(current)
            if canonical:
                return canonical, "CORE_CANONICALIZED"
        return current, "CORE_PRESERVED"

    if province_name_model == "Hồ Chí Minh":
        combined = " ".join(
            value
            for value in map(clean_optional_text, (address, title, source_url))
            if value
        )
        inferred = canonical_hcmc_district_from_text(combined)
        if inferred:
            return inferred, "TEXT_DISTRICT_RULE"
        ward_key = match_text(ward)
        for pattern, district in WARD_TO_HCMC_MODEL_DISTRICT:
            if pattern.fullmatch(ward_key):
                return district, "WARD_ANALYSIS_CROSSWALK"
    return None, "UNMAPPED"


def build_location_record(record: Mapping[str, Any]) -> dict[str, Any]:
    source_id = clean_optional_text(record.get("source_id"))
    source = clean_optional_text(record.get("source"))
    address = clean_optional_text(record.get("address"))
    ward = clean_optional_text(record.get("ward"))
    district_id = clean_optional_text(record.get("district_id"))
    district_raw = clean_optional_text(record.get("district_name"))
    title = clean_optional_text(record.get("title"))
    source_url = clean_optional_text(record.get("source_url"))
    lat, lon = valid_coordinate_pair(record.get("lat"), record.get("lon"))
    has_coord = lat is not None and lon is not None

    province_raw, province_ref = province_from_text(
        address, district_raw, source_url, ward, title
    )
    province_method = "TEXT_ALIAS" if province_ref else "UNMAPPED"
    nearest_distance: float | None = None
    if province_ref is None and has_coord:
        candidate_ref, nearest_distance = nearest_province_center(lat, lon)
        if nearest_distance <= MAX_NEAREST_CENTER_KM:
            province_ref = candidate_ref
            province_method = "NEAREST_CENTER"

    province_model = province_ref["province_name"] if province_ref else None
    district_model, district_method = infer_district_model(
        district_raw, ward, address, title, source_url, province_model
    )

    if has_coord:
        location_precision = "COORDINATE"
    elif ward:
        location_precision = "WARD"
    elif district_model:
        location_precision = "DISTRICT"
    elif province_model or address:
        location_precision = "PROVINCE_OR_ADDRESS"
    else:
        location_precision = "UNKNOWN"

    if has_coord and province_ref:
        distance = haversine_km(
            lat, lon, province_ref["center_lat"], province_ref["center_lon"]
        )
        if province_method == "NEAREST_CENTER":
            nearest_distance = distance
        distance = round(distance, 6)
    else:
        distance = None

    reasons: list[str] = []
    if not source_id:
        reasons.append("LQ00_SOURCE_ID_MISSING")
    if location_precision == "UNKNOWN":
        reasons.append("LQ01_LOCATION_MISSING")
    if not province_model:
        reasons.append("LQ02_PROVINCE_UNKNOWN")
    if not has_coord:
        reasons.append("LQ03_COORDINATE_MISSING")
    if province_method == "NEAREST_CENTER":
        reasons.append("LQ04_PROVINCE_INFERRED_BY_NEAREST_CENTER")
    if distance is not None and distance > MAX_NEAREST_CENTER_KM:
        reasons.append("LQ05_PROVINCE_COORDINATE_CONFLICT")

    dq_status = (
        "REJECT" if "LQ00_SOURCE_ID_MISSING" in reasons else "WARN" if reasons else "PASS"
    )
    hash_values = [
        match_text(address), match_text(ward), match_text(district_model),
        match_text(province_model), "" if lat is None else f"{lat:.7f}",
        "" if lon is None else f"{lon:.7f}",
    ]
    location_hash = hashlib.sha256("|".join(hash_values).encode("utf-8")).hexdigest()
    output = {
        "source_id": source_id,
        "source": source,
        "address": address,
        "ward": ward,
        "district_id": district_id,
        "district_name_raw": district_raw,
        "district_name_model": district_model,
        "district_mapping_method": district_method,
        "district_model_version": DISTRICT_MODEL_VERSION,
        "province_name_raw": province_raw,
        "province_name_model": province_model,
        "province_mapping_method": province_method,
        "province_model_version": PROVINCE_MODEL_VERSION,
        "lat": lat,
        "lon": lon,
        "has_coord": has_coord,
        "location_precision": location_precision,
        "center_name": province_ref["center_name"] if province_ref else None,
        "center_type": CENTER_TYPE if province_ref else None,
        "center_lat": province_ref["center_lat"] if province_ref else None,
        "center_lon": province_ref["center_lon"] if province_ref else None,
        "distance_to_center_km": distance,
        "location_dq_status": dq_status,
        "location_dq_reasons": reasons,
        "location_hash": location_hash,
    }
    return {column: output.get(column) for column in LOCATION_COLUMNS}


def transform_partition(rows: Iterator[Any]) -> Iterator[tuple[Any, ...]]:
    for row in rows:
        output = build_location_record(row.asDict(recursive=True))
        yield tuple(output[column] for column in LOCATION_COLUMNS)


def main() -> None:
    from pyspark.sql import functions as F
    from pyspark.sql import types as T
    from pyspark.storagelevel import StorageLevel

    from src.common.spark_session import build_spark_session, ensure_silver_namespace

    spark = build_spark_session("SilverListingLocationNationwide")
    spark.conf.set("spark.sql.shuffle.partitions", str(OUTPUT_PARTITIONS))
    namespace = ensure_silver_namespace(spark)

    string_fields = set(LOCATION_COLUMNS) - {
        "lat", "lon", "has_coord", "center_lat", "center_lon",
        "distance_to_center_km", "location_dq_reasons",
    }
    non_nullable = {
        "district_mapping_method", "district_model_version",
        "province_mapping_method", "province_model_version", "has_coord",
        "location_precision", "location_dq_status", "location_dq_reasons",
        "location_hash",
    }
    schema_fields = []
    for column in LOCATION_COLUMNS:
        if column in string_fields:
            data_type = T.StringType()
        elif column == "has_coord":
            data_type = T.BooleanType()
        elif column == "location_dq_reasons":
            data_type = T.ArrayType(T.StringType(), False)
        else:
            data_type = T.DoubleType()
        schema_fields.append(T.StructField(column, data_type, column not in non_nullable))
    schema = T.StructType(schema_fields)

    input_path = f"{namespace}.silver_listings_current_27"
    output_path = f"{namespace}.listing_location"
    started_at = datetime.now(timezone.utc)
    try:
        core = spark.table(input_path)
    except Exception as exc:
        raise RuntimeError(
            f"Cannot read Silver Core: {input_path}. Run build_listing_core_spark.py first."
        ) from exc

    required = {
        "source_id", "source", "title", "address", "ward", "district_id",
        "district_name", "lat", "lon", "source_url",
    }
    missing = sorted(required - set(core.columns))
    if missing:
        raise RuntimeError(f"Silver Core is missing location columns: {missing}")
    # Select only location fields before crossing the JVM/Python boundary. This
    # also keeps unrelated timestamp columns out of the location transformer.
    core = core.select(*sorted(required))
    input_rows = core.count()
    distinct_source_ids = core.select("source_id").distinct().count()
    if input_rows == 0 or distinct_source_ids != input_rows:
        raise RuntimeError(
            f"Invalid Silver Core grain: rows={input_rows:,}, distinct={distinct_source_ids:,}"
        )

    location = spark.createDataFrame(
        core.rdd.mapPartitions(transform_partition), schema
    ).persist(StorageLevel.MEMORY_AND_DISK)
    output_rows = location.count()
    if output_rows != input_rows:
        raise RuntimeError(
            f"Location row mismatch: input={input_rows:,}, output={output_rows:,}"
        )
    reject_rows = location.filter(F.col("location_dq_status") == "REJECT").count()
    if reject_rows:
        raise RuntimeError(f"Location contains {reject_rows:,} REJECT rows")

    location.writeTo(output_path).using("iceberg").tableProperty("format-version", "2").createOrReplace()
    verified = spark.table(output_path)
    if verified.count() != output_rows:
        raise RuntimeError("Location read-back row count mismatch")
    if verified.columns != LOCATION_COLUMNS:
        raise RuntimeError(f"Location schema/order mismatch: {verified.columns}")

    def grouped_counts(column: str) -> dict[str, int]:
        return {
            str(row[column]): int(row["count"])
            for row in location.groupBy(column).count().collect()
        }

    reason_counts = {
        str(row["reason"]): int(row["count"])
        for row in (
            location.select(F.explode("location_dq_reasons").alias("reason"))
            .groupBy("reason").count().collect()
        )
    }
    distance_row = (
        location.where(F.col("distance_to_center_km").isNotNull())
        .agg(
            F.count("*").alias("count"),
            F.min("distance_to_center_km").alias("min_km"),
            F.avg("distance_to_center_km").alias("avg_km"),
            F.max("distance_to_center_km").alias("max_km"),
        ).first()
    )
    summary = {
        "started_at": started_at.isoformat(),
        "finished_at": datetime.now(timezone.utc).isoformat(),
        "input": input_path,
        "output": output_path,
        "input_rows": input_rows,
        "output_rows": output_rows,
        "province_model_version": PROVINCE_MODEL_VERSION,
        "province_counts": grouped_counts("province_name_model"),
        "province_mapping_method_counts": grouped_counts("province_mapping_method"),
        "location_precision_counts": grouped_counts("location_precision"),
        "dq_status_counts": grouped_counts("location_dq_status"),
        "dq_reason_counts": reason_counts,
        "distance_to_center_stats": {
            "count": int(distance_row["count"] or 0),
            "min_km": distance_row["min_km"],
            "avg_km": distance_row["avg_km"],
            "max_km": distance_row["max_km"],
        },
        "status": "PASS",
    }
    for output_dir in (PROJECT_ROOT / "outputs/validation", PROJECT_ROOT / "docs/validation"):
        output_dir.mkdir(parents=True, exist_ok=True)
        (output_dir / "silver_location_summary.json").write_text(
            json.dumps(summary, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
        )
    print(json.dumps(summary, ensure_ascii=False, indent=2))
    location.unpersist()
    spark.stop()


if __name__ == "__main__":
    main()
