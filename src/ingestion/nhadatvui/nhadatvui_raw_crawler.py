"""
NhaDatVui.vn RAW crawler
=========================

Nguồn:
    https://www.nhadatvui.vn/mua-ban-nha-dat

Mục đích:
- Crawl dữ liệu RAW từ trang danh sách NhaDatVui.
- KHÔNG chuẩn hóa về schema 27 cột ở bước này.
- Lưu nguyên response từng page để làm bằng chứng nguồn.
- Lưu JSONL full RAW.
- Lưu CSV rút gọn để mở Excel kiểm tra nhanh.

NhaDatVui đang dùng Next.js / React Server Components.
Response chứa block dạng:

    "result": {
        "data": [...],
        "skip": 18,
        "limit": 18,
        "total": 16724,
        "count": 18
    }

Tool sẽ tự tìm và parse block "result".

Chỉ cần chỉnh:
    START_PAGE
    PAGES_TO_CRAWL
    DELAY_SECONDS
"""

from __future__ import annotations

import csv
import json
import random
import re
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import urlencode

import requests


# ============================================================
# CONFIG - CHỈNH Ở ĐÂY
# ============================================================

BASE_URL = "https://www.nhadatvui.vn/mua-ban-nha-dat"

# Trang bắt đầu
START_PAGE = 1

# Số trang muốn crawl
PAGES_TO_CRAWL = 10

# Delay cơ bản giữa 2 trang
DELAY_SECONDS = 2.0

# Thêm jitter ngẫu nhiên để request không quá đều
DELAY_JITTER_SECONDS = 0.8

# Timeout
REQUEST_TIMEOUT = 30

# Retry nếu lỗi mạng / 5xx / 429
MAX_RETRIES = 3


# ============================================================
# OUTPUT
# ============================================================

RUN_ID = datetime.now().strftime("%Y%m%d_%H%M%S")

# File này được đặt tại:
#   TLCN_BDS_Lakehouse/src/ingestion/nhadatvui/nhadatvui_raw_crawler.py
#
# Tự tìm thư mục gốc project, KHÔNG hard-code ổ D:/C: hay tên user.
# Vì vậy gửi project cho máy khác vẫn chạy được nếu giữ đúng cấu trúc thư mục.
PROJECT_ROOT = Path(__file__).resolve().parents[3]

OUTPUT_DIR = (
    PROJECT_ROOT
    / "data"
    / "incoming"
    / "snapshots"
    / "nhadatvui"
    / RUN_ID
)

PAGES_DIR = OUTPUT_DIR / "pages"

JSONL_PATH = OUTPUT_DIR / "nhadatvui_raw.jsonl"
CSV_PATH = OUTPUT_DIR / "nhadatvui_raw.csv"
SUMMARY_PATH = OUTPUT_DIR / "crawl_summary.json"


# ============================================================
# SESSION
# ============================================================

session = requests.Session()

session.headers.update(
    {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/152.0.0.0 Safari/537.36"
        ),
        "Accept": (
            "text/html,application/xhtml+xml,"
            "application/xml;q=0.9,image/avif,"
            "image/webp,*/*;q=0.8"
        ),
        "Accept-Language": (
            "vi-VN,vi;q=0.9,en-US;q=0.7,en;q=0.6"
        ),
        "Cache-Control": "no-cache",
        "Pragma": "no-cache",
        "Connection": "keep-alive",
        "Upgrade-Insecure-Requests": "1",
    }
)


# ============================================================
# UTILS
# ============================================================

def now_iso() -> str:
    return (
        datetime.now(timezone.utc)
        .astimezone()
        .isoformat(timespec="seconds")
    )


def build_page_url(page: int) -> str:
    return (
        f"{BASE_URL}?"
        + urlencode({"page": page})
    )


def safe_dict(value: Any) -> dict:
    return value if isinstance(value, dict) else {}


def safe_list(value: Any) -> list:
    return value if isinstance(value, list) else []


# ============================================================
# NEXT.JS / RSC PARSER
# ============================================================

def extract_result_object_from_text(
    text: str,
) -> dict | None:
    """
    Tìm marker:
        "result":
    rồi dùng JSONDecoder.raw_decode() để parse đúng object
    ngay sau marker.

    Cách này không phụ thuộc độ dài danh sách data.
    """

    marker = '"result":'
    search_from = 0
    decoder = json.JSONDecoder()

    while True:
        marker_index = text.find(
            marker,
            search_from,
        )

        if marker_index == -1:
            return None

        object_start = text.find(
            "{",
            marker_index + len(marker),
        )

        if object_start == -1:
            return None

        try:
            result, _ = decoder.raw_decode(
                text[object_start:]
            )

            if (
                isinstance(result, dict)
                and isinstance(
                    result.get("data"),
                    list,
                )
                and "total" in result
                and "count" in result
            ):
                return result

        except json.JSONDecodeError:
            pass

        search_from = (
            marker_index + len(marker)
        )


def extract_next_f_stream(
    html: str,
) -> str:
    """
    Với response HTML đầy đủ của Next.js, RSC payload thường
    nằm trong:

        self.__next_f.push([...])

    Ta decode các array JSON đó rồi nối phần string lại.
    """

    chunks: list[str] = []

    pattern = re.compile(
        r"self\.__next_f\.push\((.*?)\)</script>",
        flags=re.DOTALL,
    )

    for match in pattern.finditer(html):
        payload = match.group(1).strip()

        try:
            decoded = json.loads(payload)
        except json.JSONDecodeError:
            continue

        if (
            isinstance(decoded, list)
            and len(decoded) >= 2
            and isinstance(decoded[1], str)
        ):
            chunks.append(decoded[1])

    return "\n".join(chunks)


def parse_nhadatvui_result(
    response_text: str,
) -> dict:
    """
    Hỗ trợ cả:
    1. RSC response trực tiếp
    2. HTML Next.js có embedded self.__next_f.push(...)
    """

    # Cách 1: response bản thân đã là RSC stream
    result = extract_result_object_from_text(
        response_text
    )

    if result is not None:
        return result

    # Cách 2: response là full HTML
    rsc_stream = extract_next_f_stream(
        response_text
    )

    if rsc_stream:
        result = extract_result_object_from_text(
            rsc_stream
        )

        if result is not None:
            return result

    raise ValueError(
        'Không tìm thấy block "result.data" '
        "trong response của NhaDatVui."
    )


# ============================================================
# REQUEST
# ============================================================

def request_page(
    page: int,
) -> tuple[str, str, int]:

    url = build_page_url(page)

    last_error: Exception | None = None

    for attempt in range(
        1,
        MAX_RETRIES + 1,
    ):
        try:
            response = session.get(
                url,
                timeout=REQUEST_TIMEOUT,
            )

            status = response.status_code

            if status == 403:
                raise RuntimeError(
                    f"Page {page}: HTTP 403. "
                    "Website đang chặn request tự động."
                )

            if status == 429:
                wait_seconds = max(
                    10.0,
                    DELAY_SECONDS * 4,
                )

                print(
                    f"[WARN] Page {page}: HTTP 429. "
                    f"Chờ {wait_seconds:.1f}s..."
                )

                time.sleep(wait_seconds)
                continue

            if status >= 500:
                raise requests.HTTPError(
                    f"HTTP {status}"
                )

            response.raise_for_status()

            # Quan trọng:
            # ép UTF-8 để tránh lỗi kiểu "BÃ¡n nhÃ ..."
            response.encoding = "utf-8"

            return (
                response.text,
                url,
                status,
            )

        except Exception as exc:
            last_error = exc

            # 403 thì không cố bypass
            if "403" in str(exc):
                break

            if attempt < MAX_RETRIES:
                wait_seconds = (
                    max(DELAY_SECONDS, 2.0)
                    * attempt
                )

                print(
                    f"[WARN] Page {page}: "
                    f"lỗi lần {attempt}/{MAX_RETRIES}: "
                    f"{exc}"
                )

                print(
                    f"[WAIT] Retry sau "
                    f"{wait_seconds:.1f}s..."
                )

                time.sleep(wait_seconds)

    raise RuntimeError(
        f"Không lấy được page={page}: "
        f"{last_error}"
    )


# ============================================================
# RAW RECORD
# ============================================================

def enrich_raw_record(
    record: dict,
    page: int,
    source_url: str,
) -> dict:
    """
    Chỉ gắn metadata ingestion.
    KHÔNG chuẩn hóa sang schema 27 cột.
    """

    output = dict(record)

    output["_source"] = "nhadatvui"
    output["_source_page"] = page
    output["_scraped_at"] = now_iso()
    output["_crawl_url"] = source_url

    return output


# ============================================================
# CSV INSPECTION VIEW
# ============================================================

def first_image_url(
    record: dict,
) -> str | None:

    for item in safe_list(
        record.get("attachments")
    ):
        if not isinstance(item, dict):
            continue

        if (
            item.get("type") == "image"
            and item.get("url")
        ):
            return str(item["url"])

    return None


def get_detail(
    record: dict,
    *keys: str,
) -> Any:

    details = safe_dict(
        record.get("details")
    )

    for key in keys:
        if key in details:
            value = details.get(key)

            if value not in (
                None,
                "",
            ):
                return value

    return None


def to_csv_record(
    record: dict,
) -> dict:
    """
    CSV chỉ để nhìn nhanh bằng Excel.
    Full record vẫn nằm trong JSONL.

    Không đưa nguyên content/attachments/createdBy vào CSV
    để file nhẹ và dễ xem.
    """

    product = safe_dict(
        record.get("productId")
    )

    province = safe_dict(
        record.get("provinceCode")
    )

    ward = safe_dict(
        record.get("wardCode")
    )

    attachments = safe_list(
        record.get("attachments")
    )

    return {
        "_source":
            record.get("_source"),

        "_source_page":
            record.get("_source_page"),

        "_scraped_at":
            record.get("_scraped_at"),

        "_crawl_url":
            record.get("_crawl_url"),

        "listing_id":
            record.get("_id"),

        "title":
            record.get("title"),

        "product_name":
            product.get("name"),

        "product_slug":
            product.get("slug"),

        "unit":
            record.get("unit"),

        "area":
            record.get("area"),

        "price":
            record.get("price"),

        "min_price":
            record.get("minPrice"),

        "max_price":
            record.get("maxPrice"),

        "lat":
            record.get("lat"),

        "lng":
            record.get("lng"),

        "province_code":
            province.get("code"),

        "province_name":
            province.get("name"),

        "ward_code":
            ward.get("code"),

        "ward_name":
            ward.get("name"),

        "street_code":
            record.get("streetCode"),

        "address":
            record.get("address"),

        "is_verified":
            record.get("isVerified"),

        "is_sponsored":
            record.get("isSponsored"),

        "is_negotiable":
            record.get("isNegotiable"),

        "status":
            record.get("status"),

        "created_at_ms":
            record.get("createdAt"),

        "public_date_ms":
            record.get("publicDate"),

        "expired_date_ms":
            record.get("expiredDate"),

        "rooms":
            get_detail(
                record,
                "So_phong_ngu",
            ),

        "toilets":
            get_detail(
                record,
                "So_phong_tam,_vi_sinh",
            ),

        "floors":
            get_detail(
                record,
                "So_tang",
            ),

        "frontage_m":
            get_detail(
                record,
                "Mat_tien_(m)",
            ),

        "length_m":
            get_detail(
                record,
                "Chieu_dai_(m)",
            ),

        "road_width_m":
            get_detail(
                record,
                "Đuong_vao_(m)",
                "Duong_vao_(m)",
                "Äuong_vao_(m)",
            ),

        "direction":
            get_detail(
                record,
                "Huong",
            ),

        "legal_document":
            get_detail(
                record,
                "Giay_to_phap_ly",
            ),

        "property_subtype":
            get_detail(
                record,
                "Loai_dat",
                "Loai_can_ho",
            ),

        "image_count":
            len(attachments),

        "first_image_url":
            first_image_url(record),

        # Chỉ số để biết RAW có mô tả,
        # không nhét cả content dài vào CSV.
        "has_content":
            bool(record.get("content")),
    }


# ============================================================
# SAVE
# ============================================================

def save_page_raw(
    page: int,
    response_text: str,
) -> None:

    path = (
        PAGES_DIR
        / f"page_{page:05d}.txt"
    )

    path.write_text(
        response_text,
        encoding="utf-8",
    )


def append_jsonl(
    records: list[dict],
) -> None:

    with JSONL_PATH.open(
        "a",
        encoding="utf-8",
    ) as file:

        for record in records:
            file.write(
                json.dumps(
                    record,
                    ensure_ascii=False,
                )
                + "\n"
            )


def append_csv(
    records: list[dict],
    header_written: bool,
) -> bool:

    if not records:
        return header_written

    csv_records = [
        to_csv_record(record)
        for record in records
    ]

    fieldnames = list(
        csv_records[0].keys()
    )

    mode = (
        "a"
        if header_written
        else "w"
    )

    with CSV_PATH.open(
        mode,
        encoding="utf-8-sig",
        newline="",
    ) as file:

        writer = csv.DictWriter(
            file,
            fieldnames=fieldnames,
        )

        if not header_written:
            writer.writeheader()

        writer.writerows(
            csv_records
        )

    return True


# ============================================================
# MAIN
# ============================================================

def main() -> None:

    if START_PAGE <= 0:
        raise ValueError(
            "START_PAGE phải >= 1."
        )

    if PAGES_TO_CRAWL <= 0:
        raise ValueError(
            "PAGES_TO_CRAWL phải > 0."
        )

    if DELAY_SECONDS < 0:
        raise ValueError(
            "DELAY_SECONDS không được âm."
        )

    OUTPUT_DIR.mkdir(
        parents=True,
        exist_ok=True,
    )

    PAGES_DIR.mkdir(
        parents=True,
        exist_ok=True,
    )

    print("=" * 76)
    print("NHADATVUI.VN RAW CRAWLER")
    print("=" * 76)

    print(
        f"Base URL      : {BASE_URL}"
    )

    print(
        f"Start page    : {START_PAGE}"
    )

    print(
        f"Pages         : {PAGES_TO_CRAWL}"
    )

    print(
        f"Delay         : {DELAY_SECONDS}s"
    )

    print(
        f"Output        : {OUTPUT_DIR}"
    )

    print("=" * 76)

    total_records = 0
    completed_pages: list[int] = []
    failed_pages: list[dict] = []
    seen_listing_ids: set[str] = set()
    header_written = False

    site_total: int | None = None
    site_limit: int | None = None

    last_page = (
        START_PAGE
        + PAGES_TO_CRAWL
        - 1
    )

    for page in range(
        START_PAGE,
        last_page + 1,
    ):
        print()
        print(
            f"[PAGE {page}] Đang crawl..."
        )

        try:
            response_text, source_url, status = (
                request_page(page)
            )

            print(
                f"[HTTP] {status}"
            )

            # Giữ response gốc từng page
            save_page_raw(
                page,
                response_text,
            )

            result = parse_nhadatvui_result(
                response_text
            )

            raw_records = safe_list(
                result.get("data")
            )

            skip = result.get("skip")
            limit = result.get("limit")
            total = result.get("total")
            count = result.get("count")

            if isinstance(total, int):
                site_total = total

            if isinstance(limit, int):
                site_limit = limit

            records = [
                enrich_raw_record(
                    record=record,
                    page=page,
                    source_url=source_url,
                )
                for record in raw_records
                if isinstance(record, dict)
            ]

            duplicate_count = 0

            for record in records:
                listing_id = record.get("_id")

                if listing_id is None:
                    continue

                listing_id = str(
                    listing_id
                )

                if (
                    listing_id
                    in seen_listing_ids
                ):
                    duplicate_count += 1

                seen_listing_ids.add(
                    listing_id
                )

            # RAW không dedup khỏi output.
            append_jsonl(records)

            header_written = append_csv(
                records,
                header_written,
            )

            total_records += len(records)

            completed_pages.append(
                page
            )

            print(
                f"[PAGE {page}] "
                f"records={len(records)} "
                f"| skip={skip} "
                f"| limit={limit} "
                f"| total={total} "
                f"| count={count} "
                f"| duplicate_seen={duplicate_count}"
            )

            # Hết data
            if len(records) == 0:
                print(
                    "[STOP] Trang không còn listing."
                )
                break

            # Theo response hiện tại:
            # page 2 -> skip 18, limit 18.
            # Nếu skip + count >= total thì đã tới cuối.
            if (
                isinstance(skip, int)
                and isinstance(count, int)
                and isinstance(total, int)
                and skip + count >= total
            ):
                print(
                    "[STOP] Đã tới cuối danh sách."
                )
                break

        except Exception as exc:

            failed_pages.append(
                {
                    "page": page,
                    "url": build_page_url(
                        page
                    ),
                    "error": str(exc),
                }
            )

            print(
                f"[ERROR] Page {page}: "
                f"{exc}"
            )

            # 403 thì dừng, không cố bypass.
            if "403" in str(exc):
                print(
                    "[STOP] Website đang chặn "
                    "request tự động."
                )
                break

        if (
            page < last_page
            and DELAY_SECONDS > 0
        ):
            delay = (
                DELAY_SECONDS
                + random.uniform(
                    0,
                    DELAY_JITTER_SECONDS,
                )
            )

            print(
                f"[WAIT] {delay:.2f}s..."
            )

            time.sleep(delay)

    # ========================================================
    # SUMMARY
    # ========================================================

    summary = {
        "source":
            "nhadatvui",

        "base_url":
            BASE_URL,

        "run_id":
            RUN_ID,

        "started_page":
            START_PAGE,

        "requested_pages":
            PAGES_TO_CRAWL,

        "completed_pages":
            completed_pages,

        "failed_pages":
            failed_pages,

        "records_written":
            total_records,

        "unique_listing_ids":
            len(seen_listing_ids),

        "site_total":
            site_total,

        "site_page_limit":
            site_limit,

        "delay_seconds":
            DELAY_SECONDS,

        "finished_at":
            now_iso(),

        "output_jsonl":
            str(JSONL_PATH),

        "output_csv":
            str(CSV_PATH),

        "pages_dir":
            str(PAGES_DIR),
    }

    with SUMMARY_PATH.open(
        "w",
        encoding="utf-8",
    ) as file:

        json.dump(
            summary,
            file,
            ensure_ascii=False,
            indent=2,
        )

    print()
    print("=" * 76)
    print("DONE")
    print("=" * 76)

    print(
        f"Pages OK      : "
        f"{len(completed_pages)}"
    )

    print(
        f"Pages failed  : "
        f"{len(failed_pages)}"
    )

    print(
        f"Records       : "
        f"{total_records}"
    )

    print(
        f"Unique IDs    : "
        f"{len(seen_listing_ids)}"
    )

    print(
        f"Site total    : "
        f"{site_total}"
    )

    print(
        f"Page limit    : "
        f"{site_limit}"
    )

    print(
        f"JSONL         : "
        f"{JSONL_PATH}"
    )

    print(
        f"CSV           : "
        f"{CSV_PATH}"
    )

    print(
        f"Raw pages     : "
        f"{PAGES_DIR}"
    )

    print(
        f"Summary       : "
        f"{SUMMARY_PATH}"
    )

    print("=" * 76)


if __name__ == "__main__":
    main()
