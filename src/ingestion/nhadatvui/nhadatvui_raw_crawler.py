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
    RUN_NAME        -> đặt tên cố định cho thư mục/file output
    RESUME_RUN_ID   -> để crawl lại từ chỗ dừng nếu bị ngắt giữa chừng
                       (set = giá trị RUN_NAME của lần chạy trước)
"""

from __future__ import annotations

import csv
import json
import logging
import os
import random
import re
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from zoneinfo import ZoneInfo
from typing import Any
from urllib.parse import urlencode

import requests


# ============================================================
# CONFIG - CHỈNH Ở ĐÂY
# ============================================================

BASE_URL = "https://www.nhadatvui.vn/mua-ban-nha-dat"

# Trang bắt đầu
START_PAGE = 1

# Số trang muốn crawl (site có ~16724 tin, limit=18/trang -> cần ~930 trang).
# Giữ cố định giữa các lần crawl; đổi qua biến môi trường CRAWL_PAGES.
PAGES_TO_CRAWL = int(os.getenv("CRAWL_PAGES", "200"))

# Delay cơ bản giữa 2 trang
# Tăng lên so với bản gốc (2.0) vì crawl liên tục ~900+ trang trong 1 lần
# chạy có rủi ro bị rate-limit/block cao hơn crawl vài chục trang.
DELAY_SECONDS = 3.5

# Thêm jitter ngẫu nhiên để request không quá đều
DELAY_JITTER_SECONDS = 1.2

# Timeout
REQUEST_TIMEOUT = 30

# Retry nếu lỗi mạng / 5xx / 429
MAX_RETRIES = 3

# ------------------------------------------------------------
# RESUME
# ------------------------------------------------------------
# Nếu script bị ngắt giữa chừng (mất mạng, máy sleep, bị kill...),
# chạy lại với RESUME_RUN_ID = "<run_id của lần chạy trước>"
# (chính là tên thư mục trong data/incoming/snapshots/nhadatvui/)
# để tiếp tục từ trang chưa crawl, KHÔNG crawl lại từ đầu.
#
# Để trống (None) nếu đây là lần chạy mới.
RESUME_RUN_ID: str | None = None

# ------------------------------------------------------------
# TÊN CHẠY (RUN NAME)
# ------------------------------------------------------------
# Đặt tên cố định cho lần crawl này thay vì để script tự sinh
# theo timestamp. Tên này sẽ dùng làm:
#   - tên thư mục output: data/incoming/snapshots/nhadatvui/<RUN_NAME>/
#   - tên file: <RUN_NAME>.jsonl, <RUN_NAME>.csv
#
# Muốn crawl tiếp (resume) đúng lần chạy này vào lúc khác:
# set RESUME_RUN_ID = giá trị RUN_NAME này.
#
# Để None nếu muốn quay lại kiểu tên tự sinh theo timestamp.
RUN_NAME: str | None = None


# ============================================================
# OUTPUT
# ============================================================

# Thứ tự: RESUME_RUN_ID (crawl tiếp) -> RUN_NAME -> CRAWL_BATCH_ID -> ngày hôm nay
# (giờ Việt Nam). Tên batch phải là YYYYMMDD để job ingest Bronze nhận được.
RUN_ID = (
    RESUME_RUN_ID
    or RUN_NAME
    or os.getenv("CRAWL_BATCH_ID", datetime.now(ZoneInfo("Asia/Ho_Chi_Minh")).strftime("%Y%m%d"))
)
if not re.fullmatch(r"\d{8}", RUN_ID):
    raise ValueError("Batch id (RESUME_RUN_ID / RUN_NAME / CRAWL_BATCH_ID) must be YYYYMMDD")
IS_RESUME = RESUME_RUN_ID is not None

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

# Fixed names: the Bronze snapshot ingest reads <source>_raw.csv.
JSONL_PATH = OUTPUT_DIR / "nhadatvui_raw.jsonl"
CSV_PATH = OUTPUT_DIR / "nhadatvui_raw.csv"
SUMMARY_PATH = OUTPUT_DIR / "crawl_summary.json"
LOG_PATH = OUTPUT_DIR / "crawl.log"


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
# LOGGING
# ============================================================
# Ghi log ra cả console lẫn file trong OUTPUT_DIR, để nếu chạy
# qua đêm (đóng terminal / mất SSH) vẫn xem lại được đã crawl
# tới đâu, lỗi ở trang nào.

logger = logging.getLogger("nhadatvui")
logger.setLevel(logging.INFO)


def setup_logging() -> None:
    logger.handlers.clear()

    formatter = logging.Formatter(
        "%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    file_handler = logging.FileHandler(
        LOG_PATH,
        encoding="utf-8",
        mode="a" if IS_RESUME else "w",
    )
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)


def log(msg: str) -> None:
    logger.info(msg)


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

                log(
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
            # ép UTF-8 để tránh lỗi kiểu "BÃ¡n nhÃ ..."
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

                log(
                    f"[WARN] Page {page}: "
                    f"lỗi lần {attempt}/{MAX_RETRIES}: "
                    f"{exc}"
                )

                log(
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
                "Äuong_vao_(m)",
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

def page_raw_path(page: int) -> Path:
    return PAGES_DIR / f"page_{page:05d}.txt"


def page_already_done(page: int) -> bool:
    """
    Dùng cho resume: nếu file raw của page này đã tồn tại và
    không rỗng, coi như page đã crawl xong ở lần chạy trước
    (JSONL/CSV cũng đã được ghi cùng lúc với file raw đó).
    """

    path = page_raw_path(page)

    return path.exists() and path.stat().st_size > 0


def save_page_raw(
    page: int,
    response_text: str,
) -> None:

    path = page_raw_path(page)

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


def load_existing_listing_ids() -> set[str]:
    """
    Dùng cho resume: đọc lại JSONL cũ (nếu có) để nạp lại
    seen_listing_ids, giúp summary cuối cùng vẫn đúng dù
    chạy nhiều lần nối tiếp nhau.
    """

    ids: set[str] = set()

    if not JSONL_PATH.exists():
        return ids

    with JSONL_PATH.open(
        "r",
        encoding="utf-8",
    ) as file:

        for line in file:
            line = line.strip()

            if not line:
                continue

            try:
                record = json.loads(line)
            except json.JSONDecodeError:
                continue

            listing_id = record.get("_id")

            if listing_id is not None:
                ids.add(str(listing_id))

    return ids


# ============================================================
# MAIN
# ============================================================

def main() -> None:
    # A new run never overwrites a finished batch; a resume continues it.
    if CSV_PATH.exists() and not IS_RESUME:
        raise FileExistsError(f"Existing batch is preserved: {CSV_PATH}")

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

    setup_logging()

    csv_had_rows_before = (
        CSV_PATH.exists()
        and CSV_PATH.stat().st_size > 0
    )

    log("=" * 76)
    log("NHADATVUI.VN RAW CRAWLER")
    log("=" * 76)
    log(f"Run ID        : {RUN_ID}"
        + (" (RESUME)" if IS_RESUME else " (NEW RUN)"))
    log(f"Base URL      : {BASE_URL}")
    log(f"Start page    : {START_PAGE}")
    log(f"Pages         : {PAGES_TO_CRAWL}")
    log(f"Delay         : {DELAY_SECONDS}s")
    log(f"Output        : {OUTPUT_DIR}")
    log("=" * 76)

    total_records = 0
    completed_pages: list[int] = []
    failed_pages: list[dict] = []
    skipped_pages: list[int] = []
    seen_listing_ids: set[str] = load_existing_listing_ids()
    header_written = csv_had_rows_before

    site_total: int | None = None
    site_limit: int | None = None

    last_page = (
        START_PAGE
        + PAGES_TO_CRAWL
        - 1
    )

    stop_reason = "requested range completed"

    # Toàn bộ vòng crawl bọc trong try/finally để summary luôn
    # được ghi ra, kể cả khi có lỗi bất ngờ (vd: hết dung lượng
    # đĩa, Ctrl+C, lỗi không lường trước) khiến vòng lặp dừng
    # giữa chừng.
    try:
        for page in range(
            START_PAGE,
            last_page + 1,
        ):
            log("")

            # ----- RESUME: bỏ qua page đã crawl xong -----
            if IS_RESUME and page_already_done(page):
                skipped_pages.append(page)
                completed_pages.append(page)
                log(f"[PAGE {page}] Đã có từ lần chạy trước, bỏ qua.")
                continue

            log(f"[PAGE {page}] Đang crawl...")

            try:
                response_text, source_url, status = (
                    request_page(page)
                )

                log(f"[HTTP] {status}")

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

                log(
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
                    stop_reason = "empty page (no more listings)"
                    log("[STOP] Trang không còn listing.")
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
                    stop_reason = "reached end of listing (skip+count>=total)"
                    log("[STOP] Đã tới cuối danh sách.")
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

                log(
                    f"[ERROR] Page {page}: "
                    f"{exc}"
                )

                # 403 thì dừng, không cố bypass.
                if "403" in str(exc):
                    stop_reason = "blocked by site (HTTP 403)"
                    log(
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

                log(f"[WAIT] {delay:.2f}s...")

                time.sleep(delay)

    except KeyboardInterrupt:
        stop_reason = "interrupted by user (Ctrl+C)"
        log("[STOP] Bị ngắt bởi người dùng (Ctrl+C).")

    except Exception as exc:
        stop_reason = f"unexpected error: {exc}"
        log(f"[FATAL] Lỗi không lường trước: {exc}")

    finally:
        # ====================================================
        # SUMMARY - luôn ghi, kể cả khi crawl bị ngắt/lỗi giữa chừng
        # ====================================================

        summary = {
            "source":
                "nhadatvui",

            "base_url":
                BASE_URL,

            "run_id":
                RUN_ID,

            "is_resume":
                IS_RESUME,

            "started_page":
                START_PAGE,

            "requested_pages":
                PAGES_TO_CRAWL,

            "completed_pages":
                completed_pages,

            "skipped_pages_resume":
                skipped_pages,

            "failed_pages":
                failed_pages,

            "stop_reason":
                stop_reason,

            "records_written_this_run":
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

            "resume_instructions": (
                f'Nếu cần crawl tiếp: set RESUME_RUN_ID = "{RUN_ID}" '
                "rồi chạy lại script."
            ),
        }

        try:
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
        except Exception as exc:
            log(f"[ERROR] Không ghi được summary: {exc}")

        log("")
        log("=" * 76)
        log("DONE")
        log("=" * 76)
        log(f"Stop reason    : {stop_reason}")
        log(f"Pages OK       : {len(completed_pages)}")
        log(f"Pages skipped  : {len(skipped_pages)} (resume)")
        log(f"Pages failed   : {len(failed_pages)}")
        log(f"Records (run)  : {total_records}")
        log(f"Unique IDs     : {len(seen_listing_ids)}")
        log(f"Site total     : {site_total}")
        log(f"Page limit     : {site_limit}")
        log(f"JSONL          : {JSONL_PATH}")
        log(f"CSV            : {CSV_PATH}")
        log(f"Raw pages      : {PAGES_DIR}")
        log(f"Summary        : {SUMMARY_PATH}")
        log(f"Log file       : {LOG_PATH}")
        log("=" * 76)

        if failed_pages:
            log(
                f"[NOTE] Có {len(failed_pages)} trang lỗi. "
                f'Xem "failed_pages" trong {SUMMARY_PATH.name} '
                "để crawl bù."
            )


if __name__ == "__main__":
    main()