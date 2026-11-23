from __future__ import annotations

import csv
import json
import logging
import random
import re
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import urlencode

import requests

PROJECT_ROOT = Path(__file__).resolve().parents[3]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))
from src.common.atomic_io import write_json_atomic
from src.ingestion.crawl_runtime import (
    CrawlBlocked,
    append_raw_jsonl,
    batch_id,
    integer_setting,
    float_setting,
    output_directory,
    page_fingerprint,
    request_response,
    validate_nhadatvui_page,
    write_progress,
)


BASE_URL = "https://www.nhadatvui.vn/mua-ban-nha-dat"


START_PAGE = integer_setting("CRAWL_START_PAGE", 1, maximum=10000)


PAGES_TO_CRAWL = integer_setting("CRAWL_PAGES", 200, maximum=5000)


DELAY_SECONDS = float_setting("CRAWL_DELAY_SECONDS", 3.5)


DELAY_JITTER_SECONDS = 1.2


REQUEST_TIMEOUT = integer_setting("CRAWL_REQUEST_TIMEOUT", 30, maximum=60)


MAX_RETRIES = integer_setting("CRAWL_MAX_RETRIES", 3, maximum=5)
MAX_CONSECUTIVE_FAILURES = integer_setting("CRAWL_MAX_CONSECUTIVE_FAILURES", 2, maximum=5)
RUN_TIMEOUT_SECONDS = integer_setting("CRAWL_RUN_TIMEOUT_SECONDS", 2100)


RESUME_RUN_ID: str | None = None


RUN_NAME: str | None = None


RUN_ID = RESUME_RUN_ID or RUN_NAME or batch_id()
if not re.fullmatch(r"\d{8}", RUN_ID):
    raise ValueError("Batch id (RESUME_RUN_ID / RUN_NAME / CRAWL_BATCH_ID) must be YYYYMMDD")
IS_RESUME = RESUME_RUN_ID is not None


OUTPUT_DIR = output_directory("nhadatvui", RUN_ID)

PAGES_DIR = OUTPUT_DIR / "pages"


JSONL_PATH = OUTPUT_DIR / "nhadatvui_raw.jsonl"
CSV_PATH = OUTPUT_DIR / "nhadatvui_raw.csv"
SUMMARY_PATH = OUTPUT_DIR / "crawl_summary.json"
LOG_PATH = OUTPUT_DIR / "crawl.log"


session = requests.Session()

session.headers.update(
    {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/152.0.0.0 Safari/537.36"
        ),
        "Accept": (
            "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8"
        ),
        "Accept-Language": ("vi-VN,vi;q=0.9,en-US;q=0.7,en;q=0.6"),
        "Cache-Control": "no-cache",
        "Pragma": "no-cache",
        "Connection": "keep-alive",
        "Upgrade-Insecure-Requests": "1",
    }
)


logger = logging.getLogger("nhadatvui")
logger.setLevel(logging.INFO)


def setup_logging() -> None:
    logger.handlers.clear()

    formatter = logging.Formatter(
        "%(asctime)s [%(levelname)s] %(message)s", datefmt="%Y-%m-%d %H:%M:%S"
    )

    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    file_handler = logging.FileHandler(LOG_PATH, encoding="utf-8", mode="a" if IS_RESUME else "w")
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)


def log(msg: str) -> None:
    logger.info(msg)


def now_iso() -> str:
    return datetime.now(timezone.utc).astimezone().isoformat(timespec="seconds")


def build_page_url(page: int) -> str:
    return f"{BASE_URL}?" + urlencode({"page": page})


def safe_dict(value: Any) -> dict:
    return value if isinstance(value, dict) else {}


def safe_list(value: Any) -> list:
    return value if isinstance(value, list) else []


def extract_result_object_from_text(text: str) -> dict | None:
    marker = '"result":'
    search_from = 0
    decoder = json.JSONDecoder()

    while True:
        marker_index = text.find(marker, search_from)

        if marker_index == -1:
            return None

        object_start = text.find("{", marker_index + len(marker))

        if object_start == -1:
            return None

        try:
            result, _ = decoder.raw_decode(text[object_start:])

            if (
                isinstance(result, dict)
                and isinstance(result.get("data"), list)
                and "total" in result
                and "count" in result
            ):
                return result

        except json.JSONDecodeError:
            pass

        search_from = marker_index + len(marker)


def extract_next_f_stream(html: str) -> str:
    chunks: list[str] = []

    pattern = re.compile(r"self\.__next_f\.push\((.*?)\)</script>", flags=re.DOTALL)

    for match in pattern.finditer(html):
        payload = match.group(1).strip()

        try:
            decoded = json.loads(payload)
        except json.JSONDecodeError:
            continue

        if isinstance(decoded, list) and len(decoded) >= 2 and isinstance(decoded[1], str):
            chunks.append(decoded[1])

    return "\n".join(chunks)


def parse_nhadatvui_result(response_text: str) -> dict:
    result = extract_result_object_from_text(response_text)

    if result is not None:
        return result

    rsc_stream = extract_next_f_stream(response_text)

    if rsc_stream:
        result = extract_result_object_from_text(rsc_stream)

        if result is not None:
            return result

    raise ValueError('Không tìm thấy block "result.data" trong response của NhaDatVui.')


def request_page(page: int) -> tuple[str, str, int]:
    url = build_page_url(page)
    response = request_response(
        session, url, timeout=REQUEST_TIMEOUT, retries=MAX_RETRIES, delay=DELAY_SECONDS
    )
    response.encoding = "utf-8"
    return response.text, url, response.status_code


def enrich_raw_record(record: dict, page: int, source_url: str) -> dict:
    output = dict(record)

    output["_source"] = "nhadatvui"
    output["_source_page"] = page
    output["_scraped_at"] = now_iso()
    output["_crawl_url"] = source_url

    return output


def first_image_url(record: dict) -> str | None:
    for item in safe_list(record.get("attachments")):
        if not isinstance(item, dict):
            continue

        if item.get("type") == "image" and item.get("url"):
            return str(item["url"])

    return None


def get_detail(record: dict, *keys: str) -> Any:
    details = safe_dict(record.get("details"))

    for key in keys:
        if key in details:
            value = details.get(key)

            if value not in (None, ""):
                return value

    return None


def to_csv_record(record: dict) -> dict:
    product = safe_dict(record.get("productId"))

    province = safe_dict(record.get("provinceCode"))

    ward = safe_dict(record.get("wardCode"))

    attachments = safe_list(record.get("attachments"))

    return {
        "_source": record.get("_source"),
        "_source_page": record.get("_source_page"),
        "_scraped_at": record.get("_scraped_at"),
        "_crawl_url": record.get("_crawl_url"),
        "listing_id": record.get("_id"),
        "title": record.get("title"),
        "product_name": product.get("name"),
        "product_slug": product.get("slug"),
        "unit": record.get("unit"),
        "area": record.get("area"),
        "price": record.get("price"),
        "min_price": record.get("minPrice"),
        "max_price": record.get("maxPrice"),
        "lat": record.get("lat"),
        "lng": record.get("lng"),
        "province_code": province.get("code"),
        "province_name": province.get("name"),
        "ward_code": ward.get("code"),
        "ward_name": ward.get("name"),
        "street_code": record.get("streetCode"),
        "address": record.get("address"),
        "is_verified": record.get("isVerified"),
        "is_sponsored": record.get("isSponsored"),
        "is_negotiable": record.get("isNegotiable"),
        "status": record.get("status"),
        "created_at_ms": record.get("createdAt"),
        "public_date_ms": record.get("publicDate"),
        "expired_date_ms": record.get("expiredDate"),
        "rooms": get_detail(record, "So_phong_ngu"),
        "toilets": get_detail(record, "So_phong_tam,_vi_sinh"),
        "floors": get_detail(record, "So_tang"),
        "frontage_m": get_detail(record, "Mat_tien_(m)"),
        "length_m": get_detail(record, "Chieu_dai_(m)"),
        "road_width_m": get_detail(record, "Đuong_vao_(m)", "Duong_vao_(m)", "Äuong_vao_(m)"),
        "direction": get_detail(record, "Huong"),
        "legal_document": get_detail(record, "Giay_to_phap_ly"),
        "property_subtype": get_detail(record, "Loai_dat", "Loai_can_ho"),
        "image_count": len(attachments),
        "first_image_url": first_image_url(record),
        "has_content": bool(record.get("content")),
    }


def page_raw_path(page: int) -> Path:
    return PAGES_DIR / f"page_{page:05d}.txt"


def page_already_done(page: int) -> bool:
    path = page_raw_path(page)
    if not path.exists() or not path.stat().st_size or not SUMMARY_PATH.exists():
        return False
    try:
        summary = json.loads(SUMMARY_PATH.read_text(encoding="utf-8"))
    except (ValueError, OSError):
        return False
    return page in summary.get("completed_pages", [])


def save_page_raw(page: int, response_text: str) -> None:
    path = page_raw_path(page)

    path.write_text(response_text, encoding="utf-8")


def append_jsonl(records: list[dict]) -> None:
    append_raw_jsonl(JSONL_PATH, records)


def append_csv(records: list[dict], header_written: bool) -> bool:
    if not records:
        return header_written

    csv_records = [to_csv_record(record) for record in records]

    fieldnames = list(csv_records[0].keys())

    mode = "a" if header_written else "w"

    with CSV_PATH.open(mode, encoding="utf-8-sig", newline="") as file:
        writer = csv.DictWriter(file, fieldnames=fieldnames)

        if not header_written:
            writer.writeheader()

        writer.writerows(csv_records)

    return True


def load_existing_listing_ids() -> set[str]:
    ids: set[str] = set()

    if not JSONL_PATH.exists():
        return ids

    with JSONL_PATH.open("r", encoding="utf-8") as file:
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


def fetch_listing_page(page):
    response_text, source_url, status = request_page(page)

    log(f"[HTTP] {status}")

    save_page_raw(page, response_text)

    result = parse_nhadatvui_result(response_text)

    at_end = validate_nhadatvui_page(result, page)
    raw_records = safe_list(result.get("data"))

    records = [
        enrich_raw_record(record=record, page=page, source_url=source_url)
        for record in raw_records
        if isinstance(record, dict)
    ]
    return result, records, at_end


def publish_raw_page(records, header_written, seen_listing_ids):
    duplicate_count = 0

    for record in records:
        listing_id = record.get("_id")

        if listing_id is None:
            continue

        listing_id = str(listing_id)

        if listing_id in seen_listing_ids:
            duplicate_count += 1

        seen_listing_ids.add(listing_id)

    append_jsonl(records)

    header_written = append_csv(records, header_written)
    return header_written, duplicate_count


def main() -> int:
    if CSV_PATH.exists() and not IS_RESUME:
        raise FileExistsError(f"Existing batch is preserved: {CSV_PATH}")

    if START_PAGE <= 0:
        raise ValueError("START_PAGE phải >= 1.")

    if PAGES_TO_CRAWL <= 0:
        raise ValueError("PAGES_TO_CRAWL phải > 0.")

    if DELAY_SECONDS < 0:
        raise ValueError("DELAY_SECONDS không được âm.")

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    PAGES_DIR.mkdir(parents=True, exist_ok=True)

    setup_logging()

    csv_had_rows_before = CSV_PATH.exists() and CSV_PATH.stat().st_size > 0
    existing_rows = 0
    if csv_had_rows_before:
        with CSV_PATH.open(encoding="utf-8-sig", newline="") as handle:
            existing_rows = sum(1 for _ in csv.DictReader(handle))

    log("=" * 76)
    log("NHADATVUI.VN RAW CRAWLER")
    log("=" * 76)
    log(f"Run ID        : {RUN_ID}" + (" (RESUME)" if IS_RESUME else " (NEW RUN)"))
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

    last_page = START_PAGE + PAGES_TO_CRAWL - 1

    stop_reason = "requested range completed"
    end_of_listing = False
    consecutive = 0
    fingerprints = set()
    started = time.monotonic()
    previous_completed = []
    if IS_RESUME and SUMMARY_PATH.exists():
        previous_completed = json.loads(SUMMARY_PATH.read_text(encoding="utf-8")).get(
            "completed_pages", []
        )
    write_progress(SUMMARY_PATH, "nhadatvui", RUN_ID, previous_completed, existing_rows)

    try:
        for page in range(START_PAGE, last_page + 1):
            if time.monotonic() - started >= RUN_TIMEOUT_SECONDS:
                stop_reason = "run time budget exceeded"
                break
            log("")

            if IS_RESUME and page_already_done(page):
                skipped_pages.append(page)
                completed_pages.append(page)
                log(f"[PAGE {page}] Đã có từ lần chạy trước, bỏ qua.")
                continue

            log(f"[PAGE {page}] Đang crawl...")

            try:
                result, records, at_end = fetch_listing_page(page)
                skip, limit, total, count = (
                    result.get(k) for k in ("skip", "limit", "total", "count")
                )
                if isinstance(total, int):
                    site_total = total
                if isinstance(limit, int):
                    site_limit = limit

                if records:
                    fingerprint = page_fingerprint(records)
                    if fingerprint in fingerprints:
                        stop_reason = "repeated listing page"
                        failed_pages.append({"page": page, "error": stop_reason})
                        break
                    fingerprints.add(fingerprint)
                consecutive = 0
                header_written, duplicate_count = publish_raw_page(
                    records, header_written, seen_listing_ids
                )

                total_records += len(records)

                completed_pages.append(page)

                log(
                    f"[PAGE {page}] "
                    f"records={len(records)} "
                    f"| skip={skip} "
                    f"| limit={limit} "
                    f"| total={total} "
                    f"| count={count} "
                    f"| duplicate_seen={duplicate_count}"
                )

                write_progress(
                    SUMMARY_PATH,
                    "nhadatvui",
                    RUN_ID,
                    sorted(set(previous_completed + completed_pages)),
                    existing_rows + total_records,
                )
                if len(records) == 0:
                    end_of_listing = at_end
                    stop_reason = "empty page (no more listings)"
                    log("[STOP] Trang không còn listing.")
                    break

                if (
                    isinstance(skip, int)
                    and isinstance(count, int)
                    and isinstance(total, int)
                    and skip + count >= total
                ):
                    end_of_listing = at_end
                    stop_reason = "reached end of listing (skip+count>=total)"
                    log("[STOP] Đã tới cuối danh sách.")
                    break

            except Exception as exc:
                failed_pages.append({"page": page, "url": build_page_url(page), "error": str(exc)})

                log(f"[ERROR] Page {page}: {exc}")

                consecutive += 1
                if isinstance(exc, CrawlBlocked):
                    stop_reason = "blocked or rate limited by site"
                    log("[STOP] Website đang chặn request tự động.")
                    break
                if consecutive >= MAX_CONSECUTIVE_FAILURES:
                    stop_reason = "consecutive failure limit reached"
                    break

            if page < last_page and DELAY_SECONDS > 0:
                delay = DELAY_SECONDS + random.uniform(0, DELAY_JITTER_SECONDS)

                log(f"[WAIT] {delay:.2f}s...")

                time.sleep(delay)

    except KeyboardInterrupt:
        stop_reason = "interrupted by user (Ctrl+C)"
        log("[STOP] Bị ngắt bởi người dùng (Ctrl+C).")

    except Exception as exc:
        stop_reason = f"unexpected error: {exc}"
        log(f"[FATAL] Lỗi không lường trước: {exc}")

    finally:
        summary = {
            "crawl_contract_version": 1,
            "records_written": existing_rows + total_records,
            "status": "PASS"
            if existing_rows + total_records > 0
            and not failed_pages
            and stop_reason
            in {
                "requested range completed",
                "empty page (no more listings)",
                "reached end of listing (skip+count>=total)",
            }
            else "FAIL",
            "source": "nhadatvui",
            "base_url": BASE_URL,
            "run_id": RUN_ID,
            "is_resume": IS_RESUME,
            "end_of_listing": end_of_listing,
            "elapsed_seconds": round(time.monotonic() - started, 2),
            "max_consecutive_failures": MAX_CONSECUTIVE_FAILURES,
            "started_page": START_PAGE,
            "requested_pages": PAGES_TO_CRAWL,
            "completed_pages": completed_pages,
            "skipped_pages_resume": skipped_pages,
            "failed_pages": failed_pages,
            "stop_reason": stop_reason,
            "records_written_this_run": total_records,
            "unique_listing_ids": len(seen_listing_ids),
            "site_total": site_total,
            "site_page_limit": site_limit,
            "delay_seconds": DELAY_SECONDS,
            "finished_at": now_iso(),
            "output_jsonl": str(JSONL_PATH),
            "output_csv": str(CSV_PATH),
            "pages_dir": str(PAGES_DIR),
            "resume_instructions": (
                f'Nếu cần crawl tiếp: set RESUME_RUN_ID = "{RUN_ID}" rồi chạy lại script.'
            ),
        }

        write_json_atomic(SUMMARY_PATH, summary)

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

    return 0 if summary["status"] == "PASS" else 1


if __name__ == "__main__":
    raise SystemExit(main())
