"""
Guland RAW crawler
- Chỉ crawl và lưu dữ liệu RAW.
- KHÔNG chuẩn hóa về schema 27 cột.
- Chỉ cần sửa PAGES_TO_CRAWL và DELAY_SECONDS trong phần CONFIG.
"""

from __future__ import annotations

import csv
import json
import re
import time
from datetime import datetime, timezone
from html import unescape
from pathlib import Path
from urllib.parse import unquote, urljoin

import requests
from bs4 import BeautifulSoup

# ============================================================
# CONFIG - CHỈNH Ở ĐÂY
# ============================================================

BASE_URL = "https://guland.vn/mua-ban-nha-mat-pho-mat-tien"
START_PAGE = 1

# Muốn crawl bao nhiêu page thì sửa số này.
PAGES_TO_CRAWL = 10

# Delay giữa 2 request, đơn vị giây.
DELAY_SECONDS = 2.0

REQUEST_TIMEOUT = 30
MAX_RETRIES = 3

# ============================================================
# OUTPUT
# ============================================================

RUN_ID = datetime.now().strftime("%Y%m%d_%H%M%S")

# File này được đặt tại:
#   TLCN_BDS_Lakehouse/src/ingestion/guland/guland_raw_crawler.py
#
# Tự tìm thư mục gốc project, KHÔNG hard-code ổ D:/C: hay tên user.
# Vì vậy gửi project cho máy khác vẫn chạy được nếu giữ đúng cấu trúc thư mục.
PROJECT_ROOT = Path(__file__).resolve().parents[3]

OUTPUT_DIR = (
    PROJECT_ROOT
    / "data"
    / "incoming"
    / "snapshots"
    / "guland"
    / RUN_ID
)

PAGES_DIR = OUTPUT_DIR / "pages"
JSONL_PATH = OUTPUT_DIR / "guland_raw.jsonl"
CSV_PATH = OUTPUT_DIR / "guland_raw.csv"
SUMMARY_PATH = OUTPUT_DIR / "crawl_summary.json"

session = requests.Session()
session.headers.update({
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/152.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "vi-VN,vi;q=0.9,en-US;q=0.7,en;q=0.6",
    "Connection": "keep-alive",
})


def now_iso() -> str:
    return datetime.now(timezone.utc).astimezone().isoformat(timespec="seconds")


def clean_text(value: str | None) -> str | None:
    if value is None:
        return None
    value = unescape(value)
    value = re.sub(r"\s+", " ", value).strip()
    return value or None


def get_text(node) -> str | None:
    if node is None:
        return None
    return clean_text(node.get_text(" ", strip=True))


def get_xhr_headers() -> dict[str, str]:
    headers = {
        "Accept": "application/json, text/javascript, */*; q=0.01",
        "X-Requested-With": "XMLHttpRequest",
        "Referer": BASE_URL,
        "X-Client-Type": "web_browser",
    }

    # Dùng token session do website cấp, không hard-code token browser.
    xsrf = session.cookies.get("XSRF-TOKEN")
    if xsrf:
        headers["X-XSRF-TOKEN"] = unquote(xsrf)

    return headers


def bootstrap_session() -> None:
    print("[BOOTSTRAP] Mở trang Guland để lấy cookie/session...")
    response = session.get(BASE_URL, timeout=REQUEST_TIMEOUT)
    response.raise_for_status()
    print(f"[BOOTSTRAP] OK | status={response.status_code} | cookies={len(session.cookies)}")


def request_page(page: int) -> dict:
    last_error = None

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            response = session.get(
                BASE_URL,
                params={"page": page},
                headers=get_xhr_headers(),
                timeout=REQUEST_TIMEOUT,
            )

            if response.status_code == 429:
                wait_seconds = max(DELAY_SECONDS * 3, 10)
                print(f"[WARN] Page {page}: HTTP 429. Chờ {wait_seconds:.1f}s rồi thử lại...")
                time.sleep(wait_seconds)
                continue

            response.raise_for_status()
            return response.json()

        except (requests.RequestException, ValueError) as exc:
            last_error = exc
            if attempt < MAX_RETRIES:
                wait_seconds = max(DELAY_SECONDS, 2) * attempt
                print(f"[WARN] Page {page}: lỗi lần {attempt}/{MAX_RETRIES}: {exc}")
                print(f"[WARN] Chờ {wait_seconds:.1f}s rồi thử lại...")
                time.sleep(wait_seconds)

    raise RuntimeError(
        f"Không lấy được page={page} sau {MAX_RETRIES} lần thử: {last_error}"
    )


def extract_lat_lon(card) -> tuple[str | None, str | None]:
    for a in card.select("a[href]"):
        href = a.get("href", "")

        match = re.search(
            r"[?&]lat=(-?\d+(?:\.\d+)?)&lng=(-?\d+(?:\.\d+)?)",
            href,
        )
        if match:
            return match.group(1), match.group(2)

        match = re.search(
            r"query=(-?\d+(?:\.\d+)?),(-?\d+(?:\.\d+)?)",
            href,
        )
        if match:
            return match.group(1), match.group(2)

    return None, None


def extract_listing_id(card) -> str | None:
    node = card.select_one("[data-id]")
    if node and node.get("data-id"):
        return clean_text(node.get("data-id"))

    text = card.get_text(" ", strip=True)
    match = re.search(r"Mã\s*tin\s*:\s*(\d+)", text, re.IGNORECASE)
    if match:
        return match.group(1)

    title_link = card.select_one(".c-sdb-card__tle a[href]")
    if title_link:
        href = title_link.get("href", "")
        match = re.search(r"-(\d+)(?:/)?$", href)
        if match:
            return match.group(1)

    return None


def extract_location_rows(card) -> list[str]:
    rows = []
    for node in card.select(".data-type-adr"):
        text = get_text(node)
        if text:
            rows.append(text)
    return rows


def extract_features(card) -> list[str]:
    feature_box = card.select_one(".c-sdb-card__spc")
    if not feature_box:
        return []

    values = []
    for a in feature_box.select("a"):
        text = get_text(a)
        if text:
            values.append(text)
    return values


def extract_info_values(card) -> list[str]:
    first_info_row = card.select_one(".c-sdb-card__inf .sdb-inf-row")
    if not first_info_row:
        return []

    values = []
    for node in first_info_row.select(".sdb-inf-data"):
        text = get_text(node)
        if text:
            values.append(text)
    return values


def extract_posted_text(card) -> str | None:
    profile = card.select_one(".profile-info__stl")
    if not profile:
        return None
    return get_text(profile.select_one("span"))


def parse_card(card, page: int, card_index: int) -> dict:
    title_link = card.select_one(".c-sdb-card__tle a[href]")
    title = get_text(title_link)

    listing_url = None
    if title_link:
        listing_url = urljoin(BASE_URL, title_link.get("href", ""))

    image_node = card.select_one(
        ".c-sdb-card__img img[data-original], .c-sdb-card__img img[src]"
    )
    image_url = None
    if image_node:
        image_url = image_node.get("data-original") or image_node.get("src")
        if image_url:
            image_url = urljoin(BASE_URL, image_url)

    info_values = extract_info_values(card)
    price_text = info_values[0] if len(info_values) > 0 else None
    area_text = info_values[1] if len(info_values) > 1 else None
    price_per_m2_text = info_values[2] if len(info_values) > 2 else None
    frontage_price_text = info_values[3] if len(info_values) > 3 else None

    lat, lon = extract_lat_lon(card)

    description = get_text(card.select_one(".c-sdb-card__exc"))
    seller_name = get_text(card.select_one(".profile-info__tle"))

    seller_contact_node = card.select_one(".profile-info[data-phone]")
    seller_phone_raw = (
        seller_contact_node.get("data-phone") if seller_contact_node else None
    )

    return {
        # Crawl metadata
        "_source": "guland",
        "_source_page": page,
        "_card_index": card_index,
        "_scraped_at": now_iso(),
        "_crawl_url": f"{BASE_URL}?page={page}",

        # Raw fields lấy từ Guland, chưa chuẩn hóa
        "listing_id": extract_listing_id(card),
        "title": title,
        "listing_url": listing_url,
        "image_url": image_url,
        "price_text": price_text,
        "area_text": area_text,
        "price_per_m2_text": price_per_m2_text,
        "frontage_price_text": frontage_price_text,
        "location_rows": extract_location_rows(card),
        "lat": lat,
        "lon": lon,
        "posted_text": extract_posted_text(card),
        "features": extract_features(card),
        "description": description,
        "seller_name": seller_name,
        "seller_phone_raw": seller_phone_raw,

        # Giữ HTML card để sau này có thể parse lại nếu cần
        "raw_card_html": str(card),
    }


def parse_page_html(html: str, page: int) -> list[dict]:
    soup = BeautifulSoup(html, "html.parser")
    cards = soup.select(".l-sdb-list__single")

    return [
        parse_card(card, page, index)
        for index, card in enumerate(cards, start=1)
    ]


def save_page_response(page: int, payload: dict) -> None:
    path = PAGES_DIR / f"page_{page:05d}.json"
    with path.open("w", encoding="utf-8") as file:
        json.dump(payload, file, ensure_ascii=False, indent=2)


def append_jsonl(records: list[dict]) -> None:
    with JSONL_PATH.open("a", encoding="utf-8") as file:
        for record in records:
            file.write(json.dumps(record, ensure_ascii=False) + "\n")


def csv_safe(value):
    if isinstance(value, (list, dict)):
        return json.dumps(value, ensure_ascii=False)
    return value


def append_csv(records: list[dict], header_written: bool) -> bool:
    if not records:
        return header_written

    mode = "a" if header_written else "w"
    fieldnames = list(records[0].keys())

    with CSV_PATH.open(mode, encoding="utf-8-sig", newline="") as file:
        writer = csv.DictWriter(file, fieldnames=fieldnames)
        if not header_written:
            writer.writeheader()

        for record in records:
            writer.writerow({key: csv_safe(value) for key, value in record.items()})

    return True


def main() -> None:
    if PAGES_TO_CRAWL <= 0:
        raise ValueError("PAGES_TO_CRAWL phải lớn hơn 0.")
    if DELAY_SECONDS < 0:
        raise ValueError("DELAY_SECONDS không được âm.")

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    PAGES_DIR.mkdir(parents=True, exist_ok=True)

    print("=" * 72)
    print("GULAND RAW CRAWLER")
    print("=" * 72)
    print(f"URL           : {BASE_URL}")
    print(f"Start page    : {START_PAGE}")
    print(f"Pages         : {PAGES_TO_CRAWL}")
    print(f"Delay         : {DELAY_SECONDS} giây")
    print(f"Output        : {OUTPUT_DIR}")
    print("=" * 72)

    bootstrap_session()

    total_records = 0
    completed_pages = []
    failed_pages = []
    header_written = False

    last_page = START_PAGE + PAGES_TO_CRAWL - 1

    for page in range(START_PAGE, last_page + 1):
        print(f"\n[PAGE {page}] Đang crawl...")

        try:
            payload = request_page(page)
            save_page_response(page, payload)

            html = payload.get("data", "") or ""
            records = parse_page_html(html, page)

            append_jsonl(records)
            header_written = append_csv(records, header_written)

            total_records += len(records)
            completed_pages.append(page)

            print(
                f"[PAGE {page}] records={len(records)} "
                f"| total_site={payload.get('total')} "
                f"| has_more={payload.get('has_more')}"
            )

            if payload.get("has_more") is False:
                print(f"[STOP] Guland báo has_more=false tại page={page}.")
                break

        except Exception as exc:
            failed_pages.append({"page": page, "error": str(exc)})
            print(f"[ERROR] Page {page}: {exc}")

        if page < last_page and DELAY_SECONDS > 0:
            print(f"[WAIT] {DELAY_SECONDS} giây...")
            time.sleep(DELAY_SECONDS)

    summary = {
        "source": "guland",
        "base_url": BASE_URL,
        "run_id": RUN_ID,
        "start_page": START_PAGE,
        "requested_pages": PAGES_TO_CRAWL,
        "completed_pages": completed_pages,
        "failed_pages": failed_pages,
        "records_written": total_records,
        "delay_seconds": DELAY_SECONDS,
        "finished_at": now_iso(),
        "output_jsonl": str(JSONL_PATH),
        "output_csv": str(CSV_PATH),
        "pages_dir": str(PAGES_DIR),
    }

    with SUMMARY_PATH.open("w", encoding="utf-8") as file:
        json.dump(summary, file, ensure_ascii=False, indent=2)

    print("\n" + "=" * 72)
    print("DONE")
    print("=" * 72)
    print(f"Pages OK      : {len(completed_pages)}")
    print(f"Pages failed  : {len(failed_pages)}")
    print(f"Records       : {total_records}")
    print(f"JSONL         : {JSONL_PATH}")
    print(f"CSV           : {CSV_PATH}")
    print(f"Raw pages     : {PAGES_DIR}")
    print(f"Summary       : {SUMMARY_PATH}")
    print("=" * 72)


if __name__ == "__main__":
    main()
