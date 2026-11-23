from __future__ import annotations

import csv
import json
import re
import sys
import time
from datetime import datetime, timezone
from html import unescape
from pathlib import Path
from urllib.parse import unquote, urljoin

import requests
from bs4 import BeautifulSoup

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
    write_progress,
)


BASE_URL = "https://guland.vn/mua-ban-nha-mat-pho-mat-tien"
START_PAGE = integer_setting("CRAWL_START_PAGE", 1, maximum=10000)


PAGES_TO_CRAWL = integer_setting("CRAWL_PAGES", 20, maximum=5000)


DELAY_SECONDS = float_setting("CRAWL_DELAY_SECONDS", 2)

REQUEST_TIMEOUT = integer_setting("CRAWL_REQUEST_TIMEOUT", 30, maximum=60)
MAX_RETRIES = integer_setting("CRAWL_MAX_RETRIES", 3, maximum=5)
MAX_CONSECUTIVE_FAILURES = integer_setting("CRAWL_MAX_CONSECUTIVE_FAILURES", 2, maximum=5)
RUN_TIMEOUT_SECONDS = integer_setting("CRAWL_RUN_TIMEOUT_SECONDS", 2100)


RUN_ID = batch_id()
if not re.fullmatch(r"\d{8}", RUN_ID):
    raise ValueError("CRAWL_BATCH_ID must be YYYYMMDD")


OUTPUT_DIR = output_directory("guland", RUN_ID)

PAGES_DIR = OUTPUT_DIR / "pages"
JSONL_PATH = OUTPUT_DIR / "guland_raw.jsonl"
CSV_PATH = OUTPUT_DIR / "guland_raw.csv"
SUMMARY_PATH = OUTPUT_DIR / "crawl_summary.json"

session = requests.Session()
session.headers.update(
    {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/152.0.0.0 Safari/537.36"
        ),
        "Accept-Language": "vi-VN,vi;q=0.9,en-US;q=0.7,en;q=0.6",
        "Connection": "keep-alive",
    }
)


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

    xsrf = session.cookies.get("XSRF-TOKEN")
    if xsrf:
        headers["X-XSRF-TOKEN"] = unquote(xsrf)

    return headers


def bootstrap_session() -> None:
    response = request_response(
        session, BASE_URL, timeout=REQUEST_TIMEOUT, retries=MAX_RETRIES, delay=DELAY_SECONDS
    )
    print(f"[BOOTSTRAP] HTTP {response.status_code}; cookies={len(session.cookies)}", flush=True)


def request_page(page: int) -> dict:
    response = request_response(
        session,
        BASE_URL,
        params={"page": page},
        headers=get_xhr_headers,
        timeout=REQUEST_TIMEOUT,
        retries=MAX_RETRIES,
        delay=DELAY_SECONDS,
    )
    payload = response.json()
    if not isinstance(payload, dict) or not isinstance(payload.get("data"), str):
        raise ValueError("Unexpected Guland payload: HTML listing data is missing")
    if "has_more" in payload and type(payload["has_more"]) is not bool:
        raise ValueError("Invalid Guland has_more flag")
    return payload


def extract_lat_lon(card) -> tuple[str | None, str | None]:
    for a in card.select("a[href]"):
        href = a.get("href", "")

        match = re.search(r"[?&]lat=(-?\d+(?:\.\d+)?)&lng=(-?\d+(?:\.\d+)?)", href)
        if match:
            return match.group(1), match.group(2)

        match = re.search(r"query=(-?\d+(?:\.\d+)?),(-?\d+(?:\.\d+)?)", href)
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


def _extract_texts(nodes) -> list[str]:
    return [text for node in nodes if (text := get_text(node))]


def extract_location_rows(card) -> list[str]:
    return _extract_texts(card.select(".data-type-adr"))


def extract_features(card) -> list[str]:
    feature_box = card.select_one(".c-sdb-card__spc")
    return _extract_texts(feature_box.select("a")) if feature_box else []


def extract_info_values(card) -> list[str]:
    first_info_row = card.select_one(".c-sdb-card__inf .sdb-inf-row")
    return _extract_texts(first_info_row.select(".sdb-inf-data")) if first_info_row else []


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

    image_node = card.select_one(".c-sdb-card__img img[data-original], .c-sdb-card__img img[src]")
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
    seller_phone_raw = seller_contact_node.get("data-phone") if seller_contact_node else None

    return {
        "_source": "guland",
        "_source_page": page,
        "_card_index": card_index,
        "_scraped_at": now_iso(),
        "_crawl_url": f"{BASE_URL}?page={page}",
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
        "raw_card_html": str(card),
    }


def parse_page_html(html: str, page: int) -> list[dict]:
    soup = BeautifulSoup(html, "html.parser")
    cards = soup.select(".l-sdb-list__single")

    return [parse_card(card, page, index) for index, card in enumerate(cards, start=1)]


def save_page_response(page: int, payload: dict) -> None:
    path = PAGES_DIR / f"page_{page:05d}.json"
    with path.open("w", encoding="utf-8") as file:
        json.dump(payload, file, ensure_ascii=False, indent=2)


def append_jsonl(records: list[dict]) -> None:
    append_raw_jsonl(JSONL_PATH, records)


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


def main() -> int:
    if CSV_PATH.exists():
        raise FileExistsError(f"Existing batch is preserved: {CSV_PATH}")
    if PAGES_TO_CRAWL <= 0 or DELAY_SECONDS < 0:
        raise ValueError("Pages must be positive and delay non-negative")
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    PAGES_DIR.mkdir(parents=True, exist_ok=True)
    completed, failed, fingerprints = [], [], set()
    total_records, consecutive = 0, 0
    header_written = False
    started = time.monotonic()
    stop_reason = "requested page limit reached"
    end_of_listing = False
    write_progress(SUMMARY_PATH, "guland", RUN_ID, completed, total_records)
    try:
        bootstrap_session()
        for page in range(START_PAGE, START_PAGE + PAGES_TO_CRAWL):
            if time.monotonic() - started >= RUN_TIMEOUT_SECONDS:
                stop_reason = "run time budget exceeded"
                break
            try:
                payload = request_page(page)
                save_page_response(page, payload)
                records = parse_page_html(payload["data"], page)
                at_end = payload.get("has_more") is False
                if not records and (not at_end or not total_records):
                    raise ValueError("Empty/unparseable listing page before a verified end")
                if records:
                    fingerprint = page_fingerprint(records)
                    if fingerprint in fingerprints:
                        stop_reason = "repeated listing page"
                        failed.append({"page": page, "error": stop_reason})
                        break
                    fingerprints.add(fingerprint)
                append_jsonl(records)
                header_written = append_csv(records, header_written)
                total_records += len(records)
                completed.append(page)
                consecutive = 0
                write_progress(SUMMARY_PATH, "guland", RUN_ID, completed, total_records)
                print(
                    f"[PAGE {page}] records={len(records)}; total={total_records}; has_more={payload.get('has_more')}",
                    flush=True,
                )
                if at_end:
                    end_of_listing = True
                    stop_reason = "verified end of listing"
                    break
            except Exception as exc:
                failed.append({"page": page, "error": str(exc)})
                consecutive += 1
                print(f"[ERROR] Page {page}: {exc}", flush=True)
                if isinstance(exc, CrawlBlocked):
                    stop_reason = "blocked or rate limited by site"
                    break
                if consecutive >= MAX_CONSECUTIVE_FAILURES:
                    stop_reason = "consecutive failure limit reached"
                    break
            if page < START_PAGE + PAGES_TO_CRAWL - 1 and DELAY_SECONDS:
                time.sleep(DELAY_SECONDS)
    except KeyboardInterrupt:
        stop_reason = "interrupted"
    except Exception as exc:
        stop_reason = "bootstrap or unexpected error"
        failed.append({"page": START_PAGE, "error": str(exc)})
    finally:
        status = (
            "PASS"
            if total_records > 0
            and not failed
            and stop_reason in ("requested page limit reached", "verified end of listing")
            else "FAIL"
        )
        summary = {
            "crawl_contract_version": 1,
            "source": "guland",
            "base_url": BASE_URL,
            "run_id": RUN_ID,
            "status": status,
            "start_page": START_PAGE,
            "requested_pages": PAGES_TO_CRAWL,
            "completed_pages": completed,
            "failed_pages": failed,
            "records_written": total_records,
            "stop_reason": stop_reason,
            "end_of_listing": end_of_listing,
            "max_consecutive_failures": MAX_CONSECUTIVE_FAILURES,
            "elapsed_seconds": round(time.monotonic() - started, 2),
            "finished_at": now_iso(),
            "output_jsonl": str(JSONL_PATH),
            "output_csv": str(CSV_PATH),
            "pages_dir": str(PAGES_DIR),
        }
        write_json_atomic(SUMMARY_PATH, summary)
        print(f"[DONE] status={status}; records={total_records}; reason={stop_reason}", flush=True)
    return 0 if status == "PASS" else 1


if __name__ == "__main__":
    raise SystemExit(main())
