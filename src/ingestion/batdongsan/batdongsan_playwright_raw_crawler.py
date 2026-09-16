"""
Batdongsan.com.vn RAW crawler - Playwright version
==================================================

Mục đích:
- Mở Batdongsan.com.vn bằng trình duyệt thật (Chrome/Chromium).
- Crawl dữ liệu RAW từ các trang danh sách.
- KHÔNG chuẩn hóa về schema 27 cột.
- KHÔNG cố vượt CAPTCHA / anti-bot. Nếu website hiện challenge,
  crawler sẽ dừng để tránh lấy dữ liệu sai.

Chỉ cần chỉnh ở phần CONFIG:
- START_URL
- START_PAGE
- PAGES_TO_CRAWL
- DELAY_SECONDS

Ví dụ:
START_PAGE = 1
PAGES_TO_CRAWL = 15

=> crawl page 1 -> page 15.
"""

from __future__ import annotations

import csv
import json
import re
import time
from datetime import datetime, timezone
from html import unescape
from pathlib import Path
from urllib.parse import urljoin, urlsplit, urlunsplit

from bs4 import BeautifulSoup
from playwright.sync_api import (
    TimeoutError as PlaywrightTimeoutError,
    sync_playwright,
)


# ============================================================
# CONFIG - CHỈNH Ở ĐÂY
# ============================================================

START_URL = (
    "https://batdongsan.com.vn/ban-can-ho-chung-cu"
    "?clds=650,362,44,45,48"
)

# Trang bắt đầu
START_PAGE = 1

# Muốn crawl bao nhiêu trang thì sửa số này
PAGES_TO_CRAWL = 5

# Delay giữa 2 trang, đơn vị giây
DELAY_SECONDS = 2.0

# Timeout khi load trang, milliseconds
PAGE_TIMEOUT_MS = 60_000

# Chờ listing xuất hiện, milliseconds
LISTING_WAIT_MS = 20_000

# False = hiện trình duyệt để website xử lý như người dùng bình thường.
# Khuyên giữ False.
HEADLESS = False

# True = ưu tiên Chrome cài trên máy.
# Nếu không mở được Chrome, code sẽ fallback sang Chromium của Playwright.
USE_INSTALLED_CHROME = True


# ============================================================
# OUTPUT
# ============================================================

RUN_ID = datetime.now().strftime("%Y%m%d_%H%M%S")

OUTPUT_DIR = (
    Path("data/incoming/snapshots/batdongsan")
    / RUN_ID
)

PAGES_DIR = OUTPUT_DIR / "pages"

JSONL_PATH = OUTPUT_DIR / "batdongsan_raw.jsonl"
CSV_PATH = OUTPUT_DIR / "batdongsan_raw.csv"
SUMMARY_PATH = OUTPUT_DIR / "crawl_summary.json"


# ============================================================
# UTILS
# ============================================================

def now_iso() -> str:
    return (
        datetime.now(timezone.utc)
        .astimezone()
        .isoformat(timespec="seconds")
    )


def clean_text(value: str | None) -> str | None:
    if value is None:
        return None

    value = unescape(value)
    value = re.sub(r"\s+", " ", value).strip()

    return value or None


def get_text(node) -> str | None:
    if node is None:
        return None

    return clean_text(
        node.get_text(" ", strip=True)
    )


def first_attr(node, names: list[str]) -> str | None:
    if node is None:
        return None

    for name in names:
        value = node.get(name)

        if value:
            return clean_text(
                str(value)
            )

    return None


# ============================================================
# URL
# ============================================================

def normalize_start_url(url: str) -> str:
    """
    Nếu START_URL đang là /p2, /p3...
    thì tự bỏ /pN để lấy base URL.
    """

    parts = urlsplit(
        url.strip()
    )

    path = re.sub(
        r"/p\d+/?$",
        "",
        parts.path.rstrip("/"),
        flags=re.IGNORECASE,
    )

    return urlunsplit(
        (
            parts.scheme,
            parts.netloc,
            path,
            parts.query,
            "",
        )
    )


BASE_URL = normalize_start_url(
    START_URL
)


def build_page_url(page_number: int) -> str:
    parts = urlsplit(
        BASE_URL
    )

    base_path = (
        parts.path.rstrip("/")
    )

    if page_number <= 1:
        page_path = base_path
    else:
        page_path = (
            f"{base_path}/p{page_number}"
        )

    return urlunsplit(
        (
            parts.scheme,
            parts.netloc,
            page_path,
            parts.query,
            "",
        )
    )


# ============================================================
# PAGE CHECKS
# ============================================================

def detect_block_or_challenge(
    html: str,
) -> str | None:
    """
    Chỉ detect để dừng.
    Không cố bypass anti-bot.
    """

    text = html.lower()

    signals = [
        "cf-turnstile",
        "challenge-platform",
        "verify you are human",
        "checking your browser",
        "access denied",
        "captcha",
    ]

    for signal in signals:
        if signal in text:
            return signal

    return None


# ============================================================
# PAGE TRACKING METADATA
# ============================================================

def extract_page_tracking_metadata(
    html: str,
) -> dict[str, dict]:

    """
    Batdongsan có window.pageTrackingData chứa:
    productId, projectId, cateId, cityCode,
    districtId, wardId, streetId...
    """

    result: dict[str, dict] = {}

    match = re.search(
        r"JSON\.parse\('(\{.*?\"products\"\s*:\s*\[.*?\]\})'\)",
        html,
        flags=re.DOTALL,
    )

    if not match:
        return result

    json_text = (
        match
        .group(1)
        .replace("\\'", "'")
    )

    try:
        parsed = json.loads(
            json_text
        )

    except json.JSONDecodeError:
        return result

    for product in parsed.get(
        "products",
        [],
    ):
        product_id = product.get(
            "productId"
        )

        if product_id is not None:
            result[
                str(product_id)
            ] = product

    return result


# ============================================================
# EXTRACT HELPERS
# ============================================================

def extract_images(
    card,
) -> list[str]:

    images = []

    for img in card.select(
        ".re__card-image img"
    ):
        url = first_attr(
            img,
            [
                "data-img",
                "data-src",
                "src",
            ],
        )

        if not url:
            continue

        url = urljoin(
            BASE_URL,
            url,
        )

        if url not in images:
            images.append(url)

    return images


def extract_video(
    card,
) -> tuple[
    str | None,
    str | None,
]:

    video = card.select_one(
        ".re__card-image video"
    )

    if not video:
        return None, None

    video_url = first_attr(
        video,
        [
            "data-src",
            "src",
        ],
    )

    poster_url = first_attr(
        video,
        [
            "data-poster",
            "poster",
        ],
    )

    if video_url:
        video_url = urljoin(
            BASE_URL,
            video_url,
        )

    if poster_url:
        poster_url = urljoin(
            BASE_URL,
            poster_url,
        )

    return (
        video_url,
        poster_url,
    )


def extract_image_count(
    card,
) -> str | None:

    # Có thể thay class theo layout,
    # nên thử một số selector.
    selectors = [
        ".re__card-image-feature span",
        ".re__card-image-count",
        ".js__card-image-count",
    ]

    for selector in selectors:
        node = card.select_one(
            selector
        )

        text = get_text(node)

        if text:
            return text

    return None


def extract_bedrooms(
    card,
) -> tuple[
    str | None,
    str | None,
]:

    node = card.select_one(
        ".re__card-config-bedroom"
    )

    if not node:
        return None, None

    value = get_text(
        node.select_one("span")
    )

    aria = first_attr(
        node,
        ["aria-label"],
    )

    return value, aria


def extract_location_text(
    card,
) -> str | None:

    location = card.select_one(
        ".re__card-location"
    )

    if not location:
        return None

    texts = []

    for span in location.select(
        "span"
    ):
        classes = (
            span.get("class")
            or []
        )

        if (
            "re__card-config-dot"
            in classes
        ):
            continue

        text = get_text(
            span
        )

        if text:
            texts.append(text)

    if texts:
        return clean_text(
            " ".join(texts)
        )

    return get_text(
        location
    )


def extract_agent(
    card,
) -> tuple[
    str | None,
    str | None,
]:

    node = card.select_one(
        ".js__entry-professional-agent"
    )

    if not node:
        return None, None

    agent_link = first_attr(
        node,
        ["data-bds-pa-link"],
    )

    tracking = first_attr(
        node,
        ["tracking-label"],
    )

    if agent_link:
        agent_link = urljoin(
            BASE_URL,
            agent_link,
        )

    return (
        agent_link,
        tracking,
    )


# ============================================================
# PARSE CARD
# ============================================================

def parse_card(
    card,
    page_number: int,
    card_index: int,
    page_url: str,
    tracking_meta: dict[str, dict],
) -> dict:

    listing_id = first_attr(
        card,
        ["prid"],
    )

    link = card.select_one(
        "a.js__product-link-for-product-id"
    )

    listing_url = None

    if link:
        href = link.get(
            "href"
        )

        if href:
            listing_url = urljoin(
                BASE_URL,
                href,
            )

    title = get_text(
        card.select_one(
            ".js__card-title"
        )
    )

    price_text = get_text(
        card.select_one(
            ".re__card-config-price"
        )
    )

    area_text = get_text(
        card.select_one(
            ".re__card-config-area"
        )
    )

    price_per_m2_text = get_text(
        card.select_one(
            ".re__card-config-price_per_m2"
        )
    )

    (
        bedrooms_text,
        bedrooms_aria,
    ) = extract_bedrooms(
        card
    )

    description = get_text(
        card.select_one(
            ".js__card-description"
        )
    )

    location_text = (
        extract_location_text(
            card
        )
    )

    images = extract_images(
        card
    )

    (
        video_url,
        video_poster,
    ) = extract_video(
        card
    )

    # Có layout có field ngày đăng ở card,
    # có layout không có. Giữ RAW nếu có.
    published_info = None

    published_selectors = [
        ".re__card-published-info",
        ".re__card-published",
        ".js__card-published-info",
    ]

    for selector in published_selectors:
        node = card.select_one(
            selector
        )

        published_info = (
            get_text(node)
        )

        if published_info:
            break

    (
        agent_link,
        agent_tracking,
    ) = extract_agent(
        card
    )

    verified = (
        "re__card-full-label-verified"
        in (
            card.get("class")
            or []
        )
    )

    meta = (
        tracking_meta.get(
            str(listing_id),
            {},
        )
        if listing_id
        else {}
    )

    return {
        # Crawl metadata
        "_source":
            "batdongsan",

        "_source_page":
            page_number,

        "_card_index":
            card_index,

        "_scraped_at":
            now_iso(),

        "_crawl_url":
            page_url,

        # Card raw attributes
        "listing_id":
            listing_id,

        "user_id":
            first_attr(
                card,
                ["uid"],
            ),

        "vip_type":
            first_attr(
                card,
                ["vtp"],
            ),

        "video_bds":
            first_attr(
                card,
                ["video-bds"],
            ),

        "position":
            first_attr(
                card,
                ["ipos"],
            ),

        "page_number_attr":
            first_attr(
                card,
                ["pgno"],
            ),

        "representative_image":
            first_attr(
                card,
                ["prav"],
            ),

        "tracking_label":
            first_attr(
                card,
                ["tracking-label"],
            ),

        "verified":
            verified,

        # Listing raw fields
        "listing_url":
            listing_url,

        "title":
            title,

        "price_text":
            price_text,

        "area_text":
            area_text,

        "price_per_m2_text":
            price_per_m2_text,

        "bedrooms_text":
            bedrooms_text,

        "bedrooms_aria":
            bedrooms_aria,

        "location_text":
            location_text,

        "description":
            description,

        "published_info_text":
            published_info,

        # Media
        "image_count":
            extract_image_count(
                card
            ),

        "image_urls":
            images,

        "video_url":
            video_url,

        "video_poster":
            video_poster,

        # Agent
        "agent_profile_url":
            agent_link,

        "agent_tracking_label":
            agent_tracking,

        # Tracking metadata trong HTML
        "intent":
            meta.get("intent"),

        "page_type":
            meta.get("pageType"),

        "project_id":
            meta.get("projectId"),

        "category_id":
            meta.get("cateId"),

        "city_code":
            meta.get("cityCode"),

        "district_id":
            meta.get("districtId"),

        "ward_id":
            meta.get("wardId"),

        "street_id":
            meta.get("streetId"),

        "product_type":
            meta.get("productType"),

        "tracking_verified":
            meta.get("verified"),

        "tracking_expired":
            meta.get("expired"),

        # Full HTML của card,
        # chỉ giữ ở JSONL.
        "raw_card_html":
            str(card),
    }


# ============================================================
# PARSE PAGE
# ============================================================

def parse_page_html(
    html: str,
    page_number: int,
    page_url: str,
) -> list[dict]:

    soup = BeautifulSoup(
        html,
        "html.parser",
    )

    cards = soup.select(
        ".js__card.js__card-listing"
    )

    tracking_meta = (
        extract_page_tracking_metadata(
            html
        )
    )

    records = []

    for index, card in enumerate(
        cards,
        start=1,
    ):
        records.append(
            parse_card(
                card=card,
                page_number=page_number,
                card_index=index,
                page_url=page_url,
                tracking_meta=tracking_meta,
            )
        )

    return records


# ============================================================
# SAVE RAW HTML
# ============================================================

def save_page_html(
    page_number: int,
    html: str,
) -> None:

    path = (
        PAGES_DIR
        / f"page_{page_number:05d}.html"
    )

    path.write_text(
        html,
        encoding="utf-8",
    )


# ============================================================
# SAVE JSONL
# ============================================================

def save_jsonl(
    records: list[dict],
) -> None:

    # JSONL giữ FULL RAW,
    # bao gồm raw_card_html.

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


# ============================================================
# SAVE CSV
# ============================================================

def csv_safe_value(
    value,
):

    if isinstance(
        value,
        (list, dict),
    ):
        return json.dumps(
            value,
            ensure_ascii=False,
        )

    return value


def append_csv(
    records: list[dict],
    header_written: bool,
) -> bool:

    """
    CSV chỉ để xem nhanh bằng Excel.
    Không ghi raw_card_html để file không quá nặng.
    """

    if not records:
        return header_written

    csv_records = []

    for record in records:
        csv_record = {
            key: value
            for key, value
            in record.items()
            if key != "raw_card_html"
        }

        csv_records.append(
            csv_record
        )

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

        for record in csv_records:
            writer.writerow(
                {
                    key:
                        csv_safe_value(
                            value
                        )
                    for key, value
                    in record.items()
                }
            )

    return True


# ============================================================
# BROWSER
# ============================================================

def launch_browser(
    playwright,
):
    """
    Ưu tiên Chrome cài sẵn.
    Nếu không có thì fallback Chromium của Playwright.
    """

    if USE_INSTALLED_CHROME:
        try:
            print(
                "[BROWSER] Mở Google Chrome..."
            )

            return (
                playwright
                .chromium
                .launch(
                    channel="chrome",
                    headless=HEADLESS,
                )
            )

        except Exception as exc:
            print(
                "[WARN] Không mở được "
                f"Chrome cài sẵn: {exc}"
            )

            print(
                "[BROWSER] Fallback "
                "sang Chromium Playwright..."
            )

    return (
        playwright
        .chromium
        .launch(
            headless=HEADLESS,
        )
    )


# ============================================================
# MAIN
# ============================================================

def main() -> None:

    if PAGES_TO_CRAWL <= 0:
        raise ValueError(
            "PAGES_TO_CRAWL phải lớn hơn 0."
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

    print("=" * 78)
    print(
        "BATDONGSAN.COM.VN RAW CRAWLER "
        "- PLAYWRIGHT"
    )
    print("=" * 78)

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
        f"Delay         : "
        f"{DELAY_SECONDS} giây"
    )

    print(
        f"Headless      : {HEADLESS}"
    )

    print(
        f"Output        : {OUTPUT_DIR}"
    )

    print("=" * 78)

    total_records = 0
    completed_pages = []
    failed_pages = []
    header_written = False
    seen_listing_ids = set()

    last_page = (
        START_PAGE
        + PAGES_TO_CRAWL
        - 1
    )

    with sync_playwright() as p:

        browser = launch_browser(
            p
        )

        context = (
            browser.new_context(
                locale="vi-VN",
                viewport={
                    "width": 1440,
                    "height": 1000,
                },
            )
        )

        page = context.new_page()

        page.set_default_timeout(
            PAGE_TIMEOUT_MS
        )

        for page_number in range(
            START_PAGE,
            last_page + 1,
        ):

            page_url = build_page_url(
                page_number
            )

            print()
            print(
                f"[PAGE {page_number}] "
                f"Đang mở..."
            )

            print(
                f"[URL] {page_url}"
            )

            try:
                response = page.goto(
                    page_url,
                    wait_until="domcontentloaded",
                    timeout=PAGE_TIMEOUT_MS,
                )

                if response is not None:
                    status = (
                        response.status
                    )

                    print(
                        f"[HTTP] {status}"
                    )

                    if status == 403:
                        raise RuntimeError(
                            "HTTP 403 từ website."
                        )

                # Chờ card xuất hiện.
                try:
                    page.wait_for_selector(
                        ".js__card.js__card-listing",
                        timeout=LISTING_WAIT_MS,
                    )

                except PlaywrightTimeoutError:
                    # Vẫn lấy HTML để xem
                    # có challenge hay không.
                    pass

                # Cho DOM ổn định thêm một chút.
                page.wait_for_timeout(
                    1000
                )

                html = page.content()

                blocked_signal = (
                    detect_block_or_challenge(
                        html
                    )
                )

                if blocked_signal:
                    raise RuntimeError(
                        "Website hiện anti-bot/"
                        f"challenge: {blocked_signal}"
                    )

                # Lưu nguyên HTML trang.
                save_page_html(
                    page_number=page_number,
                    html=html,
                )

                records = parse_page_html(
                    html=html,
                    page_number=page_number,
                    page_url=page_url,
                )

                if not records:
                    print(
                        f"[WARN] Page "
                        f"{page_number}: "
                        "không tìm thấy listing."
                    )

                    # Nếu không có card thì dừng
                    # để tránh ghi dữ liệu sai.
                    break

                duplicate_count = 0

                for record in records:
                    listing_id = (
                        record.get(
                            "listing_id"
                        )
                    )

                    if (
                        listing_id
                        and listing_id
                        in seen_listing_ids
                    ):
                        duplicate_count += 1

                    if listing_id:
                        seen_listing_ids.add(
                            listing_id
                        )

                # RAW: không dedup khỏi output.
                save_jsonl(
                    records
                )

                header_written = append_csv(
                    records=records,
                    header_written=(
                        header_written
                    ),
                )

                total_records += len(
                    records
                )

                completed_pages.append(
                    page_number
                )

                print(
                    f"[PAGE {page_number}] "
                    f"records={len(records)} "
                    f"| duplicate_seen="
                    f"{duplicate_count}"
                )

            except Exception as exc:

                failed_pages.append(
                    {
                        "page":
                            page_number,

                        "url":
                            page_url,

                        "error":
                            str(exc),
                    }
                )

                print(
                    f"[ERROR] "
                    f"Page {page_number}: "
                    f"{exc}"
                )

                # Challenge / 403:
                # dừng, không cố bypass.
                error_text = str(
                    exc
                ).lower()

                if (
                    "403" in error_text
                    or "challenge"
                    in error_text
                    or "captcha"
                    in error_text
                    or "anti-bot"
                    in error_text
                ):
                    print(
                        "[STOP] Website đang "
                        "yêu cầu xác minh hoặc "
                        "chặn request tự động."
                    )

                    break

            if (
                page_number < last_page
                and DELAY_SECONDS > 0
            ):
                print(
                    f"[WAIT] "
                    f"{DELAY_SECONDS} giây..."
                )

                time.sleep(
                    DELAY_SECONDS
                )

        context.close()
        browser.close()

    # ========================================================
    # SUMMARY
    # ========================================================

    summary = {
        "source":
            "batdongsan",

        "crawler":
            "playwright",

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
    print("=" * 78)
    print("DONE")
    print("=" * 78)

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

    print("=" * 78)


if __name__ == "__main__":
    main()
