from __future__ import annotations

import hashlib
import json
import math
import os
import time
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import requests

from src.common.atomic_io import write_json_atomic

ROOT = Path(__file__).resolve().parents[2]


class CrawlBlocked(RuntimeError):
    pass


def integer_setting(name, default, minimum=1, maximum=2400):
    value = int(os.getenv(name, str(default)))
    if not minimum <= value <= maximum:
        raise ValueError(f"{name} must be between {minimum} and {maximum}")
    return value


def float_setting(name, default, minimum=0, maximum=60):
    value = float(os.getenv(name, str(default)))
    if not math.isfinite(value) or not minimum <= value <= maximum:
        raise ValueError(f"{name} must be finite and between {minimum} and {maximum}")
    return value


def batch_id():
    value = os.getenv("CRAWL_BATCH_ID") or datetime.now(ZoneInfo("Asia/Ho_Chi_Minh")).strftime(
        "%Y%m%d"
    )
    if len(value) != 8 or datetime.strptime(value, "%Y%m%d").strftime("%Y%m%d") != value:
        raise ValueError("CRAWL_BATCH_ID must be an actual YYYYMMDD date")
    return value


def output_directory(source, batch):
    custom = os.getenv("CRAWL_OUTPUT_DIR")
    return Path(custom).resolve() if custom else ROOT / "data/incoming/snapshots" / source / batch


def request_response(session, url, *, timeout, retries, delay=2, headers=None, params=None):
    last_error = None
    for attempt in range(retries):
        try:
            response = session.get(
                url,
                params=params,
                headers=headers() if callable(headers) else headers,
                timeout=timeout,
            )
            if response.status_code in (401, 403):
                raise CrawlBlocked(f"HTTP {response.status_code}: website refused automated access")
            if response.status_code == 429:
                last_error = CrawlBlocked("HTTP 429: rate limit persisted after bounded retries")
                if attempt + 1 < retries:
                    try:
                        wait = float(response.headers.get("Retry-After", max(10, delay)))
                    except (ValueError, TypeError):
                        wait = max(10, delay)
                    response.close()
                    time.sleep(max(0, min(wait, 30)))
                    continue
                raise last_error
            response.raise_for_status()
            return response
        except CrawlBlocked:
            raise
        except requests.RequestException as exc:
            last_error = exc
            if attempt + 1 < retries:
                time.sleep(min(max(delay, 1) * (attempt + 1), 10))
    raise RuntimeError(f"Request failed after {retries} attempts: {last_error}")


def page_fingerprint(records):
    keys = [
        str(r.get("listing_id") or r.get("_id") or r.get("listing_url") or r.get("url") or "")
        for r in records
    ]
    if not keys or any(not key for key in keys):
        raise ValueError("Listing payload lacks stable IDs/URLs; cannot certify pagination")
    return hashlib.sha256("\n".join(sorted(set(keys))).encode()).hexdigest()


def validate_nhadatvui_page(result, page):
    data = result.get("data")
    values = [result.get(k) for k in ("total", "skip", "count")]
    if not isinstance(data, list) or not all(type(v) is int and v >= 0 for v in values):
        raise ValueError("Invalid listing pagination metadata")
    total, skip, count = values
    limit = result.get("limit")
    if limit is not None and (type(limit) is not int or limit <= 0 or skip != (page - 1) * limit):
        raise ValueError("Site returned an unexpected pagination offset")
    if count != len(data) or any(not isinstance(r, dict) for r in data):
        raise ValueError("Listing count does not match the response data")

    if not data and skip < total:
        raise ValueError(f"Empty page {page} before advertised end ({skip} < {total})")
    return skip + count >= total


def write_progress(path, source, batch, completed, rows, **extra):
    payload = {
        "source": source,
        "run_id": batch,
        "status": "RUNNING",
        "completed_pages": list(completed),
        "records_written": rows,
        "failed_pages": [],
        **extra,
    }
    write_json_atomic(path, payload)


def append_raw_jsonl(path, records):
    with path.open("a", encoding="utf-8") as file:
        for record in records:
            file.write(json.dumps(record, ensure_ascii=False) + "\n")
