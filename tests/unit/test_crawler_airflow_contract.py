import csv
import json
from datetime import datetime
from zoneinfo import ZoneInfo
import pytest
from src.orchestration import pipeline


def configured_crawler(tmp_path, monkeypatch, interrupted=False):
    from src.ingestion.nhadatvui import nhadatvui_raw_crawler as crawler
    batch = datetime.now(ZoneInfo("Asia/Ho_Chi_Minh")).strftime("%Y%m%d")
    folder = tmp_path / "data/incoming/snapshots/nhadatvui" / batch
    for key, value in {"RUN_ID": batch, "OUTPUT_DIR": folder, "PAGES_DIR": folder / "pages",
        "CSV_PATH": folder / "nhadatvui_raw.csv", "JSONL_PATH": folder / "nhadatvui_raw.jsonl",
        "SUMMARY_PATH": folder / "crawl_summary.json", "IS_RESUME": False,
        "START_PAGE": 1, "PAGES_TO_CRAWL": 2 if interrupted else 1, "DELAY_SECONDS": 0}.items():
        monkeypatch.setattr(crawler, key, value)
    def request_page(page):
        if interrupted and page == 2:
            raise KeyboardInterrupt()
        return "fixture", "https://example.test/listings", 200
    monkeypatch.setattr(crawler, "request_page", request_page)
    monkeypatch.setattr(crawler, "parse_nhadatvui_result", lambda text: {"data": [{"_id": "fixture1"}], "total": 100, "skip": 0, "count": 1})
    monkeypatch.setattr(crawler, "to_csv_record", lambda row: {"listing_id": row["_id"]})
    monkeypatch.setattr(crawler, "setup_logging", lambda: None)
    monkeypatch.setattr(crawler, "log", lambda message: None)
    monkeypatch.setattr(pipeline, "PROJECT_ROOT", tmp_path)
    monkeypatch.setattr(pipeline.subprocess, "run", lambda *args, **kwargs: crawler.main())
    return crawler, batch


def test_new_batch_crawler_matches_airflow_contract(tmp_path, monkeypatch):
    crawler, batch = configured_crawler(tmp_path, monkeypatch)
    pipeline.prepare_web_source("nhadatvui", batch)
    summary = json.loads(crawler.SUMMARY_PATH.read_text())
    assert summary["status"] == "PASS"
    assert summary["records_written"] == summary["records_written_this_run"] == 1
    with crawler.CSV_PATH.open(encoding="utf-8-sig", newline="") as handle:
        assert len(list(csv.DictReader(handle))) == 1
    before = crawler.CSV_PATH.read_bytes()
    pipeline.prepare_web_source("nhadatvui", batch)
    assert crawler.CSV_PATH.read_bytes() == before
    with pytest.raises(FileExistsError):
        crawler.main()


def test_interrupted_partial_crawl_cannot_pass_airflow(tmp_path, monkeypatch):
    crawler, batch = configured_crawler(tmp_path, monkeypatch, interrupted=True)
    with pytest.raises(RuntimeError, match="Incomplete"):
        pipeline.prepare_web_source("nhadatvui", batch)
    summary = json.loads(crawler.SUMMARY_PATH.read_text())
    assert summary["records_written"] == 1
    assert summary["status"] == "FAIL" and not summary["failed_pages"]
    with pytest.raises(RuntimeError, match="incomplete"):
        pipeline.prepare_web_source("nhadatvui", batch)


def test_resume_does_not_skip_raw_only_uncommitted_page(tmp_path, monkeypatch):
    crawler, _ = configured_crawler(tmp_path, monkeypatch)
    crawler.PAGES_DIR.mkdir(parents=True)
    crawler.page_raw_path(1).write_text("response saved before parsing failed")
    assert not crawler.page_already_done(1)
    crawler.SUMMARY_PATH.write_text(json.dumps({"completed_pages": [], "failed_pages": [{"page": 1}]}))
    assert not crawler.page_already_done(1)
    crawler.SUMMARY_PATH.write_text(json.dumps({"completed_pages": [1], "failed_pages": []}))
    assert crawler.page_already_done(1)
