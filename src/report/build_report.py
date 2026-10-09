"""Build docs/report/Bao_cao_Silver_Feature_Gold.docx from pipeline evidence.

Inputs (all produced by the pipelines, nothing typed by hand):
- docs/validation/*.json          build summaries and verification results
- docs/report/data/report_data.json  Gold examples (src/report/export_report_data.py)
- pytest results (run here) and source code excerpts (read from src/)

Usage: python -m src.report.build_report
"""

from __future__ import annotations

import json
import re
import subprocess
import sys
import xml.etree.ElementTree as ET
from collections import Counter, defaultdict
from datetime import date
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.gold.gold_rules import (  # noqa: E402
    DUP_AREA_TOLERANCE, DUP_MIN_TITLE_JACCARD, DUP_PRICE_TOLERANCE, MIN_PEER_GROUP_SIZE,
    MIN_SUBSTITUTION_SIMILARITY, MODEL_CATEGORY_LABELS, load_bands,
)
from src.report import report_charts as charts  # noqa: E402
from src.report.report_body import write_body  # noqa: E402
from src.report.report_docx import ReportDocument  # noqa: E402

VALIDATION = PROJECT_ROOT / "docs/validation"
REPORT_DIR = PROJECT_ROOT / "docs/report"
FIGURES = REPORT_DIR / "figures"
OUTPUT = REPORT_DIR / "Bao_cao_Silver_Feature_Gold.docx"
CATEGORY_LABELS = {code: label for code, (_, label) in MODEL_CATEGORY_LABELS.items()} | {"khong_ro": "Không rõ"}
FLAG_LABELS = {"legal": "pháp lý", "furnished": "nội thất", "frontage": "mặt tiền đường", "elevator": "thang máy", "car_access": "ô tô tiếp cận"}


# ----------------------------------------------------------------- formatting
def n(value) -> str:
    return charts.vn(float(value or 0), 0)


def dec(value, digits: int = 1) -> str:
    return "–" if value is None else charts.vn(float(value), digits)


def pct(value, digits: int = 1) -> str:
    return "–" if value is None else charts.vn(float(value) * 100, digits) + "%"


def ty(value) -> str:
    return "–" if value is None else charts.vn(float(value) / 1e9, 2) + " tỷ"


def trm2(value) -> str:
    return "–" if value is None else charts.vn(float(value) / 1e6, 1)


def yes(flag) -> str:
    return "Có" if flag else ""


def load(name: str) -> dict:
    path = VALIDATION / name
    return json.loads(path.read_text(encoding="utf-8")) if path.exists() else {}


def snippet(rel_path: str, start: str, end: str | None = None, max_lines: int = 30) -> str:
    """Excerpt real source code so the report never drifts from the repository."""

    lines = (PROJECT_ROOT / rel_path).read_text(encoding="utf-8").splitlines()
    first = next(i for i, line in enumerate(lines) if start in line)
    out = []
    for line in lines[first:first + max_lines]:
        if end and out and end in line:
            break
        out.append(line.rstrip())
    while out and not out[-1].strip():
        out.pop()
    return f"# {rel_path}\n" + "\n".join(out)


def run_pytest() -> dict:
    junit = VALIDATION / "pytest.xml"
    subprocess.run([sys.executable, "-m", "pytest", "-q", f"--junitxml={junit}"], cwd=PROJECT_ROOT,
                   capture_output=True, text=True)
    root = ET.parse(junit).getroot()
    suite = root if root.tag == "testsuite" else root.find("testsuite")
    per_file: Counter = Counter()
    failed: Counter = Counter()
    for case in suite.iter("testcase"):
        module = case.get("classname", "").split(".")[-1]
        per_file[module] += 1
        if case.find("failure") is not None or case.find("error") is not None:
            failed[module] += 1
    return {"total": int(suite.get("tests", 0)), "failures": int(suite.get("failures", 0)) + int(suite.get("errors", 0)),
            "per_file": dict(per_file), "failed": dict(failed)}


def changed_files() -> list[tuple[str, str]]:
    """Files added or modified relative to ``main`` (committed or not)."""

    def git(*args: str) -> list[str]:
        result = subprocess.run(["git", *args], cwd=PROJECT_ROOT, capture_output=True, text=True, encoding="utf-8")
        return result.stdout.splitlines()

    files: dict[str, str] = {}
    for line in git("diff", "--name-status", "main"):
        status, path = line.split("\t", 1)
        files[path.strip()] = "Tạo mới" if status.startswith("A") else "Sửa"
    for path in git("ls-files", "--others", "--exclude-standard"):
        files[path.strip()] = "Tạo mới"
    return sorted(
        ((status, path) for path, status in files.items()
         if not path.startswith(("docs/report/", "src/ingestion/")) and "__pycache__" not in path),
        key=lambda item: item[1],
    )


FILE_PURPOSE = {
    "config/gold_bands.csv": "Ngưỡng band giá, diện tích, giá/m², số phòng",
    "src/silver/feature_rules.py": "Quy tắc feature thuần Python",
    "src/silver/build_listing_feature.py": "Job Spark xây listing_feature",
    "src/silver/verify_silver.py": "Thêm check listing_feature và vị trí bucket",
    "src/silver/build_listing_core_spark.py": "Ghi bảng qua write_iceberg_table (bucket Silver)",
    "src/silver/build_listing_history.py": "Ghi bảng qua write_iceberg_table (bucket Silver)",
    "src/silver/build_location.py": "Ghi bảng qua write_iceberg_table (bucket Silver)",
    "src/silver/iceberg_smoke_check.py": "Smoke test ghi vào bucket Silver và PURGE sau khi chạy",
    "docs/architecture/lakehouse_architecture.md": "Mô tả lưu trữ theo bucket từng tầng",
    "docs/data/canonical_listing_schema.md": "Mô tả schema listing_feature",
    "config/sources.yaml": "Danh mục nguồn",
    "src/common/spark_session.py": "Namespace gold, ghi bảng theo bucket từng tầng",
    "src/common/compare_rebuild_counts.py": "So row count giữa hai lần rebuild",
    "src/gold/gold_rules.py": "Quy tắc Gold thuần Python (band, dedup, Pareto...)",
    "src/gold/gold_common.py": "Hàm dùng chung cho các job Gold",
    "src/gold/build_dimensions.py": "Xây 9 dimension",
    "src/gold/build_fact_listing.py": "Xây fact_listing và cờ trùng nguồn",
    "src/gold/build_price_benchmark.py": "3a – BQ2 price benchmark",
    "src/gold/build_budget_tradeoff.py": "3b – BQ1 budget trade-off và Pareto",
    "src/gold/build_area_substitution.py": "3c – BQ3 khu vực thay thế",
    "src/gold/build_data_quality.py": "3d – Data Quality KPI",
    "src/gold/build_repricing.py": "3e – lịch sử đổi giá",
    "src/gold/build_market_overview.py": "3f – tổng quan thị trường",
    "src/gold/verify_gold.py": "Kiểm chứng toàn bộ Gold",
    "src/report/export_report_data.py": "Xuất ví dụ Gold cho báo cáo",
    "src/report/build_report.py": "Sinh báo cáo Word",
    "src/report/report_docx.py": "Định dạng Word theo chuẩn báo cáo",
    "src/report/report_charts.py": "Biểu đồ cho báo cáo",
    "scripts/pipeline_utils.ps1": "Chạy lệnh native không bị dừng vì stderr",
    "scripts/run_silver_pipeline.ps1": "Pipeline Silver (thêm bước feature)",
    "scripts/run_gold_pipeline.ps1": "Pipeline Gold đầy đủ",
    "scripts/run_spark.ps1": "Tự dọn thư mục work của Spark worker",
    "scripts/check_deterministic_rebuild.ps1": "Rebuild 2 lần và so row count",
    "tests/unit/test_feature_rules.py": "Unit test quy tắc feature",
    "tests/unit/test_gold_rules.py": "Unit test quy tắc Gold",
    "src/silver/feature_rules.py": "Quy tắc feature: loại hình theo tín hiệu mạnh/yếu, mâu thuẫn, cờ đặc điểm",
    "src/silver/build_location.py": "Chọn tỉnh theo thứ tự bằng chứng, xử lý LQ05",
    "src/silver/build_listing_core.py": "Loại hình từ taxonomy của site, category_evidence, địa chỉ nhadatvui",
    "src/common/table_fingerprints.py": "Fingerprint nội dung từng bảng để so hai lần rebuild",
    "src/report/report_body.py": "Nội dung Chương 1–8 của báo cáo",
    "tests/unit/test_location_rules.py": "Regression test chọn tỉnh",
    "config/vietnam_province_centers.csv": "Thêm alias TPHCM, HCM",
    "docs/business/problem_definition.md": "Thống nhất BQ1–BQ3",
    "docs/business/dashboard_questions.md": "Câu hỏi dashboard theo BQ",
    "docs/data/silver_location_schema.md": "Thứ tự bằng chứng tỉnh, LQ05",
    "docs/validation/review_before_fix.json": "Số liệu trước đợt sửa",
    "docs/validation/dedup_pair_sample.csv": "Mẫu cặp nghi trùng để kiểm tra thủ công",
    "README.md": "Cập nhật tiến độ và số liệu",
    "requirements-dev.txt": "Thêm python-docx, matplotlib",
}

COLUMN_DOC = {
    "source_id": "Khóa nghiệp vụ của tin (source + ad_id)",
    "source": "Tên nguồn",
    "dq_status": "Trạng thái DQ của observation tạo ra dòng current",
    "is_rent": "TRUE nếu là tin cho thuê",
    "record_hash": "SHA-256 các trường nghiệp vụ",
    "category_name": "Loại BĐS canonical từ Silver Core",
    "model_category": "Nhóm loại BĐS dùng cho phân tích",
    "model_category_method": "CATEGORY_NAME / TITLE_FALLBACK / UNMAPPED",
    "feature_completeness_score": "Trung bình 5 cờ *_known, trong [0, 1]",
    "feature_rule_version": "Phiên bản bộ quy tắc feature",
    "feature_built_at": "Thời điểm build",
    "date_key": "Ngày quan sát (yyyymmdd)",
    "posted_date_key": "Ngày đăng (yyyymmdd), -1 nếu không rõ",
    "price": "Giá chào bán (VND)",
    "area": "Diện tích (m²)",
    "price_per_m2": "Giá/m² (VND)",
    "rooms": "Số phòng ngủ",
    "distance_to_center_km": "Khoảng cách tới tâm tỉnh/thành (km)",
    "dup_group_id": "Mã nhóm nghi trùng giữa các nguồn",
    "is_cross_source_dup_suspect": "TRUE nếu thuộc một nhóm nghi trùng",
    "is_dup_representative": "Tin được tính trong bảng tổng hợp (đại diện theo luật nghi trùng)",
    "title": "Tiêu đề tin", "ad_url": "URL tin", "scraped_at": "Thời điểm quan sát",
    "peer_group_id": "Mã nhóm tương đồng (cấp|khóa)",
    "benchmark_level": "Cấp nhóm: LOC_CAT_AREA_ROOM / LOC_CAT_AREA / LOC_CAT",
    "n_listings": "Số tin đại diện trong nhóm",
    "price_ratio": "Giá/m² của tin ÷ median nhóm",
    "price_position": "duoi_p25 / p25_p75 / tren_p75 / khong_du_du_lieu (vị trí, không phải đánh giá giá hợp lý)",
    "category_evidence": "Nguồn gốc nhãn: SOURCE_STRUCTURED / SOURCE_LABEL / ENDPOINT_CONTEXT / NONE",
    "title_model_category": "Loại hình đọc từ title (NULL nếu không đủ tín hiệu)",
    "category_title_conflict": "Nhãn nguồn và title chỉ hai họ loại hình khác nhau",
    "title_negated_features": "Đặc điểm title nói rõ là không có",
    "location_dq_status": "Trạng thái DQ vị trí từ listing_location",
    "is_location_conflict": "Tỉnh theo text mâu thuẫn tọa độ (LQ05); location_key = -1",
    "is_province_inferred_from_coordinates": "Tỉnh suy từ tâm gần nhất (LQ04)",
    "dup_group_status": "RESOLVED (một tin mỗi nguồn) / AMBIGUOUS (không gộp)",
    "target_area": "Diện tích mục tiêu = median diện tích nhóm gốc",
    "estimated_cost_origin_at_target_area": "Median giá/m² gốc × diện tích mục tiêu",
    "estimated_cost_alternative_at_target_area": "Median giá/m² thay thế × diện tích mục tiêu",
    "estimated_saving_at_target_area": "Chênh chi phí ước tính cho cùng diện tích (không bảo đảm)",
    "median_total_price_diff": "Chênh median tổng giá hai nhóm (chỉ để đọc)",
    "max_flag_share_gap": "Chênh lệch lớn nhất của một tỷ lệ cờ",
    "feature_count": "Số cờ đặc điểm title nhắc tới (0–5)",
    "feature_count_vs_peer": "feature_count trừ số cờ kỳ vọng của nhóm",
    "is_low_price_missing_legal": "Dưới P25 và title không nêu pháp lý",
    "is_pareto_efficient": "Không bị trội theo tiêu chí đã chọn, trong cùng nhóm",
    "group_size": "Số tin trong nhóm ngân sách", "frontier_size": "Số tin Pareto trong nhóm",
    "feature_similarity": "1 − trung bình |chênh tỷ lệ 5 cờ|",
    "price_gap_pct": "Mức rẻ hơn của khu vực thay thế (theo median giá/m²)",
    "distance_diff_km": "Chênh median khoảng cách tới tâm (km)",
    "substitution_rank": "Thứ hạng theo price_gap_pct",
    "direction": "giam / tang", "version_number": "Phiên bản trong listing_history",
    "price_change_pct": "Tỷ lệ thay đổi giá", "top_warn_reasons": "3 mã WARN phổ biến nhất",
    "province_name": "Tỉnh/thành (mô hình 34 đơn vị)", "district_name": "Quận/huyện",
    "location_level": "DISTRICT / PROVINCE / UNKNOWN",
}


def describe(column: str) -> str:
    if column in COLUMN_DOC:
        return COLUMN_DOC[column]
    if column.endswith("_key"):
        return "Khóa (surrogate key / FK tới dimension)"
    if column.startswith("share_"):
        return f"Tỷ lệ tin có cờ {column[6:]} trong nhóm"
    if column.startswith("title_has_"):
        return f"Title nhắc đặc điểm {FLAG_LABELS.get(column[10:], column[10:])}"
    if column.endswith("_known"):
        return "TRUE nếu thông tin tương ứng có giá trị"
    if column.startswith("coverage_"):
        return f"Tỷ lệ phủ của {column[9:]}"
    if column.startswith(("median_", "p25_", "p75_")):
        return "Thống kê (percentile chính xác) của nhóm"
    if column.endswith("_rows") or column.endswith("_rate"):
        return "Số dòng / tỷ lệ trong phễu dữ liệu"
    return ""


# ----------------------------------------------------------------------- main
def main() -> None:
    feature = load("silver_feature_summary.json")
    silver_verify = load("silver_final_verification.json")
    foundation = load("silver_foundation_summary.json")
    dims_summary = load("gold_dimensions_summary.json")
    fact = load("gold_fact_listing_summary.json")
    gold_verify = load("gold_verification.json")
    benchmark = load("gold_price_benchmark_summary.json")
    budget = load("gold_budget_tradeoff_summary.json")
    substitution = load("gold_area_substitution_summary.json")
    dq = load("gold_data_quality_summary.json")
    repricing = load("gold_repricing_summary.json")
    overview = load("gold_market_overview_summary.json")
    determinism = load("deterministic_rebuild.json")
    data = json.loads((REPORT_DIR / "data/report_data.json").read_text(encoding="utf-8"))
    tests = run_pytest()
    bands = load_bands()

    r = ReportDocument()

    # ------------------------------------------------------------- cover + TOC
    r.cover([
        ("TIỂU LUẬN CHUYÊN NGÀNH – NHÓM 6", 14, True),
        ("", 13, False), ("", 13, False),
        ("XÂY DỰNG DATA LAKEHOUSE", 18, True),
        ("PHỤC VỤ PHÂN TÍCH DỮ LIỆU THỊ TRƯỜNG", 18, True),
        ("BẤT ĐỘNG SẢN TẠI VIỆT NAM", 18, True),
        ("", 13, False),
        ("Báo cáo tiến độ: Silver Feature và Gold Layer", 15, True),
        ("", 13, False), ("", 13, False),
        ("Thành viên thực hiện:", 13, True),
        ("23133024 – Võ Đức Hoàng", 13, False),
        ("23133029 – Vương Đức Huy", 13, False),
        ("23133040 – Nguyễn Lê Hoàng Kiệt", 13, False),
        ("", 13, False), ("", 13, False),
        (f"Ngày {date.today():%d/%m/%Y}", 13, False),
    ])
    r.toc()

    evidence = {
        "feature": feature, "silver_verify": silver_verify, "dims": dims_summary, "fact": fact, "gold_verify": gold_verify,
        "benchmark": benchmark, "budget": budget, "substitution": substitution, "dq": dq, "repricing": repricing,
        "overview": overview, "determinism": determinism, "location": load("silver_location_summary.json"),
        "before": load("review_before_fix.json"), "dedup_sample": VALIDATION / "dedup_pair_sample.csv",
    }
    write_body(r, evidence, data, tests, FIGURES, snippet, CATEGORY_LABELS, bands)

    # ============================================================== PHỤ LỤC
    r.appendix("PHỤ LỤC A. CÁCH CHẠY LẠI PIPELINE VÀ BÁO CÁO")
    r.para("Chạy trong PowerShell tại thư mục gốc repository, sau khi Docker Desktop đã chạy:")
    r.code("\n".join([
        "# 1. Môi trường",
        "Copy-Item .env.example .env",
        "docker compose up -d minio minio-init iceberg-rest spark-master spark-worker",
        "python -m pip install -r requirements-dev.txt",
        "",
        "# 2. Nạp Bronze (dữ liệu historical đặt ở data/incoming/historical/<nguồn>/)",
        '.\\scripts\\run_spark.ps1 "src\\bronze\\ingest_bronze.py"',
        '.\\scripts\\run_spark.ps1 "src\\bronze\\ingest_snapshots_bronze.py"',
        "",
        "# 3. Silver và Gold (dừng ngay khi một bước lỗi)",
        ".\\scripts\\run_silver_pipeline.ps1",
        ".\\scripts\\run_gold_pipeline.ps1",
        "",
        "# 4. (Tùy chọn) Rebuild 2 lần, so số dòng, khóa và hash nội dung từng bảng",
        ".\\scripts\\check_deterministic_rebuild.ps1",
        "",
        "# 5. Báo cáo: xuất ví dụ từ Gold rồi sinh file Word",
        '.\\scripts\\run_spark.ps1 "src\\report\\export_report_data.py"',
        "python -m src.report.build_report",
        "",
        "# 6. Xem dữ liệu bằng SQL qua Trino",
        "docker compose --profile query up -d trino",
        "docker exec -it tlcn_trino trino",
    ]))
    r.appendix("PHỤ LỤC B. DANH SÁCH FILE ĐÃ TẠO/SỬA")
    files = changed_files()
    r.table("File đã tạo hoặc sửa trong giai đoạn", ["Trạng thái", "File", "Nội dung"], [
        [status, path, FILE_PURPOSE.get(path, "Bằng chứng kiểm chứng (JSON)" if path.startswith("docs/validation") else "")]
        for status, path in files
    ], widths_cm=[2.0, 7.4, 6.6], font_size=9)
    r.appendix("PHỤ LỤC C. SCHEMA CÁC BẢNG MỚI")
    for table_name, columns in data["schemas"].items():
        r.table(f"Schema bảng {table_name}", ["Cột", "Kiểu", "Mô tả"], [
            [column, dtype, describe(column)] for column, dtype in columns
        ], widths_cm=[5.0, 2.8, 8.2], font_size=9)

    r.save(OUTPUT)
    print(f"Wrote {OUTPUT}")


if __name__ == "__main__":
    main()
