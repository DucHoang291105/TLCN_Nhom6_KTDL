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
from src.report.report_docx import ReportDocument  # noqa: E402

VALIDATION = PROJECT_ROOT / "docs/validation"
REPORT_DIR = PROJECT_ROOT / "docs/report"
FIGURES = REPORT_DIR / "figures"
OUTPUT = REPORT_DIR / "Bao_cao_Silver_Feature_Gold.docx"
CATEGORY_LABELS = {code: label for code, (_, label) in MODEL_CATEGORY_LABELS.items()} | {"khong_ro": "Không rõ"}
POSITION_LABELS = {"thap": "Thấp", "hop_ly": "Hợp lý", "cao": "Cao", "khong_du_du_lieu": "Không đủ dữ liệu"}
FLAG_LABELS = {"legal": "pháp lý", "furnished": "nội thất", "frontage": "mặt tiền", "elevator": "thang máy", "car_access": "ô tô vào"}


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
    result = subprocess.run(["git", "status", "--porcelain", "--untracked-files=all"], cwd=PROJECT_ROOT,
                            capture_output=True, text=True, encoding="utf-8")
    files = []
    for line in result.stdout.splitlines():
        status, path = line[:2], line[3:].strip().strip('"')
        if path.startswith(("docs/report/", "src/ingestion/")):
            continue
        files.append(("Tạo mới" if "?" in status or "A" in status else "Sửa", path))
    return sorted(files, key=lambda item: item[1])


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
    "is_dup_representative": "TRUE nếu là tin được tính trong bảng tổng hợp",
    "title": "Tiêu đề tin", "ad_url": "URL tin", "scraped_at": "Thời điểm quan sát",
    "peer_group_id": "Mã nhóm tương đồng (cấp|khóa)",
    "benchmark_level": "Cấp nhóm: LOC_CAT_AREA_ROOM / LOC_CAT_AREA / LOC_CAT",
    "n_listings": "Số tin đại diện trong nhóm",
    "price_ratio": "Giá/m² của tin ÷ median nhóm",
    "price_position": "thap / hop_ly / cao / khong_du_du_lieu",
    "feature_count": "Số cờ đặc điểm title nhắc tới (0–5)",
    "feature_count_vs_peer": "feature_count trừ số cờ kỳ vọng của nhóm",
    "is_low_price_missing_legal": "Giá thấp nhưng title không nhắc pháp lý",
    "is_pareto_efficient": "TRUE nếu không bị tin nào khác trong nhóm trội hơn",
    "group_size": "Số tin trong nhóm ngân sách", "frontier_size": "Số tin Pareto trong nhóm",
    "feature_similarity": "1 − trung bình |chênh tỷ lệ 5 cờ|",
    "price_gap_pct": "Mức rẻ hơn của khu vực thay thế (theo median giá/m²)",
    "typical_budget_saving": "Chênh median tổng giá (VND)",
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

    # ================================================================ CHƯƠNG 1
    r.h1("Tổng quan")
    r.h2("1.1. Mục tiêu giai đoạn")
    r.para(
        "Ở tuần 5, nhóm đã hoàn thành Silver Data Foundation: 9 nguồn tin đăng (3 nguồn crawl, 6 nguồn historical) "
        "được chuẩn hóa về schema 27 cột, kiểm tra chất lượng, khử trùng theo batch và lưu dưới dạng bảng Apache Iceberg. "
        "Tuy nhiên, schema 27 cột chỉ mô tả tin đăng; nó chưa có các biến mà người mua thực sự cân nhắc (loại hình được "
        "gom nhóm, tin có nhắc pháp lý, mặt tiền, hẻm ô tô…), và chưa có lớp dữ liệu nào được tổ chức để trả lời câu hỏi nghiệp vụ."
    )
    r.para("Giai đoạn này có ba mục tiêu, tương ứng ba bước triển khai:")
    r.bullets([
        "**Bước 1 – `listing_feature` (Silver):** trích xuất nhóm loại hình và các cờ đặc điểm từ dữ liệu đã chuẩn hóa.",
        "**Bước 2 – Gold nền:** tổ chức dữ liệu theo mô hình star schema gồm 9 dimension và bảng `fact_listing`, có gắn cờ tin trùng giữa các nguồn.",
        "**Bước 3 – Gold theo Business Question:** xây các bảng tổng hợp trả lời trực tiếp ba câu hỏi nghiệp vụ, cùng các bảng hỗ trợ về chất lượng dữ liệu, lịch sử giá và tổng quan thị trường.",
    ])
    r.h2("1.2. Business Questions")
    r.para("Cả ba câu hỏi được đặt từ góc nhìn **người mua**. Giá trong hệ thống là giá chào bán (giá đăng), không phải giá giao dịch.")
    r.table("Ba Business Question của giai đoạn", ["Mã", "Câu hỏi", "Ý nghĩa với người mua"], [
        ["BQ1", "Trong cùng một mức ngân sách, người mua phải đánh đổi những gì về vị trí, diện tích, số phòng và đặc điểm; phương án nào mang lại giá trị phù hợp nhất?",
         "Không có BĐS hoàn hảo: gần trung tâm thì nhỏ hơn, rộng hơn thì xa hơn hoặc thiếu pháp lý."],
        ["BQ2", "Tin đăng nào có giá lệch đáng kể so với các BĐS tương đồng, và chênh lệch đó có được giải thích bởi vị trí, diện tích hay đặc điểm không?",
         "Nhận diện tin định giá cao, tin có vẻ là cơ hội, và tin rẻ nhưng thiếu thông tin quan trọng."],
        ["BQ3", "Nếu không mua được ở khu vực mong muốn, khu vực nào có BĐS tương đương nhưng yêu cầu ngân sách thấp hơn?",
         "Thực tế hơn câu hỏi 'khu vực nào rẻ nhất' vì phải tương đương về loại hình, diện tích, đặc điểm."],
    ], widths_cm=[1.4, 8.6, 6.0])
    r.h2("1.3. Kiến trúc và phần được bổ sung")
    r.para(
        "Kiến trúc giữ nguyên mô hình Medallion trên MinIO và Apache Iceberg. Mỗi tầng có một bucket riêng: "
        "`lakehouse-bronze` (Parquet), `lakehouse-silver` và `lakehouse-gold` (bảng Iceberg). Spark 3.5 thực hiện toàn bộ biến đổi; "
        "Trino đọc trực tiếp các bảng Gold qua Iceberg REST Catalog để phục vụ truy vấn và dashboard."
    )
    r.figure(charts.architecture(FIGURES / "architecture.png"), "Kiến trúc Bronze → Silver → Gold; phần viền xanh là phần xây dựng trong giai đoạn này")
    r.h2("1.4. Tóm tắt các hạng mục")
    completed = "Hoàn thành"
    r.table("Trạng thái các hạng mục trong giai đoạn", ["Hạng mục", "File chính", "Bảng output", "Trạng thái"], [
        ["Silver feature", "build_listing_feature.py", "listing_feature", completed],
        ["9 dimension", "build_dimensions.py", "dim_*", completed],
        ["Fact và dedup giữa nguồn", "build_fact_listing.py", "fact_listing", "Hoàn thành (gắn cờ, không xóa)"],
        ["3a – BQ2 price benchmark", "build_price_benchmark.py", "agg_peer_group_benchmark, fact_listing_price_assessment", completed],
        ["3b – BQ1 budget trade-off", "build_budget_tradeoff.py", "agg_budget_tradeoff, fact_budget_pareto", completed],
        ["3c – BQ3 khu vực thay thế", "build_area_substitution.py", "agg_area_substitution", completed],
        ["3d – Data Quality KPI", "build_data_quality.py", "agg_dq_kpi", completed],
        ["3e – Lịch sử đổi giá", "build_repricing.py", "fact_listing_price_change", completed],
        ["3f – Tổng quan thị trường", "build_market_overview.py", "agg_market_overview", completed],
        ["Kiểm chứng và rebuild 2 lần", "verify_*.py, check_deterministic_rebuild.ps1", "docs/validation/*.json",
         completed if determinism.get("status") == "PASS" else "Một phần"],
        ["Mô hình giá hedonic (BQ2)", "–", "–", "Chưa làm"],
        ["Airflow DAG, dashboard Superset", "–", "–", "Chưa làm"],
    ], widths_cm=[4.2, 4.6, 4.8, 2.4])

    # ================================================================ CHƯƠNG 2
    r.h1("Bước 1: Silver listing_feature")
    r.h2("2.1. Mục tiêu")
    r.para(
        "Bảng `silver_listings_current_27` có `category_name` với "
        f"{len(feature['category_name_counts'])} nhãn khác nhau giữa 9 nguồn (ví dụ \"Nhà ở\", \"Căn hộ/Chung cư\", \"Bất động sản khác\"), "
        "và không có cột nào cho biết tin có pháp lý, mặt tiền hay hẻm ô tô. Cả ba BQ đều cần so sánh các tin 'cùng loại' và "
        "'cùng đặc điểm', vì vậy cần một bảng feature với grain **một dòng cho mỗi `source_id`** của bảng current."
    )
    r.h2("2.2. Cách làm")
    r.steps([
        ("Khảo sát dữ liệu", "Thống kê toàn bộ giá trị `category_name` và chạy thử quy tắc trên CSV snapshot cục bộ để ước lượng tỷ lệ map được và tỷ lệ của từng cờ trước khi viết job Spark."),
        ("Viết quy tắc thuần Python", "File `src/silver/feature_rules.py`: chuẩn hóa văn bản (bỏ dấu, đưa về chữ thường), bảng quy tắc gom `category_name` thành 5 nhóm `model_category`, quy tắc fallback đọc title khi nhãn nguồn quá chung, và regex cho 5 cờ `title_has_*`."),
        ("Xử lý các trường hợp dễ sai", "Phủ định (\"chưa có sổ\", \"không thang máy\"), vị trí gần (\"cách mặt tiền 20m\" không phải nhà mặt tiền), viết tắt (\"SHR\", \"HXH\", \"MT\", \"NTCC\"), từ bị trùng sau khi bỏ dấu (\"sơ đồ\" và \"sổ đỏ\") và \"TM\" chỉ được hiểu là thang máy khi đi cùng số tầng."),
        ("Viết unit test", f"File `tests/unit/test_feature_rules.py` với {tests['per_file'].get('test_feature_rules', 0)} ca test: mỗi cờ có ca dương, ca âm, ca viết tắt, ca phủ định và ca khớp nhầm."),
        ("Viết job Spark", "File `src/silver/build_listing_feature.py`: đọc current, `listing_location` và `listing_observation`; lấy `dq_status`, `record_hash` từ đúng observation đã tạo ra dòng current (cùng thứ tự với `current_from()`); áp dụng quy tắc bằng `mapPartitions` để kết quả Spark trùng khớp với unit test; ghi bảng Iceberg vào `lakehouse-silver`."),
        ("Bổ sung kiểm chứng", "Thêm vào `verify_silver.py` các check: số dòng bằng bảng current, `source_id` không NULL và không trùng, không mồ côi, điểm completeness trong [0, 1]; cảnh báo nếu một cờ bằng 0% hoặc vượt 80%, hoặc nhóm `khong_ro` vượt 2%."),
        ("Hiệu chỉnh theo dữ liệu thật", "Lần chạy đầu cho `khong_ro` = 2,69% do mogi/homedy dùng nhãn \"Bất động sản khác\" cho nhiều tin. Nhóm bổ sung bộ quy tắc fallback riêng cho title (nhận \"CH\", \"CC\", \"2PN\", \"lô\", \"lầu\"…), nâng phiên bản lên "
         f"`{feature.get('feature_rule_version')}`; tỷ lệ `khong_ro` giảm còn {pct(feature.get('model_category_unmapped_share'), 2)}."),
    ])
    r.h2("2.3. Đoạn code chính")
    r.para("Hàm xác định trạng thái một đặc điểm trong title (POSITIVE / NEGATIVE / không nhắc tới):")
    r.code(snippet("src/silver/feature_rules.py", "def feature_status(", "def map_model_category", 42))
    r.para("Chọn observation đã tạo ra dòng current để lấy `dq_status` và `record_hash`:")
    r.code(snippet("src/silver/build_listing_feature.py", "# Same ordering as current_from()", "input_rows = current.count()", 14))
    r.h2("2.4. Kết quả")
    mc = feature["model_category_counts"]
    total_feature = feature["output_rows"]
    r.table("Phân bố model_category trên bảng current", ["model_category", "Ý nghĩa", "Số tin", "Tỷ lệ"], [
        [code, CATEGORY_LABELS.get(code, code), n(count), pct(count / total_feature)]
        for code, count in sorted(mc.items(), key=lambda kv: -kv[1])
    ], numeric=[2, 3], widths_cm=[3.4, 7.0, 3.0, 2.6])
    methods = feature["model_category_method_counts"]
    r.para(
        f"Bảng `listing_feature` có {n(total_feature)} dòng, đúng bằng bảng current. {pct(methods.get('CATEGORY_NAME', 0) / total_feature)} số tin "
        f"được phân loại trực tiếp từ nhãn nguồn, {pct(methods.get('TITLE_FALLBACK', 0) / total_feature)} nhờ đọc title, "
        f"và chỉ {n(methods.get('UNMAPPED', 0))} tin không xác định được loại."
    )
    rates = feature["true_rates"]
    r.figure(charts.feature_heatmap(FIGURES / "feature_heatmap.png", data["feature_by_category"], CATEGORY_LABELS),
             "Tỷ lệ title nhắc từng đặc điểm theo model_category (toàn bộ tin current)")
    by_cat = {row["model_category"]: row for row in data["feature_by_category"]}
    frontage_top = max((row for row in data["feature_by_category"] if row["model_category"] != "khong_ro"), key=lambda row: row["frontage"])
    elevator_top = max((row for row in data["feature_by_category"] if row["model_category"] != "khong_ro"), key=lambda row: row["elevator"])
    r.para(
        f"Trên toàn bộ tin, title nhắc pháp lý ở {pct(rates['title_has_legal'])} tin, nội thất {pct(rates['title_has_furnished'])}, "
        f"mặt tiền {pct(rates['title_has_frontage'])}, thang máy {pct(rates['title_has_elevator'])} và ô tô vào {pct(rates['title_has_car_access'])}. "
        f"Hình 2.1 cho thấy các cờ phân hóa đúng theo loại hình: mặt tiền xuất hiện nhiều nhất ở nhóm "
        f"\"{CATEGORY_LABELS[frontage_top['model_category']]}\" ({pct(frontage_top['frontage'])}), thang máy ở nhóm "
        f"\"{CATEGORY_LABELS[elevator_top['model_category']]}\" ({pct(elevator_top['elevator'])}), còn nội thất tập trung ở căn hộ "
        f"({pct(by_cat.get('can_ho', {}).get('furnished'))}). Sự phân hóa này là điều kiện để các cờ có giá trị khi so sánh trong Gold."
    )
    r.table("Độ đầy đủ thông tin theo nguồn", ["Nguồn", "Số tin", "rooms_known", "legal_known", "Điểm completeness TB"], [
        [row["source"], n(row["n"]), pct(row["rooms_known"]), pct(row["legal_known"]), dec(row["avg_completeness"], 3)]
        for row in data["feature_by_source"]
    ], numeric=[1, 2, 3, 4], widths_cm=[3.6, 2.8, 3.0, 3.0, 3.6])
    r.para(
        "**Tác dụng phân tích.** `model_category` cho phép so sánh 'cùng loại' giữa 9 nguồn vốn đặt tên loại hình khác nhau; "
        "các cờ `title_has_*` trở thành biến đặc điểm cho BQ1 (đánh đổi tiện ích) và BQ2 (giải thích chênh lệch giá); "
        "các cờ `*_known` và điểm completeness cho biết mức tin cậy của từng tin, ví dụ một nguồn có `rooms_known` thấp sẽ "
        "làm nhóm tương đồng theo số phòng kém đại diện."
    )
    if data.get("feature_examples"):
        r.table("Ví dụ tin có đồng thời cờ pháp lý, ô tô vào và thang máy", ["Nguồn", "model_category", "Title"], [
            [row["source"], row["model_category"], row["title"][:110]] for row in data["feature_examples"]
        ], widths_cm=[2.4, 2.8, 10.8], font_size=10)
    r.h2("2.5. Kiểm chứng")
    feature_checks = {k: v for k, v in silver_verify.get("checks", {}).items() if k.startswith("feature") or k == "tables_in_silver_bucket"}
    r.table("Các check bổ sung cho listing_feature trong verify_silver.py", ["Check", "Kết quả"], [
        [name, "PASS" if passed else "FAIL"] for name, passed in feature_checks.items()
    ], widths_cm=[11.0, 5.0])
    r.para(
        f"Unit test: {tests['per_file'].get('test_feature_rules', 0)} ca của `test_feature_rules.py` đều PASS. "
        f"Cảnh báo coverage: {'không có' if not silver_verify.get('warnings') else '; '.join(silver_verify['warnings'])}."
    )
    r.h2("2.6. Hạn chế")
    r.bullets([
        "Cờ chỉ đọc từ title; Silver Core không giữ phần mô tả chi tiết. `title_has_* = FALSE` nghĩa là title không nhắc tới, không khẳng định BĐS thiếu đặc điểm đó.",
        f"`legal_known` chỉ đạt {pct(rates['legal_known'])}: phần lớn tin không nhắc pháp lý trong title, nên các phân tích về pháp lý chỉ mang tính tham khảo.",
        "Regex có thể khớp nhầm ở các cách viết hiếm gặp; bộ test mới bao phủ các mẫu phổ biến đã khảo sát.",
    ])

    # ================================================================ CHƯƠNG 3
    r.h1("Bước 2: Gold nền – dimension và fact")
    r.h2("3.1. Mục tiêu")
    r.para(
        "Tổ chức dữ liệu phân tích theo **star schema**: một bảng fact ở grain chi tiết nhất (một tin bán đang hiển thị) "
        "và các dimension mô tả 'ở đâu, loại gì, giá bao nhiêu, rộng bao nhiêu'. Mọi bảng tổng hợp ở Bước 3 đều đọc từ "
        "đây, nên Gold nền phải bảo đảm: khóa ngoại không bao giờ NULL, các band không chồng lấn, và một BĐS đăng ở nhiều "
        "nguồn không bị đếm nhiều lần."
    )
    r.h2("3.2. Cách làm")
    r.steps([
        ("Khai báo band trong cấu hình", "File `config/gold_bands.csv` định nghĩa 4 loại band (tổng giá, diện tích, giá/m², số phòng) theo khoảng nửa mở [lower, upper). Hàm `validate_bands` từ chối cấu hình có lỗ hổng, chồng lấn hoặc band cuối bị chặn trên."),
        ("Viết quy tắc thuần Python", "File `src/gold/gold_rules.py`: gán band, tách token title, luật nghi trùng giữa nguồn, gom cụm bằng union-find và chọn tin đại diện."),
        ("Xây 9 dimension", "File `src/gold/build_dimensions.py`: mỗi dimension có khóa INT và đúng một dòng `-1 = Không rõ`; khóa được đánh từ khóa tự nhiên đã sắp xếp nên rebuild không làm đổi khóa."),
        ("Xây fact_listing", "File `src/gold/build_fact_listing.py`: lọc tin bán (`is_rent = FALSE`) và loại REJECT; gán band bằng `mapPartitions`; nối các dimension, giá trị không map được trỏ về `-1`."),
        ("Gắn cờ trùng giữa các nguồn", f"Cặp nghi trùng khi: khác nguồn, cùng location và loại hình, diện tích lệch ≤ {pct(DUP_AREA_TOLERANCE, 0)}, giá lệch ≤ {pct(DUP_PRICE_TOLERANCE, 0)}, độ tương đồng Jaccard của token title ≥ {dec(DUP_MIN_TITLE_JACCARD, 2)}. Spark chỉ so các tin cùng khối (location, loại hình, bin log-diện tích) để tránh nhân chéo toàn bảng; các cặp được gom cụm bằng union-find; tin đại diện là tin đầy đủ thông tin nhất."),
        ("Lưu theo tầng và kiểm chứng", "Hàm `write_iceberg_table` ghi mọi bảng vào đúng bucket `lakehouse-gold`; `src/gold/verify_gold.py` kiểm tra dimension, band, khóa ngoại, số dòng và nhóm trùng."),
    ])
    r.h2("3.3. Đoạn code chính")
    r.para("Gán band theo khoảng nửa mở; giá trị thiếu hoặc không hợp lệ trả về `-1`:")
    r.code(snippet("src/gold/gold_rules.py", "def band_key(", "def title_tokens", 16))
    r.para("Tìm cặp nghi trùng giữa các nguồn bằng blocking trên Spark:")
    r.code(snippet("src/gold/build_fact_listing.py", "# Cross-source duplicate candidates", "pair_count = pairs.count()", 34))
    r.h2("3.4. Kết quả")
    dim_grain = {
        "dim_source": "1 nguồn", "dim_date": "1 ngày", "dim_location": "tỉnh – quận/huyện", "dim_property_category": "1 model_category",
        "dim_price_band": "khoảng tổng giá", "dim_area_band": "khoảng diện tích", "dim_unit_price_band": "khoảng giá/m²",
        "dim_room_band": "nhóm số phòng", "dim_dq_status": "PASS / WARN",
    }
    r.figure(charts.star_schema(FIGURES / "star_schema.png", [(name, grain) for name, grain in dim_grain.items()]),
             "Star schema của Gold nền: fact_listing và 9 dimension")
    r.table("Các dimension", ["Dimension", "Grain", "Số dòng (gồm -1)"], [
        [name, grain, n(dims_summary["table_counts"].get(name))] for name, grain in dim_grain.items()
    ], numeric=[2], widths_cm=[5.0, 7.0, 4.0])
    band_rows = []
    for dimension, label in (("price", "Tổng giá"), ("area", "Diện tích"), ("unit_price", "Giá/m²"), ("room", "Số phòng")):
        band_rows.append([label, "; ".join(band.band_label for band in bands[dimension])])
    r.table("Các band khai báo trong config/gold_bands.csv", ["Band", "Các khoảng"], band_rows, widths_cm=[3.0, 13.0], font_size=10)
    fc = fact["filter_counts"]
    r.table("Từ bảng current tới fact_listing", ["Bước", "Số dòng"], [
        ["Tin current (Silver)", n(fc["current_rows"])],
        ["Loại: tin cho thuê", n(fc["excluded_is_rent_true"])],
        ["Loại: không xác định bán/thuê", n(fc["excluded_is_rent_null"])],
        ["Loại: REJECT", n(fc["excluded_dq_reject"])],
        ["fact_listing", n(fact["output_rows"])],
        ["Trong đó: tin đại diện (dùng cho tổng hợp)", n(fact["dedup"]["representative_rows"])],
    ], numeric=[1], widths_cm=[11.0, 5.0])
    unknown = fact["unknown_fk_counts"]
    r.table("Số dòng fact có khóa ngoại trỏ về -1 (Không rõ)", ["Khóa ngoại", "Số dòng", "Tỷ lệ", "Nguyên nhân chính"], [
        ["room_band_key", n(unknown["room_band_key"]), pct(unknown["room_band_key"] / fact["output_rows"]), "Nguồn không ghi số phòng ngủ"],
        ["posted_date_key", n(unknown["posted_date_key"]), pct(unknown["posted_date_key"] / fact["output_rows"]), "Nhiều nguồn không có ngày đăng tin cậy"],
        ["price_band_key", n(unknown["price_band_key"]), pct(unknown["price_band_key"] / fact["output_rows"]), "Giá thỏa thuận"],
        ["unit_price_band_key", n(unknown["unit_price_band_key"]), pct(unknown["unit_price_band_key"] / fact["output_rows"]), "Thiếu giá hoặc diện tích"],
        ["location_key", n(unknown["location_key"]), pct(unknown["location_key"] / fact["output_rows"]), "Không xác định được tỉnh"],
        ["property_category_key", n(unknown["property_category_key"]), pct(unknown["property_category_key"] / fact["output_rows"]), "model_category = khong_ro"],
    ], numeric=[1, 2], widths_cm=[4.2, 2.6, 2.2, 7.0])
    dedup = fact["dedup"]
    r.para(
        f"Luật nghi trùng tìm được {n(dedup['candidate_pairs'])} cặp, gom thành {n(dedup['dup_groups'])} nhóm với "
        f"{n(dedup['suspect_rows'])} tin ({pct(dedup['suspect_rate'], 2)} fact). Sau khi giữ một tin đại diện mỗi nhóm, "
        f"còn {n(dedup['representative_rows'])} tin dùng cho mọi bảng tổng hợp."
    )
    pairs = list(dedup["pairs_by_source_pair"].items())[:8]
    r.table("Các cặp nguồn có nhiều tin nghi trùng nhất", ["Cặp nguồn", "Số cặp tin"], [[k, n(v)] for k, v in pairs], numeric=[1], widths_cm=[10.0, 6.0])
    fact_by_source = data["fact_by_source"]
    r.figure(charts.horizontal_bars(
        FIGURES / "fact_by_source.png", [row["source"] for row in fact_by_source], [row["fact_rows"] for row in fact_by_source],
        "Số tin bán trong fact_listing",
        annotations=[f"({pct((row['dup_suspect'] or 0) / row['fact_rows'])} nghi trùng)" for row in fact_by_source]),
        "Số tin bán theo nguồn trong fact_listing và tỷ lệ nghi trùng với nguồn khác")
    r.para(
        "**Tác dụng phân tích.** Star schema cho phép mọi câu hỏi được trả lời bằng cùng một cách: lọc và nhóm `fact_listing` "
        "theo các dimension. Dòng `-1` giữ lại tin thiếu thông tin thay vì loại bỏ, nên tổng số tin luôn khớp với Silver và "
        "người phân tích biết rõ bao nhiêu tin không được tính ở từng chiều. Cờ trùng nguồn ngăn một BĐS đăng trên hai trang "
        "bị đếm hai lần trong median giá, nhất là ở cặp nguồn có nhiều tin trùng nhất trong Bảng 3.5."
    )
    r.h2("3.5. Kiểm chứng")
    base_checks = {k: v for k, v in gold_verify.get("checks", {}).items()
                   if k.startswith(("dim_", "fact_", "dup_", "non_suspect", "tables_in_gold"))}
    r.table("Các check của Gold nền trong verify_gold.py", ["Check", "Kết quả"], [
        [name, "PASS" if passed else "FAIL"] for name, passed in base_checks.items()
    ], widths_cm=[11.0, 5.0], font_size=10)
    r.h2("3.6. Hạn chế")
    r.bullets([
        "Luật nghi trùng là heuristic: hai tin khác nhau có cùng diện tích, giá và cách đặt title gần giống vẫn có thể bị gắn cờ; ngược lại, cùng một BĐS mà title viết khác hẳn sẽ không bị phát hiện.",
        f"{pct(unknown['room_band_key'] / fact['output_rows'])} tin không có số phòng, làm giảm độ chi tiết của nhóm tương đồng theo số phòng.",
        "`dim_location` dừng ở cấp quận/huyện; chưa có mã hành chính chuẩn và chưa có ranh giới GIS.",
    ])

    # ================================================================ CHƯƠNG 4
    r.h1("Bước 3: Gold theo Business Question")
    r.para(
        "Mỗi bảng ở bước này là một job độc lập, chỉ đọc `fact_listing` và dimension, và chỉ dùng tin đại diện "
        "(`is_dup_representative = TRUE`). Phần trình bày dưới đây đi theo thứ tự triển khai 3a → 3f."
    )
    r.table("Các job của Bước 3", ["Mục", "File", "Bảng output", "Phục vụ"], [
        ["3a", "build_price_benchmark.py", "agg_peer_group_benchmark, fact_listing_price_assessment", "BQ2 (nền cho BQ1, BQ3)"],
        ["3b", "build_budget_tradeoff.py", "agg_budget_tradeoff, fact_budget_pareto", "BQ1"],
        ["3c", "build_area_substitution.py", "agg_area_substitution", "BQ3"],
        ["3d", "build_data_quality.py", "agg_dq_kpi", "Bằng chứng chất lượng"],
        ["3e", "build_repricing.py", "fact_listing_price_change", "Kiểm chứng ngược BQ2"],
        ["3f", "build_market_overview.py", "agg_market_overview", "Trang tổng quan"],
    ], widths_cm=[1.2, 4.6, 6.6, 3.6])

    # 3a
    r.h2("4.1. 3a – Price benchmark theo nhóm tương đồng")
    r.h3("Mục tiêu và cách làm")
    levels = benchmark["benchmark_groups_by_level"]
    r.steps([
        ("Định nghĩa nhóm tương đồng", "Ba cấp từ chi tiết đến thô: (quận, loại hình, band diện tích, band số phòng) → (quận, loại hình, band diện tích) → (quận, loại hình)."),
        ("Chỉ công bố nhóm đủ lớn", f"Một nhóm chỉ được dùng khi có ít nhất {MIN_PEER_GROUP_SIZE} tin đại diện. Kết quả: {n(levels.get('LOC_CAT_AREA_ROOM'))} nhóm cấp 1, {n(levels.get('LOC_CAT_AREA'))} nhóm cấp 2, {n(levels.get('LOC_CAT'))} nhóm cấp 3."),
        ("Tính thống kê chính xác", "P25, median, P75 của giá/m² bằng percentile chính xác (không dùng xấp xỉ, để kết quả ổn định giữa các lần chạy), cùng median tổng giá, diện tích, khoảng cách và tỷ lệ từng cờ đặc điểm."),
        ("Đánh giá từng tin", "Mỗi tin lấy nhóm chi tiết nhất có công bố; `price_ratio` = giá/m² ÷ median nhóm; `price_position` là thấp (< P25), hợp lý (P25–P75) hoặc cao (> P75)."),
        ("Bổ sung biến giải thích", "`feature_count_vs_peer` so số đặc điểm của tin với mức kỳ vọng của nhóm; `is_low_price_missing_legal` đánh dấu tin giá thấp mà title không nhắc pháp lý."),
    ])
    r.code(snippet("src/gold/build_price_benchmark.py", "position = (", "assessment = assessed.select(", 8))
    r.h3("Kết quả và kiểm chứng")
    positions = benchmark["price_position_counts"]
    r.table("Phân loại vị trí giá của tin đại diện", ["price_position", "Số tin", "Tỷ lệ"], [
        [POSITION_LABELS[key], n(positions.get(key, 0)), pct(positions.get(key, 0) / benchmark["assessment_rows"])]
        for key in ("thap", "hop_ly", "cao", "khong_du_du_lieu")
    ], numeric=[1, 2], widths_cm=[7.0, 4.5, 4.5])
    by_level = benchmark["assessment_by_level"]
    r.para(
        f"{n(by_level.get('LOC_CAT_AREA_ROOM'))} tin được so với nhóm chi tiết nhất (cùng quận, loại hình, diện tích và số phòng), "
        f"{n(by_level.get('LOC_CAT_AREA'))} tin phải nới bỏ số phòng và {n(by_level.get('LOC_CAT'))} tin chỉ so được ở cấp quận – loại hình. "
        f"Trong quá trình phát triển, kiểm chứng phát hiện tin giá thỏa thuận (giá/m² NULL) bị xếp nhầm là 'hợp lý' do phép so sánh với NULL; "
        "lỗi đã được sửa và được khóa bằng check `assessment_position_consistent`."
    )
    r.h3("Hạn chế")
    r.para("Vị trí P25–P75 là thống kê mô tả, chưa phải mô hình định giá; một tin 'cao' có thể có lý do hợp lệ mà title không thể hiện.")

    # 3b
    r.h2("4.2. 3b – Budget trade-off và tập Pareto")
    r.h3("Mục tiêu và cách làm")
    r.steps([
        ("Tổng hợp theo ngân sách", "Nhóm theo (band tổng giá, quận, loại hình): số tin, median diện tích, số phòng, giá/m², khoảng cách tới tâm và tỷ lệ từng cờ đặc điểm."),
        ("Xác định tin không bị trội", "Trong cùng nhóm, tin A bị trội nếu có tin B giá thấp hơn hoặc bằng, đồng thời rộng hơn hoặc bằng, nhiều phòng hơn hoặc bằng, có mọi đặc điểm A có, và tốt hơn hẳn ở ít nhất một tiêu chí."),
        ("Thuật toán", "Sắp xếp theo giá tăng dần, chỉ so mỗi tin với tập Pareto hiện tại (tính bắc cầu của quan hệ trội bảo đảm tính đúng); hàm `pareto_efficient_ids` được unit test và chạy trên từng nhóm bằng `groupByKey`."),
    ])
    r.code(snippet("src/gold/gold_rules.py", "def pareto_efficient_ids(", "def feature_similarity", 20))
    r.h3("Kết quả và kiểm chứng")
    r.para(
        f"`agg_budget_tradeoff` có {n(budget['tradeoff_groups'])} nhóm, bao phủ {n(budget['tradeoff_listings'])} tin có giá. "
        f"Trong {n(budget['pareto_rows'])} tin được xét, {n(budget['pareto_efficient_rows'])} tin ({pct(budget['pareto_efficient_rows'] / budget['pareto_rows'])}) "
        "không bị trội. Nói cách khác, khoảng bốn phần năm số tin luôn có một lựa chọn khác cùng ngân sách, cùng khu vực tốt hơn hoặc bằng ở mọi tiêu chí."
    )
    r.table("Tỷ lệ tin không bị trội theo ngân sách", ["Ngân sách", "Số tin", "Không bị trội", "Tỷ lệ"], [
        [row["price_band"], n(row["n"]), n(row["efficient"]), pct(row["efficient"] / row["n"])] for row in data["bq1_pareto_overall"]
    ], numeric=[1, 2, 3], widths_cm=[5.2, 3.6, 3.6, 3.6])
    r.h3("Hạn chế")
    r.para("Số phòng không rõ được tính là 0 nên tin không ghi số phòng ít có lợi thế; các cờ đặc điểm chỉ phản ánh điều title có nhắc tới.")

    # 3c
    r.h2("4.3. 3c – Khu vực thay thế")
    r.h3("Mục tiêu và cách làm")
    r.steps([
        ("Dùng lại nhóm tương đồng 3a", "Lấy các nhóm cấp (quận, loại hình, band diện tích) đã công bố, nên cả hai phía đều có tối thiểu "
         f"{MIN_PEER_GROUP_SIZE} tin."),
        ("Ghép cặp trong cùng tỉnh", "Ghép mỗi quận gốc với các quận khác cùng tỉnh, cùng loại hình và band diện tích, có median giá/m² thấp hơn."),
        ("Lọc theo độ tương đồng đặc điểm", f"Độ tương đồng = 1 − trung bình |chênh lệch tỷ lệ| của 5 cờ; chỉ giữ cặp ≥ {dec(MIN_SUBSTITUTION_SIMILARITY, 1)}. Mỗi cặp ghi mức rẻ hơn, chênh median tổng giá và chênh khoảng cách tới tâm."),
    ])
    r.code(snippet("src/gold/build_area_substitution.py", "similarity = ", ".filter(F.col(\"feature_similarity\")", 8))
    r.h3("Kết quả và kiểm chứng")
    r.para(
        f"Từ {n(substitution['district_groups'])} nhóm cấp quận, bảng có {n(substitution['substitution_pairs'])} cặp thay thế cho "
        f"{n(substitution['origins_with_alternative'])} nhóm gốc; mức rẻ hơn trung vị là {pct(substitution['median_price_gap_pct'])}."
    )
    r.table("Cặp thay thế theo loại hình", ["Loại hình", "Số cặp", "Mức rẻ hơn (trung vị)", "Chênh khoảng cách (km, trung vị)"], [
        [row["category_label"], n(row["pairs"]), pct(row["median_gap"]), dec(row["median_distance_diff"], 1)] for row in data["bq3_by_category"]
    ], numeric=[1, 2, 3], widths_cm=[5.6, 2.6, 3.6, 4.2])
    r.h3("Hạn chế")
    r.para("Mức rẻ hơn trung vị lớn vì bảng giữ cả cặp quận trung tâm – huyện ngoại thành; người dùng cần lọc theo `distance_diff_km`. Khoảng cách chỉ có ở tin có tọa độ.")

    # 3d
    r.h2("4.4. 3d – Data Quality KPI")
    r.para(
        "Bảng `agg_dq_kpi` theo dõi mỗi nguồn qua toàn bộ phễu Bronze → Silver → Gold. Khi xây bảng, nhóm phát hiện cột `source` "
        "của 664 dòng quarantine chứa nội dung rác do dòng CSV bị vỡ bởi mô tả HTML nhiều dòng; các dòng này được quy về đúng nguồn qua lineage `bronze_path`."
    )
    r.table("Phễu dữ liệu theo nguồn (agg_dq_kpi)", ["Nguồn", "Bronze", "Reject", "Trùng batch", "Current", "Thuê", "Fact", "WARN chính"], [
        [row["source"], n(row["bronze_rows"]), n(row["reject_rows"]), n(row["duplicate_rows_dropped"]), n(row["current_rows"]),
         n(row["rent_rows"]), n(row["fact_rows"]), ", ".join((row.get("top_warn_reasons") or [])[:2])]
        for row in data["dq_kpi"]
    ], numeric=[1, 2, 3, 4, 5, 6], widths_cm=[2.3, 1.8, 1.4, 1.7, 1.8, 1.6, 1.7, 3.7], font_size=9)
    totals = dq["totals"]
    r.para(
        f"Tổng phễu khớp với Silver: {n(totals['bronze_rows'])} dòng Bronze, {n(totals['reject_rows'])} reject, "
        f"{n(totals['duplicate_rows_dropped'])} dòng trùng trong batch, {n(totals['observation_rows'])} observation, "
        f"{n(totals['current_rows'])} tin current và {n(totals['fact_rows'])} tin bán trong fact. Check `dq_kpi_matches_silver_and_gold` khóa sự khớp này."
    )
    r.figure(charts.horizontal_bars(
        FIGURES / "dq_funnel.png",
        ["Bronze", "Accepted", "Observation", "Current", "fact_listing", "Tin đại diện"],
        [totals["bronze_rows"], totals["accepted_rows"], totals["observation_rows"], totals["current_rows"], totals["fact_rows"], totals["representative_rows"]],
        "Số dòng", height=3.4), "Phễu dữ liệu từ Bronze tới tin đại diện dùng cho Gold")

    # 3e
    r.h2("4.5. 3e – Lịch sử đổi giá")
    direction = repricing["direction_counts"]
    medians = repricing["median_price_change_pct"]
    r.para(
        f"`fact_listing_price_change` ghi mỗi lần giá của một tin bán thay đổi giữa hai phiên bản liên tiếp trong `listing_history`. "
        f"Có {n(repricing['price_change_events'])} lần đổi giá trên {n(repricing['listings_with_change'])} tin: "
        f"{n(direction.get('giam'))} lần giảm (trung vị {pct(medians.get('giam'))}) và {n(direction.get('tang'))} lần tăng (trung vị {pct(medians.get('tang'))})."
    )
    drops = repricing["price_drop_by_current_position"]
    r.table("Tỷ lệ tin từng giảm giá theo vị trí giá hiện tại", ["Vị trí giá hiện tại", "Số tin", "Từng giảm giá", "Tỷ lệ"], [
        [POSITION_LABELS[key], n(drops[key]["listings"]), n(drops[key]["with_price_drop"]), pct(drops[key]["share"], 2)]
        for key in ("thap", "hop_ly", "cao", "khong_du_du_lieu") if key in drops
    ], numeric=[1, 2, 3], widths_cm=[5.2, 3.6, 3.6, 3.6])
    r.para(
        "Bảng này chỉ cho thấy mối liên hệ, không phải dự báo: vị trí giá được tính trên giá hiện tại, tức là giá sau lần giảm. "
        "Số lần đổi giá còn ít vì phần lớn tin mới được quan sát qua một đến bốn snapshot; cần thêm nhiều snapshot để kiểm chứng ngược BQ2 một cách có ý nghĩa."
    )

    # 3f
    r.h2("4.6. 3f – Tổng quan thị trường")
    r.para(
        f"`agg_market_overview` có {n(overview['rows'])} dòng (tỉnh × loại hình) trên {n(overview['provinces'])} giá trị tỉnh/thành, "
        f"tổng {n(overview['listings'])} tin đại diện. Đây là trang đầu của dashboard: mặt bằng giá chào bán theo khu vực trước khi đi vào từng BQ."
    )
    mo = data["market_overview"]
    provinces = data["top_provinces"][:5]
    series_names = [CATEGORY_LABELS["nha_pho"], CATEGORY_LABELS["can_ho"], CATEGORY_LABELS["dat"]]
    values = {(row["province_name"], row["category_label"]): row for row in mo}
    r.figure(charts.grouped_bars(
        FIGURES / "market_overview.png", provinces,
        [(label, [(values.get((p, label)) or {}).get("median_price_per_m2", 0) / 1e6 if values.get((p, label)) else None for p in provinces]) for label in series_names],
        "Median giá/m² (triệu đồng)", value_fmt=lambda v: charts.vn(v, 0)),
        "Median giá chào bán/m² theo tỉnh/thành và loại hình (5 tỉnh nhiều tin nhất)")
    r.table("Mặt bằng giá theo tỉnh và loại hình", ["Tỉnh/thành", "Loại hình", "Số tin", "Median giá", "Median giá/m² (tr)", "P25–P75 giá/m² (tr)"], [
        [row["province_name"], row["category_label"], n(row["n_listings"]), ty(row["median_price"]), trm2(row["median_price_per_m2"]),
         f"{trm2(row['p25_price_per_m2'])} – {trm2(row['p75_price_per_m2'])}"]
        for row in mo
    ], numeric=[2, 3, 4, 5], widths_cm=[2.8, 3.8, 1.8, 2.4, 2.4, 2.8], font_size=9)

    # ================================================================ CHƯƠNG 5
    r.h1("Trả lời Business Questions")
    r.para(
        f"Các ví dụ dưới đây lấy từ {data['focus_province']} – nơi có nhiều tin nhất nên nhóm tương đồng đủ lớn. "
        "Ví dụ được chọn bằng quy tắc cố định (nhóm lớn nhất, sắp xếp xác định) trong `export_report_data.py`, nên chạy lại trên cùng dữ liệu cho cùng ví dụ."
    )

    # BQ1
    r.h2("5.1. BQ1 – Đánh đổi trong cùng ngân sách")
    r.para("**Câu hỏi.** Với ngân sách cho trước, người mua nhận được gì ở từng khu vực, và tin nào đáng xem nhất?")
    r.para("**Bảng Gold sử dụng.** `agg_budget_tradeoff` (người mua được gì), `fact_budget_pareto` (tin nào không bị trội), nối với `dim_location`, `dim_price_band`.")
    house = data["bq1_tradeoff_nha_pho"]
    if house:
        top = house[:12]
        r.figure(charts.horizontal_bars(
            FIGURES / "bq1_area_by_district.png", [row["district_name"] for row in top], [row["median_area"] for row in top],
            "Median diện tích (m²)", value_fmt=lambda v: charts.vn(v, 0) + " m²",
            annotations=[f"– cách tâm {dec(row['median_distance_km'], 1)} km" if row["median_distance_km"] is not None else "" for row in top]),
            f"Nhà phố ngân sách {data['bq1_budget_label']} tại {data['focus_province']}: median diện tích theo quận (nhóm ≥ 20 tin)")
        r.table(f"Nhà phố ngân sách {data['bq1_budget_label']}: người mua nhận được gì ở từng quận", [
            "Quận/huyện", "Số tin", "Diện tích", "Phòng", "Giá/m² (tr)", "Cách tâm (km)", "Pháp lý", "Mặt tiền", "Ô tô vào"], [
            [row["district_name"], n(row["n_listings"]), dec(row["median_area"], 0), dec(row["median_rooms"], 0), trm2(row["median_price_per_m2"]),
             dec(row["median_distance_km"], 1), pct(row["share_legal"], 0), pct(row["share_frontage"], 0), pct(row["share_car_access"], 0)]
            for row in house
        ], numeric=[1, 2, 3, 4, 5, 6, 7, 8], widths_cm=[3.2, 1.4, 1.6, 1.3, 1.9, 1.9, 1.6, 1.6, 1.5], font_size=9)
        largest, smallest = house[0], house[-1]
        r.para(
            f"**Diễn giải.** Với cùng {data['bq1_budget_label']}, nhà phố ở {largest['district_name']} có diện tích trung vị "
            f"{dec(largest['median_area'], 0)} m², gấp khoảng {dec(largest['median_area'] / smallest['median_area'], 1)} lần ở {smallest['district_name']} "
            f"({dec(smallest['median_area'], 0)} m²); đổi lại, giá/m² ở {smallest['district_name']} cao hơn và khu vực thường gần trung tâm hơn. "
            "Bảng cũng cho thấy đánh đổi không chỉ nằm ở diện tích: tỷ lệ tin có mặt tiền hay hẻm ô tô khác nhau rõ giữa các quận cùng ngân sách. "
            "Người mua có thể bắt đầu từ ưu tiên của mình (rộng hơn, gần hơn, hay cần hẻm ô tô) rồi đọc ngang bảng để thấy mình phải hy sinh điều gì."
        )
    pg = data["bq1_pareto_group"]
    r.figure(charts.pareto_scatter(FIGURES / "bq1_pareto.png", data["bq1_pareto_points"],
                                   f"Nhà phố {data['bq1_budget_label']} – {pg['district_name']}"),
             f"Tin không bị trội (Pareto) trong nhóm nhà phố {data['bq1_budget_label']} tại {pg['district_name']}")
    r.table(f"Một số tin không bị trội trong nhóm {pg['district_name']}", ["Title", "Giá", "m²", "Phòng", "Pháp lý", "Ô tô", "MT", "TM"], [
        [row["title"][:60], ty(row["price"]), dec(row["area"], 0), "" if row["rooms"] is None else str(row["rooms"]),
         yes(row["title_has_legal"]), yes(row["title_has_car_access"]), yes(row["title_has_frontage"]), yes(row["title_has_elevator"])]
        for row in data["bq1_pareto_examples"]
    ], numeric=[1, 2, 3], widths_cm=[6.6, 1.8, 1.3, 1.3, 1.3, 1.2, 1.2, 1.3], font_size=9)
    r.para(
        f"**Diễn giải.** Trong nhóm này có {n(pg['group_size'])} tin nhưng chỉ {n(pg['frontier_size'])} tin không bị trội. "
        "Các điểm xanh ở Hình trên là những lựa chọn 'đáng tiền' theo nghĩa chặt: muốn rộng hơn hoặc thêm đặc điểm thì phải trả nhiều tiền hơn. "
        "Mọi tin còn lại đều có ít nhất một tin khác rẻ hơn hoặc bằng mà không kém ở tiêu chí nào. Với người mua, đây là danh sách rút gọn để đi xem nhà trước, "
        "còn việc chọn giữa các tin Pareto phụ thuộc vào ưu tiên cá nhân (diện tích hay số phòng hay pháp lý)."
    )

    # BQ2
    r.h2("5.2. BQ2 – Tin có giá bất thường")
    r.para("**Câu hỏi.** Tin nào có giá lệch đáng kể so với các BĐS tương đồng, và chênh lệch có được giải thích bởi đặc điểm không?")
    r.para("**Bảng Gold sử dụng.** `agg_peer_group_benchmark` (mặt bằng nhóm), `fact_listing_price_assessment` (đánh giá từng tin).")
    pos_rows = defaultdict(dict)
    for row in data["position_by_category"]:
        pos_rows[row["category_label"]][row["price_position"]] = row["count"]
    category_order = [label for label in CATEGORY_LABELS.values() if label in pos_rows and label != "Không rõ"]
    shares = {key: [] for key in ("thap", "hop_ly", "cao", "khong_du_du_lieu")}
    for label in category_order:
        total = sum(pos_rows[label].values())
        for key in shares:
            shares[key].append(pos_rows[label].get(key, 0) / total * 100)
    r.figure(charts.position_stacked(FIGURES / "bq2_positions.png", category_order, shares),
             "Vị trí giá của tin đại diện so với nhóm tương đồng, theo loại hình")
    pg2 = data["bq2_peer_group"]
    r.para(
        f"Ví dụ nhóm tương đồng lớn nhất cho căn hộ: **{pg2['district_name']}, {pg2['area_band']}, {pg2['room_band']}** "
        f"– {n(pg2['n_listings'])} tin, giá/m² P25 – median – P75 = {trm2(pg2['p25_price_per_m2'])} – {trm2(pg2['median_price_per_m2'])} – "
        f"{trm2(pg2['p75_price_per_m2'])} triệu."
    )
    r.table("Ví dụ đánh giá giá trong nhóm tương đồng", ["Title", "Giá", "m²", "Giá/m² (tr)", "Tỷ lệ/median", "Vị trí", "Số đặc điểm (so nhóm)"], [
        [row["title"][:55], ty(row["price"]), dec(row["area"], 0), trm2(row["price_per_m2"]), dec(row["price_ratio"], 2),
         POSITION_LABELS[row["price_position"]], f"{row['feature_count']} ({'+' if (row['feature_count_vs_peer'] or 0) >= 0 else ''}{dec(row['feature_count_vs_peer'], 1)})"]
        for row in data["bq2_examples"]
    ], numeric=[1, 2, 3, 4, 6], widths_cm=[5.6, 1.7, 1.1, 1.8, 1.8, 1.6, 2.4], font_size=9)
    gap = {row["price_position"]: row for row in data["position_feature_gap"]}
    r.table("Đặc điểm trung bình theo vị trí giá", ["Vị trí giá", "Số tin", "Số đặc điểm TB", "So với kỳ vọng nhóm", "Thấp + không nhắc pháp lý"], [
        [POSITION_LABELS[key], n(gap[key]["n"]), dec(gap[key]["avg_feature_count"], 2), dec(gap[key]["avg_feature_count_vs_peer"], 2),
         pct(gap[key]["share_low_missing_legal"]) if key == "thap" else "–"]
        for key in ("thap", "hop_ly", "cao") if key in gap
    ], numeric=[1, 2, 3, 4], widths_cm=[3.0, 2.6, 3.0, 3.6, 3.8])
    high, low = gap.get("cao", {}), gap.get("thap", {})
    r.para(
        f"**Diễn giải.** Theo định nghĩa, khoảng một nửa số tin nằm trong P25–P75 của nhóm; phần còn lại là ứng viên cho 'định giá cao' hoặc 'cơ hội'. "
        f"Tin ở vị trí cao trung bình có {dec(high.get('avg_feature_count'), 2)} đặc điểm, so với {dec(low.get('avg_feature_count'), 2)} ở tin vị trí thấp; "
        f"chênh lệch so với kỳ vọng nhóm lần lượt là {dec(high.get('avg_feature_count_vs_peer'), 2)} và {dec(low.get('avg_feature_count_vs_peer'), 2)}. "
        "Như vậy một phần chênh lệch giá đi kèm với đặc điểm được nêu (mặt tiền, thang máy, pháp lý), nhưng phần lớn chưa được giải thích bởi các cờ hiện có. "
        + (f"Các tỷ lệ cực đoan như {dec(max(row['price_ratio'] or 0 for row in data['bq2_examples']), 1)} lần median nhiều khả năng là lỗi nhập giá hoặc diện tích hơn là định giá thật, nên cũng là tín hiệu kiểm tra chất lượng dữ liệu. " if max(row['price_ratio'] or 0 for row in data['bq2_examples']) > 3 else "")
        + "Với người mua, tin 'cao' mà không có thêm đặc điểm là tin cần thương lượng; tin 'thấp' mà title không nhắc pháp lý "
        f"({pct(low.get('share_low_missing_legal'))} tin thấp) là tin cần kiểm tra giấy tờ trước khi xem như cơ hội."
    )

    # BQ3
    r.h2("5.3. BQ3 – Khu vực thay thế")
    r.para("**Câu hỏi.** Nếu không mua được ở khu vực mong muốn, khu vực nào có BĐS tương đương nhưng cần ngân sách thấp hơn?")
    r.para("**Bảng Gold sử dụng.** `agg_area_substitution`, nối `dim_location` cho tên quận gốc và quận thay thế.")
    origin = data["bq3_origin"]
    examples = data["bq3_examples"]
    if examples:
        r.figure(charts.horizontal_bars(
            FIGURES / "bq3_alternatives.png", [row["alternative_district"] for row in examples], [row["price_gap_pct"] * 100 for row in examples],
            "Rẻ hơn theo median giá/m² (%)", value_fmt=lambda v: charts.vn(v, 0) + "%",
            annotations=[f"– tương đồng {dec(row['feature_similarity'], 2)}, {'+' if (row['distance_diff_km'] or 0) >= 0 else ''}{dec(row['distance_diff_km'], 1)} km" for row in examples]),
            f"Khu vực thay thế cho nhà phố {origin['area_band']} tại {origin['origin_district']}")
        r.table(f"Khu vực thay thế cho nhà phố {origin['area_band']} tại {origin['origin_district']}", [
            "Hạng", "Quận thay thế", "Giá/m² gốc (tr)", "Giá/m² thay thế (tr)", "Rẻ hơn", "Tiết kiệm (median)", "Chênh km", "Tương đồng", "Số tin"], [
            [row["substitution_rank"], row["alternative_district"], trm2(row["origin_median_price_per_m2"]), trm2(row["alternative_median_price_per_m2"]),
             pct(row["price_gap_pct"], 0), ty(row["typical_budget_saving"]), dec(row["distance_diff_km"], 1), dec(row["feature_similarity"], 2),
             n(row["alternative_n_listings"])]
            for row in examples
        ], numeric=[0, 2, 3, 4, 5, 6, 7, 8], widths_cm=[1.1, 2.8, 1.8, 2.0, 1.4, 2.0, 1.5, 1.7, 1.7], font_size=9)
        nearby = [row for row in examples if row["distance_diff_km"] is not None and row["distance_diff_km"] <= 5]
        best = nearby[0] if nearby else examples[0]
        r.para(
            f"**Diễn giải.** Người muốn mua nhà phố {origin['area_band']} ở {origin['origin_district']} có {len(examples)} khu vực thay thế được liệt kê. "
            f"Lựa chọn hợp lý nhất không nhất thiết là quận rẻ nhất: ví dụ {best['alternative_district']} rẻ hơn khoảng {pct(best['price_gap_pct'], 0)} "
            f"theo giá/m², trung vị tiết kiệm {ty(best['typical_budget_saving'])} và chỉ xa tâm hơn khoảng {dec(best['distance_diff_km'], 1)} km, "
            f"với độ tương đồng đặc điểm {dec(best['feature_similarity'], 2)}. "
            "Các quận có mức rẻ hơn lớn nhất thường đi kèm khoảng cách xa hơn nhiều, nên người dùng cần đọc đồng thời cột mức rẻ hơn và cột chênh km. "
            "Ngưỡng tương đồng đặc điểm giúp loại các cặp 'rẻ vì khác loại' (ví dụ khu vực gần như không có nhà mặt tiền)."
        )

    # ================================================================ CHƯƠNG 6
    r.h1("Tổng hợp kiểm chứng")
    r.h2("6.1. Unit test")
    r.table("Kết quả pytest theo file", ["File test", "Số ca", "Kết quả"], [
        [f"tests/unit/{name}.py", n(count), "PASS" if not tests["failed"].get(name) else f"FAIL ({tests['failed'][name]})"]
        for name, count in sorted(tests["per_file"].items())
    ] + [["Tổng", n(tests["total"]), "PASS" if tests["failures"] == 0 else f"FAIL ({tests['failures']})"]],
        numeric=[1], widths_cm=[9.0, 3.0, 4.0])
    r.h2("6.2. verify_silver.py")
    r.table("Các check của verify_silver.py", ["Check", "Kết quả"], [
        [name, "PASS" if passed else "FAIL"] for name, passed in silver_verify.get("checks", {}).items()
    ], widths_cm=[11.0, 5.0], font_size=10)
    r.h2("6.3. verify_gold.py")
    gold_checks = gold_verify.get("checks", {})
    r.para(f"`verify_gold.py` chạy {len(gold_checks)} check, kết quả: {sum(gold_checks.values())}/{len(gold_checks)} PASS.")
    r.table("Các check của verify_gold.py", ["Check", "Kết quả"], [
        [name, "PASS" if passed else "FAIL"] for name, passed in gold_checks.items()
    ], widths_cm=[11.0, 5.0], font_size=9)
    r.h2("6.4. Rebuild deterministic")
    if determinism:
        r.para(
            "Script `scripts/check_deterministic_rebuild.ps1` chạy toàn bộ pipeline Silver và Gold hai lần liên tiếp trên cùng dữ liệu Bronze, "
            f"sau đó so số dòng của {determinism['tables_compared']} bảng. Kết quả: **{determinism['status']}**"
            + ("." if determinism["status"] == "PASS" else f", bảng lệch: {', '.join(determinism['mismatches'])}.")
        )
        r.table("Số dòng các bảng qua hai lần rebuild", ["Bảng", "Lần 1", "Lần 2", "Khớp"], [
            [row["table"], n(row["run1"]), n(row["run2"]), "Có" if row["equal"] else "Không"] for row in determinism["tables"]
        ], numeric=[1, 2], widths_cm=[8.0, 2.8, 2.8, 2.4], font_size=9)
    else:
        r.para("Chưa có file `docs/validation/deterministic_rebuild.json`; chạy `scripts/check_deterministic_rebuild.ps1` rồi sinh lại báo cáo.")

    # ================================================================ CHƯƠNG 7
    r.h1("Hạn chế và hướng phát triển")
    r.h2("7.1. Hạn chế")
    r.bullets([
        "**Chưa có mô hình hedonic.** BQ2 mới so sánh mô tả (P25–P75) và đếm đặc điểm; chưa ước lượng được phần chênh lệch giá do từng đặc điểm gây ra.",
        "**Giá là giá chào bán**, không phải giá giao dịch; kết luận của BQ2 là 'giá chào lệch so với thị trường chào bán'.",
        "**Đặc điểm chỉ đọc từ title**, nên tỷ lệ pháp lý, nội thất… là cận dưới của thực tế.",
        "**Vị trí** dừng ở quận/huyện theo text; chỉ một phần tin có tọa độ, chưa có GIS point-in-polygon và mã hành chính chuẩn.",
        "**Lịch sử ngắn**: mới có vài snapshot, nên bảng đổi giá chưa đủ để kiểm chứng ngược BQ2.",
        "**Pipeline rebuild toàn bộ** (`createOrReplace`); chưa có incremental `MERGE INTO`, chưa dọn snapshot Iceberg cũ.",
    ])
    r.h2("7.2. Giả định cần nhóm xác nhận")
    r.bullets([
        "Cách gom 5 nhóm `model_category` (shophouse, liền kề xếp chung biệt thự; officetel xếp chung căn hộ).",
        "Ngưỡng band trong `config/gold_bands.csv`, ngưỡng nhóm tương đồng tối thiểu "
        f"{MIN_PEER_GROUP_SIZE} tin, ngưỡng tương đồng đặc điểm {dec(MIN_SUBSTITUTION_SIMILARITY, 1)}.",
        f"Luật nghi trùng (diện tích ±{pct(DUP_AREA_TOLERANCE, 0)}, giá ±{pct(DUP_PRICE_TOLERANCE, 0)}, Jaccard ≥ {dec(DUP_MIN_TITLE_JACCARD, 2)}).",
        "Loại tin không rõ bán/thuê khỏi Gold; số phòng không rõ được tính là 0 khi xét Pareto.",
        "Ví dụ trong báo cáo tập trung vào TP. Hồ Chí Minh vì có nhiều tin nhất.",
    ])
    r.h2("7.3. Hướng phát triển")
    r.bullets([
        "Mô hình hedonic (hồi quy log giá/m² theo vị trí, loại hình, diện tích, số phòng, đặc điểm) bằng `pyspark.ml`, lưu `model_version` và sai số vào bảng đánh giá giá.",
        "K-Means phân cụm quận theo hồ sơ giá và đặc điểm để mở rộng BQ3.",
        "Airflow DAG điều phối Bronze → Silver → Gold → verify; dashboard Superset cho từng BQ qua Trino.",
        "Ranh giới hành chính GIS, incremental `MERGE INTO` và dọn snapshot Iceberg (`expire_snapshots`).",
    ])

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
        "# 4. (Tùy chọn) Kiểm chứng rebuild deterministic: chạy 2 lần và so row count",
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
