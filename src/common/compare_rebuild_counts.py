"""Compare the table fingerprints of two full rebuilds.

Usage: python -m src.common.compare_rebuild_counts <run1.json> <run2.json>
Each file is produced by ``src/common/table_fingerprints.py`` after one
rebuild. Per table the row count, the number of distinct grain keys and the
order-independent content hash (build-time columns excluded) must be equal.
"""

from __future__ import annotations

import json
import sys
from datetime import datetime, timezone
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]


def main(run1: str, run2: str) -> None:
    first = json.loads(Path(run1).read_text(encoding="utf-8"))["tables"]
    second = json.loads(Path(run2).read_text(encoding="utf-8"))["tables"]
    rows = []
    for table in sorted(set(first) | set(second)):
        a, b = first.get(table, {}), second.get(table, {})
        row = {
            "table": table,
            "rows_run1": a.get("rows"), "rows_run2": b.get("rows"),
            "rows_equal": a.get("rows") == b.get("rows"),
            "keys_equal": a.get("distinct_keys") == b.get("distinct_keys"),
            "grain_unique": a.get("distinct_keys") == a.get("rows"),
            "content_equal": a.get("content_hash") is not None and a.get("content_hash") == b.get("content_hash"),
            "excluded_columns": a.get("excluded_columns", []),
        }
        rows.append(row)
    mismatches = [r["table"] for r in rows if not (r["rows_equal"] and r["keys_equal"] and r["content_equal"])]
    summary = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "method": "two full Silver+Gold rebuilds from the same Bronze input; per table row count, distinct grain keys and an order-independent SHA-256 content hash (build-time columns excluded)",
        "tables_compared": len(rows),
        "tables": rows,
        "status": "PASS" if not mismatches else "FAIL",
        "mismatches": mismatches,
    }
    out = PROJECT_ROOT / "docs/validation/deterministic_rebuild.json"
    out.write_text(json.dumps(summary, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(json.dumps({k: v for k, v in summary.items() if k != "tables"}, ensure_ascii=False, indent=2))
    if mismatches:
        raise SystemExit(f"Rebuilds differ: {mismatches}")


if __name__ == "__main__":
    main(*sys.argv[1:3])
