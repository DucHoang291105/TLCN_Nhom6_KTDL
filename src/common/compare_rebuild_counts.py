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
from src.common.table_fingerprints import MULTISET_TABLES

PROJECT_ROOT = Path(__file__).resolve().parents[2]


def main(run1: str, run2: str) -> None:
    payload_a = json.loads(Path(run1).read_text(encoding="utf-8"))
    payload_b = json.loads(Path(run2).read_text(encoding="utf-8"))
    first, second = payload_a["tables"], payload_b["tables"]
    version_equal = payload_a.get("fingerprint_version") == payload_b.get("fingerprint_version") == "sha256_multiset_v2"
    rows = []
    for table in sorted(set(first) | set(second)):
        a, b = first.get(table, {}), second.get(table, {})
        row = {
            "table": table,
            "rows_run1": a.get("rows"), "rows_run2": b.get("rows"),
            "rows_equal": a.get("rows") == b.get("rows"),
            "keys_equal": a.get("distinct_keys") == b.get("distinct_keys"),
            "grain_unique": (a.get("rows") is not None and b.get("rows") is not None
                             and a.get("distinct_keys") == a.get("rows")
                             and b.get("distinct_keys") == b.get("rows")),
            "grain_uniqueness_required": table not in MULTISET_TABLES,
            "schema_equal": a.get("schema") == b.get("schema"),
            "content_equal": version_equal and a.get("hash_method") == b.get("hash_method") and a.get("content_hash") is not None and a.get("content_hash") == b.get("content_hash"),
            "excluded_columns": a.get("excluded_columns", []),
        }
        rows.append(row)
    mismatches = [r["table"] for r in rows if not (
        r["rows_equal"] and r["keys_equal"]
        and (r["grain_unique"] or not r["grain_uniqueness_required"])
        and r["schema_equal"] and r["content_equal"])]
    if not rows:
        mismatches.append("<no tables compared>")
    summary = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "method": "Compare two materialized Silver/Gold states: row count, distinct grain keys and full multiset SHA256; build-time columns excluded. Both files must use the same fingerprint version.",
        "fingerprint_version": payload_a.get("fingerprint_version"),
        "tables_compared": len(rows),
        "tables": rows,
        "status": "PASS" if not mismatches else "FAIL",
        "mismatches": mismatches,
    }
    out = PROJECT_ROOT / "docs/validation/deterministic_rebuild.json"
    from src.common.atomic_io import write_json_atomic
    write_json_atomic(out, summary)
    print(json.dumps({k: v for k, v in summary.items() if k != "tables"}, ensure_ascii=False, indent=2))
    if mismatches:
        raise SystemExit(f"Rebuilds differ: {mismatches}")


if __name__ == "__main__":
    main(*sys.argv[1:3])
