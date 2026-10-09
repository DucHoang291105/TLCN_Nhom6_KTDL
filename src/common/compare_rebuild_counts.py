"""Compare table row counts of two full rebuilds (deterministic rebuild check).

Usage: python -m src.common.compare_rebuild_counts <run1_dir> <run2_dir>
Each directory holds the silver_final_verification.json and
gold_verification.json produced by one rebuild.
"""

from __future__ import annotations

import json
import sys
from datetime import datetime, timezone
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
EVIDENCE = ("silver_final_verification.json", "gold_verification.json")


def load_counts(run_dir: Path) -> dict[str, int]:
    counts: dict[str, int] = {}
    for name in EVIDENCE:
        data = json.loads((run_dir / name).read_text(encoding="utf-8"))
        layer = "silver" if name.startswith("silver") else "gold"
        counts.update({f"{layer}.{table}": int(rows) for table, rows in data["table_counts"].items()})
    return counts


def main(run1: str, run2: str) -> None:
    first, second = load_counts(Path(run1)), load_counts(Path(run2))
    tables = sorted(set(first) | set(second))
    rows = [{"table": t, "run1": first.get(t), "run2": second.get(t), "equal": first.get(t) == second.get(t)} for t in tables]
    mismatches = [row["table"] for row in rows if not row["equal"]]
    summary = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "tables_compared": len(rows),
        "tables": rows,
        "status": "PASS" if not mismatches else "FAIL",
        "mismatches": mismatches,
    }
    out = PROJECT_ROOT / "docs/validation/deterministic_rebuild.json"
    out.write_text(json.dumps(summary, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(summary, ensure_ascii=False, indent=2))
    if mismatches:
        raise SystemExit(f"Row counts differ between rebuilds: {mismatches}")


if __name__ == "__main__":
    main(*sys.argv[1:3])
