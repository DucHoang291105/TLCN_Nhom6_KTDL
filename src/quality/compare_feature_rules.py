"""Compare the shipped feature rules with the current ones on the labelled sample.

The labelled sample is split once, by hash, into a dev part (rules may be
changed by looking at its errors) and a test part (only used to report). The
"before" predictions are ``labeling_predictions.csv`` (the rules that built the
current Silver); the "after" predictions are recomputed here with the current
``src.silver.feature_rules`` from the same title, category name and evidence,
which is what ``build_listing_feature`` does.

Writes docs/validation/label_accuracy_rules_compare.json.
Run on the host: python -m src.quality.compare_feature_rules
"""

from __future__ import annotations

import csv
import hashlib
import json
from datetime import datetime, timezone

from src.quality.evaluate_labels import FLAGS, LABEL_DIR, PROJECT_ROOT, evaluate
from src.silver.feature_rules import FEATURE_RULE_VERSION, feature_status, map_model_category

SPLIT_SALT = "labeling-split-v1"


def split_of(sample_id: str) -> str:
    """About one third of the sample is held out as test, fixed by hash."""

    digest = int(hashlib.sha1(f"{SPLIT_SALT}:{sample_id}".encode()).hexdigest(), 16)
    return "test" if digest % 3 == 0 else "dev"


def recompute(label_row: dict, prediction: dict) -> dict:
    title = label_row["title"]
    category, method, _ = map_model_category(prediction.get("category_name") or None, title, prediction.get("category_evidence") or None)
    out = {**prediction, "model_category": category, "model_category_method": method}
    for flag in FLAGS:
        out[f"title_has_{flag}"] = str(feature_status(flag, title) == "POSITIVE").lower()
    return out


def headline(result: dict) -> dict:
    return {"category_accuracy": result["category_accuracy"],
            **{f"{flag}_{k}": result["flags"][flag][k] for flag in FLAGS for k in ("precision", "recall")}}


def main() -> None:
    with (LABEL_DIR / "labeling_sheet.csv").open(encoding="utf-8-sig", newline="") as handle:
        sheet = list(csv.DictReader(handle))
    with (LABEL_DIR / "labeling_predictions.csv").open(encoding="utf-8-sig", newline="") as handle:
        before = {row["sample_id"]: row for row in csv.DictReader(handle)}
    after = {row["sample_id"]: recompute(row, before[row["sample_id"]]) for row in sheet}

    report = {"generated_at": datetime.now(timezone.utc).isoformat(), "split_salt": SPLIT_SALT,
              "rules_after": FEATURE_RULE_VERSION, "parts": {}}
    for part in ("dev", "test", "all"):
        rows = [r for r in sheet if part == "all" or split_of(r["sample_id"]) == part]
        b, a = evaluate(rows, before), evaluate(rows, after)
        report["parts"][part] = {"rows": len(rows), "before": headline(b), "after": headline(a),
                                 "after_full": a if part == "all" else None}
    changed = [
        {"sample_id": r["sample_id"], "split": split_of(r["sample_id"]), "title": r["title"],
         "changes": {k: [before[r["sample_id"]][k], after[r["sample_id"]][k]]
                     for k in ("model_category", *(f"title_has_{f}" for f in FLAGS))
                     if before[r["sample_id"]][k].lower() != after[r["sample_id"]][k].lower()}}
        for r in sheet
    ]
    report["changed_predictions"] = [c for c in changed if c["changes"]]
    out = PROJECT_ROOT / "docs/validation/label_accuracy_rules_compare.json"
    out.write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    for part, values in report["parts"].items():
        print(f"{part} (n={values['rows']})")
        for key in values["before"]:
            print(f"  {key:22s} {values['before'][key]!s:>8} -> {values['after'][key]!s:>8}")
    print("changed predictions:", len(report["changed_predictions"]))


if __name__ == "__main__":
    main()
