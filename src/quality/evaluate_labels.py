"""Measure listing_feature accuracy against the manual labels.

Reads ``labeling_sheet.csv`` (filled by a person) and
``labeling_predictions.csv`` (system output) and writes
``docs/validation/label_accuracy.json``:

- model_category: accuracy (overall and per source), per-class precision /
  recall / F1 and the confusion matrix;
- each title flag: precision, recall, F1 and support, where the truth is
  "the title states the feature" (1) or not (0). Rows marked "?" are skipped.

Pure Python, runs on the host: python -m src.quality.evaluate_labels
"""

from __future__ import annotations

import csv
import json
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable, Mapping

PROJECT_ROOT = Path(__file__).resolve().parents[2]
LABEL_DIR = PROJECT_ROOT / "docs/validation/labeling"
CATEGORIES = ("nha_pho", "can_ho", "biet_thu", "dat", "phong_tro_khac", "khong_ro")
FLAGS = ("legal", "furnished", "frontage", "elevator", "car_access")
TRUE_VALUES, FALSE_VALUES = {"1", "y", "yes", "co", "có", "x"}, {"0", "n", "no", "khong", "không"}


def parse_flag(value: str | None) -> bool | None:
    text = (value or "").strip().casefold()
    if text in TRUE_VALUES:
        return True
    if text in FALSE_VALUES:
        return False
    return None  # blank or "?" = not labelled


def binary_scores(pairs: Iterable[tuple[bool, bool]]) -> dict[str, float | int | None]:
    """pairs = (truth, prediction)."""

    counts = Counter(pairs)
    tp, fp, fn, tn = counts[(True, True)], counts[(False, True)], counts[(True, False)], counts[(False, False)]
    precision = tp / (tp + fp) if tp + fp else None
    recall = tp / (tp + fn) if tp + fn else None
    f1 = 2 * precision * recall / (precision + recall) if precision and recall else None
    total = tp + fp + fn + tn
    return {
        "support_true": tp + fn, "labelled": total, "tp": tp, "fp": fp, "fn": fn, "tn": tn,
        "precision": round(precision, 4) if precision is not None else None,
        "recall": round(recall, 4) if recall is not None else None,
        "f1": round(f1, 4) if f1 is not None else None,
        "accuracy": round((tp + tn) / total, 4) if total else None,
    }


def evaluate(labels: list[Mapping[str, str]], predictions: Mapping[str, Mapping[str, str]]) -> dict:
    category_pairs, by_source = [], defaultdict(list)
    flag_pairs: dict[str, list[tuple[bool, bool]]] = {flag: [] for flag in FLAGS}
    invalid = []
    for row in labels:
        pred = predictions.get(row["sample_id"])
        if pred is None:
            continue
        truth = (row.get("true_category") or "").strip()
        if truth:
            if truth not in CATEGORIES:
                invalid.append(row["sample_id"])
            else:
                category_pairs.append((truth, pred["model_category"]))
                by_source[pred["source"]].append(truth == pred["model_category"])
        for flag in FLAGS:
            value = parse_flag(row.get(f"true_{flag}"))
            if value is not None:
                flag_pairs[flag].append((value, str(pred[f"title_has_{flag}"]).strip().casefold() == "true"))
    confusion = Counter(category_pairs)
    per_class = {
        category: binary_scores(((t == category), (p == category)) for t, p in category_pairs)
        for category in CATEGORIES if any(category in pair for pair in category_pairs)
    }
    correct = sum(t == p for t, p in category_pairs)
    return {
        "labelled_rows": len(category_pairs),
        "invalid_category_labels": invalid,
        "category_accuracy": round(correct / len(category_pairs), 4) if category_pairs else None,
        "category_accuracy_by_source": {s: {"n": len(v), "accuracy": round(sum(v) / len(v), 4)} for s, v in sorted(by_source.items())},
        "category_per_class": per_class,
        "category_confusion": {f"{t}->{p}": n for (t, p), n in sorted(confusion.items())},
        "flags": {flag: binary_scores(pairs) for flag, pairs in flag_pairs.items()},
    }


def main() -> None:
    sheet = list(csv.DictReader((LABEL_DIR / "labeling_sheet.csv").open(encoding="utf-8-sig")))
    predictions = {row["sample_id"]: row for row in csv.DictReader((LABEL_DIR / "labeling_predictions.csv").open(encoding="utf-8-sig"))}
    result = evaluate(sheet, predictions)
    labelers = sorted({(row.get("labeler") or "").strip() for row in sheet if row.get("true_category")} - {""})
    summary = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "sample_rows": len(sheet),
        "labelers": labelers,
        "truth_definition": {
            "true_category": "property type judged by a person from title, price, area and the listing page",
            "true_<flag>": "1 if the title states the feature, 0 if not (\"?\" = cannot tell)",
        },
        **result,
        "status": "COMPLETE" if result["labelled_rows"] == len(sheet) else "PARTIAL",
    }
    out = PROJECT_ROOT / "docs/validation/label_accuracy.json"
    out.write_text(json.dumps(summary, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(json.dumps({k: summary[k] for k in ("sample_rows", "labelled_rows", "category_accuracy", "status")}, indent=2))
    for flag, scores in summary["flags"].items():
        print(f"{flag:11s} precision={scores['precision']} recall={scores['recall']} f1={scores['f1']} labelled={scores['labelled']}")


if __name__ == "__main__":
    main()
