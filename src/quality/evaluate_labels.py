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
    denominator = 2 * tp + fp + fn
    f1 = 2 * tp / denominator if denominator else None
    total = tp + fp + fn + tn
    return {
        "support_true": tp + fn, "labelled": total, "tp": tp, "fp": fp, "fn": fn, "tn": tn,
        "precision": round(precision, 4) if precision is not None else None,
        "recall": round(recall, 4) if recall is not None else None,
        "f1": round(f1, 4) if f1 is not None else None,
        "accuracy": round((tp + tn) / total, 4) if total else None,
    }


def evaluate(labels: list[Mapping[str, str]], predictions: Mapping[str, Mapping[str, str]]) -> dict:
    ids = [row.get("sample_id", "").strip() for row in labels]
    if any(not sample_id for sample_id in ids) or len(ids) != len(set(ids)):
        raise ValueError("Label sample_id must be nonempty and unique")
    if set(ids) - set(predictions):
        raise ValueError("Missing predictions for label sample IDs")
    category_pairs, by_source = [], defaultdict(list)
    flag_pairs: dict[str, list[tuple[bool, bool]]] = {flag: [] for flag in FLAGS}
    invalid = []
    for row in labels:
        pred = predictions.get(row["sample_id"])
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


def index_predictions(rows):
    ids = [row.get("sample_id", "").strip() for row in rows]
    if any(not sample_id for sample_id in ids) or len(ids) != len(set(ids)):
        raise ValueError("Prediction sample_id must be nonempty and unique")
    return dict(zip(ids, rows))


def review_complete(row, needs_review: bool = False):
    """Labelled with a valid category and five flags; pre-labelled sheets also need two different named reviewers."""

    first, second = ((row.get(column) or "").strip() for column in ("review_1", "review_2"))
    reviewed = bool(first and second and first.casefold() != second.casefold()) if needs_review else True
    return (row.get("true_category") in CATEGORIES and bool((row.get("labeler") or "").strip()) and reviewed and
            all(parse_flag(row.get(f"true_{flag}")) is not None or
                (row.get(f"true_{flag}") or "").strip() == "?" for flag in FLAGS))


def review_rounds(sheet: list[Mapping[str, str]]) -> dict:
    """How many rows each review round covered, and by how many people (names are not repeated here)."""

    names = {column: [(row.get(column) or "").strip() for row in sheet] for column in ("review_1", "review_2")}
    return {
        "review_1_rows": sum(1 for name in names["review_1"] if name),
        "review_1_reviewers": len(set(names["review_1"]) - {""}),
        "review_2_rows": sum(1 for name in names["review_2"] if name),
        "review_2_reviewers": len(set(names["review_2"]) - {""}),
        "same_person_both_rounds": sum(1 for a, b in zip(names["review_1"], names["review_2"]) if a and a.casefold() == b.casefold()),
    }


def review_changes(prelabels: list[Mapping[str, str]], sheet: list[Mapping[str, str]]) -> dict:
    """How many label cells the reviewers changed from the pre-labels, per column."""

    columns = ("true_category", *(f"true_{flag}" for flag in FLAGS))
    before = {row["sample_id"]: row for row in prelabels}
    changed = Counter()
    rows_changed = 0
    for row in sheet:
        old = before.get(row["sample_id"], {})
        diff = [c for c in columns if (row.get(c) or "").strip() != (old.get(c) or "").strip()]
        changed.update(diff)
        rows_changed += bool(diff)
    cells = len(sheet) * len(columns)
    return {"rows_changed": rows_changed, "cells_changed": sum(changed.values()), "cells": cells,
            "cell_change_rate": round(sum(changed.values()) / cells, 4) if cells else None,
            "by_column": {c: changed[c] for c in columns}}


def main() -> None:
    with (LABEL_DIR / "labeling_sheet.csv").open(encoding="utf-8-sig", newline="") as handle:
        sheet = list(csv.DictReader(handle))
    with (LABEL_DIR / "labeling_predictions.csv").open(encoding="utf-8-sig", newline="") as handle:
        predictions = index_predictions(list(csv.DictReader(handle)))
    result = evaluate(sheet, predictions)
    labelers = sorted({(row.get("labeler") or "").strip() for row in sheet if row.get("true_category")} - {""})
    # Pre-labelled by an assistant, then reviewed twice by team members:
    # the frozen pre-labels show how much the reviewers changed.
    prelabel_path = LABEL_DIR / "labeling_sheet_prelabel.csv"
    prelabels = None
    if prelabel_path.exists():
        with prelabel_path.open(encoding="utf-8-sig", newline="") as handle:
            prelabels = list(csv.DictReader(handle))
    needs_review = prelabels is not None
    rounds = review_rounds(sheet)
    summary = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "sample_rows": len(sheet),
        "labelers": labelers,
        "truth_definition": {
            "true_category": "property type judged from title, price, area and the listing page",
            "true_<flag>": "1 if the title states the feature, 0 if not (\"?\" = cannot tell)",
        },
        "label_process": ("pre-labelled by an AI assistant without seeing the system predictions, then reviewed by team members: "
                          f"review_1 on {rounds['review_1_rows']}/{len(sheet)} rows ({rounds['review_1_reviewers']} member(s), "
                          f"one person per row), review_2 on {rounds['review_2_rows']}/{len(sheet)} rows") if needs_review
                         else "labelled by a person",
        "reviewers": sorted({(row.get(c) or "").strip() for row in sheet for c in ("review_1", "review_2")} - {""}),
        "review_rounds": rounds if needs_review else None,
        **result,
        "review_changes": review_changes(prelabels, sheet) if needs_review else None,
        "reviewed_rows": sum(review_complete(row, needs_review) for row in sheet),
        "status": "COMPLETE" if sheet and all(review_complete(row, needs_review) for row in sheet) else "PARTIAL",
    }
    out = PROJECT_ROOT / "docs/validation/label_accuracy.json"
    out.write_text(json.dumps(summary, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(json.dumps({k: summary[k] for k in ("sample_rows", "labelled_rows", "category_accuracy", "status")}, indent=2))
    for flag, scores in summary["flags"].items():
        print(f"{flag:11s} precision={scores['precision']} recall={scores['recall']} f1={scores['f1']} labelled={scores['labelled']}")


if __name__ == "__main__":
    main()
