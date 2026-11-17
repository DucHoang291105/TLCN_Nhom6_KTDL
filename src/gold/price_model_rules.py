"""Pure-Python rules for the BQ2 hedonic price model (no Spark, no numpy).

The model is ordinary least squares on ``log(price_per_m2)`` with a small ridge
penalty, fitted separately for each property category:

    log(price_per_m2) = intercept + district + area band + room band + 5 title flags
                        + distance-to-centre band + district x size group + ward

Every regressor is a 0/1 indicator, so the normal equations X'X are plain
co-occurrence counts. Spark sums X'X and X'y over the training rows; the small
system (a few hundred terms per category) is solved here with a Cholesky
factorisation. This keeps the job free of numpy (the Spark image does not ship
it, and ``pyspark.ml`` needs it), deterministic, and testable without Spark.

Interpretation: ``exp(beta) - 1`` is the price/m² difference associated with a
term, holding the other terms fixed. Flags only record that a *title mentions*
a feature, so a flag coefficient is an association with that wording, not the
causal value of the feature itself.
"""

from __future__ import annotations

import json
import math
from collections.abc import Iterable, Iterator, Mapping, Sequence
from dataclasses import dataclass
from functools import cached_property
from pathlib import Path

MODEL_VERSION = "hedonic_ols_blend"
# Deterministic train/test split: hash(SPLIT_SEED, source_id) mod 100 < TRAIN_PERCENT.
SPLIT_SEED = 42
TRAIN_PERCENT = 80
# Property categories modelled (keys of dim_property_category): nha_pho, can_ho,
# biet_thu, dat. "phong_tro_khac" mixes rooms, offices and warehouses.
MODEL_CATEGORY_KEYS = (1, 2, 3, 4)
# A district needs this many training listings for its own coefficient; rarer
# districts are pooled per province, then into one "other" level.
MIN_LEVEL_TRAIN_ROWS = 20
# Ridge penalty on every term except the intercept. Small next to the
# thousands of rows per category; it only stabilises sparse levels.
RIDGE_LAMBDA = 1.0
# Training rows outside these quantiles of log(price_per_m2), per category,
# are not used to fit (mis-typed prices). They still get a prediction.
TRIM_QUANTILES = (0.01, 0.99)
# Factors with a dropped reference level (the most frequent one).
FACTORS = ("location", "area_band", "room_band", "dist_band")
# Extended terms. Distance to the province centre is NULL for ~60% of listings (no
# coordinates), so "na" is its own level. The district x size and ward terms
# are 0/1 columns kept only with enough training rows; they are the only
# stand-in for project/building, which is not observed.
DISTANCE_EDGES_KM = (3.0, 7.0, 15.0)
SIZE_GROUP_OF_AREA_BAND = {1: "S", 2: "S", 3: "M", 4: "M", 5: "L", 6: "L"}
MIN_EXTRA_TERM_TRAIN_ROWS = 30
EXTRA_FACTORS = ("loc_size", "ward")
INTERCEPT = "intercept"
OTHER_LOCATION = "other"
BLEND_CONFIG_PATH = Path(__file__).resolve().parents[2] / "config" / "price_model_blend.json"


@dataclass(frozen=True)
class BlendRule:
    """How one category's final prediction mixes the model and the baseline."""

    weight: float
    pooled_to_baseline: bool


def load_blend_config(path: Path = BLEND_CONFIG_PATH) -> dict[int, BlendRule]:
    """Per-category blend rules from config/price_model_blend.json (validated)."""

    raw = json.loads(path.read_text(encoding="utf-8"))["categories"]
    rules: dict[int, BlendRule] = {}
    for key, item in raw.items():
        weight = float(item["weight"])
        if not 0.0 <= weight <= 1.0:
            raise ValueError(f"Blend weight out of range for category {key}: {weight}")
        if float(item["ridge"]) != RIDGE_LAMBDA:
            raise ValueError(f"Category {key} was selected with ridge {item['ridge']}, but the model is fitted with {RIDGE_LAMBDA}")
        rules[int(key)] = BlendRule(weight, bool(item["pooled_to_baseline"]))
    missing = set(MODEL_CATEGORY_KEYS) - set(rules)
    if missing:
        raise ValueError(f"Blend config lacks categories: {sorted(missing)}")
    return rules


def is_pooled_location_term(term: str) -> bool:
    """True for a pooled-province (``p:``) or ``other`` location level."""

    return term.startswith("p:") or term == OTHER_LOCATION


def blend_prediction(model: float, baseline: float, rule: BlendRule, location_term: str) -> float:
    """Final price/m2: baseline alone for pooled locations when the rule says so,
    else the geometric (log-space) mix of model and baseline."""

    if rule.pooled_to_baseline and is_pooled_location_term(location_term):
        return baseline
    return math.exp(rule.weight * math.log(model) + (1 - rule.weight) * math.log(baseline))


def location_level(district_train_rows: int, pooled_province_train_rows: int,
                   location_key: int, province_name: str | None,
                   province_only: bool = False) -> str:
    """Own level if it has enough training rows, else pooled province, else other.

    ``province_only`` marks listings whose location is known down to the
    province but not the district (dim_location level PROVINCE). They get their
    own ``prov:`` level, kept apart from ``d:`` so a district coefficient is
    never mixed with a province-wide average of listings without a district.
    """

    if district_train_rows >= MIN_LEVEL_TRAIN_ROWS:
        return f"prov:{location_key}" if province_only else f"d:{location_key}"
    if province_name and pooled_province_train_rows >= MIN_LEVEL_TRAIN_ROWS:
        return f"p:{province_name}"
    return OTHER_LOCATION


def distance_band(distance_km: float | None) -> str:
    """Band of the distance to the province centre; ``na`` when unknown."""

    if distance_km is None:
        return "na"
    for index, edge in enumerate(DISTANCE_EDGES_KM):
        if distance_km < edge:
            return str(index)
    return str(len(DISTANCE_EDGES_KM))


def listing_terms(location: str, area_band_key: int, room_band_key: int,
                  flags: Mapping[str, bool], distance_km: float | None = None,
                  ward: str | None = None, extras: bool = False) -> list[str]:
    """Terms present for one listing. Unknown room band (-1) is its own level.

    With ``extras`` the distance band, the district x size-group
    interaction and the ward are added. A ward name repeats across districts
    ("Phường 1"), so it is keyed by the district. Province-level and pooled
    locations get no ward; pooled locations get no interaction either.
    """

    terms = [f"location={location}", f"area_band={area_band_key}", f"room_band={room_band_key}"]
    terms += [f"flag={name}" for name, value in sorted(flags.items()) if value]
    if extras:
        terms.append(f"dist_band={distance_band(distance_km)}")
        has_own_level = location.startswith(("d:", "prov:"))
        group = SIZE_GROUP_OF_AREA_BAND.get(area_band_key)
        if has_own_level and group:
            terms.append(f"loc_size={location}|{group}")
        if ward and location.startswith("d:"):
            terms.append(f"ward={location}|{ward}")
    return terms


def term_factor(term: str) -> str:
    return term.split("=", 1)[0]


def term_level(term: str) -> str:
    return term.split("=", 1)[1] if "=" in term else ""


@dataclass(frozen=True)
class Vocabulary:
    """Model columns of one category. Index 0 is the intercept."""

    terms: tuple[str, ...]
    references: tuple[str, ...]
    train_rows: tuple[tuple[str, int], ...]

    @cached_property
    def index(self) -> dict[str, int]:
        return {term: position for position, term in enumerate(self.terms)}

    @cached_property
    def reference_set(self) -> frozenset[str]:
        return frozenset(self.references)

    @property
    def size(self) -> int:
        return len(self.terms)


def build_vocabulary(term_counts: Mapping[str, int]) -> Vocabulary:
    """Drop one reference level per factor (the most frequent, ties by name).

    Flags have no reference: absence is 0. A flag never set in training gets
    no column. Extra terms (district x size, ward) are kept only with
    MIN_EXTRA_TERM_TRAIN_ROWS training rows.
    """

    references = []
    kept = []
    for factor in FACTORS:
        levels = sorted((term, count) for term, count in term_counts.items() if term_factor(term) == factor and count > 0)
        if not levels:
            continue
        reference = min(levels, key=lambda item: (-item[1], item[0]))[0]
        references.append(reference)
        kept += [term for term, _ in levels if term != reference]
    flags = sorted(term for term, count in term_counts.items() if term_factor(term) == "flag" and count > 0)
    extras = sorted(
        term for term, count in term_counts.items()
        if term_factor(term) in EXTRA_FACTORS and count >= MIN_EXTRA_TERM_TRAIN_ROWS
    )
    return Vocabulary(
        terms=(INTERCEPT, *kept, *flags, *extras),
        references=tuple(references),
        train_rows=tuple(sorted(term_counts.items())),
    )


def design_indices(terms: Iterable[str], vocabulary: Vocabulary) -> tuple[list[int], int]:
    """Column indices of a listing (always with the intercept) and the number
    of factor levels never seen in training (they fall back to the reference)."""

    index = vocabulary.index
    references = vocabulary.reference_set
    columns = {0}
    unseen = 0
    for term in terms:
        if term in index:
            columns.add(index[term])
        elif term_factor(term) != "flag" and term not in references:
            unseen += 1
    return sorted(columns), unseen


def normal_equation_entries(columns: Sequence[int], y: float) -> Iterator[tuple[tuple[int, int], float]]:
    """Contributions of one row to X'X (upper triangle, i <= j) and X'y (j = -1)."""

    for position, i in enumerate(columns):
        for j in columns[position:]:
            yield (i, j), 1.0
        yield (i, -1), y


def cholesky(matrix: list[list[float]]) -> list[list[float]]:
    size = len(matrix)
    lower = [[0.0] * size for _ in range(size)]
    for i in range(size):
        row_i = lower[i]
        for j in range(i + 1):
            row_j = lower[j]
            total = matrix[i][j] - sum(row_i[k] * row_j[k] for k in range(j))
            if i == j:
                if total <= 0:
                    raise ValueError(f"Normal equations are not positive definite at column {i}")
                row_i[i] = math.sqrt(total)
            else:
                row_i[j] = total / row_j[j]
    return lower


def cholesky_solve(lower: list[list[float]], rhs: Sequence[float]) -> list[float]:
    size = len(lower)
    forward = [0.0] * size
    for i in range(size):
        forward[i] = (rhs[i] - sum(lower[i][k] * forward[k] for k in range(i))) / lower[i][i]
    solution = [0.0] * size
    for i in reversed(range(size)):
        solution[i] = (forward[i] - sum(lower[k][i] * solution[k] for k in range(i + 1, size))) / lower[i][i]
    return solution


def fit_ridge(entries: Mapping[tuple[int, int], float], size: int,
              ridge: float = RIDGE_LAMBDA) -> tuple[list[float], list[list[float]]]:
    """Solve (X'X + ridge*I) beta = X'y, intercept not penalised.

    ``entries`` holds the summed output of :func:`normal_equation_entries`.
    Returns the coefficients and the Cholesky factor (for standard errors).
    """

    matrix = [[0.0] * size for _ in range(size)]
    rhs = [0.0] * size
    for (i, j), value in entries.items():
        if j == -1:
            rhs[i] += value
        else:
            matrix[i][j] += value
            if i != j:
                matrix[j][i] += value
    for i in range(1, size):
        matrix[i][i] += ridge
    lower = cholesky(matrix)
    return cholesky_solve(lower, rhs), lower


def inverse_diagonal(lower: list[list[float]], columns: Iterable[int]) -> dict[int, float]:
    """Selected diagonal entries of (X'X + ridge*I)^-1 from its Cholesky factor."""

    size = len(lower)
    result = {}
    for column in columns:
        unit = [0.0] * size
        unit[column] = 1.0
        result[column] = cholesky_solve(lower, unit)[column]
    return result


def predict_log(columns: Iterable[int], beta: Sequence[float]) -> float:
    return sum(beta[column] for column in columns)


def premium_pct(coefficient: float) -> float:
    """Approximate % difference in price per m² for a 0/1 term."""

    return math.exp(coefficient) - 1.0


def median(values: Sequence[float]) -> float | None:
    ordered = sorted(values)
    if not ordered:
        return None
    middle = len(ordered) // 2
    return ordered[middle] if len(ordered) % 2 else (ordered[middle - 1] + ordered[middle]) / 2


def regression_metrics(pairs: Iterable[tuple[float, float]]) -> dict[str, float | int | None]:
    """MAE and MAPE on price per m²; R² on the log scale the model is fitted on.

    ``pairs`` are (actual, predicted) price per m², both > 0.
    """

    pairs = [(actual, predicted) for actual, predicted in pairs if actual and predicted and actual > 0 and predicted > 0]
    n = len(pairs)
    if n == 0:
        return {"n": 0, "mae": None, "mape": None, "median_ape": None, "r2_log": None}
    errors = [abs(actual - predicted) for actual, predicted in pairs]
    ape = [abs(actual - predicted) / actual for actual, predicted in pairs]
    logs = [(math.log(actual), math.log(predicted)) for actual, predicted in pairs]
    mean_log = sum(actual for actual, _ in logs) / n
    sst = sum((actual - mean_log) ** 2 for actual, _ in logs)
    sse = sum((actual - predicted) ** 2 for actual, predicted in logs)
    return {
        "n": n,
        "mae": round(sum(errors) / n, 2),
        "mape": round(sum(ape) / n, 6),
        "median_ape": round(median(ape), 6),
        "r2_log": round(1 - sse / sst, 6) if sst > 0 else None,
    }
