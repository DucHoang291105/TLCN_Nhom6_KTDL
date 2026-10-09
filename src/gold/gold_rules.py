"""Pure-Python rules shared by the Gold jobs.

Band assignment and cross-source duplicate clustering live here so they can be
unit tested without Spark; the Spark jobs apply the same functions.
"""

from __future__ import annotations

import csv
import math
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from src.silver.feature_rules import fold_text

PROJECT_ROOT = Path(__file__).resolve().parents[2]
BANDS_PATH = PROJECT_ROOT / "config/gold_bands.csv"
BAND_DIMENSIONS = ("price", "area", "unit_price", "room")
UNKNOWN_KEY = -1
UNKNOWN_LABEL = "Không rõ"

MODEL_CATEGORY_LABELS: dict[str, tuple[int, str]] = {
    "nha_pho": (1, "Nhà phố / nhà riêng"),
    "can_ho": (2, "Căn hộ chung cư"),
    "biet_thu": (3, "Biệt thự / liền kề / shophouse"),
    "dat": (4, "Đất"),
    "phong_tro_khac": (5, "Phòng trọ / thương mại / khác"),
}
DQ_STATUS_KEYS = {"PASS": 1, "WARN": 2}

# Cross-source duplicate rule: same location and category, area within 2%,
# price within 3%, different source and similar title tokens.
DUP_AREA_TOLERANCE = 0.02
DUP_PRICE_TOLERANCE = 0.03
DUP_MIN_TITLE_JACCARD = 0.35
# (area tolerance, price tolerance, min title Jaccard) used for the
# sensitivity check of the heuristic; "default" is what fact_listing applies.
DUP_SENSITIVITY = {
    "strict": (0.01, 0.02, 0.50),
    "default": (DUP_AREA_TOLERANCE, DUP_PRICE_TOLERANCE, DUP_MIN_TITLE_JACCARD),
    "loose": (0.03, 0.05, 0.25),
}
# Words present in most titles carry no identity signal.
TITLE_STOPWORDS = {
    "ban", "can", "nha", "gap", "gia", "tot", "re", "chinh", "chu", "ty", "trieu",
    "m2", "m", "dt", "o", "tai", "va", "co", "cho", "voi", "duong", "quan", "q",
    "phuong", "p", "tp", "huyen", "x", "lh", "nhanh", "dep", "ngay",
}


@dataclass(frozen=True)
class Band:
    dimension: str
    band_key: int
    band_code: str
    band_label: str
    lower: float
    upper: float | None

    def contains(self, value: float) -> bool:
        return self.lower <= value and (self.upper is None or value < self.upper)


def load_bands(path: Path = BANDS_PATH) -> dict[str, list[Band]]:
    bands: dict[str, list[Band]] = {dimension: [] for dimension in BAND_DIMENSIONS}
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        for raw in csv.DictReader(handle):
            dimension = raw["dimension"].strip()
            if dimension not in bands:
                raise RuntimeError(f"Unknown band dimension: {dimension}")
            upper = raw["upper"].strip()
            bands[dimension].append(Band(
                dimension, int(raw["band_key"]), raw["band_code"].strip(),
                raw["band_label"].strip(), float(raw["lower"]), float(upper) if upper else None,
            ))
    for dimension, items in bands.items():
        items.sort(key=lambda band: band.lower)
        validate_bands(dimension, items)
    return bands


def validate_bands(dimension: str, bands: list[Band]) -> None:
    """Bands must be half-open, contiguous, non-overlapping and end unbounded."""

    if not bands:
        raise RuntimeError(f"No bands configured for {dimension}")
    keys = [band.band_key for band in bands]
    if len(set(keys)) != len(keys) or UNKNOWN_KEY in keys:
        raise RuntimeError(f"Duplicate or reserved band_key in {dimension}")
    for current, following in zip(bands, bands[1:]):
        if current.upper is None or current.upper != following.lower:
            raise RuntimeError(f"Gap or overlap in {dimension} between {current.band_code} and {following.band_code}")
        if current.upper <= current.lower:
            raise RuntimeError(f"Empty band {current.band_code} in {dimension}")
    if bands[-1].upper is not None:
        raise RuntimeError(f"Last {dimension} band must be unbounded")


def band_key(value: Any, bands: list[Band]) -> int:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return UNKNOWN_KEY
    if not math.isfinite(number) or number <= 0:
        return UNKNOWN_KEY
    for band in bands:
        if band.contains(number):
            return band.band_key
    return UNKNOWN_KEY


def title_tokens(title: Any) -> list[str]:
    return sorted({token for token in fold_text(title).split() if token not in TITLE_STOPWORDS and len(token) > 1})


def jaccard(left: Iterable[str], right: Iterable[str]) -> float:
    a, b = set(left), set(right)
    return len(a & b) / len(a | b) if a | b else 0.0


def within(left: float, right: float, tolerance: float) -> bool:
    return abs(left - right) <= tolerance * max(left, right)


def is_cross_source_duplicate(left: Mapping[str, Any], right: Mapping[str, Any]) -> bool:
    """Reference implementation of the duplicate rule (Spark mirrors it)."""

    if left["source"] == right["source"]:
        return False
    if left["location_key"] == UNKNOWN_KEY or left["location_key"] != right["location_key"]:
        return False
    if left["property_category_key"] == UNKNOWN_KEY or left["property_category_key"] != right["property_category_key"]:
        return False
    if not (left["area"] and right["area"] and left["price"] and right["price"]):
        return False
    return (
        within(left["area"], right["area"], DUP_AREA_TOLERANCE)
        and within(left["price"], right["price"], DUP_PRICE_TOLERANCE)
        and jaccard(left["title_tokens"], right["title_tokens"]) >= DUP_MIN_TITLE_JACCARD
    )


def cluster_pairs(pairs: Iterable[tuple[str, str]]) -> dict[str, str]:
    """Union-find over duplicate pairs; returns member -> smallest member id."""

    parent: dict[str, str] = {}

    def find(item: str) -> str:
        parent.setdefault(item, item)
        while parent[item] != item:
            parent[item] = parent[parent[item]]
            item = parent[item]
        return item

    for left, right in pairs:
        root_left, root_right = find(left), find(right)
        if root_left != root_right:
            low, high = sorted((root_left, root_right))
            parent[high] = low
    return {item: find(item) for item in list(parent)}


def mutual_best_pairs(pairs: Iterable[Mapping[str, Any]]) -> list[tuple[str, str]]:
    """Keep a candidate pair only if each side is the other's best match in
    that source (highest title Jaccard, then smallest price and area gap).

    Without this, one generic listing can link several listings of another
    source and union-find chains unrelated listings into one group.
    """

    pairs = list(pairs)
    best: dict[tuple[str, str], tuple[tuple[Any, ...], str]] = {}

    def offer(me: str, other: str, other_source: str, pair: Mapping[str, Any]) -> None:
        key = (-pair["jaccard"], pair["price_gap"], pair["area_gap"], other)
        current = best.get((me, other_source))
        if current is None or key < current[0]:
            best[(me, other_source)] = (key, other)

    for pair in pairs:
        offer(pair["left"], pair["right"], pair["right_source"], pair)
        offer(pair["right"], pair["left"], pair["left_source"], pair)
    return sorted(
        (pair["left"], pair["right"])
        for pair in pairs
        if best[(pair["left"], pair["right_source"])][1] == pair["right"]
        and best[(pair["right"], pair["left_source"])][1] == pair["left"]
    )


def resolve_dup_groups(groups: Mapping[str, str], source_of: Mapping[str, str]) -> dict[str, str]:
    """Group status: RESOLVED (one listing per source) or AMBIGUOUS.

    A group holding two listings of the same source cannot be one property
    seen on several sites; its members are kept (not collapsed).
    """

    sources: dict[str, list[str]] = {}
    for member, group in groups.items():
        sources.setdefault(group, []).append(source_of[member])
    return {
        group: "AMBIGUOUS" if len(set(found)) < len(found) else "RESOLVED"
        for group, found in sources.items()
    }


def pick_representative(members: Iterable[Mapping[str, Any]]) -> str:
    """Highest completeness, then most recent observation, then smallest id."""

    def sort_key(member: Mapping[str, Any]) -> tuple[float, float, str]:
        observed = member.get("scraped_at")
        timestamp = observed.timestamp() if observed is not None else float("-inf")
        return (-(member.get("feature_completeness_score") or 0.0), -timestamp, member["source_id"])

    return min(members, key=sort_key)["source_id"]


# ---------------------------------------------------------------------------
# Business Question rules (Gold step 3)
# ---------------------------------------------------------------------------

# A peer group is published only with at least this many representative
# listings; smaller groups fall back to a coarser benchmark level.
MIN_PEER_GROUP_SIZE = 5
BENCHMARK_LEVELS = ("LOC_CAT_AREA_ROOM", "LOC_CAT_AREA", "LOC_CAT")
# Position against the peer group's P25-P75 range. A descriptive position, not
# a judgement that a price is fair.
PRICE_POSITIONS = ("duoi_p25", "p25_p75", "tren_p75", "khong_du_du_lieu")
# Area substitution: the title-flag profile of the two groups must be at least
# this similar (1 - mean absolute difference of the five flag shares) and no
# single flag share may differ by more than MAX_FLAG_SHARE_GAP, so a group
# without street frontage never substitutes for one where all have it.
# Similarity compares how often titles *mention* features; it does not show
# that two groups of properties are equivalent.
MIN_SUBSTITUTION_SIMILARITY = 0.8
MAX_FLAG_SHARE_GAP = 0.3
FEATURE_FLAGS = ("legal", "furnished", "frontage", "elevator", "car_access")


def price_position(value: Any, p25: Any, p75: Any) -> str:
    """Classify a listing's price per m² against its peer group's P25–P75."""

    if value is None or p25 is None or p75 is None:
        return "khong_du_du_lieu"
    if value < p25:
        return "duoi_p25"
    if value > p75:
        return "tren_p75"
    return "p25_p75"


def _rooms_at_least(better: Any, worse: Any) -> bool:
    """Unknown rooms are not comparable with known rooms (never 0)."""

    if better is None or worse is None:
        return better is None and worse is None
    return better >= worse


def _dominates(better: Mapping[str, Any], worse: Mapping[str, Any]) -> bool:
    criteria = [
        better["price"] <= worse["price"],
        better["area"] >= worse["area"],
        _rooms_at_least(better["rooms"], worse["rooms"]),
        *(b >= w for b, w in zip(better["flags"], worse["flags"])),
    ]
    strictly = (
        better["price"] < worse["price"] or better["area"] > worse["area"]
        or (better["rooms"] is not None and worse["rooms"] is not None and better["rooms"] > worse["rooms"])
        or any(b > w for b, w in zip(better["flags"], worse["flags"]))
    )
    return all(criteria) and strictly


def pareto_efficient_ids(items: Iterable[Mapping[str, Any]]) -> set[str]:
    """Listings not dominated within one comparison group.

    Criteria: lower price; larger area; more rooms; each title flag. A flag is
    TRUE only when the title states it, so "not dominated" is relative to the
    extracted information, not to the real property. Unknown rooms are
    incomparable with known rooms, so a listing is never beaten on rooms it
    does not state. Dominance is transitive and a dominator always sorts no
    later than the listing it dominates, so comparing against the frontier is
    sufficient.
    """

    ordered = sorted(
        items,
        key=lambda item: (
            item["price"], -item["area"], -(item["rooms"] if item["rooms"] is not None else -1),
            -sum(item["flags"]), item["source_id"],
        ),
    )
    frontier: list[Mapping[str, Any]] = []
    for item in ordered:
        if not any(_dominates(member, item) for member in frontier):
            frontier.append(item)
    return {item["source_id"] for item in frontier}


def feature_similarity(left: Mapping[str, float], right: Mapping[str, float]) -> float:
    """1 - mean absolute difference of the title-flag shares of two groups."""

    differences = [abs((left.get(flag) or 0.0) - (right.get(flag) or 0.0)) for flag in FEATURE_FLAGS]
    return round(1.0 - sum(differences) / len(differences), 6)


def max_flag_share_gap(left: Mapping[str, float], right: Mapping[str, float]) -> float:
    return round(max(abs((left.get(f) or 0.0) - (right.get(f) or 0.0)) for f in FEATURE_FLAGS), 6)


def cost_at_target_area(median_price_per_m2: float, target_area: float) -> float:
    """Estimated cost of ``target_area`` m² at a group's median price per m².

    Comparing two groups at the *same* area avoids the trap of a lower price
    per m² on a larger typical area meaning a larger total budget.
    """

    return median_price_per_m2 * target_area
