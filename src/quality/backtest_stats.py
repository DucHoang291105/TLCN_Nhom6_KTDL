"""Small, dependency-free statistical tests for the BQ2 backtest.

The Spark image has no numpy/scipy, so the tests are written out here and unit
tested against hand-computed values. All p-values are two-sided and use the
normal approximation, which is adequate for the group sizes in the backtest
(hundreds to tens of thousands of listings).
"""

from __future__ import annotations

import math
from collections import Counter
from collections.abc import Iterable, Mapping, Sequence


def normal_two_sided_p(z: float) -> float:
    return math.erfc(abs(z) / math.sqrt(2.0))


def chi2_1df_p(statistic: float) -> float:
    return math.erfc(math.sqrt(max(statistic, 0.0) / 2.0))


def two_proportion_test(events_a: int, n_a: int, events_b: int, n_b: int) -> dict[str, float | int | None]:
    """Compare event rates of group A and group B.

    z uses the pooled rate (test of equal rates); the 95% interval of the
    difference uses the unpooled standard error.
    """

    if n_a == 0 or n_b == 0:
        return {"n_a": n_a, "n_b": n_b, "rate_a": None, "rate_b": None, "difference": None,
                "ci95_difference": None, "relative_risk": None, "z": None, "p_value": None}
    rate_a, rate_b = events_a / n_a, events_b / n_b
    pooled = (events_a + events_b) / (n_a + n_b)
    se_pooled = math.sqrt(pooled * (1 - pooled) * (1 / n_a + 1 / n_b))
    se_diff = math.sqrt(rate_a * (1 - rate_a) / n_a + rate_b * (1 - rate_b) / n_b)
    difference = rate_a - rate_b
    z = difference / se_pooled if se_pooled > 0 else 0.0
    return {
        "n_a": n_a, "events_a": events_a, "n_b": n_b, "events_b": events_b,
        "rate_a": round(rate_a, 6), "rate_b": round(rate_b, 6),
        "difference": round(difference, 6),
        "ci95_difference": [round(difference - 1.96 * se_diff, 6), round(difference + 1.96 * se_diff, 6)],
        "relative_risk": round(rate_a / rate_b, 4) if rate_b > 0 else None,
        "z": round(z, 4),
        "p_value": normal_two_sided_p(z) if se_pooled > 0 else 1.0,
    }


def median_from_counts(counts: Mapping[int, int]) -> float | None:
    total = sum(counts.values())
    if total == 0:
        return None
    ordered = sorted(counts.items())

    def value_at(position: int) -> int:
        seen = 0
        for value, count in ordered:
            seen += count
            if position < seen:
                return value
        return ordered[-1][0]

    if total % 2:
        return float(value_at(total // 2))
    return (value_at(total // 2 - 1) + value_at(total // 2)) / 2


def mann_whitney_from_counts(counts_a: Mapping[int, int], counts_b: Mapping[int, int]) -> dict[str, float | int | None]:
    """Mann–Whitney U test for integer data given as value -> frequency.

    Mid-ranks for ties and the tie-corrected variance; no continuity
    correction. ``prob_a_greater`` = U_a / (n_a n_b): the chance a random
    listing of A has a larger value than one of B (ties count half).
    """

    n_a, n_b = sum(counts_a.values()), sum(counts_b.values())
    if n_a == 0 or n_b == 0:
        return {"n_a": n_a, "n_b": n_b, "u_a": None, "z": None, "p_value": None, "prob_a_greater": None}
    total = n_a + n_b
    rank_sum_a = 0.0
    tie_term = 0.0
    position = 0
    for value in sorted(set(counts_a) | set(counts_b)):
        tied = counts_a.get(value, 0) + counts_b.get(value, 0)
        mid_rank = position + (tied + 1) / 2
        rank_sum_a += counts_a.get(value, 0) * mid_rank
        tie_term += tied ** 3 - tied
        position += tied
    u_a = rank_sum_a - n_a * (n_a + 1) / 2
    mean = n_a * n_b / 2
    variance = n_a * n_b / 12 * ((total + 1) - tie_term / (total * (total - 1)))
    z = (u_a - mean) / math.sqrt(variance) if variance > 0 else 0.0
    return {
        "n_a": n_a, "n_b": n_b,
        "median_a": median_from_counts(counts_a), "median_b": median_from_counts(counts_b),
        "u_a": u_a, "z": round(z, 4),
        "p_value": normal_two_sided_p(z) if variance > 0 else 1.0,
        "prob_a_greater": round(u_a / (n_a * n_b), 6),
    }


def cmh_test(strata: Iterable[Sequence[int]]) -> dict[str, float | int | None]:
    """Cochran–Mantel–Haenszel test over 2x2 strata (a, b, c, d).

    a = group A with event, b = group A without, c = group B with event,
    d = group B without. Tests whether the A-vs-B association holds after
    stratification (here: by source, whose crawl rhythm differs).
    """

    numerator = denominator = 0.0
    observed_minus_expected = variance = 0.0
    used = 0
    for a, b, c, d in strata:
        n = a + b + c + d
        if n < 2 or (a + b) == 0 or (c + d) == 0:
            continue
        used += 1
        numerator += a * d / n
        denominator += b * c / n
        expected = (a + b) * (a + c) / n
        observed_minus_expected += a - expected
        variance += (a + b) * (c + d) * (a + c) * (b + d) / (n * n * (n - 1))
    if used == 0 or variance == 0:
        return {"strata": used, "odds_ratio_mh": None, "chi2": None, "p_value": None}
    chi2 = observed_minus_expected ** 2 / variance
    return {
        "strata": used,
        "odds_ratio_mh": round(numerator / denominator, 4) if denominator > 0 else None,
        "chi2": round(chi2, 4),
        "p_value": chi2_1df_p(chi2),
    }


def quantile_edges(values: Sequence[float], parts: int) -> list[float]:
    """Cut points that split ``values`` into ``parts`` groups of near-equal size."""

    ordered = sorted(values)
    if not ordered:
        return []
    return [ordered[min(len(ordered) - 1, (len(ordered) * k) // parts)] for k in range(1, parts)]


def bucket(value: float, edges: Sequence[float]) -> int:
    """1-based group of ``value`` given ``quantile_edges`` (upper edge exclusive)."""

    for index, edge in enumerate(edges):
        if value < edge:
            return index + 1
    return len(edges) + 1


def counts(values: Iterable[int]) -> Counter:
    return Counter(values)
