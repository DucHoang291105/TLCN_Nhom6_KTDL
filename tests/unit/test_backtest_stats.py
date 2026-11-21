import math
import random
from collections import Counter

from src.quality.backtest_stats import (
    bucket, chi2_1df_p, cmh_test, mann_whitney_from_counts, median_from_counts, normal_two_sided_p,
    quantile_edges, two_proportion_test,
)


def close(a, b, tol=1e-4):
    return math.isclose(a, b, rel_tol=tol, abs_tol=tol)


def test_normal_and_chi2_p_values():
    assert close(normal_two_sided_p(1.96), 0.05, 1e-3)
    assert close(normal_two_sided_p(0.0), 1.0)
    # chi2 with 1 df is z squared.
    assert close(chi2_1df_p(1.96 ** 2), normal_two_sided_p(1.96))


def test_two_proportion_test_hand_computed():
    result = two_proportion_test(30, 100, 20, 100)
    assert close(result["difference"], 0.1)
    assert close(result["z"], 0.1 / math.sqrt(0.25 * 0.75 * 0.02))
    assert close(result["p_value"], 0.1025, 1e-3)
    assert close(result["relative_risk"], 1.5)
    low, high = result["ci95_difference"]
    assert low < 0.1 < high


def test_two_proportion_test_empty_group():
    assert two_proportion_test(1, 0, 2, 10)["p_value"] is None


def test_median_from_counts():
    assert median_from_counts({1: 1, 2: 1, 10: 2}) == 6.0
    assert median_from_counts({5: 3}) == 5.0
    assert median_from_counts({}) is None


def test_mann_whitney_separated_groups():
    result = mann_whitney_from_counts(Counter([1, 2, 3]), Counter([4, 5, 6]))
    assert result["u_a"] == 0 and result["prob_a_greater"] == 0
    assert close(result["z"], -4.5 / math.sqrt(9 / 12 * 7))


def test_mann_whitney_all_tied():
    result = mann_whitney_from_counts({1: 2}, {1: 2})
    assert result["p_value"] == 1.0 and result["prob_a_greater"] == 0.5


def test_mann_whitney_matches_brute_force_with_ties():
    rng = random.Random(3)
    a = [rng.randint(0, 6) for _ in range(40)]
    b = [rng.randint(1, 8) for _ in range(55)]
    brute = sum(1.0 if x > y else 0.5 if x == y else 0.0 for x in a for y in b)
    result = mann_whitney_from_counts(Counter(a), Counter(b))
    assert close(result["u_a"], brute, 1e-9)


def test_cmh_single_stratum_equals_scaled_pearson():
    a, b, c, d = 10, 20, 5, 25
    n = a + b + c + d
    pearson = n * (a * d - b * c) ** 2 / ((a + b) * (c + d) * (a + c) * (b + d))
    result = cmh_test([(a, b, c, d)])
    assert close(result["chi2"], pearson * (n - 1) / n)
    assert close(result["odds_ratio_mh"], 2.5)
    assert result["strata"] == 1


def test_cmh_skips_empty_strata():
    assert cmh_test([(0, 0, 3, 4)])["p_value"] is None
    assert cmh_test([(0, 0, 3, 4), (10, 20, 5, 25)])["strata"] == 1


def test_quantile_edges_and_bucket():
    edges = quantile_edges(list(range(100)), 5)
    assert edges == [20, 40, 60, 80]
    assert bucket(-1, edges) == 1 and bucket(20, edges) == 2 and bucket(99, edges) == 5
    assert quantile_edges([], 5) == []


def _row(source, position, drop, days, disappeared=None, residual=None, split="train"):
    return (source, position, position, drop, days, disappeared, residual, split)


def test_backtest_compare_and_residual_groups():
    from src.quality.bq2_backtest import compare, describe, residual_analysis

    rng = random.Random(11)
    rows = []
    for i in range(600):
        residual = rng.uniform(-1, 1)
        expensive = residual > 0.6
        drop = rng.random() < (0.30 if expensive else 0.05)
        rows.append(_row(rng.choice(["batdongsan", "guland", "nhadatvui"]),
                         "tren_p75" if expensive else "p25_p75", drop, rng.randint(0, 30), drop, residual))
    stats = describe(rows)
    assert stats["listings"] == 600 and 0 < stats["price_drop_rate"] < 1
    test = compare([r for r in rows if r[1] == "tren_p75"], [r for r in rows if r[1] != "tren_p75"])
    assert test["price_drop"]["difference"] > 0 and test["price_drop"]["p_value"] < 0.05
    assert test["price_drop_by_source_cmh"]["strata"] == 3
    assert test["price_drop_higher_and_significant"] is True
    residuals = residual_analysis(rows)
    assert sum(group["listings"] for group in residuals["by_quintile"].values()) == 600
    assert residuals["by_quintile"]["Q5"]["price_drop_rate"] > residuals["by_quintile"]["Q1"]["price_drop_rate"]


def test_backtest_residual_analysis_needs_enough_rows():
    from src.quality.bq2_backtest import residual_analysis

    assert "note" in residual_analysis([_row("guland", "p25_p75", False, 3, None, 0.1)])


def _province_row(source, province, position, genuine_drop, any_drop):
    return (source, position, position, genuine_drop, 5, False, None, "train", province, any_drop)


def test_backtest_province_groups_and_outcome_swap():
    from src.quality.bq2_backtest import ANY_DROP, HAS_DROP, by_province, province_group, province_source_cmh, with_outcome

    assert province_group("Hà Nội") == "Hà Nội" and province_group("Cần Thơ") == "Tỉnh khác"
    assert province_group(None) == "Tỉnh khác"
    row = _province_row("guland", "Hà Nội", "tren_p75", False, True)
    swapped = with_outcome([row], ANY_DROP)[0]
    assert swapped[HAS_DROP] is True and len(swapped) == len(row)

    rng = random.Random(5)
    sources = ["batdongsan", "guland", "nhadatvui"]
    expensive = [_province_row(rng.choice(sources), "Hồ Chí Minh", "tren_p75", rng.random() < 0.3, False) for _ in range(300)]
    others = [_province_row(rng.choice(sources), "Hồ Chí Minh", "p25_p75", rng.random() < 0.1, False) for _ in range(600)]
    result = by_province(expensive, others)
    assert result["Hồ Chí Minh"]["listings_a"] == 300 and result["Hà Nội"]["listings_a"] == 0
    assert result["Hồ Chí Minh"]["price_drop"]["difference"] > 0
    assert province_source_cmh(expensive, others)["p_value"] < 0.05


def test_multiple_testing_uses_exact_bonferroni_threshold():
    from src.quality import bq2_backtest as backtest

    def comparison(p, cmh_p, difference=0.01):
        return {"price_drop": {"p_value": p, "difference": difference}, "price_drop_by_source_cmh": {"p_value": cmh_p}}

    assert backtest.BONFERRONI_ALPHA == 0.05 / 3
    assert not backtest.supported_after_multiple_testing(comparison(1e-9, 0.047))  # H1 of v1.1.1
    assert not backtest.supported_after_multiple_testing(comparison(1e-9, 0.0168))  # above 0.05/3, below the rounded 0.017
    assert backtest.supported_after_multiple_testing(comparison(1e-9, 0.0166))
    assert not backtest.supported_after_multiple_testing(comparison(1e-9, 0.001, difference=-0.01))
    assert not backtest.supported_after_multiple_testing(comparison(None, 0.001))


def test_split_by_crawl_depth_uses_each_sources_stable_date():
    from datetime import date

    from src.quality import bq2_backtest as backtest

    def row(source, first_seen):
        values = [None] * 11
        values[backtest.SOURCE], values[backtest.FIRST_SEEN] = source, first_seen
        return tuple(values)

    rows = [
        row("guland", date(2026, 10, 22)),      # day before Guland's 30-page batches: early
        row("guland", date(2026, 10, 23)),      # first stable day
        row("batdongsan", date(2026, 9, 25)),   # deep 14.000-row batch: early
        row("batdongsan", date(2026, 9, 29)),
        row("nhadatvui", date(2026, 11, 17)),
        row("nhadatvui", None),                 # unknown first-seen date is never counted as stable
        row("unknown_source", date(2026, 11, 1)),
    ]
    stable, early = backtest.split_by_crawl_depth(rows)
    assert [(r[backtest.SOURCE], r[backtest.FIRST_SEEN]) for r in stable] == [
        ("guland", date(2026, 10, 23)), ("batdongsan", date(2026, 9, 29)), ("nhadatvui", date(2026, 11, 17)),
    ]
    assert len(stable) + len(early) == len(rows)
    assert set(backtest.STABLE_CRAWL_DEPTH_FROM) == set(backtest.CRAWL_SOURCES)
