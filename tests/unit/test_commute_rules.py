"""Rules for BQ3 by place of work or study (agg_commute_*)."""

from src.gold.gold_rules import (
    COMMUTE_TOLERANCE_KM, PROVINCE_CENTER_KIND, commute_allowed, commute_efficient_keys, load_destinations,
)
from src.silver.build_location import haversine_km


def test_commute_tolerance():
    assert commute_allowed(8.0, 8.0 + COMMUTE_TOLERANCE_KM)
    assert commute_allowed(8.0, 3.0)                      # closer is always allowed
    assert not commute_allowed(8.0, 8.0 + COMMUTE_TOLERANCE_KM + 0.01)
    assert not commute_allowed(None, 3.0) and not commute_allowed(3.0, None)


def test_efficient_districts_are_not_beaten_on_both_commute_and_cost():
    rows = [
        {"key": "near_expensive", "commute_km": 2.0, "cost": 9.0},
        {"key": "mid", "commute_km": 6.0, "cost": 5.0},
        {"key": "far_cheap", "commute_km": 15.0, "cost": 2.0},
        {"key": "dominated", "commute_km": 7.0, "cost": 6.0},   # mid is closer and cheaper
        {"key": "tie_far", "commute_km": 6.0, "cost": 5.5},     # same km as mid, costlier
    ]
    assert commute_efficient_keys(rows) == {"near_expensive", "mid", "far_cheap"}


def test_identical_districts_do_not_dominate_each_other():
    rows = [{"key": "a", "commute_km": 4.0, "cost": 3.0}, {"key": "b", "commute_km": 4.0, "cost": 3.0}]
    assert commute_efficient_keys(rows) == {"a", "b"}


def test_destinations_include_named_places_and_every_province_centre():
    destinations = load_destinations()
    ids = [d["destination_id"] for d in destinations]
    assert len(ids) == len(set(ids))
    centres = [d for d in destinations if d["destination_kind"] == PROVINCE_CENTER_KIND]
    assert len(centres) == 34
    named = [d for d in destinations if d["destination_kind"] != PROVINCE_CENTER_KIND]
    assert {"Hồ Chí Minh", "Hà Nội", "Đà Nẵng"} <= {d["province_name"] for d in named}


def test_named_destinations_lie_near_their_province_centre():
    destinations = load_destinations()
    centre = {d["province_name"]: d for d in destinations if d["destination_kind"] == PROVINCE_CENTER_KIND}
    for d in destinations:
        c = centre[d["province_name"]]
        assert haversine_km(d["lat"], d["lon"], c["lat"], c["lon"]) < 40, d["destination_id"]


# --- BQ evidence (build_bq_evidence) ---------------------------------------------
from src.gold.build_bq_evidence import FLAG_LABELS, GENUINE_CUT, evidence_sql, two_proportion_z
from src.gold.gold_common import FLAG_COLUMNS


def test_two_proportion_z_matches_known_value():
    z, p = two_proportion_z(611, 14863, 1398, 47732)   # H1 on the 17/11 data
    assert round(z, 1) == 7.1 and p < 1e-11
    z0, p0 = two_proportion_z(10, 100, 10, 100)
    assert z0 == 0 and p0 == 1


def test_two_proportion_z_handles_empty_and_constant_groups():
    assert two_proportion_z(0, 0, 1, 5) == (None, None)
    assert two_proportion_z(0, 10, 0, 20) == (0.0, 1.0)
    assert two_proportion_z(10, 10, 20, 20) == (0.0, 1.0)


def test_evidence_stage_reads_current_run_dependencies():
    from src.orchestration.pipeline import STAGES
    jobs = [name for name, _ in STAGES["gold"]]
    assert jobs.index("bq_evidence") > max(jobs.index(name) for name in ("repricing", "listing_lifecycle", "price_model", "budget_tradeoff"))
    assert jobs.index("bq_evidence") < jobs.index("verify_gold")


def test_genuine_cut_excludes_typo_sized_drops():
    assert "0.70 * c.previous_price AND 0.99 * c.previous_price" in GENUINE_CUT


def test_reprice_example_date_is_a_genuine_cut():
    sql = evidence_sql("g", "s")["ev_reprice_examples"]
    # the date shown as "ngày hạ giá" comes from the genuine-cut events only, never MAX over every change
    assert f"MAX(CASE WHEN {GENUINE_CUT} THEN c.changed_at END) AS ngay_ha" in sql
    assert "MAX(c.changed_at)" not in sql
    assert "ch.ngay_ha IS NOT NULL" in sql


def test_evidence_tables_cover_every_flag():
    assert set(FLAG_LABELS) == set(FLAG_COLUMNS)
    sql = evidence_sql("g", "s")
    assert set(sql) == {"ev_budget_curve", "ev_budget_pairs", "ev_reprice", "ev_reprice_examples", "ev_flag_check", "ev_calibration"}
    assert all(f"f.{flag}" in sql["ev_flag_check"] for flag in FLAG_COLUMNS)
