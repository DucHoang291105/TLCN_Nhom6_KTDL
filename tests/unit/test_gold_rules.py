from datetime import datetime

import pytest

from src.gold.gold_rules import (
    UNKNOWN_KEY, Band, band_key, cluster_pairs, is_cross_source_duplicate, jaccard,
    legal_not_stated_by_category, legal_not_stated_by_position, load_bands, pick_representative, title_tokens, validate_bands,
)

BANDS = load_bands()

def test_all_band_dimensions_loaded(): assert set(BANDS)=={"price","area","unit_price","room"}

@pytest.mark.parametrize("value,expected",[(999_999_999,1),(1_000_000_000,2),(4_500_000_000,4),(20_000_000_000,8),(5e11,8)])
def test_price_band_half_open(value,expected): assert band_key(value,BANDS["price"])==expected

@pytest.mark.parametrize("value",[None,0,-5,"abc",float("nan")])
def test_band_unknown(value): assert band_key(value,BANDS["area"])==UNKNOWN_KEY

@pytest.mark.parametrize("rooms,expected",[(1,1),(3,3),(5,5),(12,5)])
def test_room_band(rooms,expected): assert band_key(rooms,BANDS["room"])==expected

def test_validate_bands_rejects_gap():
    with pytest.raises(RuntimeError): validate_bands("x",[Band("x",1,"a","a",0,10),Band("x",2,"b","b",20,None)])

def test_validate_bands_rejects_bounded_last():
    with pytest.raises(RuntimeError): validate_bands("x",[Band("x",1,"a","a",0,10)])

def test_title_tokens_drop_stopwords(): assert title_tokens("Bán nhà HXH Lê Văn Sỹ, giá tốt")==["hxh","le","sy","van"]
def test_jaccard(): assert jaccard(["a","b"],["b","c"])==pytest.approx(1/3)

def _listing(**overrides):
    row={"source":"guland","location_key":10,"property_category_key":1,"area":60.0,"price":5_000_000_000.0,"title_tokens":title_tokens("Nhà HXH Lê Văn Sỹ Phú Nhuận 4 tầng")}
    row.update(overrides); return row

def test_duplicate_match(): assert is_cross_source_duplicate(_listing(),_listing(source="batdongsan",area=61.0,price=5_100_000_000.0))
def test_duplicate_same_source(): assert not is_cross_source_duplicate(_listing(),_listing())
def test_duplicate_price_too_far(): assert not is_cross_source_duplicate(_listing(),_listing(source="batdongsan",price=5_300_000_000.0))
def test_duplicate_unknown_location(): assert not is_cross_source_duplicate(_listing(location_key=-1),_listing(source="batdongsan",location_key=-1))
def test_duplicate_different_title(): assert not is_cross_source_duplicate(_listing(),_listing(source="batdongsan",title_tokens=title_tokens("Căn hộ Vinhomes Central Park view sông")))

def test_cluster_pairs_transitive():
    groups=cluster_pairs([("b","c"),("a","b"),("x","y")])
    assert groups=={"a":"a","b":"a","c":"a","x":"x","y":"x"}

def test_pick_representative():
    members=[{"source_id":"b","feature_completeness_score":0.8,"scraped_at":datetime(2026,10,1)},
             {"source_id":"a","feature_completeness_score":0.8,"scraped_at":datetime(2026,10,2)},
             {"source_id":"c","feature_completeness_score":0.6,"scraped_at":datetime(2026,10,3)}]
    assert pick_representative(members)=="a"

from src.gold.gold_rules import feature_similarity, pareto_efficient_ids, price_position

@pytest.mark.parametrize("value,expected",[(50,"duoi_p25"),(60,"p25_p75"),(80,"p25_p75"),(81,"tren_p75"),(None,"khong_du_du_lieu")])
def test_price_position(value,expected): assert price_position(value,60,80)==expected
def test_price_position_without_peer(): assert price_position(70,None,None)=="khong_du_du_lieu"

def _item(source_id,price,area,rooms=None,flags=(0,0,0,0,0)): return {"source_id":source_id,"price":price,"area":area,"rooms":rooms,"flags":flags}

def test_pareto_dominated_listing_removed():
    items=[_item("a",3e9,60,2),_item("b",3.5e9,55,2),_item("c",4e9,80,3)]
    assert pareto_efficient_ids(items)=={"a","c"}

def test_pareto_flag_keeps_listing():
    items=[_item("a",3e9,60,2),_item("b",3.5e9,55,2,(1,0,0,0,0))]
    assert pareto_efficient_ids(items)=={"a","b"}

def test_pareto_identical_listings_both_kept():
    assert pareto_efficient_ids([_item("a",3e9,60),_item("b",3e9,60)])=={"a","b"}

def test_pareto_equal_price_better_area_wins():
    assert pareto_efficient_ids([_item("a",3e9,60),_item("b",3e9,70)])=={"b"}

def test_feature_similarity():
    assert feature_similarity({"legal":0.2,"frontage":0.5},{"legal":0.2,"frontage":0.5})==1.0
    assert feature_similarity({"legal":0.0},{"legal":1.0})==0.8


from src.gold.gold_rules import (
    cost_at_target_area, max_flag_share_gap, mutual_best_pairs, resolve_dup_groups, MAX_FLAG_SHARE_GAP,
)

def test_unknown_rooms_not_beaten_by_known_rooms():
    assert pareto_efficient_ids([_item("a",3.5e9,55,None),_item("b",3e9,60,3)])=={"a","b"}

def test_both_unknown_rooms_still_comparable():
    assert pareto_efficient_ids([_item("a",3.5e9,55),_item("b",3e9,60)])=={"b"}

def _pair(l,r,ls,rs,j,p=0.0,a=0.0): return {"left":l,"right":r,"left_source":ls,"right_source":rs,"jaccard":j,"price_gap":p,"area_gap":a}

def test_mutual_best_breaks_chain():
    pairs=[_pair("x","y1","guland","nhadatvui",0.6),_pair("x","y2","guland","nhadatvui",0.4)]
    assert mutual_best_pairs(pairs)==[("x","y1")]

def test_mutual_best_requires_both_directions():
    pairs=[_pair("x","y1","guland","nhadatvui",0.5),_pair("z","y1","guland","nhadatvui",0.9)]
    assert mutual_best_pairs(pairs)==[("z","y1")]

def test_same_source_group_is_ambiguous():
    groups=cluster_pairs([("a","b"),("b","c")])
    status=resolve_dup_groups(groups,{"a":"guland","b":"mogi","c":"guland"})
    assert status=={"a":"AMBIGUOUS"}
    assert resolve_dup_groups(cluster_pairs([("a","b")]),{"a":"guland","b":"mogi"})=={"a":"RESOLVED"}

def test_lower_price_per_m2_is_not_lower_budget():
    origin_total, alt_total = 51*100e6, 79*90e6
    assert alt_total - origin_total == pytest.approx(2.01e9)

    assert cost_at_target_area(90e6,51) < cost_at_target_area(100e6,51)

def test_similarity_threshold_alone_admits_opposite_frontage():
    left,right={"frontage":1.0},{"frontage":0.0}
    assert feature_similarity(left,right)==0.8
    assert max_flag_share_gap(left,right)>MAX_FLAG_SHARE_GAP


def test_distance_allowed_cap_and_unknown():
    from src.gold.gold_rules import MAX_DISTANCE_DIFF_KM, distance_allowed
    assert distance_allowed(MAX_DISTANCE_DIFF_KM) and distance_allowed(-25.0)
    assert not distance_allowed(MAX_DISTANCE_DIFF_KM + 0.01)
    assert not distance_allowed(None) and not distance_allowed(float("nan")) and not distance_allowed("x")


def test_legal_not_stated_share_compares_below_p25_with_other_priced_positions():
    rows = [
        ("duoi_p25", False, 80), ("duoi_p25", True, 20),
        ("p25_p75", False, 60), ("p25_p75", True, 40),
        ("tren_p75", False, 70), ("tren_p75", True, 30),
        ("khong_du_du_lieu", False, 10),
    ]
    result = legal_not_stated_by_position(rows)
    assert result["by_position"]["duoi_p25"] == {"listings": 100, "title_states_no_legal": 80, "share": 0.8}
    assert result["below_p25_share"] == 0.8
    assert result["other_priced_share"] == 0.65
    assert result["difference_share"] == 0.15


def test_legal_not_stated_share_handles_missing_groups():
    result = legal_not_stated_by_position([("duoi_p25", None, 4)])
    assert result["below_p25_share"] == 1.0
    assert result["other_priced_share"] is None and result["difference_share"] is None
    assert legal_not_stated_by_position([])["below_p25_share"] is None


def test_legal_not_stated_by_category_standardizes_the_category_mix():
    rows = [
        ("dat", "duoi_p25", False, 30), ("dat", "duoi_p25", True, 70),
        ("dat", "p25_p75", False, 50), ("dat", "p25_p75", True, 50),

        ("nha", "duoi_p25", False, 90), ("nha", "duoi_p25", True, 10),
        ("nha", "tren_p75", False, 180), ("nha", "tren_p75", True, 20),
    ]
    result = legal_not_stated_by_category(rows)
    assert result["by_category"]["dat"]["difference_share"] == -0.2
    assert result["by_category"]["nha"]["difference_share"] == 0.0

    assert result["standardized_difference_share"] == -0.1
    assert legal_not_stated_by_category([])["standardized_difference_share"] is None
