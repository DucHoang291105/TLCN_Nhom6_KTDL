"""BQ3 road distances: grid cells, road rings, OSRM table parsing (no network)."""

import pytest

from src.gold.commute_road import RINGS, cell_center, cell_index, cell_sql, ring_label, ring_sql, table_request


def test_cell_index_rounds_half_up_like_sql():
    assert cell_index(10.8414) == 1084
    assert cell_index(10.845) == 1085          # half up, not banker's rounding
    assert cell_index(106.7979) == 10680
    assert cell_center(1084) == pytest.approx(10.84)
    assert cell_sql("l.lat") == "CAST(floor(l.lat * 100 + 0.5) AS INT)"


@pytest.mark.parametrize("km,label", [(0.4, "1. Dưới 5 km"), (5.0, "2. 5–10 km"), (14.99, "3. 10–15 km"),
                                      (19.0, "4. 15–20 km"), (25.0, "5. 20–30 km"), (30.0, "6. Từ 30 km"), (None, None)])
def test_ring_label_boundaries(km, label):
    assert ring_label(km) == label


def test_ring_sql_covers_every_ring_in_order():
    sql = ring_sql("road_km")
    assert sql.startswith("CASE WHEN road_km IS NULL THEN NULL")
    assert [label for _, label in RINGS] == sorted(label for _, label in RINGS)
    for upper, label in RINGS:
        assert f"'{label}'" in sql
        if upper is not None:
            assert f"road_km < {upper} THEN '{label}'" in sql


class FakeResponse:
    status_code = 200

    def __init__(self, payload):
        self.payload = payload
        self.text = ""

    def json(self):
        return self.payload


def test_table_request_parses_distances_and_unroutable_pairs():
    calls = []

    def fake_get(url, params, headers, timeout):
        calls.append((url, params))
        return FakeResponse({"code": "Ok", "distances": [[17376.3, None], [16284.9, 900.0]],
                             "durations": [[1129.7, None], [1062.1, 120.0]]})

    destinations = [{"lat": 10.8414, "lon": 106.798}, {"lat": 10.7725, "lon": 106.698}]
    result = table_request(destinations, [(1080, 10668), (1078, 10670)], get=fake_get)
    assert result == [[(17.376, 18.8), None], [(16.285, 17.7), (0.9, 2.0)]]
    url, params = calls[0]
    # destinations first, then cell centres; sources point at the cells only
    assert url.endswith("106.798000,10.841400;106.698000,10.772500;106.68,10.80;106.70,10.78")
    assert params["sources"] == "2;3" and params["destinations"] == "0;1"
