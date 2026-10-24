import pytest
from types import SimpleNamespace
from src.orchestration.pipeline import require_catalog_write_window
from src.dashboard.insights import vn, pct, header
from src.quality.evaluate_labels import binary_scores, evaluate, index_predictions, review_complete, FLAGS
from src.quality.build_labeling_sample import has_manual_annotations


def test_missing_dashboard_values_are_not_zero():
    assert vn(None) == pct(None) == "chưa có dữ liệu"
    assert vn(0) == "0"


def test_dashboard_header_declares_filter_scope():
    assert "không tự đổi theo bộ lọc" in header("Question", ["Finding"], "Scope")


@pytest.mark.parametrize("pairs", [[(True, False)], [(False, True)], [(True, False), (False, True)]])
def test_f1_is_zero_for_wrong_predictions(pairs):
    assert binary_scores(pairs)["f1"] == 0


def test_f1_is_undefined_without_positive_class():
    assert binary_scores([(False, False)])["f1"] is None


def test_duplicate_prediction_ids_do_not_silently_overwrite():
    with pytest.raises(ValueError, match="unique"):
        index_predictions([{"sample_id": "L1"}, {"sample_id": "L1"}])


def test_duplicate_truth_ids_and_missing_predictions_rejected():
    with pytest.raises(ValueError, match="unique"):
        evaluate([{"sample_id": "L1"}, {"sample_id": "L1"}], {})
    with pytest.raises(ValueError, match="Missing predictions"):
        evaluate([{"sample_id": "L1"}], {})


def test_complete_review_requires_name_category_and_all_flags():
    row = {"true_category": "nha_pho", "labeler": "Reviewer", **{f"true_{f}": "0" for f in FLAGS}}
    assert review_complete(row)
    assert not review_complete({**row, "labeler": ""})
    assert not review_complete({**row, "true_legal": ""})
    assert review_complete({**row, "true_legal": "?"})


@pytest.mark.parametrize("field", ["true_category", "true_legal", "true_frontage", "labeler", "labeled_at", "note"])
def test_sample_regeneration_preserves_any_manual_input(field):
    assert has_manual_annotations([{field: "manual"}])
    assert not has_manual_annotations([{field: ""}])


@pytest.mark.parametrize("status", ["running", "restarting", "paused"])
def test_catalog_write_guard_blocks_active_query_engine(status):
    client = SimpleNamespace(containers=SimpleNamespace(list=lambda **kwargs: [SimpleNamespace(status=status)]))
    with pytest.raises(RuntimeError, match="Trino is active"):
        require_catalog_write_window(client)


@pytest.mark.parametrize("services", [[], [SimpleNamespace(status="exited")]])
def test_catalog_write_guard_allows_stopped_or_missing_query_engine(services):
    client = SimpleNamespace(containers=SimpleNamespace(list=lambda **kwargs: services))
    require_catalog_write_window(client)
