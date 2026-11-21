from src.quality.evaluate_labels import binary_scores, evaluate, parse_flag


def test_parse_flag():
    assert parse_flag("1") is True and parse_flag("Có") is True
    assert parse_flag("0") is False and parse_flag("không") is False
    assert parse_flag("?") is None and parse_flag("") is None


def test_binary_scores():
    s = binary_scores([(True, True), (True, False), (False, True), (False, False)])
    assert (s["tp"], s["fp"], s["fn"], s["tn"]) == (1, 1, 1, 1)
    assert s["precision"] == 0.5 and s["recall"] == 0.5 and s["f1"] == 0.5


def test_evaluate_category_and_flags():
    labels = [
        {"sample_id": "L1", "true_category": "nha_pho", "true_frontage": "1"},
        {"sample_id": "L2", "true_category": "can_ho", "true_frontage": "0"},
        {"sample_id": "L3", "true_category": "", "true_frontage": "?"},   # not labelled yet
        {"sample_id": "L4", "true_category": "nha", "true_frontage": ""},  # invalid code
    ]
    pred = {
        "L1": {"source": "guland", "model_category": "nha_pho", "title_has_frontage": "True"},
        "L2": {"source": "chotot", "model_category": "nha_pho", "title_has_frontage": "False"},
        "L3": {"source": "chotot", "model_category": "dat", "title_has_frontage": "False"},
        "L4": {"source": "mogi", "model_category": "dat", "title_has_frontage": "False"},
    }
    for p in pred.values():
        for f in ("legal", "furnished", "elevator", "car_access"):
            p[f"title_has_{f}"] = "False"
    r = evaluate(labels, pred)
    assert r["labelled_rows"] == 2 and r["category_accuracy"] == 0.5
    assert r["invalid_category_labels"] == ["L4"]
    assert r["category_confusion"] == {"can_ho->nha_pho": 1, "nha_pho->nha_pho": 1}
    assert r["flags"]["frontage"]["precision"] == 1.0 and r["flags"]["frontage"]["labelled"] == 2


from src.quality.evaluate_labels import review_changes, review_complete

FULL = {"sample_id": "L001", "true_category": "nha_pho", "labeler": "Claude (gán sơ bộ)",
        **{f"true_{f}": "0" for f in ("legal", "furnished", "frontage", "elevator", "car_access")}}


def test_prelabelled_rows_need_two_reviewers():
    assert review_complete(FULL)
    assert not review_complete(FULL, needs_review=True)
    assert not review_complete({**FULL, "review_1": "23133040"}, needs_review=True)
    assert review_complete({**FULL, "review_1": "23133040", "review_2": "23133029"}, needs_review=True)


def test_same_person_cannot_fill_both_review_rounds():
    assert not review_complete({**FULL, "review_1": "23133040", "review_2": "23133040"}, needs_review=True)
    assert not review_complete({**FULL, "review_1": "Kiet", "review_2": " kiet "}, needs_review=True)


def test_review_rounds_counts_rows_and_people():
    from src.quality.evaluate_labels import review_rounds
    sheet = [{"review_1": "a", "review_2": ""}, {"review_1": "b", "review_2": ""}, {"review_1": "a", "review_2": "a"}]
    assert review_rounds(sheet) == {"review_1_rows": 3, "review_1_reviewers": 2, "review_2_rows": 1,
                                    "review_2_reviewers": 1, "same_person_both_rounds": 1}


def test_review_changes_counts_edited_cells():
    before = [FULL, {**FULL, "sample_id": "L002"}]
    after = [{**FULL, "true_category": "dat", "true_legal": "1"}, {**FULL, "sample_id": "L002"}]
    changes = review_changes(before, after)
    assert changes["rows_changed"] == 1 and changes["cells_changed"] == 2 and changes["cells"] == 12
    assert changes["by_column"]["true_category"] == 1 and changes["by_column"]["true_legal"] == 1
