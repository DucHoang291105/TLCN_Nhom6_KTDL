from src.quality.person_a_release_check import release_decision, allowed_change, secret_findings


def test_local_pass_is_not_automatic_permission_to_merge():
    result = release_decision({"tests": True, "hashes": True})
    assert result["ready_to_push_branch"] is True
    assert result["ready_to_merge_main"] is False


def test_failed_or_missing_checks_cannot_release():
    assert not release_decision({})["ready_to_push_branch"]
    assert not release_decision({"tests": False}, True, True)["ready_to_merge_main"]
    assert not release_decision({"tests": "PASS"})["ready_to_push_branch"]


def test_both_current_ci_and_review_required_for_merge():
    assert release_decision({"tests": True}, True, True)["ready_to_merge_main"]
    assert not release_decision({"tests": True}, True, False)["ready_to_merge_main"]


def test_scope_does_not_include_bq_or_dashboard_implementation():
    assert allowed_change(".gitignore")
    assert allowed_change("src/gold/build_listing_lifecycle.py")
    assert allowed_change("src/silver/history.py")
    assert not allowed_change("src/gold/build_price_model.py")
    assert not allowed_change("src/gold/build_budget_tradeoff.py")
    assert not allowed_change("docker/web/index.html")


def test_secret_scan_redacts_values():
    value = "ghp_" + "a"*36
    findings = secret_findings("sample.py", value, {})
    assert findings == [{"file": "sample.py", "rule": "github_token"}]
    assert value not in repr(findings)


def test_common_service_name_is_not_a_false_positive_secret():
    short = "post" + "gres"
    assert secret_findings("config", "connector.name="+short+"ql", {"POSTGRES_PASSWORD": short}) == []


def test_common_password_literal_and_uri_still_detected():
    short = "post" + "gres"
    assert secret_findings("config", "connection-password="+short, {"POSTGRES_PASSWORD": short})
    assert secret_findings("config", "scheme://user:"+short+"@host", {"POSTGRES_PASSWORD": short})
