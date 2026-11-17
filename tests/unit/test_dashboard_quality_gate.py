from src.quality.verify_pipeline_dashboard import quality_pass


def test_additional_quality_checks_are_accepted_only_if_all_pass():
    checks = {f'check_{i}': True for i in range(113)}
    assert quality_pass({'status': 'PASS', 'checks': checks}, 108, ('check_0',))
    checks['check_112'] = False
    assert not quality_pass({'status': 'PASS', 'checks': checks}, 108)
    checks['check_112'] = 'PASS'
    assert not quality_pass({'status': 'PASS', 'checks': checks}, 108)


def test_missing_required_check_partial_suite_or_failure_cannot_pass():
    assert not quality_pass({'status': 'PASS', 'checks': {'a': True}}, 2)
    assert not quality_pass({'status': 'PASS', 'checks': {'a': True}}, 1, ('required',))
    assert not quality_pass({'status': 'FAIL', 'checks': {'a': True}}, 1)
    assert not quality_pass({}, 0)
