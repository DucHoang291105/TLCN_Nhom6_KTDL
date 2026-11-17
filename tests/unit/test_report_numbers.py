import pytest
from src.quality.verify_report_numbers import compare_table


def test_missing_and_stale_values_are_detected_and_numeric_units_ignored():
    actual = compare_table('| Current | 139.477 mỗi bảng |\n| History | 151.982 |',
                           {'Current': 139477, 'History': 154343, 'Missing': 1})
    assert actual['Current']['match']
    assert not actual['History']['match']
    assert not actual['Missing']['match']


def test_conflicting_duplicate_metrics_are_rejected():
    with pytest.raises(ValueError, match='Ambiguous'):
        compare_table('| X | 1 |\n| X | 2 |', {'X': 1})
