from datetime import datetime, timezone

import pytest

from src.ingestion.crawl_runtime import float_setting
from src.quality.defense_evidence import latest_scheduled_batch


@pytest.mark.parametrize('value', ['NaN', 'Infinity', '-Infinity', '-0.1', '60.1'])
def test_invalid_crawl_delay_cannot_disable_bounds(monkeypatch, value):
    monkeypatch.setenv('CRAWL_DELAY_SECONDS', value)
    with pytest.raises(ValueError, match='finite'):
        float_setting('CRAWL_DELAY_SECONDS', 3.5)


@pytest.mark.parametrize('stamp,expected', [
    ('2026-11-20T14:59:59+00:00', '20261117'),
    ('2026-11-20T15:00:00+00:00', '20261120'),
    ('2026-11-21T10:00:00+00:00', '20261120'),
    ('2026-11-24T14:59:59+00:00', '20261120'),
    ('2026-11-24T15:00:00+00:00', '20261124'),
])
def test_input_deadline_uses_vietnam_time_and_collection_hour(stamp, expected):
    assert latest_scheduled_batch(datetime.fromisoformat(stamp).astimezone(timezone.utc)) == expected


def test_next_semver_tag_keeps_exact_commit_gate(monkeypatch):
    from src.quality import tag_defense
    monkeypatch.setattr(tag_defense, 'git', lambda *a: ' M src/new.py')
    with pytest.raises(ValueError, match='Commit the reviewed'):
        tag_defense.create_tag('v1.1.2-bao-ve')
