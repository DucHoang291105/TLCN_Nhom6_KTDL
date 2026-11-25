import pytest

from src.quality import defense_evidence as evidence


def test_freeze_must_be_a_scheduled_collection_day():
    with pytest.raises(ValueError, match='Tuesday or Friday'):
        evidence.validate_freeze('20261121')


def test_freeze_cannot_be_later_than_latest_due_collection(monkeypatch):
    monkeypatch.setattr(evidence, 'latest_scheduled_batch', lambda at=None: '20261120')
    with pytest.raises(ValueError, match='later than'):
        evidence.validate_freeze('20261124')


def test_freeze_on_or_before_latest_due_collection_is_accepted(monkeypatch):
    monkeypatch.setattr(evidence, 'latest_scheduled_batch', lambda at=None: '20261124')
    assert evidence.validate_freeze('20261120') == '20261120'
    assert evidence.validate_freeze('20261124') == '20261124'


def test_due_batch_uses_declared_freeze_else_current_date(monkeypatch):
    monkeypatch.setattr(evidence, 'latest_scheduled_batch', lambda at=None: '20261124')
    assert evidence.scheduled_due({'data_freeze_batch': '20261120'}) == '20261120'
    assert evidence.scheduled_due({}) == '20261124'
    assert evidence.scheduled_due({'data_freeze_batch': None}) == '20261124'
