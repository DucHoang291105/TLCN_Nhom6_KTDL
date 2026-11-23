import csv
import json

import pytest

from src.quality.crawl_coverage import SOURCES, audit, coverage, scan_csv


def export(root, source, batch, rows):
    path = root / 'data/incoming/snapshots' / source / batch / f'{source}_raw.csv'
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open('w', encoding='utf-8-sig', newline='') as handle:
        writer = csv.writer(handle)
        writer.writerow(['listing_id', '_source_page', 'title'])
        writer.writerows(rows)
    return path


def setup(root):
    (root / 'config').mkdir()
    (root / 'config/source_gaps.json').write_text('{"known_gaps": []}')
    for source in SOURCES:
        export(root, source, '20261113', [('1', 1, 'a')])
        export(root, source, '20261117', [('1', 1, 'a')])


def test_raw_dedup_and_page_count_do_not_infer_missing_pages(tmp_path):
    path = export(tmp_path, 'guland', '20261113', [
        ('1', 9, 'multiline\nquoted,title'), ('1', 2, 'x'), ('2', '', 'y'), ('', 9, 'z')])
    meta, ids = scan_csv(path)
    assert ids == {'1': 2, '2': None}
    assert meta['rows'] == 4 and meta['duplicate_id_rows'] == 1
    assert meta['missing_id_rows'] == meta['missing_page_rows'] == 1
    assert meta['observed_pages'] == 2 and meta['max_page'] == 9


@pytest.mark.parametrize('content', [
    'version https://git-lfs.github.com/spec/v1\n', 'listing_id,x\n',
    'listing_id,x\n1\n', 'listing_id,x\n1,a,extra\n', 'listing_id,listing_id\n1,1\n'])
def test_invalid_csv_is_not_accepted(tmp_path, content):
    path = tmp_path / 'x.csv'
    path.write_text(content)
    with pytest.raises(ValueError):
        scan_csv(path)


def test_absence_uses_own_source_last_two_batches_and_depth_cohorts():
    records = [({'batch_id': '20261016', 'max_page': 1000}, {'a': 1, 'b': 900, 'c': None}),
               ({'batch_id': '20261023', 'max_page': 30}, {'a': 1, 'd': 30}),
               ({'batch_id': '20261027', 'max_page': 30}, {'d': 2})]
    result = coverage(records)
    assert result['absent_two_batches'] == 2
    assert result['absent_two_batches_share'] == .5
    assert result['depth_cohorts']['cohorts']['beyond_latest_depth']['absent_share'] == 1
    assert result['consecutive_pairs'][-1]['retained_share_of_old'] == .5
    assert coverage(records[:1])['absent_two_batches'] is None


def test_future_and_misnamed_data_are_not_silently_ingested(tmp_path):
    setup(tmp_path)
    export(tmp_path, 'batdongsan', '20261110_173652', [('5', 1, 'a')])
    export(tmp_path, 'guland', '20261120', [('6', 1, 'a')])
    result = audit(tmp_path, '20261117', '20261113')
    assert result['status'] == 'NEEDS_INPUT_REVIEW'
    assert result['snapshot_files'] == 6
    assert len(result['extra_csv_not_consumed_by_pipeline']) == 1
    assert len(result['excluded_after_cutoff']) == 1
    assert result['checks']['requested_batch_complete']


def test_missing_inputs_remain_pending_without_fabricating_gap_declarations(tmp_path):
    setup(tmp_path)
    result = audit(tmp_path, '20261120', '20261113')
    assert len(result['missing_scheduled_batches']) == 3
    assert not result['checks']['all_calendar_gaps_declared']
    assert not result['checks']['requested_batch_complete']
    assert result['latest_complete_local_batch'] == '20261117'
    assert json.loads((tmp_path / 'config/source_gaps.json').read_text()) == {'known_gaps': []}


def test_pinned_data_changes_and_deletions_block_readiness(tmp_path):
    setup(tmp_path)
    path = tmp_path / 'docs/validation/snapshot_manifest.json'
    path.parent.mkdir(parents=True)
    path.write_text(json.dumps({'snapshots': [
        {'source': 'guland', 'batch_id': '20261113', 'sha256': '0' * 64},
        {'source': 'nhadatvui', 'batch_id': '20261110', 'sha256': '1' * 64}]}))
    result = audit(tmp_path, '20261117', '20261113')
    assert not result['checks']['files_valid_and_pinned_bytes_unchanged']
    assert {e['error'] for e in result['errors']} == {
        'Previously ingested snapshot bytes changed', 'Pinned snapshot missing or unreadable'}


def test_complete_valid_input_can_pass_and_baseline_excludes_later_ids(tmp_path):
    setup(tmp_path)
    export(tmp_path, 'guland', '20261117', [('2', 1, 'a')])
    result = audit(tmp_path, '20261117', '20261113')
    assert result['status'] == 'PASS'
    assert result['sources']['guland']['baseline']['unique_ids_ever'] == 1
    assert result['sources']['guland']['local']['unique_ids_ever'] == 2
    with pytest.raises(ValueError, match='Baseline'):
        audit(tmp_path, '20261113', '20261117')


def test_incomplete_crawler_summary_blocks_readiness(tmp_path):
    setup(tmp_path)
    folder = tmp_path / 'data/incoming/snapshots/guland/20261117'
    (folder / 'crawl_summary.json').write_text('{"status": "FAIL", "failed_pages": [2]}')
    assert not audit(tmp_path, '20261117', '20261113')['checks']['files_valid_and_pinned_bytes_unchanged']
