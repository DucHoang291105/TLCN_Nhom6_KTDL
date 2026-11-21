"""Check README headline counts against saved JSON; no live-data certification."""
import json
import re
from datetime import datetime, timezone
from pathlib import Path

from src.common.atomic_io import write_json_atomic

ROOT = Path(__file__).resolve().parents[2]


def compare_table(markdown, expected):
    rows = {}
    for line in markdown.splitlines():
        cells = [cell.strip() for cell in line.strip().strip('|').split('|')]
        if len(cells) >= 2 and cells[0] in expected:
            if cells[0] in rows:
                raise ValueError('Ambiguous duplicate headline metric')
            number = re.match(r'^([0-9][0-9.,]*)', cells[1])
            rows[cells[0]] = int(number[1].replace('.', '').replace(',', '')) if number else None
    return {label: {'expected': value, 'actual': rows.get(label), 'match': rows.get(label) == value}
            for label, value in expected.items()}


def main():
    folder = ROOT / 'docs/validation'
    def read(name):
        return json.loads((folder / f'{name}.json').read_text(encoding='utf-8'))
    bronze, manifest, silver, gold = [read(n) for n in (
        'bronze_state_audit', 'snapshot_manifest', 'silver_final_verification', 'gold_verification')]
    historical = sum(row['row_count'] for row in bronze['batches'] if row['branch'] == 'historical')
    s, g = silver['table_counts'], gold['table_counts']
    expected = {'Bronze historical': historical,
        f"Bronze snapshot — {len(manifest['snapshots'])} CSV": manifest['snapshot_rows'],
        'Tổng Bronze': historical + manifest['snapshot_rows'],
        'Silver `listing_observation`': s['listing_observation'],
        'Silver `listing_dq_quarantine`': s['listing_dq_quarantine'],
        'Silver Current / Location / Feature': s['silver_listings_current_27'],
        'Silver `listing_history`': s['listing_history'],
        'Silver `listing_history_state`': s['listing_history_state']}
    expected.update({f'Gold `{name}`': g[name] for name in (
        'fact_listing', 'fact_listing_lifecycle', 'fact_listing_price_change',
        'fact_listing_price_model', 'agg_price_model_coefficient')})
    markdown = (ROOT / 'README.md').read_text(encoding='utf-8')
    table = compare_table(markdown, expected)
    checks = {'headline_counts_match_json': all(r['match'] for r in table.values()),
              'source_json_statuses_pass': all(v.get('status') == 'PASS' for v in (bronze, manifest, silver, gold)),
              'silver_current_location_feature_counts_equal': s['silver_listings_current_27'] == s['listing_location'] == s['listing_feature'],
              'silver_check_count_documented': f"**{len(silver['checks'])}/{len(silver['checks'])} PASS**" in markdown,
              'gold_check_count_documented': f"**{len(gold['checks'])}/{len(gold['checks'])} PASS**" in markdown,
              'acceptance_scope_disclosed': 'Kết quả chỉ áp dụng cho code và input đã kiểm chứng' in markdown,
              'current_runtime_evidence_linked': 'pipeline_dashboard_integration.json' in markdown}
    payload = {'generated_at': datetime.now(timezone.utc).isoformat(),
               'scope': 'README headline table and selected disclosures vs saved JSON; not a live pipeline acceptance',
               'checks': checks, 'metrics': table, 'status': 'PASS' if all(checks.values()) else 'FAIL'}
    write_json_atomic(folder / 'report_numbers_check.json', payload)
    print(json.dumps({'status': payload['status'], 'checks': checks}, indent=2))
    if payload['status'] != 'PASS':
        raise SystemExit(2)


if __name__ == '__main__':
    main()
