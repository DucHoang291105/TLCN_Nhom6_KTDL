"""Record two UI rehearsals against stored results, without triggering a DAG.

Optional dependency: playwright (and Chrome installed). No credentials or
storage-state files are written. Videos contain browser output, without voice.
"""
from __future__ import annotations

import argparse
import json
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

from playwright.sync_api import sync_playwright

ROOT = Path(__file__).resolve().parents[1]
LIFECYCLE = '''SELECT source, lifecycle_status, COUNT(*) AS so_tin
FROM iceberg.gold.fact_listing_lifecycle
GROUP BY source, lifecycle_status ORDER BY source, lifecycle_status;'''


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output', type=Path, default=ROOT / 'outputs/demo')
    parser.add_argument('--hold-seconds', type=float, default=8)
    parser.add_argument('--chrome', default='C:/Program Files/Google/Chrome/Application/chrome.exe')
    args = parser.parse_args()
    if args.hold_seconds < 1 or args.output.exists():
        raise ValueError('Use hold >= 1 and a new output directory to preserve recordings')
    values = dict(line.split('=', 1) for line in (ROOT / '.env').read_text(encoding='utf-8-sig').splitlines()
                  if '=' in line and not line.lstrip().startswith('#'))
    run_info = json.loads((ROOT / 'docs/validation/integration_airflow_run.json').read_text())
    run = run_info['run_id']
    batch = run_info['batch_id']
    sys.path.insert(0, str(ROOT))
    from src.dashboard.provision_superset import trino_rows
    total = int(trino_rows('SELECT SUM(n_listings) FROM iceberg.gold.agg_market_overview')[0][0])
    hcm = int(trino_rows("SELECT SUM(n_listings) FROM iceberg.gold.agg_market_overview WHERE province_name='Hồ Chí Minh'")[0][0])
    lifecycle_rows = int(trino_rows('SELECT COUNT(*) FROM (SELECT source, lifecycle_status FROM iceberg.gold.fact_listing_lifecycle GROUP BY source, lifecycle_status)')[0][0])
    failure = json.loads((ROOT / 'docs/validation/airflow_failure_gate.json').read_text())
    failure_run = failure.get('run_id') or failure.get('dag_run_id')
    if not failure_run:
        raise ValueError('Missing actual fault-probe run ID')
    iceberg = json.loads((ROOT / 'docs/validation/iceberg_features.json').read_text())
    table = iceberg['demo_table'].replace('lakehouse.', 'iceberg.', 1)
    snapshot = int(iceberg['original_snapshot_id'])
    sql = (f"SELECT 'current_after_merge' AS snapshot_view, id, price FROM {table} "
           f"UNION ALL SELECT 'original_snapshot', id, price FROM {table} "
           f"FOR VERSION AS OF {snapshot} ORDER BY snapshot_view, id")
    raw = subprocess.check_output(['docker', 'exec', 'tlcn_airflow', 'python', '-c',
        "from airflow.configuration import conf; from pathlib import Path; p=conf.get('core','simple_auth_manager_passwords_file',fallback='') or '/opt/airflow/simple_auth_manager_passwords.json.generated'; print(Path(p).read_text())"], text=True)
    password = json.loads(raw)['admin']
    args.output.mkdir(parents=True)
    results = []
    with sync_playwright() as pw:
        browser = pw.chromium.launch(executable_path=args.chrome, headless=False)
        try:
            auth = browser.new_context(viewport={'width': 1600, 'height': 900})
            page = auth.new_page()
            page.goto('http://localhost:8088/login/')
            page.get_by_label('Username:').fill(values.get('SUPERSET_ADMIN_USERNAME', 'admin'))
            page.get_by_label('Password:').fill(values['SUPERSET_ADMIN_PASSWORD'])
            page.get_by_role('button', name='Sign in', exact=True).click()
            page.wait_for_url(lambda url: '/login' not in url)
            page.goto('http://localhost:8082')
            page.get_by_role('textbox', name='Username', exact=True).fill('admin')
            page.get_by_role('textbox', name='Password', exact=True).fill(password)
            page.get_by_role('button', name='Sign in', exact=True).click()
            page.wait_for_url(lambda url: '/auth/login' not in url)
            page.goto('http://localhost:9001')
            page.locator('#accessKey').fill(values['MINIO_ROOT_USER'])
            page.locator('#secretKey').fill(values['MINIO_ROOT_PASSWORD'])
            page.get_by_role('button', name='Login', exact=True).click()
            page.wait_for_url(lambda url: '/login' not in url)
            if page.get_by_role('button', name='Acknowledge', exact=True).is_visible():
                page.get_by_role('button', name='Acknowledge', exact=True).click()
            state = auth.storage_state()
            password = raw = values = None
            auth.close()
            for number in (1, 2):
                print(f'Rehearsal {number}: starting', flush=True)
                started = time.perf_counter()
                context = browser.new_context(storage_state=state, viewport={'width': 1600, 'height': 900},
                    record_video_dir=args.output / f'rehearsal_{number}', record_video_size={'width': 1600, 'height': 900})
                page = context.new_page()
                page.set_default_timeout(60000)
                checks = []
                try:
                    # Caption is part of the recorded browser page, clearly marks historical evidence.
                    page.set_content('<html lang="vi"><meta charset="utf-8"><body style="font:28px Arial;padding:70px;background:#101827;color:white">'
                        '<h1>Pipeline và vận hành — Võ Đức Hoàng</h1><p>CSV → Bronze / MinIO → Silver / Iceberg → Gold → Trino → Superset</p>'
                        f'<p>Airflow điều phối và kiểm định chất lượng.</p><p>Diễn tập thao tác tự động; batch {batch}, run {run}.</p>'
                        '<p>Bản quay không có lời thuyết minh. Không chạy pipeline mới trong bản demo này.</p></body></html>')
                    time.sleep(args.hold_seconds)
                    page.goto(f'http://localhost:9001/browser/lakehouse-bronze/real_estate%2Fsnapshots%2Fguland%2Fbatch_id%3D{batch}%2F')
                    page.get_by_text('_SUCCESS', exact=True).wait_for()
                    checks.append('bronze_batch_visible')
                    time.sleep(args.hold_seconds)
                    page.goto(f'http://localhost:8082/dags/lakehouse_pipeline/runs/{run}/')
                    page.get_by_role('button', name='Show Graph (Press g)', exact=True).click()
                    page.get_by_text('Success', exact=True).first.wait_for()
                    checks.append('stored_success_run_visible')
                    time.sleep(args.hold_seconds)
                    page.goto(f'http://localhost:8082/dags/lakehouse_pipeline/runs/{failure_run}/')
                    page.get_by_role('button', name='Show Graph (Press g)', exact=True).click()
                    expand = page.locator('.react-flow__node[data-id="acquisition"]').get_by_role('button', name='+ 4 Tasks', exact=True)
                    expand.wait_for()
                    expand.click()
                    page.locator('.react-flow__node[data-id="acquisition.prepare_guland"]').wait_for()
                    collapse = page.get_by_role('button', name='Collapse Details Panel', exact=True)
                    if collapse.is_visible():
                        collapse.click()
                    page.get_by_role('button', name='Fit View', exact=True).click()
                    checks.append('stored_failure_graph_visible')
                    time.sleep(args.hold_seconds)
                    print(f'Rehearsal {number}: querying lifecycle and time travel', flush=True)
                    page.goto('http://localhost:8088/sqllab/')
                    for query, row_count in [(LIFECYCLE, f'{lifecycle_rows} rows'), (sql, '5 rows')]:
                        page.locator('.ace_editor').first.click()
                        page.keyboard.press('Control+A')
                        page.keyboard.insert_text(query)
                        page.get_by_role('button', name='Run', exact=True).click()
                        page.get_by_text(row_count, exact=True).wait_for()
                        checks.append('query_' + row_count.replace(' ', '_'))
                        time.sleep(args.hold_seconds)
                    # Leave the editor on lifecycle for the next rehearsal/user.
                    page.locator('.ace_editor').first.click()
                    page.keyboard.press('Control+A')
                    page.keyboard.insert_text(LIFECYCLE)
                    page.goto('http://localhost:8088/superset/dashboard/tlcn-tong-quan/')
                    print(f'Rehearsal {number}: dashboard and filter', flush=True)
                    page.get_by_text(f'{total:,}', exact=True).wait_for()
                    chart = page.get_by_text('Tin tập trung ở đâu? Số tin theo tỉnh (10 tỉnh nhiều nhất)', exact=True)
                    chart.scroll_into_view_if_needed()
                    time.sleep(args.hold_seconds)
                    combo = page.get_by_role('combobox', name='NATIVE_FILTER-6eefee667a', exact=True)
                    combo.click()
                    combo.fill('Hồ Chí Minh')
                    page.get_by_role('option', name='Hồ Chí Minh', exact=True).click()
                    combo.press('Escape')
                    page.get_by_role('button', name='Apply filters', exact=True).click()
                    page.get_by_text(f'{hcm:,}', exact=True).wait_for()
                    checks.append(f'province_filter_{total}_to_{hcm}')
                    time.sleep(args.hold_seconds)
                    page.get_by_role('button', name='Clear all', exact=True).click()
                    page.get_by_role('button', name='Apply filters', exact=True).click()
                    page.get_by_text(f'{total:,}', exact=True).wait_for()
                    results.append({'rehearsal': number, 'status': 'PASS', 'checks': checks,
                                    'elapsed_seconds': round(time.perf_counter() - started, 2),
                                    'video': str(page.video.path())})
                    print(f'Rehearsal {number} PASS', flush=True)
                finally:
                    context.close()  # Finalize the actual browser recording.
                    (args.output / 'rehearsals.json').write_text(json.dumps({
                        'generated_at': datetime.now(timezone.utc).isoformat(),
                        'scope': 'Automated UI walkthrough, no voice; not an A3 pipeline execution or human presentation rehearsal',
                        'run_id': run, 'batch_id': batch, 'failure_run_id': failure_run,
                        'expected_kpis': {'total': total, 'ho_chi_minh': hcm}, 'results': results,
                        'status': 'PASS' if len(results) == 2 else 'INCOMPLETE'}, indent=2), encoding='utf-8')
        finally:
            browser.close()


if __name__ == '__main__':
    main()
