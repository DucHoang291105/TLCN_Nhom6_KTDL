from __future__ import annotations
import json
import tempfile
import uuid
from datetime import datetime, timezone
from pathlib import Path

from src.common.atomic_io import write_json_atomic
from src.common.spark_session import build_spark_session, write_iceberg_table
from src.orchestration import publication as gate
from src.orchestration.pipeline import STAGES


def main():
    spark = build_spark_session('PublicationFailureSandbox')
    table = 'lakehouse.smoke_test.publication_' + uuid.uuid4().hex[:12]
    checks = {}
    try:
        spark.sql('CREATE NAMESPACE IF NOT EXISTS lakehouse.smoke_test')
        with tempfile.TemporaryDirectory(prefix='publication-check-') as directory:
            root = Path(directory)
            reports = root / 'docs/validation'
            reports.mkdir(parents=True)
            for name in gate.REPORTS:
                write_json_atomic(reports / f'{name}.json', {'status': 'PASS'})
            jobs = [p for stages in STAGES.values() for _, p in stages]
            gate.save({'mode': 'write', 'status': 'BUILDING', 'owner': 'fixture',
                       'identity': gate.identity(root), 'completed': jobs}, root)
            with gate.catalog_lock(root):
                gate.publish(root, 'fixture')
            checks['initial_verified_publication'] = gate.require_published(root)['status'] == 'PUBLISHED'
            with gate.catalog_lock(root):
                state = gate.begin_job('src/gold/build_price_model.py', 'fixture', root)

                write_iceberg_table(spark.createDataFrame([(1, 100)], ['id', 'price']), table)
                checks['sandbox_commit_completed'] = spark.table(table).count() == 1
                gate.finish_job(state, 'src/gold/build_price_model.py', False, root)
            try:
                gate.require_published(root)
                checks['failed_partial_build_blocks_read'] = False
            except RuntimeError:
                checks['failed_partial_build_blocks_read'] = True
            with gate.catalog_lock(root):
                for app in jobs:
                    state = gate.begin_job(app, 'retry-fixture', root)
                    gate.finish_job(state, app, True, root)
                gate.publish(root, 'retry-fixture')
            checks['full_stage_retry_can_publish'] = gate.require_published(root)['run_id'] == 'retry-fixture'
        result = {'generated_at': datetime.now(timezone.utc).isoformat(), 'table': table,
                  'scope': 'Real isolated Iceberg commit + injected subsequent failure; retry stage transitions are a fixture',
                  'checks': checks, 'status': 'PASS' if all(checks.values()) else 'FAIL'}
        write_json_atomic(gate.ROOT / 'docs/validation/publication_gate_verification.json', result)
        print(json.dumps(result, indent=2))
        if result['status'] != 'PASS':
            raise RuntimeError('Publication gate fixture failed')
    finally:
        spark.stop()


if __name__ == '__main__':
    main()
