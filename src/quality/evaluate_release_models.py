from __future__ import annotations
import json
from datetime import datetime, timezone

from src.common.atomic_io import write_json_atomic
from src.orchestration.publication import ROOT, checksum, identity


def main():
    from src.quality import price_model_selection
    from src.gold.gold_common import write_summary

    original = price_model_selection.write_summary

    def save_candidate(filename, summary):
        summary['generated_at'] = datetime.now(timezone.utc).isoformat()
        summary['publication_identity'] = identity(ROOT)
        summary['production_configuration_changed'] = False
        summary['config_sha256'] = checksum(ROOT / 'config/price_model_blend.json', normalize=True)
        summary['purpose'] = 'New-data CV candidate; frozen Gold configuration is unchanged'
        write_summary('price_model_selection_candidate.json', summary)

    try:
        price_model_selection.write_summary = save_candidate
        price_model_selection.main()
    finally:
        price_model_selection.write_summary = original
    result = json.loads((ROOT / 'docs/validation/price_model_selection_candidate.json').read_text(encoding='utf-8'))
    write_json_atomic(ROOT / 'outputs/validation/model_evaluation_run.json', {
        'status': result['status'], 'generated_at': result['generated_at'],
        'production_configuration_changed': False, 'publication_identity': result['publication_identity']})


if __name__ == '__main__':
    main()
