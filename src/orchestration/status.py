import json

from src.quality.defense_evidence import ROOT, read
from src.quality.tag_defense import verified_candidate


def main():
    path = ROOT / 'docs/validation/defense_verification.json'
    proof = read(path)
    error = None
    try:
        verified_candidate(ROOT, proof)
        fresh = True
    except (OSError, ValueError) as exc:
        fresh, error = False, str(exc)
    print(json.dumps({'status': proof['status'] if fresh else 'STALE_REQUIRES_RERUN',
                      'run_id': proof['run_id'], 'batch_id': proof['batch_id'],
                      'checks_passed': sum(v is True for v in proof['checks'].values()),
                      'checks_total': len(proof['checks']), 'verified_source_inputs_and_evidence': fresh,
                      'stale_reason': error,
                      'silver_counts': proof['silver_counts'], 'gold_counts': proof['gold_counts'],
                      'business_limits': proof['business_limits']}, ensure_ascii=False, indent=2))
    if not fresh or proof['status'] != 'PASS':
        raise SystemExit(2)


if __name__ == '__main__':
    main()
