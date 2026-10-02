"""Read pinned canonical snapshots; verify hashes; never mutate cloud state."""
import argparse
import json
from pathlib import Path
from dotenv import load_dotenv
from etl.database import get_supabase_client
from etl.cloud_store import CloudStore


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--provenance', default='docs/methodology-inputs-2026-10-01.json')
    parser.add_argument('--output', default='artifacts/methodology-v2/inputs')
    args = parser.parse_args()
    load_dotenv('.env')
    store = CloudStore(get_supabase_client())
    output = Path(args.output)
    output.mkdir(parents=True, exist_ok=True)
    provenance = json.loads(Path(args.provenance).read_text())
    requested = provenance['historical_snapshots'] + [dict(run_id=provenance['current_run_id'])]
    for saved in requested:
        snap = store.snapshot(saved['run_id'])
        is_current = snap['run_id'] == provenance['current_run_id']
        if not is_current and (not snap['complete_season'] or not snap['coverage']['complete_season']):
            raise ValueError('Only completed canonical historical sources are accepted')
        year = snap['season']
        (output / f'{year}_manifest.json').write_text(json.dumps(snap, indent=2))
        for name in ['player_stats', 'snap_counts', 'rosters', 'players', 'contracts', 'schedule']:
            descriptor = snap['manifest']['artifacts'][name]
            target = output / f'{year}_{name}.parquet'
            if target.exists():
                from hashlib import sha256
                if sha256(target.read_bytes()).hexdigest() == descriptor['sha256']:
                    continue
            target.write_bytes(store.get(descriptor))
        print(f'{year}: canonical source files verified', flush=True)


if __name__ == '__main__':
    main()
