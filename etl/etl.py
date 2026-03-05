"""
Minimal test ETL used only for CI/workflow smoke testing.
Writes a small CSV artifact to ./artifacts/roster_roi_{season}.csv
Usage:
    python etl/etl.py --season 2025 --output ./artifacts/roster_roi_2025.csv
"""
import argparse
import os
import csv
import datetime
from pathlib import Path

def make_sample_rows(season):
    now = datetime.datetime.utcnow().isoformat()
    rows = [
        {
            'season': season,
            'player_name': 'Test Player A',
            'gsis_id': 'GTEST001',
            'otc_id': 'OTC001',
            'team': 'TEST',
            'position': 'WR',
            'yearly_cap_hit': 1000000.00,
            'passing_epa': 0.0,
            'rushing_epa': 0.0,
            'receiving_epa': 25.0,
            'total_epa': 25.0,
            'snaps': 200,
            'epa_per_snap': 0.125,
            'cost_per_epa': 1000000.0/25.0,
            'cost_per_epa_per_100_snaps': None,
            'epai_lower': None,
            'epai_upper': None,
            'sample_flag': 'ok',
            'notes': 'ci-test',
            'updated_at': now
        },
        {
            'season': season,
            'player_name': 'Test Player B',
            'gsis_id': 'GTEST002',
            'otc_id': 'OTC002',
            'team': 'TEST',
            'position': 'RB',
            'yearly_cap_hit': 5000000.00,
            'passing_epa': 0.0,
            'rushing_epa': -5.0,
            'receiving_epa': 0.0,
            'total_epa': -5.0,
            'snaps': 50,
            'epa_per_snap': -0.1,
            'cost_per_epa': None,
            'cost_per_epa_per_100_snaps': None,
            'epai_lower': None,
            'epai_upper': None,
            'sample_flag': 'low_sample',
            'notes': 'ci-test-liability',
            'updated_at': now
        }
    ]
    return rows

def write_csv(path, rows):
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    # fields (order to match DDL roughly)
    fields = [
        'season','player_name','gsis_id','otc_id','team','position','yearly_cap_hit',
        'passing_epa','rushing_epa','receiving_epa','total_epa','snaps','epa_per_snap',
        'cost_per_epa','cost_per_epa_per_100_snaps','epai_lower','epai_upper',
        'sample_flag','notes','updated_at'
    ]
    with open(path, 'w', newline='', encoding='utf-8') as f:
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader()
        for r in rows:
            # ensure every key exists
            out = {k: r.get(k, "") for k in fields}
            w.writerow(out)

def main():
    p = argparse.ArgumentParser()
    p.add_argument('--season', type=int, default=2025)
    p.add_argument('--output', type=str, default=None)
    p.add_argument('--dry-run', action='store_true')
    args = p.parse_args()

    outpath = args.output or f'./artifacts/roster_roi_{args.season}.csv'
    print(f'[test-etl] season={args.season} output={outpath} dry_run={args.dry_run}')

    rows = make_sample_rows(args.season)
    write_csv(outpath, rows)
    print(f'[test-etl] Wrote {len(rows)} rows to {outpath}')

    if args.dry_run:
        print(f'[test-etl] dry-run -- not attempting Supabase writes (none implemented in test ETL)')

    print(f'[test-etl] done - success')
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
