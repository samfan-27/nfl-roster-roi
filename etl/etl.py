"""
etl/etl.py — Production-ready ETL for roster_roi (nflreadpy -> Supabase)

Features:
- Loads players, contracts, player_stats from nflreadpy
- Robust otc_id <-> gsis_id mapping, with unmatched audit CSV
- Computes total_epa, epa_per_snap, cost_per_epa, normalized cost_per_epa_per_100_snaps
- Conservative shrinkage toward position mean (tunable)
- Outputs artifact CSV and upserts to Supabase roster_roi table in batches
- Updates pipeline_meta (id=1) with last_run, last_row_count, last_status

Usage:
  python etl/etl.py --season 2025 --min-snaps 100 --output ./artifacts/roster_roi_2025.csv

Requirements:
- Python packages installed from requirements.txt (nflreadpy, polars, pandas, supabase, python-dotenv, loguru)
- .env (SUPABASE_URL & SUPABASE_SERVICE_ROLE_KEY) or env vars set in CI
"""
import sys
import argparse

from loguru import logger
from dotenv import load_dotenv

import nflreadpy as nfl

from etl.database import get_supabase_client, upsert_supabase, update_pipeline_meta
from etl.utils import write_artifacts
from etl.config import DEFAULT_BATCH
from src.analysis import build_roster_roi
from src.stats_helpers import shrink_total_epa

load_dotenv()

def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument('--season', type=int, default=None, help='Season (year), default: nflreadpy.get_current_season()')
    p.add_argument('--min-snaps', type=int, default=100, help='Minimum snaps to avoid \'low_sample\' flag')
    p.add_argument('--shrink-tau', type=float, default=200.0, help='Shrinkage prior strength (larger -> more shrinkage)')
    p.add_argument('--output', type=str, default=None, help='Artifact CSV path')
    p.add_argument('--dry-run', action='store_true', help='Produce artifacts but do not write to Supabase')
    return p.parse_args()

def main():
    args = parse_args()
    season = args.season or getattr(nfl, 'get_current_season', lambda: None)()
    if season is None:
        logger.error('Season not provided and nflreadpy.get_current_season() not available; pass --season')
        sys.exit(2)

    output = args.output or f'./artifacts/roster_roi_{season}.csv'
    logger.info('ETL start: season={}, min_snaps={}, shrink_tau={}, output={}, dry_run={}',
                season, args.min_snaps, args.shrink_tau, output, args.dry_run)

    try:
        metrics_df, merged_debug, unmatched = build_roster_roi(season, min_snaps=args.min_snaps)
    except Exception as e:
        logger.exception('Failed building roster ROI: {}', e)
        
        try:
            sup = get_supabase_client()
            update_pipeline_meta(sup, status='failed_build', row_count=0, message=str(e))
        except Exception:
            logger.warning('Could not update pipeline_meta after build failure (missing creds?)')
        sys.exit(3)

    try:
        shrunk = shrink_total_epa(metrics_df, tau=args.shrink_tau)
        metrics_df['total_epa'] = shrunk['total_epa_shrunk']
        metrics_df['epa_per_snap'] = shrunk['epa_per_snap_shrunk']
        metrics_df['cost_per_epa'] = shrunk['cost_per_epa_shrunk']
        metrics_df['cost_per_epa_per_100_snaps'] = shrunk['cost_per_epa_per_100_snaps_shrunk']
    except Exception as e:
        logger.warning('Shrinkage step failed: {}. Proceeding without shrinkage.', e)

    final_cols = [
        'season', 'player_name', 'gsis_id', 'otc_id', 'team', 'position', 'yearly_cap_hit',
        'cap_pct_of_team', 'passing_epa', 'rushing_epa', 'receiving_epa', 'total_epa',
        'snaps', 'epa_per_snap', 'cost_per_epa', 'cost_per_epa_per_100_snaps',
        'epai_lower', 'epai_upper', 'sample_flag', 'notes', 'updated_at'
    ]
    for c in final_cols:
        if c not in metrics_df.columns:
            metrics_df[c] = None
    metrics_df = metrics_df[final_cols]
    
    metrics_df = metrics_df.drop_duplicates(subset=['season', 'otc_id'], keep='first')
    
    write_artifacts(metrics_df, merged_debug, unmatched, output)

    rows_written = 0
    if not args.dry_run:
        try:
            sup = get_supabase_client()
            rows_written = upsert_supabase(sup, metrics_df, table='roster_roi', batch_size=DEFAULT_BATCH)
            update_pipeline_meta(sup, status='success', row_count=rows_written, message='ETL succeeded')
        except Exception as e:
            logger.exception('Upsert to Supabase failed: {}', e)
            try:
                sup = get_supabase_client()
                update_pipeline_meta(sup, status='failed_upsert', row_count=0, message=str(e))
            except Exception:
                logger.warning('Could not update pipeline_meta after upsert failure')
            sys.exit(4)
    else:
        logger.info('Dry-run: not writing to Supabase (rows produced: {})', len(metrics_df))

    logger.success('ETL finished. rows_written={}', rows_written)

if __name__ == "__main__":
    main()
