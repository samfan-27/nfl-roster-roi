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
import datetime

import pandas as pd

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
    p.add_argument('--seasons', nargs='+', type=int, help='Specific seasons to run (e.g., 2023 2024)')
    p.add_argument('--auto', action='store_true', help='Auto-run from 2021 to the current season')
    p.add_argument('--min-snaps', type=int, default=100, help='Minimum snaps to avoid low_sample flag')
    p.add_argument('--shrink-tau', type=float, default=200.0, help='Shrinkage prior strength')
    p.add_argument('--output', type=str, default='./artifacts/roster_roi_combined.csv', help='Artifact CSV path')
    p.add_argument('--dry-run', action='store_true', help='Produce artifacts but do not write to Supabase')
    return p.parse_args()

def main():
    args = parse_args()
    
    current_season = getattr(nfl, 'get_current_season', lambda: datetime.datetime.now().year)()
    
    if args.auto:
        seasons_to_run = list(range(2021, current_season + 1))
    elif args.seasons:
        seasons_to_run = args.seasons
    else:
        seasons_to_run = [current_season]

    logger.info('ETL start: seasons={}, min_snaps={}, shrink_tau={}, output={}, dry_run={}',
                seasons_to_run, args.min_snaps, args.shrink_tau, args.output, args.dry_run)

    all_metrics = []
    merged_debugs = []
    all_unmatched = []

    for s in seasons_to_run:
        try:
            logger.info(f'--- Fetching Data for Season {s} ---')
            metrics_df, merged_debug, unmatched = build_roster_roi(s, min_snaps=args.min_snaps)
            all_metrics.append(metrics_df)
            merged_debugs.append(merged_debug)
            all_unmatched.append(unmatched)
        except Exception as e:
            logger.exception(f'Failed building roster ROI for {s}: {e}')
            sys.exit(3)

    combined_metrics = pd.concat(all_metrics, ignore_index=True)
    combined_debug = pd.concat(merged_debugs, ignore_index=True)
    combined_unmatched = pd.concat(all_unmatched, ignore_index=True).drop_duplicates()

    # Apply shrinkage across all years for highly stable priors
    try:
        logger.info('Applying Empirical Bayes Shrinkage across all seasons...')
        shrunk = shrink_total_epa(combined_metrics, tau=args.shrink_tau)
        combined_metrics['total_epa'] = shrunk['total_epa_shrunk']
        combined_metrics['epa_per_snap'] = shrunk['epa_per_snap_shrunk']
        combined_metrics['cost_per_epa'] = shrunk['cost_per_epa_shrunk']
        combined_metrics['cost_per_epa_per_100_snaps'] = shrunk['cost_per_epa_per_100_snaps_shrunk']
    except Exception as e:
        logger.warning('Shrinkage step failed: {}. Proceeding without shrinkage.', e)

    final_cols = [
        'season', 'player_name', 'gsis_id', 'otc_id', 'team', 'position', 'yearly_cap_hit',
        'cap_pct_of_team', 'passing_epa', 'rushing_epa', 'receiving_epa', 'total_epa',
        'snaps', 'epa_per_snap', 'cost_per_epa', 'cost_per_epa_per_100_snaps',
        'epai_lower', 'epai_upper', 'sample_flag', 'notes', 'is_rookie_deal', 'updated_at'
    ]
    
    for c in final_cols:
        if c not in combined_metrics.columns:
            combined_metrics[c] = None
            
    combined_metrics = combined_metrics[final_cols]
    combined_metrics = combined_metrics.dropna(subset=['gsis_id'])
    combined_metrics = combined_metrics.drop_duplicates(subset=['season', 'gsis_id'], keep='first')
    
    write_artifacts(combined_metrics, combined_debug, combined_unmatched, args.output)

    rows_written = 0
    if not args.dry_run:
        try:
            sup = get_supabase_client()
            rows_written = upsert_supabase(sup, combined_metrics, table='roster_roi', batch_size=DEFAULT_BATCH)
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
