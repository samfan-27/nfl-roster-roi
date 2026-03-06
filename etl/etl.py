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
import os
import sys
import argparse
import datetime
import math
from pathlib import Path
from typing import Tuple

from loguru import logger
from dotenv import load_dotenv

import nflreadpy as nfl
import pandas as pd

from supabase import create_client

load_dotenv()

DEFAULT_BATCH = 200


def get_supabase_client():
    url = os.getenv('SUPABASE_URL')
    key = os.getenv('SUPABASE_SERVICE_ROLE_KEY')
    if not url or not key:
        raise RuntimeError('Missing SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY in environment')
    return create_client(url, key)


def to_pandas(df):
    if df is None:
        return pd.DataFrame()
    if hasattr(df, 'to_pandas'):
        return df.to_pandas()
    if isinstance(df, pd.DataFrame):
        return df
    return pd.DataFrame(df)


def safe_numeric(series, fill=0.0):
    if not hasattr(series, 'fillna'):
        val = pd.to_numeric(series, errors='coerce')
        return fill if pd.isna(val) else val
    return pd.to_numeric(series, errors='coerce').fillna(fill)


def compute_core_metrics(df: pd.DataFrame) -> pd.DataFrame:
    """Given df with numeric columns for passing_epa/rushing_epa/receiving_epa/snaps/yearly_cap_hit,
    compute total_epa, epa_per_snap, cost_per_epa (only for total_epa > 0), and normalized cost per 100 snaps.
    """
    out = df.copy()
    out['passing_epa'] = safe_numeric(out.get('passing_epa', 0.0))
    out['rushing_epa'] = safe_numeric(out.get('rushing_epa', 0.0))
    out['receiving_epa'] = safe_numeric(out.get('receiving_epa', 0.0))
    out['total_epa'] = out['passing_epa'] + out['rushing_epa'] + out['receiving_epa']

    out['snaps'] = pd.to_numeric(out.get('snaps', 0), errors='coerce').fillna(0).astype(int)

    # EPA per snap (0 when snaps==0)
    out['epa_per_snap'] = out.apply(lambda r: (r['total_epa'] / r['snaps']) if r['snaps'] > 0 else 0.0, axis=1)

    # yearly_cap_hit numeric
    out['yearly_cap_hit'] = safe_numeric(out.get('yearly_cap_hit', 0.0))

    def cost_per_epa(row):
        te = row['total_epa']
        if te is None or math.isclose(te, 0.0) or te <= 0:
            return None
        return row['yearly_cap_hit'] / te

    out['cost_per_epa'] = out.apply(cost_per_epa, axis=1)

    def cost_per_100(row):
        te = row['total_epa']
        snaps = row['snaps']
        if snaps <= 0 or te is None:
            return None
        te_per_100 = te * (100.0 / snaps)
        if te_per_100 <= 0:
            return None
        return row['yearly_cap_hit'] / te_per_100

    out['cost_per_epa_per_100_snaps'] = out.apply(cost_per_100, axis=1)

    return out

def shrink_total_epa(df: pd.DataFrame, tau: float = 200.0) -> pd.DataFrame:
    """
    Empirical-Bayes-like shrinkage of EPA per snap toward position mean.
    tau: prior strength — essentially 'dummy snaps' at the positional average.
    We shrink the rate, then re-multiply by actual snaps for volume.
    """
    out = df.copy()
    
    # positional EPA/snap averages
    pos_totals = out.groupby('position')[['total_epa', 'snaps']].sum()
    pos_rates = (pos_totals['total_epa'] / pos_totals['snaps'].replace(0, 1)).to_dict()
    
    global_total_epa = out['total_epa'].sum()
    global_snaps = out['snaps'].sum()
    global_rate = global_total_epa / global_snaps if global_snaps > 0 else 0.0

    def shrink_row(r):
        pos = r.get('position')
        prior_rate = pos_rates.get(pos, global_rate)
        
        actual_epa = r.get('total_epa', 0.0)
        n = max(float(r.get('snaps', 0.0)), 0.0)
        
        denom = n + tau
        if denom <= 0:
            return actual_epa, 0.0
            
        # Shrunk Rate = (Actual EPA + (Prior Rate * Tau)) / (Actual Snaps + Tau)
        shrunk_rate = (actual_epa + prior_rate * tau) / denom
        
        # Shrunk Total = Shrunk Rate * Actual Snaps
        shrunk_total = shrunk_rate * n
        
        return shrunk_total, shrunk_rate

    # Apply the shrinkage
    shrunk_results = out.apply(shrink_row, axis=1, result_type='expand')
    out['total_epa_shrunk'] = shrunk_results[0]
    out['epa_per_snap_shrunk'] = shrunk_results[1]

    def cost_per_epa_shrunk(r):
        te = r['total_epa_shrunk']
        if te is None or math.isclose(te, 0.0) or te <= 0:
            return None
        return r['yearly_cap_hit'] / te

    out['cost_per_epa_shrunk'] = out.apply(cost_per_epa_shrunk, axis=1)

    def cost_per_100_shrunk(r):
        te = r['total_epa_shrunk']
        snaps = r['snaps']
        if snaps <= 0 or te is None:
            return None
        te_per_100 = te * (100.0 / snaps)
        if te_per_100 <= 0:
            return None
        return r['yearly_cap_hit'] / te_per_100

    out['cost_per_epa_per_100_snaps_shrunk'] = out.apply(cost_per_100_shrunk, axis=1)
    
    return out

def build_roster_roi(season: int, min_snaps: int = 100) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Returns:
      metrics_df: pandas DataFrame ready to write to CSV/upsert (columns match roster_roi DDL)
      merged_debug: full merged dataframe for auditing (written to artifacts)
    """
    logger.info('Loading nflreadpy tables for season {}', season)
    players = to_pandas(nfl.load_players())
    contracts = to_pandas(nfl.load_contracts())
    contracts = contracts[contracts['is_active'] == True].copy()
    player_stats = to_pandas(nfl.load_player_stats([season]))
    player_stats = player_stats.rename(columns={'player_id': 'gsis_id'})
    
    player_stats = player_stats.rename(columns={'team': 'recent_team'})
    
    agg_dict = {
        'passing_epa': 'sum',
        'rushing_epa': 'sum',
        'receiving_epa': 'sum'
    }
    
    if 'recent_team' in player_stats.columns:
        agg_dict['recent_team'] = 'last'
        
    player_stats = player_stats.groupby('gsis_id', as_index=False).agg(agg_dict)
    
    snap_counts = to_pandas(nfl.load_snap_counts([season]))
    snap_counts['offense_snaps'] = pd.to_numeric(snap_counts['offense_snaps'], errors='coerce').fillna(0)
    snap_counts['defense_snaps'] = pd.to_numeric(snap_counts['defense_snaps'], errors='coerce').fillna(0)
    snap_counts['total_snaps'] = snap_counts['offense_snaps'] + snap_counts['defense_snaps']
    
    season_snaps = snap_counts.groupby('pfr_player_id', as_index=False)['total_snaps'].sum()

    logger.info('players rows: {}, contracts rows: {}, player_stats rows: {}, snap_counts rows: {}', 
                len(players), len(contracts), len(player_stats), len(snap_counts))

    players = players.rename(columns={'display_name': 'player_name'})

    player_stats = player_stats.rename(columns={'player_id': 'gsis_id'})
    logger.info('Renamed player_stats column player_id -> gsis_id')

    logger.info('Merging contracts with players on otc_id (left join)')
    
    contracts['otc_id'] = pd.to_numeric(contracts['otc_id'], errors='coerce').astype('Int64')
    players['otc_id'] = pd.to_numeric(players['otc_id'], errors='coerce').astype('Int64')

    desired_cols = ['otc_id', 'gsis_id', 'pfr_id', 'player_name', 'latest_team', 'position']
    keep_cols = [c for c in desired_cols if c in players.columns]
    
    merged = pd.merge(contracts, players[keep_cols], on='otc_id', how='left', suffixes=('', '_ply'))

    logger.info('Merging merged/contracts with player_stats on gsis_id (left join)')
    merged = pd.merge(merged, player_stats, on='gsis_id', how='left', suffixes=('', '_stat'))
    
    logger.info('Merging season_snaps on pfr_id (left join)')
    merged = pd.merge(merged, season_snaps, left_on='pfr_id', right_on='pfr_player_id', how='left')
    
    if 'recent_team' in merged.columns:
        merged['team'] = merged['recent_team'].fillna(merged.get('latest_team'))
    else:
        merged['team'] = merged.get('latest_team')

    if 'player_name' not in merged.columns:
        merged['player_name'] = None
        
    if 'player_name_stat' in merged.columns:
        merged['player_name'] = merged['player_name'].fillna(merged['player_name_stat'])
    if 'player' in merged.columns:
        merged['player_name'] = merged['player'].fillna(merged['player'])

    out = pd.DataFrame()
    out['player_name'] = merged.get('player_name').fillna('unknown')
    out['season'] = season
    out['team'] = merged.get('team')
    out['position'] = merged.get('position')
    out['gsis_id'] = merged.get('gsis_id')
    out['otc_id'] = merged.get('otc_id')
    out['yearly_cap_hit'] = safe_numeric(merged.get('apy', 0.0))
    out['cap_pct_of_team'] = safe_numeric(merged.get('apy_cap_pct', 0.0))
    out['passing_epa'] = safe_numeric(merged.get('passing_epa', 0.0))
    out['rushing_epa'] = safe_numeric(merged.get('rushing_epa', 0.0))
    out['receiving_epa'] = safe_numeric(merged.get('receiving_epa', 0.0))
    out['snaps'] = pd.to_numeric(merged['total_snaps'], errors='coerce').fillna(0).astype(int)

    out = compute_core_metrics(out)

    def sample_flag_fn(r):
        if r['snaps'] < min_snaps:
            return 'low_sample'
        if r['total_epa'] <= 0:
            return 'liability_or_zero'
        return 'ok'

    out['sample_flag'] = out.apply(sample_flag_fn, axis=1)
    out['notes'] = None
    out['updated_at'] = datetime.datetime.now(datetime.UTC).isoformat()

    unmatched = out[(out['gsis_id'].isna()) & (out['otc_id'].notna())][['player_name', 'otc_id']].copy()
    unmatched = unmatched.drop_duplicates()
    logger.info('Unmatched mapping rows (otc->no gsis): {}', len(unmatched))

    return out, merged, unmatched


def write_artifacts(df: pd.DataFrame, merged: pd.DataFrame, unmatched: pd.DataFrame, outpath: str):
    Path(outpath).parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(outpath, index=False)
    logger.info('Wrote artifact CSV to {}', outpath)

    merged_path = Path(outpath).with_name(Path(outpath).stem + '_merged.csv')
    unmatched_path = Path(outpath).with_name(Path(outpath).stem + '_unmatched.csv')
    try:
        merged.to_csv(merged_path, index=False)
        unmatched.to_csv(unmatched_path, index=False)
        logger.info('Wrote merged debug to {} and unmatched mapping to {}', merged_path, unmatched_path)
    except Exception as e:
        logger.warning('Failed to write debug artifacts: {}', e)


def upsert_supabase(supabase, df: pd.DataFrame, table: str = 'roster_roi', batch_size: int = DEFAULT_BATCH) -> int:
    raw_records = df.to_dict(orient='records')
    
    records = []
    for row in raw_records:
        clean_row = {}
        for k, v in row.items():
            # pd.isna() catches NaN, pd.NA, NaT, and None
            if pd.isna(v):
                clean_row[k] = None
            else:
                clean_row[k] = v
        records.append(clean_row)

    total = len(records)
    logger.info('Upserting {} records to Supabase table \'{}\'', total, table)
    
    for i in range(0, total, batch_size):
        batch = records[i : i + batch_size]
        resp = supabase.table(table).upsert(batch, on_conflict='season,otc_id').execute()

        try:
            code = getattr(resp, 'status_code', None)
            if code and code >= 400:
                logger.error('Supabase upsert error status {}: {}', code, getattr(resp, 'data', resp))
                raise RuntimeError('Supabase upsert failed')
        except Exception:
            pass
            
    logger.info('Upsert complete')
    return total


def update_pipeline_meta(supabase, status: str, row_count: int = 0, message: str = ""):
    payload = {
        'id': 1,
        'last_run': datetime.datetime.now(datetime.UTC).isoformat(),
        'last_row_count': int(row_count),
        'last_status': status,
        'last_message': message or "",
    }
    try:
        supabase.table('pipeline_meta').upsert(payload).execute()
    except Exception as e:
        logger.warning('Failed to update pipeline_meta: {}', e)


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
