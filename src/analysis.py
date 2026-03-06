import pandas as pd
import datetime
from loguru import logger
from typing import Tuple

import nflreadpy as nfl

from etl.utils import to_pandas, safe_numeric
from src.stats_helpers import compute_core_metrics

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
