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
    contracts['year_signed'] = pd.to_numeric(contracts['year_signed'], errors='coerce')
    contracts['otc_id'] = pd.to_numeric(contracts['otc_id'], errors='coerce').astype('Int64')
    contracts = contracts[contracts['year_signed'] <= season].copy()
    contracts = contracts.sort_values('year_signed').groupby('otc_id').tail(1)
    
    player_stats = to_pandas(nfl.load_player_stats([season]))
    player_stats = player_stats.rename(columns={'player_id': 'gsis_id', 'team': 'recent_team'})
    
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
    
    logger.info('Loading rosters to get years_exp, team, position, and active status')
    rosters = to_pandas(nfl.load_rosters([season]))
    
    rosters['birth_date'] = pd.to_datetime(rosters['birth_date'], errors='coerce')
    cutoff_date = pd.to_datetime(f'{season}-09-01')
    rosters['age'] = (cutoff_date - rosters['birth_date']).dt.days / 365.25
    
    rosters_unique = rosters.dropna(subset=['gsis_id']).drop_duplicates(subset=['gsis_id'])
    
    cols_to_extract = ['gsis_id', 'years_exp', 'team', 'position', 'age']
    keep_cols = [c for c in cols_to_extract if c in rosters_unique.columns]
    
    roster_info = rosters_unique[keep_cols].rename(
        columns={'team': 'roster_team', 'position': 'roster_position'}
    )

    players['otc_id'] = pd.to_numeric(players['otc_id'], errors='coerce').astype('Int64')
    desired_cols = ['otc_id', 'gsis_id', 'pfr_id', 'player_name', 'latest_team', 'position', 'draft_year', 'entry_year', 'draft_round']
    keep_cols = [c for c in desired_cols if c in players.columns]
    
    logger.info('Merging contracts with players on otc_id (left join)')
    merged = pd.merge(contracts, players[keep_cols], on='otc_id', how='left', suffixes=('', '_ply'))

    logger.info('Merging merged/contracts with player_stats on gsis_id (left join)')
    merged = pd.merge(merged, player_stats, on='gsis_id', how='left', suffixes=('', '_stat'))
    
    logger.info('Merging season_snaps on pfr_id (left join)')
    merged = pd.merge(merged, season_snaps, left_on='pfr_id', right_on='pfr_player_id', how='left')
    
    logger.info('Merging roster info on gsis_id (left join)')
    merged = pd.merge(merged, roster_info, on='gsis_id', how='left')
    
    is_active_this_season = merged['gsis_id'].isin(rosters_unique['gsis_id']) | (merged['total_snaps'] > 0)
    merged = merged[is_active_this_season].copy()
    
    merged['team'] = merged.get('roster_team').fillna(merged.get('recent_team')).fillna(merged.get('latest_team'))

    base_pos = merged.get('roster_position').fillna(merged.get('position_ply')).fillna(merged.get('position'))
    
    generic_buckets = ['OL', 'DB', 'DL', 'LB']
    
    granular_fallback = merged.get('position_ply').fillna(merged.get('position'))
    
    is_generic = base_pos.isin(generic_buckets)
    merged['position'] = base_pos.mask(is_generic & granular_fallback.notna(), granular_fallback)

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
    out['age'] = safe_numeric(merged.get('age', 0.0))
    out['years_exp'] = pd.to_numeric(merged.get('years_exp'), errors='coerce').fillna(0).astype(int)
    out['yearly_cap_hit'] = safe_numeric(merged.get('apy', 0.0))
    out['cap_pct_of_team'] = safe_numeric(merged.get('apy_cap_pct', 0.0))
    out['passing_epa'] = safe_numeric(merged.get('passing_epa', 0.0))
    out['rushing_epa'] = safe_numeric(merged.get('rushing_epa', 0.0))
    out['receiving_epa'] = safe_numeric(merged.get('receiving_epa', 0.0))
    out['snaps'] = pd.to_numeric(merged['total_snaps'], errors='coerce').fillna(0).astype(int)
    draft_year = pd.to_numeric(merged.get('draft_year'), errors='coerce')
    draft_round = pd.to_numeric(merged.get('draft_round'), errors='coerce')
    year_signed = pd.to_numeric(merged.get('year_signed'), errors='coerce')
    entry_year = pd.to_numeric(merged.get('entry_year'), errors='coerce')
    years_exp = pd.to_numeric(merged.get('years_exp'), errors='coerce')
    
    # UDFAs vs. Drafted
    is_udfa = draft_round.isna() | (draft_round == 0)
    
    # Rule A: Drafted Players (Rounds 1-7)
    # Active contract must be signed in their draft year.
    # Any extension (year_signed > draft_year) means they have been repriced by the market.
    is_drafted_rookie = (~is_udfa) & (year_signed == draft_year)
    
    # Rule B: UDFAs
    # The CBA limits UDFAs to ERFA minimum deals for their first 3 accrued seasons.
    missing_exp_mask = years_exp.isna()
    years_since_entry = season - entry_year
    
    years_exp_filled = years_exp.fillna(years_since_entry)
    
    is_udfa_rookie = is_udfa & (years_exp_filled < 3)
    
    out['is_rookie_deal'] = (is_drafted_rookie | is_udfa_rookie).fillna(False).astype(bool)
    
    fallback_count = missing_exp_mask.sum()
    if fallback_count > 0:
        logger.warning(f"Audit: 'years_exp' missing for {fallback_count} players. Used 'entry_year' fallback to compute UDFA rookie window.")

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
