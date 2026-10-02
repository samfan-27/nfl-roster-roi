"""Pure roster/contract joins and descriptive production metrics."""
import datetime
import numpy as np
import pandas as pd

from src.contracts import select_contracts, reconcile_contract_identities
from src.domain import SALARY_CAP_MILLIONS, OTC_TEAM_CODES, POSITION_FAMILIES, numeric, rookie_contract_mask
from src.opportunities import shared_regular_games, COUNTS
from src.stats_helpers import compute_core_metrics, shrink_total_epa


def _player_contract_join(players, contracts, season, snapshot_at, rosters):
    ids = players.rename(columns={'display_name': 'player_name'}).copy()
    ids['otc_id'] = numeric(ids.otc_id, fill=np.nan).astype('Int64')
    if ids.dropna(subset=['otc_id']).groupby('otc_id').gsis_id.nunique().gt(1).any():
        raise ValueError('Conflicting OTC/GSIS player identities')
    ids = ids.dropna(subset=['gsis_id']).drop_duplicates('gsis_id')
    selected = select_contracts(reconcile_contract_identities(contracts, players), season, snapshot_at)
    identity = ids.dropna(subset=['otc_id'])[['otc_id', 'gsis_id']].drop_duplicates('otc_id')
    audit = selected.merge(identity, on='otc_id', how='left', suffixes=('', '_player'), validate='one_to_one')
    conflicts = audit.gsis_id.notna() & audit.gsis_id_player.notna() & audit.gsis_id.ne(audit.gsis_id_player)
    if conflicts.any():
        raise ValueError('Contract/player GSIS identity conflict')
    audit['gsis_id'] = audit.gsis_id.fillna(audit.gsis_id_player)
    unmatched = audit.loc[audit.gsis_id.isna(), ['player', 'otc_id']].rename(columns={'player': 'player_name'})
    selected = audit.dropna(subset=['gsis_id'])
    # Multiple surviving OTC identities for one GSIS cannot be priced safely.
    ambiguous_ids = selected.loc[selected.gsis_id.duplicated(False), 'gsis_id']
    selected = selected.loc[~selected.gsis_id.isin(ambiguous_ids)]
    absent = selected.loc[~selected.gsis_id.isin(ids.gsis_id)]
    ids = pd.concat([ids, absent[['gsis_id', 'otc_id', 'position', 'player']].rename(columns={'player':'player_name'})], ignore_index=True)
    roster_ids = rosters.dropna(subset=['gsis_id']).drop_duplicates('gsis_id', keep='last')
    absent = roster_ids.loc[~roster_ids.gsis_id.isin(ids.gsis_id)]
    fields = [c for c in ['gsis_id','pfr_id','position','full_name'] if c in absent]
    ids = pd.concat([ids, absent[fields].rename(columns={'full_name':'player_name'})], ignore_index=True)
    if 'pfr_id' in roster_ids:
        alternative = ids.gsis_id.map(roster_ids.set_index('gsis_id').pfr_id)
        ids['pfr_identity_status'] = np.where(ids.pfr_id.notna() & alternative.notna() & ids.pfr_id.ne(alternative),
                                            'master_mapping_roster_disagreement', 'master_or_unambiguous_roster')
        ids['pfr_id'] = ids.pfr_id.fillna(ids.gsis_id.map(roster_ids.set_index('gsis_id').pfr_id))
    else:
        ids['pfr_identity_status'] = 'master_mapping'
    ids['otc_id'] = ids.otc_id.fillna(ids.gsis_id.map(selected.set_index('gsis_id').otc_id))
    # Players without a contract stay in the spine and in financial coverage.
    fields = [c for c in selected if (c.startswith('contract_') or c.startswith('season_') or c.startswith('annual_')) and c not in ['contract_history', 'season_history']]
    fields += ['apy', 'year_signed']
    joined = ids.merge(selected[['gsis_id', *fields]], on='gsis_id', how='left', validate='one_to_one')
    for c in ['contract_type', 'annual_financial_status', 'contract_selection_status']:
        joined[c] = joined[c].fillna('Unknown' if c == 'contract_type' else 'missing')
    return joined, audit, unmatched


def _aggregate_stats(stats):
    stats = stats.rename(columns={'player_id': 'gsis_id', 'team': 'recent_team'}).sort_values(['week', 'game_id'])
    aggregations = {c: (c, 'sum') for c in ['passing_epa', 'rushing_epa', 'receiving_epa']}
    aggregations.update(recent_team=('recent_team', 'last'), games_with_stats=('game_id', 'nunique'),
                        production_team_count=('recent_team','nunique'), through_week=('week', 'max'))
    for col in COUNTS:
        if col in stats:
            aggregations[col] = (col, lambda x: x.sum(min_count=1))
    result = stats.groupby('gsis_id', as_index=False).agg(**aggregations)
    # Role shares are game averages, not percentages summed across games.
    for col in ['passing_cpoe', 'target_share', 'air_yards_share']:
        if col in stats:
            result = result.merge(stats.groupby('gsis_id')[col].mean().rename(col + '_game_mean'), on='gsis_id')
    return result


def build_roster_roi(season, *, players, contracts, player_stats, snap_counts,
                     rosters, min_snaps=100, shrink_tau=200.0, snapshot_at=None):
    """Shared regular-season coverage; unknown financial values are retained."""
    if season not in SALARY_CAP_MILLIONS:
        raise ValueError(f'Salary cap is not configured for season {season}')
    stats, snaps = shared_regular_games(season, player_stats, snap_counts)
    season_stats = _aggregate_stats(stats)
    snaps['offensive_snaps'] = numeric(snaps.offense_snaps)
    season_snaps = snaps.groupby('pfr_player_id', as_index=False).agg(
        offensive_snaps=('offensive_snaps', 'sum'), games_played=('game_id', 'nunique'))
    merged, audit, unmatched = _player_contract_join(players, contracts, season, snapshot_at, rosters)
    merged = merged.merge(season_stats, on='gsis_id', how='left', validate='one_to_one')
    merged = merged.merge(season_snaps, left_on='pfr_id', right_on='pfr_player_id', how='left', validate='many_to_one')
    roster = rosters.dropna(subset=['gsis_id']).copy()
    if 'season' in roster:
        roster = roster.loc[roster.season.eq(season)]
    if 'week' in roster:
        roster = roster.sort_values('week', kind='stable')
    roster = roster.drop_duplicates('gsis_id', keep='last')
    roster['age'] = (pd.Timestamp(f'{season}-09-01') - pd.to_datetime(roster.birth_date, errors='coerce')).dt.days / 365.25
    if 'entry_year' not in roster:
        roster['entry_year'] = np.nan
    merged = merged.merge(roster[['gsis_id', 'team', 'position', 'age', 'years_exp', 'entry_year']],
                          on='gsis_id', how='left', suffixes=('', '_roster'), validate='one_to_one')
    merged = merged.loc[merged.gsis_id.isin(roster.gsis_id) | merged.offensive_snaps.gt(0)].copy()
    if 'team_roster' in merged:
        merged['team'] = merged.team_roster.fillna(merged.recent_team)
    else:
        merged['team'] = merged.team.fillna(merged.recent_team)
    merged['position'] = merged.position_roster.fillna(merged.position)
    merged['position'] = merged.position.map(POSITION_FAMILIES).fillna(merged.position)
    out = merged[[c for c in merged if c.startswith('contract_') or c.startswith('season_') or c.startswith('annual_')]].copy()
    out['season'] = season
    for c in ['gsis_id', 'otc_id', 'player_name', 'team', 'position', 'age', 'years_exp', 'games_played', 'pfr_identity_status']:
        out[c] = merged[c]
    out['production_team_count'] = numeric(merged.production_team_count).astype(int)
    financial_team = out.season_financial_team.map(OTC_TEAM_CODES)
    team_unresolved = out.annual_financial_status.eq('resolved') & (
        financial_team.isna() | financial_team.ne(out.team.map(OTC_TEAM_CODES)) | out.production_team_count.gt(1))
    out.loc[team_unresolved, 'annual_financial_status'] = 'team_allocation_unresolved'
    # Extracted provider values remain in the join audit. Player-season budget
    # costs require compatible production/team scope, not a current-team guess.
    out.loc[team_unresolved, ['season_cap_charge_m','season_cash_m']] = np.nan
    out['years_exp'] = numeric(out.years_exp, fill=np.nan).astype('Int64')
    out['yearly_cap_hit'] = out.contract_apy_m  # Deprecated APY alias, never cap charge.
    out['cap_pct_of_team'] = out.contract_apy_m / SALARY_CAP_MILLIONS[season]
    for c in ['passing_epa', 'rushing_epa', 'receiving_epa']:
        out[c] = numeric(merged[c])
    for c in COUNTS:
        out[c] = numeric(merged[c], fill=np.nan).fillna(0) if c in merged else np.nan
    out['pass_opportunities'] = out.attempts + out.sacks_suffered
    out['snaps'] = numeric(merged.offensive_snaps).astype(int)
    out['offensive_snaps'] = out.snaps
    out['games_played'] = numeric(out.games_played).astype(int)
    for c in merged:
        if c.endswith('_game_mean'):
            out[c] = merged[c]
    out['is_rookie_deal'] = rookie_contract_mask(merged, season)
    out = compute_core_metrics(out)
    adjusted = shrink_total_epa(out, tau=shrink_tau)
    out['cost_per_epa_per_100_snaps'] = adjusted.cost_per_epa_per_100_snaps_shrunk
    out['sample_flag'] = 'ok'
    out.loc[out.total_epa.le(0), 'sample_flag'] = 'nonpositive_attributed_epa'
    out.loc[out.snaps.lt(min_snaps), 'sample_flag'] = 'low_sample'
    out.loc[out.contract_apy_m.le(0) | out.contract_apy_m.isna(), 'sample_flag'] = 'missing_contract'
    week = int(stats.week.max())
    stat_games = set(player_stats.loc[player_stats.season.eq(season) & player_stats.season_type.eq('REG')].game_id)
    snap_games = set(snap_counts.loc[snap_counts.season.eq(season) & snap_counts.game_type.eq('REG')].game_id)
    out['through_week'] = week
    out['shared_games'] = len(set(stats.game_id))
    out['notes'] = (f'Regular season through week {week}; {out.shared_games.iloc[0]} shared games; '
                    f'{len(stat_games ^ snap_games)} games excluded for source mismatch. '
                    'APY in millions; latest contract signed by season year; historical approximation. '
                    'Offensive snaps only; raw attributed EPA; normalized cost uses heuristic position shrinkage.')
    out['updated_at'] = datetime.datetime.now(datetime.UTC).isoformat()
    out['epai_lower'] = None
    out['epai_upper'] = None
    return out.reset_index(drop=True), audit.reset_index(drop=True), unmatched
