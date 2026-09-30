"""Season-to-date tables using the same coverage for both comparison years."""

import pandas as pd
from src.domain import OFFENSIVE_POSITIONS


def position_summary(frame):
    offense = frame.loc[frame['position'].isin(OFFENSIVE_POSITIONS) & frame['snaps'].gt(0)].copy()
    result = offense.groupby('position').agg(
        players=('gsis_id', 'nunique'), snaps=('snaps', 'sum'),
        player_attributed_epa=('total_epa', 'sum'), annual_apy_m=('yearly_cap_hit', 'sum'),
    )
    result['epa_per_snap'] = result['player_attributed_epa'] / result['snaps']
    return result


def matched_position_comparison(current, previous):
    """Caller must supply both seasons cut to the same NFL week."""
    now, before = position_summary(current), position_summary(previous)
    comparison = now.join(before, lsuffix='_current', rsuffix='_previous')
    comparison['epa_per_snap_change'] = comparison['epa_per_snap_current'] - comparison['epa_per_snap_previous']
    return comparison.reset_index()


def top_value_players(frame, min_snaps=100, limit=10):
    eligible = frame.loc[
        frame['position'].isin(OFFENSIVE_POSITIONS) & frame['snaps'].ge(min_snaps)
        & frame['total_epa'].gt(0) & frame['yearly_cap_hit'].gt(0)
        & frame['cost_per_epa'].notna()
    ].copy()
    eligible['cost_per_epa_dollars'] = eligible['cost_per_epa'] * 1_000_000
    return eligible.sort_values('cost_per_epa').head(limit)[[
        'player_name', 'team', 'position', 'is_rookie_deal', 'snaps',
        'total_epa', 'yearly_cap_hit', 'cost_per_epa_dollars',
    ]]
