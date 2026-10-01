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


def analysis_outputs(history, current, previous, season, week, min_snaps=100):
    """Render the shared research report from explicit validated season frames."""
    from src.valuation import score_completed_seasons
    if current.empty or previous.empty or not current.season.eq(season).all() or not previous.season.eq(season-1).all():
        raise ValueError('Matched comparisons require explicit current and previous seasons')
    if history.empty or not history.season.lt(season).all():
        raise ValueError('Only validated completed prior seasons may enter annual research')
    scored, diagnostics = score_completed_seasons(history, incomplete_season=season)
    outputs = dict(summary=position_summary(current).reset_index(),
                   comparison=matched_position_comparison(current, previous),
                   candidates=top_value_players(current, min_snaps=min_snaps),
                   diagnostics=diagnostics, scored=scored)
    def table(frame):
        return '```text\n' + frame.to_string(index=False, float_format=lambda x:f'{x:,.3f}') + '\n```\n'
    text = f'''# NFL roster ROI analysis: {season}, regular season through week {week}

Generated at {pd.Timestamp.now(tz='UTC').isoformat()}.

{current['notes'].iloc[0]}

## Interpretation

- Production is season-to-date, not a full-season forecast.
- APY is in millions and is not the actual annual cap charge. Annual APY divided by partial-season EPA is sensitive to the number of games; compare within the same season and position.
- Player-attributed passing and receiving EPA overlap; totals are not net team offensive EPA.
- Contract selection uses signing year and approximates historical timing. Rookie labels are estimates, not CBA legal classifications.
- Comparisons use the same fully covered NFL week. Bye weeks can produce different game counts between seasons.

## Current position totals

{table(outputs['summary'])}
## Matched coverage: {season} versus {season-1}, through week {week}

{table(outputs['comparison'])}
## Positive-EPA value candidates: minimum {min_snaps} snaps

These are descriptive rankings, not definitive contract valuations.

{table(outputs['candidates'])}
## Completed-season APY research model

Only validated completed prior seasons enter annual-volume training and scoring. Nested validation groups all seasons of each player together and tunes inside each outer training fold. Historical fitted surplus remains research output, not a held-out valuation for every row or a forecast of future offers.

{table(diagnostics)}
'''
    return text, outputs
