"""Season-to-date tables using the same coverage for both comparison years."""

import pandas as pd
from src.domain import OFFENSIVE_POSITIONS


def position_summary(frame):
    offense = frame.loc[frame['position'].isin(OFFENSIVE_POSITIONS) & frame['snaps'].gt(0)].copy()
    result = offense.groupby('position').agg(
        players=('gsis_id', 'nunique'), snaps=('snaps', 'sum'),
        player_attributed_epa=('total_epa', 'sum'), annual_apy_m=('yearly_cap_hit', lambda x:x.sum(min_count=1)),
        known_apy_players=('yearly_cap_hit','count'),
    )
    result['epa_per_snap'] = result['player_attributed_epa'] / result['snaps']
    return result


def matched_position_comparison(current, previous):
    """Caller must supply both seasons cut to the same NFL week."""
    now, before = position_summary(current), position_summary(previous)
    comparison = now.join(before, lsuffix='_current', rsuffix='_previous')
    comparison['epa_per_snap_change'] = comparison['epa_per_snap_current'] - comparison['epa_per_snap_previous']
    return comparison.reset_index()


def top_value_players(frame, min_snaps=100, limit=10, financial_field='contract_apy_m'):
    """Within-position/mechanism descriptive rankings with an explicit cost basis."""
    data = frame.copy()
    if 'contract_apy_m' not in data:
        data['contract_apy_m'] = data.yearly_cap_hit
    if 'contract_type' not in data:
        data['contract_type'] = 'Unknown'
    if financial_field not in ['contract_apy_m', 'season_cap_charge_m', 'season_cash_m']:
        raise ValueError('Unknown financial definition')
    if financial_field not in data:
        data[financial_field] = float('nan')
    eligible = data.loc[data.position.isin(OFFENSIVE_POSITIONS) & data.snaps.ge(min_snaps)
                        & data.total_epa.gt(0) & data[financial_field].gt(0)].copy()
    eligible['financial_measure'] = financial_field
    eligible['cost_per_epa_dollars'] = eligible[financial_field] / eligible.total_epa * 1_000_000
    eligible['within_position_mechanism_rank'] = eligible.groupby(['position','contract_type']).cost_per_epa_dollars.rank(method='min')
    eligible = eligible.loc[eligible.within_position_mechanism_rank.le(limit)].sort_values(['position','contract_type','cost_per_epa_dollars'])
    fields = ['player_name','team','position','contract_type','is_rookie_deal','snaps','total_epa',
              'contract_apy_m', financial_field, 'cost_per_epa_dollars','within_position_mechanism_rank']
    fields += [c for c in ['season_financial_team','annual_financial_status','pass_opportunities','carries','targets','games_played'] if c in eligible]
    return eligible[list(dict.fromkeys(fields))]


def analysis_outputs(history, current, previous, season, week, min_snaps=100):
    """Render the shared research report from explicit validated season frames."""
    from src.valuation import score_completed_seasons
    if current.empty or previous.empty or not current.season.eq(season).all() or not previous.season.eq(season-1).all():
        raise ValueError('Matched comparisons require explicit current and previous seasons')
    if history.empty or not history.season.lt(season).all():
        raise ValueError('Only validated completed prior seasons may enter annual research')
    scored, diagnostics = score_completed_seasons(history, incomplete_season=season)
    from src.contracts import financial_coverage
    coverage = financial_coverage(current) if 'annual_financial_status' in current else pd.DataFrame()
    outputs = dict(financial_coverage=coverage, summary=position_summary(current).reset_index(),
                   comparison=matched_position_comparison(current, previous),
                   candidates=top_value_players(current, min_snaps=min_snaps),
                   diagnostics=diagnostics, scored=scored,
                   temporal_predictions=pd.DataFrame(scored.attrs.get('temporal_predictions', [])))
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

## Financial source coverage

{table(coverage)}
## Current position totals

{table(outputs['summary'])}
## Matched coverage: {season} versus {season-1}, through week {week}

{table(outputs['comparison'])}
## Positive-EPA APY efficiency: minimum {min_snaps} snaps

Ranks are calculated within position and recorded contract mechanism. Contribution volume and observed opportunities accompany each ratio. Unknown costs are unranked. These are descriptive rankings, not definitive contract valuations.

{table(outputs['candidates'])}
## Completed-season APY research model

Only validated completed prior seasons enter annual-volume training and scoring. Nested validation groups all seasons of each player together and tunes inside each outer training fold. Every historical row uses a player-held-out prediction, including rookies with later veteran seasons. Saved fold IDs, training signatures and dependency versions make the calculation reproducible. The inverse log target estimates a transformed geometric center; its APY difference is an association, not economic surplus. Chronological tests use completed test-year production and are not future-offer forecasts. Bounds use training-only cap shares in both validation and scoring.

{table(diagnostics)}
'''
    return text, outputs
