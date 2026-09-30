"""Refresh research artifacts and compare the latest season at matched weeks."""

import argparse
from pathlib import Path
import re
import pandas as pd
from dotenv import load_dotenv
from etl.sources import configure_sources, current_season, load_reference_tables, load_season_tables
from src.analysis import build_roster_roi
from src.reporting import position_summary, matched_position_comparison, top_value_players
from src.valuation import score_completed_seasons


def table_text(frame):
    return '```text\n' + frame.to_string(index=False, float_format=lambda x: f'{x:,.3f}') + '\n```\n'


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--input', default='artifacts/roster_roi_combined.csv')
    parser.add_argument('--output', default='artifacts/latest_analysis.md')
    args = parser.parse_args()
    load_dotenv()
    frame = pd.read_csv(args.input)
    latest = int(frame['season'].max())
    now = frame.loc[frame['season'] == latest]
    match = re.search(r'through week (\d+)', str(now['notes'].iloc[0]))
    if not match:
        raise ValueError('Input lacks production coverage metadata; rerun ETL first')
    week = int(match.group(1))
    configure_sources()
    references = load_reference_tables()
    previous_tables = load_season_tables(latest - 1)
    for name in ['player_stats', 'snap_counts']:
        previous_tables[name] = previous_tables[name].loc[previous_tables[name]['week'] <= week]
    before, _, _ = build_roster_roi(latest - 1, **references, **previous_tables)
    scored, diagnostics = score_completed_seasons(frame, incomplete_season=current_season())
    scored.to_csv('artifacts/roster_roi_scored.csv', index=False)
    diagnostics.to_csv('artifacts/model_diagnostics.csv', index=False)
    comparison = matched_position_comparison(now, before)
    comparison.to_csv('artifacts/matched_week_comparison.csv', index=False)
    text = f'''# NFL roster ROI analysis: {latest}, regular season through week {week}

Generated at {pd.Timestamp.now(tz='UTC').isoformat()}.

{now['notes'].iloc[0]}

## Interpretation

- Production is season-to-date, not a completed season or forecast.
- Annual APY divided by partial-season EPA rises mechanically when fewer games are recorded. Compare within the same season and position.
- All financial values are millions of dollars unless explicitly labeled as dollars. APY is not the season's actual cap charge.
- Team and position totals sum player-attributed EPA; passing and receiving EPA overlap and must not be interpreted as net offensive team EPA.
- Contract selection is a historical approximation using signing year. Same-year transactions and extension effective dates are not fully resolved.
- Rookie labels are estimates, not CBA legal classifications.

## Current position totals

'''
    text += table_text(position_summary(now).reset_index())
    text += f'\n## Matched coverage: {latest} versus {latest - 1}, through week {week}\n\n'
    text += table_text(comparison)
    text += '\n## Positive-EPA value candidates: minimum 100 snaps\n\nThese are descriptive rankings, not definitive contract valuations.\n\n'
    text += table_text(top_value_players(now))
    text += '\n## Completed-season APY research model\n\nThe incomplete season is excluded from both model training and annual-volume scoring. Validation uses nested cross-validation grouped by player. These metrics are out-of-fold; historical contract approximation still limits interpretation.\n\n'
    text += table_text(diagnostics)
    text += '\n## Artifacts\n\n- `roster_roi_scored.csv`: completed-season expected APY and surplus estimates.\n- `model_diagnostics.csv`: nested grouped validation.\n- `matched_week_comparison.csv`: prior/current production at matched week coverage.\n'
    Path(args.output).write_text(text)
    print(f'Wrote {args.output}; scored {len(scored)} completed player-seasons')


if __name__ == '__main__':
    main()
