"""Offline Phase 1-3 validation on pinned, hash-verified canonical sources.

Run the read-only download scripts first. This command never writes Supabase,
changes schedules, or treats an incomplete season as completed production.
"""
import argparse
from hashlib import sha256
import json
from pathlib import Path

import numpy as np
import pandas as pd

from src.analysis import build_roster_roi
from src.contracts import contract_events, financial_coverage
from src.contribution import (fit_rates, estimate_rates, validate_rates, replacement_scenarios,
                              game_block_rank_stability)
from src.forecasting import forecast_returning_players
from src.opportunities import game_production, production_sensitivity, validate_game_aggregation
from src.pricing import build_price_rows, validate_event_prices
from src.reporting import top_value_players
from src.valuation import score_completed_seasons, dependency_versions
from src.domain import OFFENSIVE_POSITIONS


TABLES = ['players', 'contracts', 'player_stats', 'snap_counts', 'rosters']


def _read_canonical_tables(inputs, snapshot):
    tables = {}
    for name in TABLES:
        source = inputs/f"{snapshot['season']}_{name}.parquet"
        if sha256(source.read_bytes()).hexdigest() != snapshot['manifest']['artifacts'][name]['sha256']:
            raise ValueError(f"{snapshot['season']} {name}: canonical input hash mismatch")
        tables[name] = pd.read_parquet(source)
    return tables


def _verified_history(inputs, incomplete_season):
    metrics, games, manifests = [], [], []
    for path in sorted(inputs.glob('*_manifest.json')):
        snapshot = json.loads(path.read_text())
        if snapshot['season'] == incomplete_season:
            continue
        if not snapshot['complete_season'] or not snapshot['coverage']['complete_season']:
            raise ValueError('Research inputs must be canonical completed seasons')
        year = snapshot['season']
        tables = _read_canonical_tables(inputs, snapshot)
        pbp_path = inputs/f'{year}_pbp.parquet'
        provenance = json.loads((inputs/f'{year}_pbp_provenance.json').read_text())
        if sha256(pbp_path.read_bytes()).hexdigest() != provenance['subset_sha256']:
            raise ValueError('PBP subset hash mismatch')
        pbp = pd.read_parquet(pbp_path)
        metrics.append(build_roster_roi(year, **tables, snapshot_at=snapshot['manifest']['fetched_at'])[0])
        games.append(game_production(year, **{k:v for k,v in tables.items() if k!='contracts'}, play_by_play=pbp))
        validate_game_aggregation(metrics[-1], games[-1])
        if set(games[-1].game_id) != set(snapshot['coverage']['shared_games']):
            raise ValueError('Regular-season game coverage differs from the canonical manifest')
        manifests.append(dict(season=year, run_id=snapshot['run_id'], source_artifacts=snapshot['manifest']['artifacts'], pbp=provenance))
        print(f'{year}: definitions and PBP exposures reconciled', flush=True)
    if not metrics:
        raise ValueError('No pinned canonical historical manifests found; run the downloader first')
    return pd.concat(metrics,ignore_index=True), pd.concat(games,ignore_index=True), manifests


def _current(inputs, season):
    snapshot = json.loads((inputs/f'{season}_manifest.json').read_text())
    provenance = dict(current_run_id=snapshot['run_id'],current_manifest=snapshot['manifest'],current_coverage=snapshot['coverage'])
    tables = _read_canonical_tables(inputs, snapshot)
    current = build_roster_roi(season, **tables, snapshot_at=provenance['current_manifest']['fetched_at'])[0]
    pbp_path = inputs/f'{season}_pbp.parquet'
    pbp_meta = json.loads((inputs/f'{season}_pbp_provenance.json').read_text())
    if sha256(pbp_path.read_bytes()).hexdigest() != pbp_meta['subset_sha256']:
        raise ValueError('Current PBP subset hash mismatch')
    games = game_production(season, **{k:v for k,v in tables.items() if k!='contracts'}, play_by_play=pd.read_parquet(pbp_path))
    validate_game_aggregation(current, games)
    if set(games.game_id) != set(provenance['current_coverage']['shared_games']):
        raise ValueError('Current game coverage differs from audit snapshot')
    return current, games, tables, provenance


def _write_frame(output, name, data):
    data.to_csv(output/f'{name}.csv', index=False)


def run_research(inputs, output, *, season=2026, draws=200):
    output.mkdir(parents=True, exist_ok=True)
    history, games, manifests = _verified_history(inputs, season)
    if history.season.max() >= season:
        raise ValueError('The incomplete season cannot enter historical research')
    current, current_games, tables, provenance = _current(inputs, season)
    events = contract_events(tables['contracts'], tables['players'], provenance['current_manifest']['fetched_at'])
    outputs = dict(historical_metrics=history, current_metrics=current, historical_games=games,
                   current_games=current_games, financial_coverage=financial_coverage(pd.concat([history,current])),
                   contract_events=events, apy_efficiency=top_value_players(current),
                   cap_efficiency=top_value_players(current,financial_field='season_cap_charge_m'),
                   cash_efficiency=top_value_players(current,financial_field='season_cash_m'),
                   sensitivity=production_sensitivity(current,current_games))
    for name, data in outputs.items():
        _write_frame(output,name,data)
    print('Phase 1: nested player-held-out APY validation',flush=True)
    scored, diagnostics = score_completed_seasons(history, incomplete_season=season)
    outputs.update(apy_associations=scored, apy_diagnostics=diagnostics,
                   chronological_apy_predictions=pd.DataFrame(scored.attrs['temporal_predictions']))
    for name in ['apy_associations','apy_diagnostics','chronological_apy_predictions']:
        _write_frame(output,name,outputs[name])
    print('Phase 2: chronological component-rate validation and replacement sensitivity',flush=True)
    model = fit_rates(games)
    rates = estimate_rates(model,current_games)
    observed_history = estimate_rates(model,games)
    # Historical rates used for comparator definitions are descriptive only;
    # they never become pre-event pricing or preseason forecast features.
    rate_diagnostics, rate_predictions, rate_selection = validate_rates(games)
    replacements, above_replacement = replacement_scenarios(rates, observed_history, events,before_season=season)
    eligible = current.loc[current.position.isin(OFFENSIVE_POSITIONS)&current.snaps.ge(100)&current.contract_apy_m.gt(0)]
    stability = game_block_rank_stability(current_games,eligible,draws=draws)
    outputs.update(rate_priors=model.priors, current_component_rates=rates, rate_diagnostics=rate_diagnostics,
                   rate_predictions=rate_predictions, rate_selection=rate_selection, replacement_cohorts=replacements,
                   above_replacement=above_replacement, rank_stability=stability)
    for name in ['rate_priors','current_component_rates','rate_diagnostics','rate_predictions','rate_selection','replacement_cohorts','above_replacement','rank_stability']:
        _write_frame(output,name,outputs[name])
    print('Phase 3: later contract events and future production/availability',flush=True)
    price_rows = build_price_rows(events,history,incomplete_season=season)
    price_predictions, price_diagnostics = validate_event_prices(price_rows)
    forecasts, forecast_diagnostics = forecast_returning_players(games,history,forecast_season=season)
    outputs.update(price_rows=price_rows,price_predictions=price_predictions,price_diagnostics=price_diagnostics,
                   forecasts=forecasts,forecast_diagnostics=forecast_diagnostics)
    for name in ['price_rows','price_predictions','price_diagnostics','forecasts','forecast_diagnostics']:
        _write_frame(output,name,outputs[name])
    from etl.cloud_store import records
    manifest = dict(methodology_version=2, dependencies=dependency_versions(), current_season=season,
                    current_run_id=provenance['current_run_id'], history=manifests,
                    current_input_sha256={name:sha256((inputs/f'{season}_{name}.parquet').read_bytes()).hexdigest() for name in TABLES},
                    random_seed=20261001, bootstrap_draws=draws,
                    evaluation_tasks=['pooled unseen-player salary association','chronological completed-production salary association',
                                      'future observed component rates','chronological player-purged contract events','preseason returning-player production'],
                    outer_holdout_prices_from=2025, price_feature_lag_seasons=2,
                    production_status='research candidates; no calibrated economic surplus',
                    design_status='retrospective design refined during this audit; outer targets excluded from numerical tuning; independent prospective validation still required',
                    outputs={name:dict(rows=len(data),sha256=sha256((output/f'{name}.csv').read_bytes()).hexdigest()) for name,data in outputs.items()})
    (output/'manifest.json').write_text(json.dumps(manifest,indent=2,default=str,allow_nan=False))
    summary = dict(current_rows=len(current), historical_rows=len(history), historical_games=games.game_id.nunique(),
                   current_games=current_games.game_id.nunique(), event_status_counts=events.event_status.value_counts().to_dict(),
                   price_row_status_counts=price_rows.price_row_status.value_counts().to_dict(),
                   raw_epa_identity_max_error=float((games.total_epa-games[['passing_epa','rushing_epa','receiving_epa']].sum(axis=1)).abs().max()),
                   pbp_epa_max_reconciliation_error=float(games[[c for c in games if c.endswith('_epa_reconciliation_error')]].abs().max().max()),
                   missing_epa_play_counts={c:int(games[c].sum()) for c in games if c.endswith('_missing_epa_plays')},
                   latest_observed_rate_coverage=records(rate_diagnostics.loc[rate_diagnostics.model.eq('pooled') & rate_diagnostics.test_season.eq(season-1)]),
                   economic_surplus_status='unknown: validated replacement forecasts, verified equivalent contract terms, full obligations and contribution price are required')
    (output/'summary.json').write_text(json.dumps(summary,indent=2,allow_nan=False))
    report = _research_report(summary,outputs,season)
    (output/'validation_report.md').write_text(report)
    print(f'All phases saved to {output}; no cloud state changed',flush=True)
    return outputs


def _research_report(summary, outputs, season):
    def table(data):
        return '```text\n'+data.to_string(index=False,float_format=lambda v:f'{v:.3f}')+'\n```\n'
    latest_rates = outputs['rate_diagnostics'].loc[outputs['rate_diagnostics'].test_season.eq(season-1)]
    price = outputs['price_diagnostics'].loc[outputs['price_diagnostics'].market.eq('All')]
    return f'''# Phase 1–3 methodology validation

Canonical historical shared games: {summary['historical_games']}; current shared games: {summary['current_games']}.
PBP/weekly maximum EPA reconciliation error: {summary['pbp_epa_max_reconciliation_error']:.6f} (historical rounding tolerance 0.002).
Source manifests, dependency versions, row-level folds and hashes are saved alongside this report.

## Definitions and salary associations

Unknown contracts remain in coverage. APY, annual cap and annual cash are distinct.
Pooled historical rows hold out each player's entire history, including historical rookies.
Chronological salary tests use completed test-year production and are not forecasts.
Inverse log predictions estimate a transformed geometric center, not arithmetic mean APY.
The saved heuristic upper bound uses outer-training cap shares in every validation/scoring path.

{table(outputs['financial_coverage'])}
{table(outputs['apy_diagnostics'])}
## Contribution and reliability

EPA exposures follow the exact PBP filters, including two-point attempts. Missing-EPA plays are counted and excluded from rate exposures.
Raw EPA is preserved. Team/opponent effects are descriptive associations. Rate pooling and context are selected using earlier transitions only.
Conditional normal bands fix estimated hyperparameters. Earlier transitions estimate future innovation variance. Coverage below measures later observed rates conditional on their realized exposure, not latent talent coverage. The design was refined during this audit; chronological target exclusions do not make these results independent of model-development choices.
Week-block bootstrap ranks are sensitivity results; three observed weeks provide limited independent information.
Replacement comparators use documented one-year acquisitions. SFA and expanded SFA/UFA cohorts are sensitivity proxies; feasibility for a particular roster is not established.

{table(latest_rates)}
{table(outputs['replacement_cohorts'])}
## Negotiated price and roster decisions

Year-only signing timestamps require completed production two seasons before signing. Old labels are retrospective histories, not contemporaneous snapshots.
Ambiguous same-player/year events, inconsistent APY terms and missing histories are excluded explicitly.
All alternatives use identical test events and player-purged training. Regularization uses earlier chronological folds; later tests do not select a winner.
Gamma/direct Ridge target a conditional mean; median quantile minimizes pinball loss; log Ridge has a different estimand.
No current three-week production enters an annual contract-pricing model.

{table(price)}
Preseason candidate forecasts separate offensive participation, directly supervised total opportunities and component rates, retaining future zero outcomes. Opportunities per active game are a ratio of forecasts. Rate-times-volume is a marginal-product proxy; the direct EPA benchmark allows rate/volume dependence.
Forecast errors are measured against the previous-season baseline. Research candidates are not automatically production-approved.
APY pricing discounts require verified equivalent terms. Economic surplus remains unknown without validated replacement forecasts, an explicit contribution price, full cap/cash/guarantee schedules and scenario-specific exit costs.
No global MAE or price prediction band is presented as uncertainty about a player's latent surplus.

{table(outputs['forecast_diagnostics'])}
'''


def main(argv=None):
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--inputs',default='artifacts/methodology-v2/inputs')
    parser.add_argument('--output',default='artifacts/methodology-v2/results')
    parser.add_argument('--season',type=int,default=2026)
    parser.add_argument('--draws',type=int,default=200)
    args=parser.parse_args(argv)
    run_research(Path(args.inputs),Path(args.output),season=args.season,draws=args.draws)


if __name__=='__main__':
    main()
