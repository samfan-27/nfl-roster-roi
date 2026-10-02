"""Explicit roster-decision scenarios, separated from observed APY efficiency.

Neither lambda (dollars per attributed contribution) nor complete exit/cap
obligations is identified by public salary regressions. Require these inputs
rather than mislabeling a fitted salary residual as economic roster surplus.
"""
import numpy as np
import pandas as pd


COST_COLUMNS = ['player_cap_m', 'replacement_cap_m', 'player_cash_m', 'replacement_cash_m',
                'player_exit_cap_m', 'replacement_exit_cap_m', 'player_exit_cash_m', 'replacement_exit_cash_m', 'player_guaranteed_cash_m', 'replacement_guaranteed_cash_m']


def incremental_costs(obligations, *, horizon, discount_rate=0.0):
    """Complete year/team-specific obligations; guarantees are disclosures.

    Cap and cash schedules already include applicable guaranteed amounts; adding
    guarantees again would double-count them. Exit costs are scenario-specific
    additional obligations, documented by the caller, not inferred from APY.
    """
    if not horizon or len(set(horizon)) != len(horizon) or discount_rate < 0:
        raise ValueError('A unique year horizon and nonnegative discount rate are required')
    required = {'gsis_id', 'scenario', 'team', 'season', 'obligation_source', *COST_COLUMNS}
    if not required.issubset(obligations):
        raise ValueError('Verified cap, cash, guarantee and exit schedules are required')
    rows = []
    first = min(horizon)
    for (pid, scenario, team), group in obligations.groupby(['gsis_id', 'scenario', 'team']):
        g = group.loc[group.season.isin(horizon)].copy()
        complete = (set(g.season) == set(horizon) and not g.season.duplicated().any()
                    and g[COST_COLUMNS].notna().all().all() and g.obligation_source.notna().all())
        if g[COST_COLUMNS].lt(0).any().any():
            raise ValueError('Costs must be nonnegative; encode releases in the scenario schedule')
        weights = (1+discount_rate)**(-(g.season-first))
        rows.append(dict(gsis_id=pid, scenario=scenario, team=team, horizon='|'.join(map(str,sorted(horizon))),
                         cost_status='complete_scenario' if complete else 'missing_or_ambiguous_obligations',
                         incremental_cap_m=float(((g.player_cap_m-g.replacement_cap_m)*weights).sum()) if complete else np.nan,
                         incremental_cash_m=float(((g.player_cash_m-g.replacement_cash_m)*weights).sum()) if complete else np.nan,
                         incremental_exit_cap_m=float(((g.player_exit_cap_m-g.replacement_exit_cap_m)*weights).sum()) if complete else np.nan,
                         incremental_exit_cash_m=float(((g.player_exit_cash_m-g.replacement_exit_cash_m)*weights).sum()) if complete else np.nan,
                         incremental_guaranteed_cash_m=float(((g.player_guaranteed_cash_m-g.replacement_guaranteed_cash_m)*weights).sum()) if complete else np.nan))
    return pd.DataFrame(rows)


def roster_surplus_scenarios(contributions, costs, *, lambda_m_per_epa, cost_basis='cap'):
    """Sensitivity calculations on supplied forecasts; no latent lower quantile.

    Contributions require compatible per-component replacement forecasts and a
    chosen horizon. A caller-provided lambda is a scenario assumption, never a
    causal salary coefficient. Cap and cash remain separate decision objectives.
    """
    if not np.isfinite(lambda_m_per_epa) or lambda_m_per_epa < 0 or cost_basis not in ['cap', 'cash']:
        raise ValueError('An explicit nonnegative contribution price and cap/cash basis are required')
    keys = ['gsis_id', 'scenario', 'team', 'horizon']
    required = {*keys, 'forecast_above_replacement_epa', 'forecast_validated', 'contribution_definition'}
    if not required.issubset(contributions):
        raise ValueError('Explicit horizon-compatible replacement forecast and validation status are required')
    if contributions.duplicated(keys).any() or costs.duplicated(keys).any():
        raise ValueError('Duplicate decision scenarios')
    out = contributions.merge(costs, on=keys, how='left', validate='one_to_one')
    ready = out.cost_status.eq('complete_scenario') & out.forecast_validated.eq(True) & out.forecast_above_replacement_epa.notna()
    out['scenario_surplus_m'] = (lambda_m_per_epa*out.forecast_above_replacement_epa
                                - out['incremental_' + cost_basis + '_m'] - out['incremental_exit_' + cost_basis + '_m']).where(ready)
    out['lambda_m_per_epa'] = lambda_m_per_epa
    out['cost_basis'] = cost_basis
    out['decision_status'] = np.where(ready, 'explicit_assumption_scenario', 'insufficient_costs_or_unvalidated_forecast')
    out['latent_surplus_lower_quantile_m'] = np.nan
    return out
