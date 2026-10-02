"""Game-cluster empirical Bayes rates and chronological production checks.

This is an attributed-production proxy. Conditional normal reference bands
hold estimated hyperparameters fixed; they are not validated latent-talent
confidence intervals. Context controls describe associations, not attribution.
"""
from dataclasses import dataclass

import numpy as np
import pandas as pd
from scipy.optimize import minimize_scalar
from sklearn.compose import ColumnTransformer
from sklearn.linear_model import Ridge
from sklearn.pipeline import make_pipeline
from sklearn.preprocessing import OneHotEncoder

from src.opportunities import component_rows

KEYS = ['season', 'gsis_id', 'position', 'component']
REFERENCE_Z = 1.2815515655446004  # Central 80% normal reference band.


@dataclass
class RateModel:
    priors: pd.DataFrame
    contexts: dict
    variance_scale: float = 1.0


def _context_model(data):
    encoder = ColumnTransformer([('context', OneHotEncoder(handle_unknown='ignore', sparse_output=False), ['team', 'opponent_team'])])
    model = make_pipeline(encoder, Ridge(alpha=100.0))
    model.fit(data[['team', 'opponent_team']].fillna('Unknown'), data.epa / data.opportunities,
              ridge__sample_weight=data.opportunities)
    return model


def _observations(rows, contexts):
    data = rows.loc[rows.opportunities.gt(0)].copy()
    data['context_rate'] = 0.0
    for key, model in contexts.items():
        mask = data.position.eq(key[0]) & data.component.eq(key[1])
        if mask.any():
            data.loc[mask, 'context_rate'] = model.predict(data.loc[mask, ['team', 'opponent_team']].fillna('Unknown'))
    data['adjusted_epa'] = data.epa - data.opportunities * data.context_rate
    aggregates = data.groupby(KEYS).agg(epa=('epa', 'sum'), adjusted_epa=('adjusted_epa', 'sum'),
                                        opportunities=('opportunities', 'sum'), games=('game_id', 'nunique'))
    aggregates['observed_rate'] = aggregates.epa / aggregates.opportunities
    aggregates['adjusted_rate'] = aggregates.adjusted_epa / aggregates.opportunities
    aggregates['average_context_rate'] = (aggregates.epa - aggregates.adjusted_epa) / aggregates.opportunities
    data = data.join(aggregates.adjusted_rate, on=KEYS)
    data['cluster_residual_squared'] = (data.adjusted_epa - data.opportunities * data.adjusted_rate) ** 2
    aggregates['observation_variance'] = data.groupby(KEYS).cluster_residual_squared.sum() / aggregates.opportunities**2
    m = aggregates.games
    aggregates['observation_variance'] *= m / (m - 1).replace(0, np.nan)
    return aggregates.reset_index()


def _normal_prior(rates, variances):
    """Profile marginal normal likelihood with heterogeneous observation noise.

    Subtracting average noise from unweighted sample variance can collapse the
    prior when a few one-opportunity observations have very large variances.
    This likelihood weights each observation by its own sampling uncertainty.
    Repeated players/context dependence still limit hyperparameter inference.
    """
    r, v = np.asarray(rates,float), np.maximum(np.asarray(variances,float),1e-8)
    def objective(s2):
        weights = 1/(s2+v)
        mu = np.sum(weights*r)/weights.sum()
        loss = np.sum(np.log(s2+v)+(r-mu)**2/(s2+v))
        return float(loss), float(mu)
    upper = max(float(np.var(r)+np.mean(v)),1e-6)*10
    fit = minimize_scalar(lambda z:objective(np.exp(z))[0], bounds=(np.log(1e-10),np.log(upper)),method='bounded')
    s2 = float(np.exp(fit.x))
    if objective(0)[0] <= objective(s2)[0]:
        s2 = 0.0
    return objective(s2)[1], s2


def fit_rates(games, *, context=False, variance_scale=1.0):
    if variance_scale < 0:
        raise ValueError('Prior variance scale must be nonnegative')
    rows = component_rows(games)
    contexts = {}
    if context:
        for key, group in rows.loc[rows.opportunities.gt(0)].groupby(['position', 'component']):
            if group.game_id.nunique() >= 10:
                contexts[key] = _context_model(group)
    obs = _observations(rows, contexts)
    priors = []
    for (pos, component), group in obs.groupby(['position', 'component']):
        if len(group) < 3:
            continue
        valid = group.loc[group.games.ge(2)]
        # Fallback for a one-game observation uses TRAINING cluster variability.
        noise = (valid.observation_variance * valid.opportunities).mean()
        if not np.isfinite(noise):
            continue
        noise = max(float(noise), 1e-8)
        v = group.observation_variance.fillna(noise / group.opportunities)
        mu, s2 = _normal_prior(group.adjusted_rate, v)
        priors.append(dict(position=pos, component=component, prior_mean=mu,
                           prior_variance=s2, effective_noise=noise, training_rows=len(group),
                           training_players=group.gsis_id.nunique(), training_max_season=int(games.season.max()),
                           equivalent_opportunities=noise/s2 if s2 > 0 else np.inf,
                           context_enabled=context))
    return RateModel(pd.DataFrame(priors), contexts, variance_scale)


def estimate_rates(model, games):
    obs = _observations(component_rows(games), model.contexts)
    if model.priors.empty:
        return pd.DataFrame()
    out = obs.merge(model.priors, on=['position', 'component'], how='inner', validate='many_to_one')
    out['game_cluster_variance_raw'] = out.observation_variance
    out['observation_variance'] = out.observation_variance.fillna(out.effective_noise / out.opportunities).clip(lower=1e-8)
    s2 = out.prior_variance * model.variance_scale
    v = out.observation_variance
    out['data_weight'] = s2 / (s2 + v)
    out['pooled_rate'] = out.average_context_rate + out.data_weight * out.adjusted_rate + (1 - out.data_weight) * out.prior_mean
    out['conditional_posterior_variance'] = s2 * v / (s2 + v)
    error = REFERENCE_Z * np.sqrt(out.conditional_posterior_variance)
    out['conditional_rate_lower'] = out.pooled_rate - error
    out['conditional_rate_upper'] = out.pooled_rate + error
    out['pooled_attributed_epa'] = out.pooled_rate * out.opportunities
    out['uncertainty_scope'] = '80% conditional normal reference; estimated hyperparameters fixed'
    return out


def _future_rate_pairs(train_games, test_games, *, context=False, variance_scale=1.0):
    year = int(test_games.season.min())
    if train_games.season.max() >= year:
        raise ValueError('Future-rate validation must train strictly before the test season')
    model = fit_rates(train_games, context=context, variance_scale=variance_scale)
    prior_season = train_games.loc[train_games.season.eq(year-1)]
    estimates = estimate_rates(model, prior_season)
    if estimates.empty:
        return pd.DataFrame()
    future = _observations(component_rows(test_games), {})
    paired = estimates.merge(future[['gsis_id', 'position', 'component', 'observed_rate', 'opportunities']],
                             on=['gsis_id', 'position', 'component'], suffixes=('', '_future'), validate='one_to_one')
    # Predict a neutral-context future rate. Current-team effects do not become
    # a known future team's effect; past team/opponent assignments may change.
    paired['forecast_rate'] = paired.pooled_rate - paired.average_context_rate
    if context:
        # Restore training-wide context intercept using the training exposure mix.
        rows = component_rows(train_games)
        for key, fit in model.contexts.items():
            mask = rows.position.eq(key[0]) & rows.component.eq(key[1]) & rows.opportunities.gt(0)
            mean_context = np.average(fit.predict(rows.loc[mask, ['team','opponent_team']].fillna('Unknown')), weights=rows.loc[mask, 'opportunities'])
            paired.loc[paired.position.eq(key[0]) & paired.component.eq(key[1]), 'forecast_rate'] += mean_context
    paired['test_season'] = year
    predictive_var = paired.conditional_posterior_variance + paired.effective_noise / paired.opportunities_future
    error = REFERENCE_Z * np.sqrt(predictive_var)
    paired['future_rate_lower'] = paired.forecast_rate - error
    paired['future_rate_upper'] = paired.forecast_rate + error
    paired['future_observed_rate_covered'] = paired.observed_rate_future.between(paired.future_rate_lower, paired.future_rate_upper)
    paired['future_reference_variance'] = predictive_var
    return paired


def _rate_choices(earlier, context, scale):
    measured = pd.concat(earlier).assign(squared_error=lambda x:(x.forecast_rate-x.observed_rate_future)**2)
    losses = measured.groupby(['position','component']).squared_error.mean()
    return [dict(position=key[0],component=key[1],context=context,scale=scale,loss=loss) for key,loss in losses.items()]


def _calibrate_rate_band(pair, earlier):
    """Earlier-transition innovation variance, separate from latent uncertainty."""
    if earlier:
        calibration = pd.concat(earlier)
        if calibration.test_season.max() >= pair.test_season.min():
            raise ValueError('Interval calibration must precede the outer test year')
        residual = (calibration.forecast_rate-calibration.observed_rate_future)**2-calibration.future_reference_variance
        innovation = max(0.0,float(residual.mean()))
    else:
        innovation = 0.0
    pair = pair.copy()
    pair['future_innovation_variance'] = innovation
    pair['future_observed_rate_covered_before_drift'] = pair.future_observed_rate_covered
    error = REFERENCE_Z*np.sqrt(pair.future_reference_variance+innovation)
    pair['future_rate_lower'] = pair.forecast_rate-error
    pair['future_rate_upper'] = pair.forecast_rate+error
    pair['future_observed_rate_covered'] = pair.observed_rate_future.between(pair.future_rate_lower,pair.future_rate_upper)
    return pair


def _rate_validation_metrics(pair, year):
    results = []
    for (pos,component), group in pair.groupby(['position','component']):
        prior = group.forecast_rate-group.data_weight*group.adjusted_rate+group.data_weight*group.prior_mean
        for name,predicted in [('pooled',group.forecast_rate),('raw_previous',group.observed_rate),('prior_mean',prior)]:
            error = predicted-group.observed_rate_future
            results.append(dict(test_season=year,position=pos,component=component,model=name,
                                player_rows=len(group),rate_rmse=float(np.sqrt(np.mean(error**2))),
                                rate_mae=float(np.mean(abs(error))),rate_bias=float(error.mean()),
                                observed_future_rate_coverage_80=float(group.future_observed_rate_covered.mean()) if name=='pooled' else np.nan,
                                coverage_before_drift=float(group.future_observed_rate_covered_before_drift.mean()) if name=='pooled' else np.nan,
                                innovation_variance=float(group.future_innovation_variance.iloc[0]) if name=='pooled' else np.nan))
    return results


def validate_rates(games, first_test_season=2023):
    """Earlier-transition pooling/context selection, then chronological testing.

    Loss equally weights player-component rates. Future zero opportunities have
    no defined rate and stay in the separate volume/availability validation.
    Numerical fitting, model selection and calibration exclude the outer year.
    """
    configs = [(False,.5),(False,1.),(False,2.),(True,1.)]
    results,predictions,selections,cache = [],[],[],{}
    years = sorted(games.season.unique())
    def prediction_for(year,context,scale):
        key = (year,context,scale)
        if key not in cache:
            cache[key] = _future_rate_pairs(games.loc[games.season.lt(year)],games.loc[games.season.eq(year)],
                                            context=context,variance_scale=scale)
        return cache[key]
    for year in years:
        if year < first_test_season:
            continue
        earlier_years = [y for y in years if years[0] < y < year]
        losses = []
        for context,scale in configs:
            earlier = [prediction_for(y,context,scale) for y in earlier_years]
            earlier = [p for p in earlier if not p.empty]
            if earlier:
                losses.extend(_rate_choices(earlier,context,scale))
        if not losses:
            continue
        selected = pd.DataFrame(losses).sort_values(['loss','context','scale']).drop_duplicates(['position','component'])
        parts = []
        for row in selected.itertuples():
            outer = prediction_for(year,row.context,row.scale)
            pair = outer.loc[outer.position.eq(row.position)&outer.component.eq(row.component)]
            if pair.empty:
                continue
            earlier = [prediction_for(y,row.context,row.scale) for y in earlier_years]
            earlier = [p.loc[p.position.eq(row.position)&p.component.eq(row.component)] for p in earlier if not p.empty]
            pair = _calibrate_rate_band(pair,[p for p in earlier if not p.empty])
            pair['selected_context'] = row.context
            pair['selected_variance_scale'] = row.scale
            parts.append(pair)
        if not parts:
            continue
        pair = pd.concat(parts,ignore_index=True)
        predictions.append(pair)
        selections.append(selected.assign(test_season=year,tuning_max_season=year-1))
        results.extend(_rate_validation_metrics(pair,year))
    return (pd.DataFrame(results),pd.concat(predictions,ignore_index=True) if predictions else pd.DataFrame(),
            pd.concat(selections,ignore_index=True) if selections else pd.DataFrame())


def replacement_scenarios(estimates, history, events, *, before_season):
    """Acquisition-based proxies, not a proven feasible replacement distribution.

    One-year SFA acquisitions supply a primary street-market comparator. An
    expanded one-year SFA/UFA comparator is a sensitivity case. Existing
    multi-year incumbents and rookie deals are never declared replacements.
    """
    scenarios = [('one_year_SFA', ['SFA']), ('one_year_SFA_or_UFA', ['SFA', 'UFA'])]
    baselines, contributions = [], []
    for name, types in scenarios:
        deals = events.loc[events.event_status.eq('usable_year_only') & events.contract_type.isin(types)
                           & events.contract_years.eq(1) & events.year_signed.lt(before_season)]
        evidence = history.merge(deals[['gsis_id', 'year_signed', 'event_id']],
                                 left_on=['gsis_id', 'season'], right_on=['gsis_id', 'year_signed'], how='inner')
        for (pos, component), group in evidence.groupby(['position', 'component']):
            # Equal acquired-player-season weights avoid defining replacement
            # as the production-weighted best/highest-usage SFA.
            if group.gsis_id.nunique() < 5:
                continue
            rate = group.observed_rate.mean()
            baseline = dict(scenario=name, position=pos, component=component, replacement_rate=rate,
                            replacement_players=group.gsis_id.nunique(), replacement_rows=len(group),
                            evidence_max_season=int(group.season.max()), interpretation='acquisition cohort sensitivity proxy')
            baselines.append(baseline)
            scored = estimates.loc[estimates.position.eq(pos) & estimates.component.eq(component)].copy()
            scored['scenario'] = name
            scored['replacement_rate'] = rate
            scored['above_replacement_attributed_epa'] = scored.opportunities * (scored.pooled_rate-rate)
            contributions.append(scored)
    return pd.DataFrame(baselines), pd.concat(contributions, ignore_index=True) if contributions else pd.DataFrame()


def game_block_rank_stability(games, costs, *, draws=200, seed=20261001, top_n=10):
    """Resample entire weeks jointly, preserving shared-game/player dependence.

    This is descriptive block-bootstrap stability at observed financial costs,
    not a confidence interval or a predictive latent surplus distribution.
    Full-window snap eligibility is supplied by the caller and held fixed.
    """
    if draws < 1:
        raise ValueError('At least one bootstrap draw is required')
    rng = np.random.default_rng(seed)
    weeks = sorted(games.week.unique())
    totals = games.groupby(['week', 'gsis_id']).total_epa.sum().unstack(fill_value=0).reindex(weeks)
    ids = costs.gsis_id.tolist()
    totals = totals.reindex(columns=ids, fill_value=0).to_numpy()
    weights = rng.multinomial(len(weeks), np.full(len(weeks), 1/len(weeks)), size=draws)
    epa = weights @ totals
    result = []
    for pos, cohort in costs.groupby('position'):
        indices = [ids.index(p) for p in cohort.gsis_id]
        ratios = np.divide(cohort.contract_apy_m.to_numpy()[None, :], epa[:, indices],
                           out=np.full((draws, len(indices)), np.inf), where=epa[:, indices] > 0)
        rank = pd.DataFrame(ratios).rank(axis=1, method='min').to_numpy()
        rank[~np.isfinite(ratios)] = np.nan
        for j, row in enumerate(cohort.itertuples()):
            finite = rank[:, j][np.isfinite(rank[:, j])]
            result.append(dict(gsis_id=row.gsis_id, position=pos, draws=draws, seed=seed,
                               block_unit='NFL week; all games jointly', positive_epa_fraction=float((epa[:, indices[j]] > 0).mean()),
                               top_n_fraction=float((rank[:, j] <= top_n).mean()),
                               rank_p10=float(np.quantile(finite,.1)) if len(finite) else np.nan,
                               rank_p90=float(np.quantile(finite,.9)) if len(finite) else np.nan,
                               distinct_observed_weeks=len(weeks)))
    return pd.DataFrame(result)
