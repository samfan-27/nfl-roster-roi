"""Preseason returning-player forecasts; unavailable future outcomes stay zero.

Availability means games with observed offensive participation, not health.
Opportunity allocation is predicted separately from attributed EPA per
opportunity. Component outputs must not be summed across players as team EPA.
"""
import numpy as np
import pandas as pd
from sklearn.linear_model import Ridge
from sklearn.impute import SimpleImputer
from sklearn.pipeline import make_pipeline
from sklearn.preprocessing import StandardScaler

from src.contribution import fit_rates, estimate_rates
from src.opportunities import component_rows

VOLUME_FEATURES = ['opportunities', 'active_games', 'age', 'years_exp', 'opportunities_per_game']


def season_components(games, roster_seasons):
    source = roster_seasons[['season', 'gsis_id', 'position', 'age', 'years_exp']].drop_duplicates(['season', 'gsis_id']).copy()
    source = source.loc[source.position.isin(['QB', 'RB', 'WR', 'TE'])]
    rows = component_rows(games)
    totals = rows.groupby(['season', 'gsis_id', 'component'], as_index=False).agg(
        opportunities=('opportunities', 'sum'), epa=('epa', 'sum'))
    active = games.loc[games.offensive_snaps.gt(0)].groupby(['season', 'gsis_id']).game_id.nunique()
    result = []
    for component in ['pass', 'rush', 'receive']:
        base = source.copy()
        base['component'] = component
        base = base.merge(totals, on=['season', 'gsis_id', 'component'], how='left', validate='one_to_one')
        base['active_games'] = pd.MultiIndex.from_frame(base[['season','gsis_id']]).map(active).fillna(0).to_numpy()
        base[['opportunities', 'epa']] = base[['opportunities', 'epa']].fillna(0)
        base['opportunities_per_game'] = base.opportunities / base.active_games.replace(0, np.nan)
        base['opportunities_per_game'] = base.opportunities_per_game.fillna(0)
        result.append(base)
    return pd.concat(result, ignore_index=True)


def _transitions(seasons):
    future = seasons[['season', 'gsis_id', 'component', 'active_games', 'opportunities', 'epa']].copy()
    future['season'] -= 1
    pair = seasons.merge(future, on=['season', 'gsis_id', 'component'], how='left', suffixes=('', '_future'), validate='one_to_one')
    # Caller supplies fully covered test seasons. A disappearing returning player
    # is an observed zero, not a dropped successful-participation condition.
    pair[['active_games_future', 'opportunities_future', 'epa_future']] = pair[['active_games_future', 'opportunities_future', 'epa_future']].fillna(0)
    pair['test_season'] = pair.season + 1
    return pair


def _volume_predictions(train, test):
    x_train = train[VOLUME_FEATURES]
    x_test = test[VOLUME_FEATURES]
    model = make_pipeline(SimpleImputer(strategy='median',add_indicator=True),StandardScaler(), Ridge(alpha=10.0))
    model.fit(x_train, train[['active_games_future','opportunities_future','epa_future']])
    predicted = model.predict(x_test)
    active = np.clip(predicted[:,0],0,17)
    volume = np.maximum(0,predicted[:,1])
    volume = np.where(active>0,volume,0)
    # Direct volume supervision retains zero outcomes and avoids multiplying
    # E[games] by E[opportunities/game] under an unsupported independence claim.
    per_game = np.divide(volume,active,out=np.zeros_like(volume),where=active>0)
    return active,per_game,volume,predicted[:,2]


def forecast_returning_players(games, seasons, *, first_test_season=2023, forecast_season=None):
    if games.empty:
        raise ValueError('Verified game-level historical sources are required')
    summaries = season_components(games, seasons)
    pairs = _transitions(summaries)
    outputs, diagnostics = [], []
    completed_years = sorted(games.season.unique())
    years = [y for y in completed_years if y >= first_test_season]
    if forecast_season is not None:
        if forecast_season != max(completed_years)+1:
            raise ValueError('Prospective forecast season must immediately follow completed inputs')
        years.append(forecast_season)
    for year in years:
        train = pairs.loc[pairs.test_season.lt(year) & pairs.test_season.isin(completed_years)]
        test = pairs.loc[pairs.test_season.eq(year)].copy()
        past_games = games.loc[games.season.lt(year)]
        # Every prior and forecast feature is fitted before the preseason cutoff.
        rate_model = fit_rates(past_games)
        rates = estimate_rates(rate_model, past_games.loc[past_games.season.eq(year-1)])
        if rates.empty:
            continue
        test = test.merge(rates[['gsis_id', 'position', 'component', 'pooled_rate', 'prior_mean']],
                          on=['gsis_id', 'position', 'component'], how='left', validate='one_to_one')
        test['prior_mean'] = test.prior_mean.fillna(pd.Series(pd.MultiIndex.from_frame(test[['position','component']]).map(rate_model.priors.set_index(['position','component']).prior_mean).to_numpy(), index=test.index))
        for (pos, component), group in test.groupby(['position', 'component']):
            history = train.loc[train.position.eq(pos) & train.component.eq(component)]
            if len(history) < 10:
                continue
            active, per_game, volume, joint_epa = _volume_predictions(history, group)
            group = group.copy()
            group['forecast_active_games'] = active
            group['forecast_opportunities_per_active_game'] = per_game
            group['forecast_opportunities'] = volume
            group['forecast_rate'] = group.pooled_rate.fillna(group.prior_mean)
            group['forecast_attributed_epa'] = group.forecast_opportunities * group.forecast_rate
            group['forecast_joint_attributed_epa'] = joint_epa
            group['composition_assumption'] = 'rate-times-volume is a marginal-product proxy; direct EPA benchmark allows dependence'
            group['forecast_validated'] = False
            group['forecast_status'] = 'preseason_research_candidate'
            group['information_cutoff'] = f'{year}-06-01'
            group['training_max_outcome_season'] = history.test_season.max()
            group['prediction_provenance'] = 'chronological_preseason_returning_player'
            # Prospective outputs cannot expose fabricated realized outcomes.
            if year not in completed_years:
                group[['active_games_future', 'opportunities_future', 'epa_future']] = np.nan
            else:
                for model in ['candidate', 'previous_season']:
                    for metric, prediction, actual in [
                        ('active_games', active if model=='candidate' else group.active_games, group.active_games_future),
                        ('opportunities', group.forecast_opportunities if model=='candidate' else group.opportunities, group.opportunities_future),
                        ('attributed_epa', group.forecast_attributed_epa if model=='candidate' else group.epa, group.epa_future),
                    ]:
                        valid = pd.Series(np.asarray(prediction), index=group.index).notna() & actual.notna()
                        error = np.asarray(prediction)[valid] - actual[valid].to_numpy()
                        diagnostics.append(dict(test_season=year, position=pos, component=component, model=model,
                                                outcome=metric, rows=int(valid.sum()),
                                                future_zero_opportunity_rows=int(group.opportunities_future.eq(0).sum()),
                                                mae=float(np.mean(abs(error))) if len(error) else np.nan,
                                                rmse=float(np.sqrt(np.mean(error**2))) if len(error) else np.nan,
                                                bias=float(np.mean(error)) if len(error) else np.nan))
                error = group.forecast_joint_attributed_epa-group.epa_future
                diagnostics.append(dict(test_season=year,position=pos,component=component,model='direct_joint_epa',outcome='attributed_epa',
                                        rows=len(group),future_zero_opportunity_rows=int(group.opportunities_future.eq(0).sum()),
                                        mae=float(np.mean(abs(error))),rmse=float(np.sqrt(np.mean(error**2))),bias=float(error.mean())))
            outputs.append(group)
    return (pd.concat(outputs, ignore_index=True) if outputs else pd.DataFrame(), pd.DataFrame(diagnostics))
