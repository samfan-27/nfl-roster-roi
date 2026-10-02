"""Player-held-out completed-season APY associations, never causal surplus."""
from hashlib import sha256
from importlib.metadata import version
import json

import numpy as np
import pandas as pd
from sklearn.linear_model import Ridge
from sklearn.model_selection import GroupKFold, GridSearchCV
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
from src.domain import OFFENSIVE_POSITIONS, SALARY_CAP_MILLIONS

FEATURES = ['total_epa', 'snaps', 'epa_per_snap', 'age', 'years_exp']
ALPHAS = np.logspace(-3, 3, 25)


def dependency_versions():
    return {name: version(name) for name in ['numpy', 'pandas', 'scikit-learn', 'scipy']}


def regression_metrics(y, prediction):
    y, prediction = np.asarray(y, float), np.asarray(prediction, float)
    return dict(mae_m=mean_absolute_error(y, prediction),
                rmse_m=float(np.sqrt(mean_squared_error(y, prediction))),
                mean_error_m=float(np.mean(prediction - y)),
                r2=r2_score(y, prediction) if len(y) > 1 and np.var(y) > 0 else np.nan)


def _search(frame, groups):
    pipeline = Pipeline([('scaler', StandardScaler()), ('ridge', Ridge())])
    folds = min(5, pd.Series(groups).nunique())
    if folds < 2:
        raise ValueError('At least two distinct veteran players are required')
    search = GridSearchCV(pipeline, {'ridge__alpha': ALPHAS},
                          cv=GroupKFold(n_splits=folds), scoring='neg_root_mean_squared_error', n_jobs=1)
    search.fit(frame[FEATURES].astype(float), np.log1p(frame.cap_pct_of_team * 100), groups=groups)
    return search


def _prepare(frame, incomplete_season):
    complete = frame.loc[frame.season.lt(incomplete_season)].copy()
    if 'contract_apy_m' in complete:
        complete['yearly_cap_hit'] = complete.contract_apy_m
    complete['season_cap_m'] = complete.season.map(SALARY_CAP_MILLIONS)
    complete[FEATURES + ['season_cap_m']] = complete[FEATURES + ['season_cap_m']].replace([np.inf, -np.inf], np.nan)
    complete = complete.dropna(subset=FEATURES + ['gsis_id', 'season_cap_m', 'is_rookie_deal'])
    complete = complete.loc[complete.snaps.ge(100) & complete.yearly_cap_hit.gt(0)]
    if complete.duplicated(['season', 'gsis_id']).any():
        raise ValueError('Duplicate player-season training rows')
    complete['cap_pct_of_team'] = complete.yearly_cap_hit / complete.season_cap_m
    return complete.sort_values(['position', 'gsis_id', 'season']).reset_index(drop=True)


def _predict(fit, train, test, provenance, fold):
    """One inverse and one training-only cap-share bound in every validation path."""
    out = test.copy()
    raw_share = np.maximum(0, np.expm1(fit.predict(test[FEATURES].astype(float))))
    bound = 1.1 * (train.cap_pct_of_team * 100).max()
    out['expected_apy_raw'] = raw_share / 100 * test.season_cap_m
    out['expected_apy'] = np.minimum(raw_share, bound) / 100 * test.season_cap_m
    out['prediction_bound_cap_share_pct'] = bound
    out['prediction_was_bounded'] = raw_share > bound
    out['apy_association_center_m'] = out.expected_apy
    out['apy_association_gap_m'] = out.expected_apy - out.yearly_cap_hit
    out['surplus_value'] = out.apy_association_gap_m  # Deprecated; no economic surplus claim.
    out['prediction_provenance'] = provenance
    out['prediction_estimand'] = 'geometric center of 1 + APY cap-share percentage'
    out['outer_fold'] = fold
    out['model_alpha'] = fit.best_params_['ridge__alpha']
    out['training_min_season'] = train.season.min()
    out['training_max_season'] = train.season.max()
    keys = train[['season', 'gsis_id']].sort_values(['gsis_id', 'season']).to_json(orient='records')
    out['training_rows_sha256'] = sha256(keys.encode()).hexdigest()
    out['is_extrapolated'] = (out[FEATURES].lt(train[FEATURES].min()).any(axis=1)
                              | out[FEATURES].gt(train[FEATURES].max()).any(axis=1))
    out['mean_baseline_apy_m'] = train.cap_pct_of_team.mean() * out.season_cap_m
    out['median_baseline_apy_m'] = train.cap_pct_of_team.median() * out.season_cap_m
    return out


def _diagnostic(predictions, position, validation, test_season=None):
    metrics = regression_metrics(predictions.yearly_cap_hit, predictions.expected_apy)
    result = dict(position=position, validation=validation, test_season=test_season,
                  veteran_rows=len(predictions), veteran_players=predictions.gsis_id.nunique(),
                  best_alpha=float(predictions.model_alpha.median()),
                  **{'grouped_oof_' + k: v for k, v in metrics.items()}, **metrics,
                  dependency_versions=json.dumps(dependency_versions(), sort_keys=True))
    for baseline in ['mean', 'median']:
        result.update({baseline + '_baseline_' + k: v for k, v in regression_metrics(
            predictions.yearly_cap_hit, predictions[baseline + '_baseline_apy_m']).items()})
    return result


def score_completed_seasons(frame, incomplete_season, positions=OFFENSIVE_POSITIONS):
    """Nested grouped OOF for every scored row, including historical rookies.

    Whole-player exclusions cover rookies' later veteran targets too. Outer
    membership is deterministic on sorted inputs and saved with every row.
    Chronological tests are returning-player salary associations, using the
    test year's completed production; they are NOT future-offer predictions.
    """
    complete = _prepare(frame, incomplete_season)
    scored, diagnostics, temporal = [], [], []
    for position in positions:
        cohort = complete.loc[complete.position.eq(position)].copy()
        veterans = cohort.loc[cohort.is_rookie_deal.eq(False)]
        n_groups = veterans.gsis_id.nunique()
        if n_groups < 3:
            continue
        membership = {}
        folds = min(5, n_groups)
        for number, (_, held) in enumerate(GroupKFold(folds).split(veterans, groups=veterans.gsis_id)):
            membership.update({pid: number for pid in veterans.iloc[held].gsis_id.unique()})
        for pid in sorted(set(cohort.gsis_id) - set(membership)):
            membership[pid] = int(sha256(pid.encode()).hexdigest()[:8], 16) % folds
        cohort['outer_fold'] = cohort.gsis_id.map(membership)
        predictions = []
        for number in range(folds):
            test = cohort.loc[cohort.outer_fold.eq(number)]
            train = veterans.loc[~veterans.gsis_id.isin(test.gsis_id)]
            if train.gsis_id.nunique() < 2:
                continue
            assert not set(train.gsis_id) & set(test.gsis_id)
            model = _search(train, train.gsis_id)
            predictions.append(_predict(model, train, test, 'player_grouped_out_of_fold', number))
        if not predictions:
            continue
        position_scored = pd.concat(predictions, ignore_index=True)
        scored.append(position_scored)
        diagnostics.append(_diagnostic(position_scored.loc[position_scored.is_rookie_deal.eq(False)], position, 'nested_player_grouped'))
        years = sorted(veterans.season.unique())
        for year in years[2:]:
            train = veterans.loc[veterans.season.lt(year)]
            test = veterans.loc[veterans.season.eq(year)]
            if train.gsis_id.nunique() < 3 or test.empty:
                continue
            pred = _predict(_search(train, train.gsis_id), train, test, 'chronological_completed_production_association', int(year))
            temporal.append(pred)
            diagnostics.append(_diagnostic(pred, position, 'chronological_returning_player', int(year)))
    if not scored:
        raise ValueError('Insufficient completed-season veteran data for valuation')
    result = pd.concat(scored, ignore_index=True).sort_values(['position', 'gsis_id', 'season']).reset_index(drop=True)
    result.attrs['temporal_predictions'] = pd.concat(temporal, ignore_index=True).to_dict('records') if temporal else []
    result.attrs['dependencies'] = dependency_versions()
    return result, pd.DataFrame(diagnostics)
