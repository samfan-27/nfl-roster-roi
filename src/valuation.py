"""Completed-season APY research model; partial seasons are not valued."""

import numpy as np
import pandas as pd
from sklearn.linear_model import Ridge
from sklearn.model_selection import GroupKFold, GridSearchCV
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler
from sklearn.metrics import mean_absolute_error, r2_score
from src.domain import OFFENSIVE_POSITIONS, SALARY_CAP_MILLIONS

FEATURES = ['total_epa', 'snaps', 'epa_per_snap', 'age', 'years_exp']


def _search(frame, groups):
    pipeline = Pipeline([('scaler', StandardScaler()), ('ridge', Ridge())])
    folds = min(5, pd.Series(groups).nunique())
    if folds < 2:
        raise ValueError('At least two distinct veteran players are required')
    search = GridSearchCV(
        pipeline, {'ridge__alpha': np.logspace(-3, 3, 25)},
        cv=GroupKFold(n_splits=folds), scoring='neg_root_mean_squared_error', n_jobs=1,
    )
    search.fit(frame[FEATURES].astype(float), np.log1p(frame['cap_pct_of_team'] * 100), groups=groups)
    return search


def score_completed_seasons(frame, incomplete_season, positions=OFFENSIVE_POSITIONS):
    """Use nested player-group CV; same player's years stay in one fold.

    This estimates historical contract associations, not future salary offers.
    Annual volume features require completed seasons. In-season ROI remains
    available separately without inventing a full-season EPA projection.
    """
    complete = frame.loc[frame['season'] < incomplete_season].copy()
    complete['season_cap_m'] = complete['season'].map(SALARY_CAP_MILLIONS)
    complete = complete.replace([np.inf, -np.inf], np.nan)
    complete = complete.dropna(subset=FEATURES + ['gsis_id', 'season_cap_m', 'is_rookie_deal'])
    complete = complete.loc[(complete['snaps'] >= 100) & (complete['yearly_cap_hit'] > 0)]
    complete['cap_pct_of_team'] = complete['yearly_cap_hit'] / complete['season_cap_m']
    scored, diagnostics = [], []
    for position in positions:
        cohort = complete.loc[complete['position'] == position].copy()
        train = cohort.loc[cohort['is_rookie_deal'].eq(False)].copy()
        groups = train['gsis_id']
        n_groups = groups.nunique()
        if n_groups < 3:
            continue
        predictions = np.zeros(len(train))
        outer = GroupKFold(n_splits=min(5, n_groups))
        for train_idx, test_idx in outer.split(train, groups=groups):
            fold = train.iloc[train_idx]
            model = _search(fold, fold['gsis_id'])
            test = train.iloc[test_idx]
            predictions[test_idx] = np.maximum(0, np.expm1(model.predict(test[FEATURES].astype(float)))) / 100 * test['season_cap_m']
        best = _search(train, groups)
        raw = np.maximum(0, np.expm1(best.predict(cohort[FEATURES].astype(float)))) / 100 * cohort['season_cap_m']
        cohort['expected_apy_raw'] = raw
        cohort['expected_apy'] = raw.clip(upper=train['yearly_cap_hit'].max() * 1.1)
        cohort['surplus_value'] = cohort['expected_apy'] - cohort['yearly_cap_hit']
        cohort['is_extrapolated'] = (
            cohort[FEATURES].lt(train[FEATURES].min()).any(axis=1)
            | cohort[FEATURES].gt(train[FEATURES].max()).any(axis=1)
        )
        scored.append(cohort)
        diagnostics.append(dict(
            position=position, veteran_rows=len(train), veteran_players=n_groups,
            best_alpha=best.best_params_['ridge__alpha'],
            grouped_oof_mae_m=mean_absolute_error(train['yearly_cap_hit'], predictions),
            grouped_oof_r2=r2_score(train['yearly_cap_hit'], predictions),
        ))
    if not scored:
        raise ValueError('Insufficient completed-season veteran data for valuation')
    return pd.concat(scored, ignore_index=True), pd.DataFrame(diagnostics)
