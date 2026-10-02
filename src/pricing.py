"""Negotiated-event APY association benchmarks with conservative time cutoffs.

Unknown signing dates force a two-season production lag: the preceding NFL
season can end in January of the signing year. Retrospective source histories
are not contemporaneous contract snapshots or verified equivalent offers.
"""
from hashlib import sha256
import json

import numpy as np
import pandas as pd
from sklearn.compose import ColumnTransformer
from sklearn.impute import SimpleImputer
from sklearn.linear_model import GammaRegressor, QuantileRegressor, Ridge
from sklearn.pipeline import Pipeline, make_pipeline
from sklearn.preprocessing import OneHotEncoder, StandardScaler
from sklearn.metrics import mean_pinball_loss

from src.domain import OFFENSIVE_POSITIONS
from src.valuation import regression_metrics

PRICE_FEATURES = ['passing_epa', 'rushing_epa', 'receiving_epa', 'snaps', 'age', 'years_exp',
                  'pass_opportunities', 'carries', 'targets', 'games_played']
MODELS = {'ridge_mean': 'conditional_mean', 'gamma_mean': 'conditional_mean',
          'ridge_log': 'transformed_geometric_center', 'quantile_median': 'conditional_median'}
PRICE_MARKETS = ['UFA', 'Extension', 'SFA', 'RFA', 'ERFA', 'Franchise', 'Transition']


def build_price_rows(events, seasons, incomplete_season):
    data = events.copy()
    data['feature_season'] = data.year_signed - 2
    data['decision_cutoff'] = data.year_signed.map(lambda y: f'{int(y)}-01-01' if pd.notna(y) else None)
    source = seasons.loc[seasons.season.lt(incomplete_season)].copy()
    fields = ['season', 'gsis_id', *PRICE_FEATURES]
    for col in PRICE_FEATURES:
        if col not in source:
            source[col] = np.nan
    data = data.merge(source[fields], left_on=['gsis_id', 'feature_season'], right_on=['gsis_id', 'season'], how='left', validate='many_to_one')
    # Never filter by realized snap volume or positive production.
    data['price_row_status'] = data.event_status
    data.loc[~data.contract_type.isin(PRICE_MARKETS), 'price_row_status'] = 'regulated_or_unclassified_market'
    data.loc[data.season.isna(), 'price_row_status'] = 'missing_pre_event_history'
    consistent = np.isclose(data.contract_apy_m * data.contract_years, data.contract_total_m, atol=.02, rtol=.002)
    data.loc[~consistent, 'price_row_status'] = 'unreconciled_apy_convention'
    data['feature_timing'] = 'completed season two years before year-only signing'
    data['offer_interpretation'] = 'retrospective event association; equivalent new-money terms unverified'
    return data


def _model(name, alpha):
    numeric = make_pipeline(SimpleImputer(strategy='median', add_indicator=True), StandardScaler())
    transform = ColumnTransformer([('numeric', numeric, PRICE_FEATURES),
                                   ('market', OneHotEncoder(handle_unknown='ignore', sparse_output=False), ['contract_type'])])
    if name == 'gamma_mean':
        estimator = GammaRegressor(alpha=alpha, max_iter=1000)
    elif name == 'quantile_median':
        estimator = QuantileRegressor(alpha=alpha, quantile=.5, solver='highs')
    else:
        estimator = Ridge(alpha=alpha)
    return Pipeline([('features', transform), ('model', estimator)])


def _fit_predict(train, test, name, alpha):
    fit = _model(name, alpha)
    target = train.signing_cap_share_pct.to_numpy()
    fit.fit(train, np.log1p(target) if name == 'ridge_log' else target)
    raw = fit.predict(test)
    pred = np.expm1(raw) if name == 'ridge_log' else raw
    # Positive support is an estimand constraint, not an uncertainty bound.
    return np.maximum(0, pred)


def _loss(name, actual, predicted):
    if name == 'quantile_median':
        return mean_pinball_loss(actual, predicted, alpha=.5)
    if name == 'ridge_log':
        return float(np.mean((np.log1p(actual) - np.log1p(predicted))**2))
    return float(np.mean((actual-predicted)**2))


def _tune(train, name, unseen_players):
    losses = []
    for alpha in [.1, 1., 10.]:
        measured = []
        for year in sorted(train.year_signed.unique())[1:]:
            validation = train.loc[train.year_signed.eq(year)]
            inner = train.loc[train.year_signed.lt(year)]
            if unseen_players:
                inner = inner.loc[~inner.gsis_id.isin(validation.gsis_id)]
            if len(inner) < 10 or len(validation) < 3:
                continue
            prediction = _fit_predict(inner, validation, name, alpha)
            measured.append(_loss(name, validation.signing_cap_share_pct.to_numpy(), prediction))
        if measured:
            losses.append((float(np.mean(measured)), alpha))
    return min(losses)[1] if losses else 1.0


def validate_event_prices(rows, *, first_test_year=2025, unseen_players=True):
    """Untouched later events; nested earlier-year tuning on declared losses.

    Benchmarks remain separate by estimand. No winner is selected on the outer
    holdout, and no individual latent-market-value interval is fabricated.
    """
    eligible = rows.loc[rows.price_row_status.eq('usable_year_only') & rows.position.isin(OFFENSIVE_POSITIONS)].copy()
    predictions, diagnostics = [], []
    for (pos, year), test in eligible.groupby(['position', 'year_signed']):
        if year < first_test_year:
            continue
        train = eligible.loc[eligible.position.eq(pos) & eligible.year_signed.lt(year)]
        if unseen_players:
            train = train.loc[~train.gsis_id.isin(test.gsis_id)]
        if len(train) < 10 or len(test) < 3:
            continue
        for name, estimand in MODELS.items():
            alpha = _tune(train, name, unseen_players)
            pred_pct = _fit_predict(train, test, name, alpha)
            out = test[['event_id', 'gsis_id', 'position', 'contract_type', 'year_signed', 'feature_season',
                        'decision_cutoff', 'contract_apy_m', 'signing_cap_m', 'signing_cap_share_pct', 'label_provenance']].copy()
            out['model'] = name
            out['estimand'] = estimand
            out['selected_alpha'] = alpha
            out['predicted_cap_share_pct'] = pred_pct
            out['predicted_event_apy_m'] = pred_pct / 100 * out.signing_cap_m
            out['prediction_provenance'] = 'chronological_player_purged_events' if unseen_players else 'chronological_returning_player_events'
            out['training_max_signing_year'] = train.year_signed.max()
            out['training_event_ids'] = json.dumps(sorted(train.event_id.tolist()))
            out['training_events_sha256'] = sha256(out.training_event_ids.iloc[0].encode()).hexdigest()
            out['training_player_overlap'] = len(set(train.gsis_id) & set(test.gsis_id))
            out['interval_status'] = 'no calibrated latent market-value distribution'
            predictions.append(out)
            subsets = [('All', out)] + list(out.groupby('contract_type'))
            for market, group in subsets:
                metrics = regression_metrics(group.contract_apy_m, group.predicted_event_apy_m)
                diagnostics.append(dict(position=pos, test_year=int(year), market=market, model=name, estimand=estimand,
                                        event_rows=len(group), training_events=len(train), selected_alpha=alpha,
                                        cap_share_loss=_loss(name, group.signing_cap_share_pct.to_numpy(), group.predicted_cap_share_pct.to_numpy()),
                                        median_pinball_m=mean_pinball_loss(group.contract_apy_m, group.predicted_event_apy_m, alpha=.5), **metrics))
        for name, pct in [('mean_baseline', train.signing_cap_share_pct.mean()), ('median_baseline', train.signing_cap_share_pct.median())]:
            metrics = regression_metrics(test.contract_apy_m, pct/100 * test.signing_cap_m)
            diagnostics.append(dict(position=pos, test_year=int(year), market='All', model=name,
                                    estimand='conditional_mean' if name=='mean_baseline' else 'conditional_median',
                                    event_rows=len(test), training_events=len(train), **metrics))
    return (pd.concat(predictions, ignore_index=True) if predictions else pd.DataFrame(), pd.DataFrame(diagnostics))


def apy_pricing_discount(frame):
    """Compare explicit equivalent terms only; missing verification stays unknown.

    Caller supplies an independently predicted equivalent new-contract APY and
    documented convention/market/timing compatibility. This does not turn an
    event residual or APY difference into current-season budget savings.
    """
    required = ['equivalent_new_contract_apy_m', 'contract_apy_m', 'terms_verified', 'prediction_provenance']
    if not set(required).issubset(frame):
        raise ValueError('Equivalent APY, compatibility verification and prediction provenance are required')
    out = frame.copy()
    valid = out.terms_verified.eq(True) & out.prediction_provenance.isin(
        ['chronological_player_purged_events', 'prospective_pre_decision'])
    out['apy_pricing_discount_m'] = (out.equivalent_new_contract_apy_m-out.contract_apy_m).where(valid)
    out['discount_status'] = np.where(valid, 'compatible_APY_association', 'unverified_terms_or_prediction')
    return out
