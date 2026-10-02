"""Financial cases, mathematical identities and information-cutoff regressions."""
import numpy as np
import pandas as pd
import pytest

from src.contracts import select_contracts, contract_events
from src.opportunities import reconcile_epa_exposures, component_rows
from src.contribution import fit_rates, estimate_rates, replacement_scenarios, game_block_rank_stability
from src.pricing import build_price_rows, validate_event_prices, apy_pricing_discount
from src.forecasting import season_components, forecast_returning_players
from src.roster_decisions import incremental_costs, roster_surplus_scenarios, COST_COLUMNS


def contracts_fixture():
    annual = dict(year='2026',team='Jaguars',cap_number=3.722066,cash_paid=3.674)
    deal = dict(year_signed=2023,apy=1.008066,yrs=4,total=4.032264,guarantees=.192264,contract_type='Drafted',team='Jaguars')
    return pd.DataFrame([dict(otc_id=1,gsis_id='p1',player='Washington case',position='WR',year_signed=2023,
        years=4,is_active=True,apy=1.008066,team='Jaguars',season_history=[annual,annual],contract_history=[deal])])


def test_millions_escalator_and_exact_annual_duplicates():
    selected=select_contracts(contracts_fixture(),2026).iloc[0]
    assert selected.contract_apy_m==1.008066
    assert selected.season_cap_charge_m==3.722066
    assert selected.season_cash_m==3.674
    assert selected.annual_entry_count==1
    assert selected.contract_type=='Drafted'
    assert selected.contract_signed_date is None


def test_trade_competing_annual_entries_remain_unknown():
    data=contracts_fixture()
    data.at[0,'season_history'].append(dict(year='2026',team='Other club',cap_number=1,cash_paid=1))
    row=select_contracts(data,2026).iloc[0]
    assert pd.isna(row.season_cap_charge_m) and pd.isna(row.season_cash_m)
    assert row.annual_financial_status=='ambiguous'
    assert row.annual_entry_count==2


def test_early_extension_never_expires_by_term_arithmetic():
    data=contracts_fixture();data['year_signed']=2024;data['years']=2;data['apy']=19
    assert select_contracts(data,2026).iloc[0].contract_apy_m==19


def test_contract_events_deduplicate_transfer_copies_and_exclude_same_year_deals():
    data=contracts_fixture(); deal=data.iloc[0].contract_history[0].copy()
    deal['team']='New team'; deal['status']='Traded';deal['amount_earned']=2
    data.at[0,'contract_history'].append(deal)
    players=pd.DataFrame([dict(otc_id=1,gsis_id='p1',position='WR')])
    event=contract_events(data,players)
    assert len(event)==1 and event.event_status.iloc[0]=='usable_year_only'
    deal=deal.copy();deal['apy']=2;deal['total']=8
    data.at[0,'contract_history'].append(deal)
    assert contract_events(data,players).event_status.eq('ambiguous_player_year').all()


def test_two_point_epa_exposure_is_not_official_attempt_count():
    games=pd.DataFrame([dict(game_id='g',gsis_id='p',passing_epa=1.053,rushing_epa=0.,receiving_epa=0.,pass_opportunities=0)])
    pbp=pd.DataFrame([dict(game_id='g',season_type='REG',play_type='pass',passer_player_id='p',rusher_player_id=None,
                          receiver_player_id='other',qb_epa=1.053,epa=1.053)])
    result=reconcile_epa_exposures(games,pbp)
    assert result.pass_opportunities.iloc[0]==0
    assert result.pass_epa_opportunities.iloc[0]==1
    assert result.passing_epa.iloc[0]==1.053
    pbp['qb_epa']=5
    with pytest.raises(ValueError,match='mismatch'):
        reconcile_epa_exposures(games,pbp)


def test_unverified_official_counts_cannot_train_rates():
    with pytest.raises(ValueError,match='reconciliation'):
        component_rows(pd.DataFrame([dict(passing_epa=1,pass_opportunities=1)]))


def rate_games(years=(2021,2022,2023),players=12):
    rows=[]
    for year in years:
        for player in range(players):
            for week in range(1,5):
                n=10+player
                rows.append(dict(season=year,week=week,game_id=f'{year}_{week}',gsis_id=f'p{player}',position='RB',team='A',opponent_team='B',
                                 offensive_snaps=30,passing_epa=0.,rushing_epa=n*(-.3+.03*player+(-.1 if week%2 else .1)),receiving_epa=n*(.1+.01*player),
                                 pass_epa_opportunities=0,rush_epa_opportunities=n,receive_epa_opportunities=n,total_epa=n*(-.2+.04*player+(-.1 if week%2 else .1))))
    return pd.DataFrame(rows)


def test_game_cluster_pooling_and_conditional_variance_identity():
    data=rate_games()
    fit=fit_rates(data)
    result=estimate_rates(fit,data.loc[data.season.eq(2023)])
    row=result.loc[result.component.eq('rush')].iloc[0]
    assert 0<=row.data_weight<=1
    assert row.pooled_rate==pytest.approx(row.data_weight*row.observed_rate+(1-row.data_weight)*row.prior_mean)
    assert row.conditional_posterior_variance<=row.observation_variance
    assert row.conditional_posterior_variance<=row.prior_variance+1e-10
    assert result.epa.sum()==pytest.approx(data.loc[data.season.eq(2023)].rushing_epa.sum()+data.loc[data.season.eq(2023)].receiving_epa.sum())


def test_negative_raw_epa_can_exceed_acquisition_comparator():
    estimates=pd.DataFrame([dict(season=2026,gsis_id='target',position='RB',component='rush',opportunities=10,pooled_rate=-.1,epa=-1)])
    history=pd.DataFrame([dict(season=2025,gsis_id=f'p{i}',position='RB',component='rush',observed_rate=-.3) for i in range(5)])
    events=pd.DataFrame([dict(gsis_id=f'p{i}',year_signed=2025,contract_years=1,contract_type='SFA',event_status='usable_year_only',event_id=f'e{i}') for i in range(5)])
    _,result=replacement_scenarios(estimates,history,events,before_season=2026)
    assert np.allclose(result.above_replacement_attributed_epa, 2)
    events['year_signed']=2026
    assert replacement_scenarios(estimates,history,events,before_season=2026)[1].empty


def test_block_rank_stability_reproducible_and_position_specific():
    data=rate_games(years=(2026,))
    costs=pd.DataFrame([dict(gsis_id=f'p{i}',position='RB',contract_apy_m=1) for i in range(12)])
    a=game_block_rank_stability(data,costs,draws=30)
    b=game_block_rank_stability(data,costs,draws=30)
    pd.testing.assert_frame_equal(a,b)
    assert a.distinct_observed_weeks.eq(4).all()
    assert a.top_n_fraction.between(0,1).all()


def test_price_features_respect_earliest_year_only_signing_cutoff():
    event=pd.DataFrame([dict(gsis_id='p',year_signed=2025,event_status='usable_year_only',contract_type='UFA',contract_apy_m=10.,contract_years=2,contract_total_m=20.)])
    seasons=pd.DataFrame([dict(gsis_id='p',season=2023,snaps=50),dict(gsis_id='p',season=2024,snaps=999),dict(gsis_id='p',season=2025,snaps=9999)])
    row=build_price_rows(event,seasons,2026).iloc[0]
    assert row.feature_season==2023 and row.snaps==50
    assert row.decision_cutoff=='2025-01-01'
    assert row.price_row_status=='usable_year_only'


def test_event_training_purges_players_and_future_targets(monkeypatch):
    import src.pricing as pricing
    rows=[]
    for year in [2023,2024,2025]:
        for i in range(20):
            rows.append(dict(event_id=f'{year}-{i}',gsis_id=f'p{i+20*(year-2023)}',position='WR',contract_type='UFA',year_signed=year,feature_season=year-2,
                             decision_cutoff=f'{year}-01-01',contract_apy_m=5,signing_cap_m=250,signing_cap_share_pct=2,price_row_status='usable_year_only',label_provenance='test'))
    rows.append({**rows[0],'event_id':'repeat-player','year_signed':2025})
    seen=[]
    def predict(train,test,name,alpha):
        assert train.year_signed.max()<test.year_signed.min()
        assert not set(train.gsis_id)&set(test.gsis_id)
        seen.append((len(train),len(test)))
        return np.full(len(test),2.)
    monkeypatch.setattr(pricing,'_fit_predict',predict)
    output,_=validate_event_prices(pd.DataFrame(rows))
    assert seen and output.training_player_overlap.eq(0).all()
    assert output.training_max_signing_year.lt(output.year_signed).all()


def test_forecasts_include_disappearing_and_low_volume_players():
    games=rate_games()
    # Future disappearance has zero availability/opportunities in a fully covered year.
    games=games.loc[~(games.season.eq(2023)&games.gsis_id.eq('p0'))]
    seasons=pd.DataFrame([dict(season=y,gsis_id=f'p{i}',position='RB',age=25,years_exp=3) for y in [2021,2022,2023] for i in range(12)])
    forecasts,diagnostics=forecast_returning_players(games,seasons,first_test_season=2023,forecast_season=2024)
    target=forecasts.loc[forecasts.test_season.eq(2023)&forecasts.gsis_id.eq('p0')]
    assert not target.empty and target.opportunities_future.eq(0).all() and target.active_games_future.eq(0).all()
    future=forecasts.loc[forecasts.test_season.eq(2024)]
    assert future.epa_future.isna().all()
    assert forecasts.training_max_outcome_season.lt(forecasts.test_season).all()
    assert diagnostics.future_zero_opportunity_rows.gt(0).any()


def test_apy_discount_requires_equivalent_terms():
    rows=pd.DataFrame([dict(contract_apy_m=3,equivalent_new_contract_apy_m=8,terms_verified=False,prediction_provenance='chronological_player_purged_events')])
    assert apy_pricing_discount(rows).apy_pricing_discount_m.isna().all()
    rows['terms_verified']=True
    assert apy_pricing_discount(rows).apy_pricing_discount_m.iloc[0]==5


def test_roster_costs_separate_cap_cash_and_do_not_double_count_guarantees():
    rows=[]
    for year in [2026,2027]:
        rows.append(dict(gsis_id='p',scenario='keep',team='A',season=year,obligation_source='verified scenario',
                         **{c:0 for c in COST_COLUMNS},))
    data=pd.DataFrame(rows)
    data['player_cap_m']=10;data['replacement_cap_m']=2
    data['player_cash_m']=6;data['replacement_cash_m']=1
    data['player_guaranteed_cash_m']=6
    costs=incremental_costs(data,horizon=[2026,2027])
    assert costs.incremental_cap_m.iloc[0]==16
    assert costs.incremental_cash_m.iloc[0]==10
    contribution=pd.DataFrame([dict(gsis_id='p',scenario='keep',team='A',horizon='2026|2027',forecast_above_replacement_epa=50,forecast_validated=True,contribution_definition='specified proxy')])
    result=roster_surplus_scenarios(contribution,costs,lambda_m_per_epa=.5)
    assert result.scenario_surplus_m.iloc[0]==9
    assert result.latent_surplus_lower_quantile_m.isna().all()
    data.loc[0,'player_exit_cap_m']=np.nan
    costs=incremental_costs(data,horizon=[2026,2027])
    assert roster_surplus_scenarios(contribution,costs,lambda_m_per_epa=.5).scenario_surplus_m.isna().all()


def test_reused_contract_gsis_cannot_assign_homonym_price():
    from src.contracts import reconcile_contract_identities
    players=pd.DataFrame([dict(otc_id=None,gsis_id='p1',position='RB')])
    data=pd.DataFrame([dict(otc_id=1,gsis_id='p1',position='WR'),dict(otc_id=2,gsis_id='p1',position='RB')])
    result=reconcile_contract_identities(data,players)
    assert pd.isna(result.gsis_id.iloc[0])
    assert result.gsis_id.iloc[1]=='p1'
    assert result.contract_identity_status.iloc[0]=='source_gsis_position_conflict'


def test_heterogeneous_prior_does_not_follow_a_high_noise_outlier():
    from src.contribution import _normal_prior
    mean,variance=_normal_prior([-.1,0,.1,100],[.01,.01,.01,1e9])
    assert abs(mean)<.001
    assert variance<.01


def test_future_rate_calibration_never_trains_on_test_year():
    from src.contribution import validate_rates
    diagnostics,predictions,selections=validate_rates(rate_games())
    assert not diagnostics.empty
    assert predictions.training_max_season.lt(predictions.test_season).all()
    assert selections.tuning_max_season.lt(selections.test_season).all()


def test_player_season_game_aggregation_catches_lost_role_rows():
    from src.opportunities import validate_game_aggregation
    games=rate_games(years=(2026,))
    season=games.groupby(['season','gsis_id','position'],as_index=False).agg(
        passing_epa=('passing_epa','sum'),rushing_epa=('rushing_epa','sum'),receiving_epa=('receiving_epa','sum'),snaps=('offensive_snaps','sum'))
    validate_game_aggregation(season,games)
    with pytest.raises(ValueError,match='reproduce'):
        validate_game_aggregation(season,games.loc[~games.gsis_id.eq('p0')])
