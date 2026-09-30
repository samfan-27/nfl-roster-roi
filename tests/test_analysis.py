import pandas as pd
import pytest
from src.analysis import build_roster_roi
from src.domain import rookie_contract_mask
from src.stats_helpers import compute_core_metrics, shrink_total_epa


@pytest.fixture
def tables():
    return dict(
        players=pd.DataFrame([dict(otc_id=1, gsis_id='p1', pfr_id='pfr1', display_name='Test Player', position='QB', draft_year=2024, draft_round=1)]),
        contracts=pd.DataFrame([
            dict(otc_id=1, gsis_id=None, player='Test Player', position='QB', team='OLD', year_signed=2024, is_active=True, apy=3.012, draft_year=2024, draft_round=1),
            dict(otc_id=1, gsis_id='p1', player='Test Player', position='QB', team='NEW', year_signed=2027, is_active=False, apy=60, draft_year=2024, draft_round=1),
        ]),
        player_stats=pd.DataFrame([
            dict(player_id='p1', season=2026, week=1, season_type='REG', game_id='g1', team='OLD', passing_epa=10, rushing_epa=2, receiving_epa=0),
            dict(player_id='p1', season=2026, week=2, season_type='REG', game_id='g2', team='NEW', passing_epa=90, rushing_epa=0, receiving_epa=0),
            dict(player_id='p1', season=2026, week=19, season_type='POST', game_id='post', team='NEW', passing_epa=100, rushing_epa=0, receiving_epa=0),
        ]),
        snap_counts=pd.DataFrame([
            dict(pfr_player_id='pfr1', season=2026, week=1, game_type='REG', game_id='g1', offense_snaps=50, defense_snaps=0),
            dict(pfr_player_id='pfr1', season=2026, week=19, game_type='POST', game_id='post', offense_snaps=100, defense_snaps=0),
        ]),
        rosters=pd.DataFrame([
            dict(gsis_id='p1', team='OLD', position='QB', years_exp=2, birth_date='2000-01-01', entry_year=2024, week=1),
            dict(gsis_id='p1', team='NEW', position='QB', years_exp=2, birth_date='2000-01-01', entry_year=2024, week=2),
        ]),
    )


def test_regular_season_matching_games_and_asof_contract(tables):
    metrics, audit, unmatched = build_roster_roi(2026, **tables, min_snaps=100)
    row = metrics.iloc[0]
    assert row.total_epa == 12
    assert row.snaps == 50
    assert row.team == 'NEW'
    assert row.yearly_cap_hit == 3.012
    assert row.cap_pct_of_team == pytest.approx(.01)
    assert row.cost_per_epa == pytest.approx(3.012 / 12)
    assert row.total_epa == row.passing_epa + row.rushing_epa + row.receiving_epa
    assert row.sample_flag == 'low_sample'
    assert row.is_rookie_deal
    assert '1 games excluded' in row.notes
    assert unmatched.empty


def test_missing_production_cannot_publish_zero_epa(tables):
    tables['snap_counts']['game_id'] = 'unknown'
    with pytest.raises(ValueError, match='No common'):
        build_roster_roi(2026, **tables)


def test_contract_mapping_audit_precedes_roster_filter(tables):
    tables['contracts'] = pd.concat([tables['contracts'], pd.DataFrame([
        dict(otc_id=99, gsis_id=None, player='Unmapped Player', year_signed=2026, is_active=True, apy=1, position='WR')
    ])], ignore_index=True)
    _, _, unmatched = build_roster_roi(2026, **tables)
    assert unmatched.player_name.tolist() == ['Unmapped Player']


def test_shrinkage_does_not_depend_on_other_seasons():
    base = pd.DataFrame(dict(season=[2026, 2026], position=['QB', 'QB'], total_epa=[10., 0.], snaps=[100, 100], yearly_cap_hit=[10., 1.]))
    historical = base.copy()
    historical['season'] = 2025
    historical['total_epa'] = 1000
    together = shrink_total_epa(pd.concat([base, historical], ignore_index=True))
    alone = shrink_total_epa(base)
    pd.testing.assert_series_equal(together.iloc[:2].total_epa_shrunk, alone.total_epa_shrunk)


def test_rookie_window_and_extensions():
    frame = pd.DataFrame(dict(
        year_signed=[2021, 2021, 2025, 2024, 2026],
        draft_year=[2021, 2021, 2024, None, None],
        draft_round=[1, 2, 1, None, None],
        entry_year=[2021, 2021, 2024, 2024, 2024],
        years_exp=[4, 4, 1, 2, 2],
    ))
    assert rookie_contract_mask(frame, 2025).tolist() == [True, False, False, True, False]
    assert not rookie_contract_mask(frame.iloc[:1], 2026).iloc[0]


def test_unknown_cost_and_nonpositive_epa_are_not_bargains():
    frame = pd.DataFrame(dict(passing_epa=[1, -2, 0], rushing_epa=[0, 0, 0], receiving_epa=[0, 0, 0], snaps=[10, 10, 0], yearly_cap_hit=[0, 2, 2]))
    result = compute_core_metrics(frame)
    assert result.cost_per_epa.isna().all()
    assert result.epa_per_snap.iloc[-1] == 0
