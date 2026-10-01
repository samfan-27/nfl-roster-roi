import pandas as pd
import pytest
from etl.coverage import assess_coverage,require_ready,CoverageError,CoverageNotReady,through_week


def fixture_games(rows):
    schedule=pd.DataFrame([dict(season=2026,game_type='REG',game_id=g,week=w,
                         gameday='2026-09-20',gametime='13:00',home_score=10 if done else None,
                         away_score=3 if done else None) for g,w,done in rows])
    def sources(ids):
        return dict(player_stats=pd.DataFrame([dict(season=2026,season_type='REG',game_id=g,week=w) for g,w in ids],columns=['season','season_type','game_id','week']),
                    snap_counts=pd.DataFrame([dict(season=2026,game_type='REG',game_id=g,week=w) for g,w in ids],columns=['season','game_type','game_id','week']))
    return schedule,sources


def test_thursday_does_not_advance_partial_week_and_byes_need_no_fixed_game_count():
    schedule,sources=fixture_games([('g1',1,True),('g2',2,True),('g3',2,False)])
    coverage=assess_coverage(2026,schedule,sources([('g1',1),('g2',2)]))
    assert coverage['cutoff_week']==1
    assert coverage['per_week'][0]['scheduled']==1
    require_ready(coverage)


def test_both_sources_stale_are_rejected_after_grace():
    schedule,sources=fixture_games([('g1',1,True),('g2',2,True)])
    coverage=assess_coverage(2026,schedule,sources([('g1',1)]),now='2026-09-24T00:00:00Z')
    assert coverage['missing_stats']==coverage['missing_snaps']==['g2']
    with pytest.raises(CoverageError,match='beyond grace'):require_ready(coverage)


def test_missing_recent_game_defers_ingestion_but_preserves_prior_complete_week():
    schedule,sources=fixture_games([('g1',1,True),('g2',2,True)])
    coverage=assess_coverage(2026,schedule,sources([('g1',1)]),now='2026-09-21T00:00:00Z')
    assert coverage['grace_games']==['g2'] and coverage['cutoff_week']==1
    require_ready(coverage)
    with pytest.raises(CoverageError,match='Historical'):require_ready(coverage,historical=True)


def test_regression_detected_even_if_schedule_regresses():
    schedule,sources=fixture_games([('g1',1,True)])
    with pytest.raises(CoverageError,match='regressed'):
        assess_coverage(2026,schedule,sources([('g1',1)]),previous_games=['g1','g2'])


def test_no_new_games_does_not_trigger_stale_alarm():
    schedule,sources=fixture_games([('g1',1,True),('g2',2,False)])
    coverage=assess_coverage(2026,schedule,sources([('g1',1)]),now='2026-12-01T00:00:00Z')
    assert coverage['overdue_games']==[]
    require_ready(coverage)


def test_postponed_game_blocks_its_week_and_future_weeks():
    schedule,sources=fixture_games([('g1',1,True),('g2',2,False),('g3',3,True)])
    assert assess_coverage(2026,schedule,sources([('g1',1),('g3',3)]))['cutoff_week']==1


def test_no_week_ready_and_source_week_mismatch():
    schedule,sources=fixture_games([('g1',1,True)])
    coverage=assess_coverage(2026,schedule,sources([]),now='2026-09-21T00:00:00Z')
    with pytest.raises(CoverageNotReady):require_ready(coverage)
    with pytest.raises(CoverageError,match='weeks disagree'):
        assess_coverage(2026,schedule,sources([('g1',2)]))


def test_cutoff_also_truncates_weekly_roster():
    frame=pd.DataFrame(dict(week=[1,2],team=['OLD','NEW']))
    assert through_week(dict(rosters=frame),1)['rosters'].team.tolist()==['OLD']


def test_absent_cancelled_2022_game_is_documented_without_fabricating_production():
    schedule,sources=fixture_games([('g1',1,True)])
    schedule['season']=2022
    tables=sources([('g1',1)])
    for frame in tables.values():frame['season']=2022
    coverage=assess_coverage(2022,schedule,tables,exclusions={'2022_17_BUF_CIN':'Official NFL cancellation'})
    assert coverage['exclusions'] and coverage['shared_games']==['g1']
    with pytest.raises(CoverageError,match='Unknown exclusion'):
        assess_coverage(2022,schedule,tables,exclusions={'typo':'reason'})
