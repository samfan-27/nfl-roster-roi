"""Schedule-based readiness; no filesystem, credentials or domain calculations."""
from datetime import datetime, timezone
import pandas as pd


class CoverageNotReady(RuntimeError):
    """Upstream publication is still within the configured grace period."""


class CoverageError(ValueError):
    """Missing, regressed or inconsistent production cannot be published."""


def assess_coverage(season, schedule, tables, *, now=None, grace_hours=48,
                    previous_games=(), exclusions=None):
    now = pd.Timestamp(now or datetime.now(timezone.utc))
    if now.tzinfo is None:
        raise ValueError('Coverage clock must include a timezone')
    if grace_hours < 0:
        raise ValueError('Grace period must be non-negative')
    required = {'season', 'game_type', 'game_id', 'week', 'gameday', 'gametime', 'home_score', 'away_score'}
    if required - set(schedule):
        raise CoverageError(f'Schedule lacks {sorted(required - set(schedule))}')
    games = schedule.loc[schedule.season.eq(season) & schedule.game_type.eq('REG')].copy()
    if games.empty or games.game_id.isna().any() or games.game_id.duplicated().any():
        raise CoverageError('Empty schedule or invalid game IDs')
    exclusions = exclusions or {}
    if any(not str(v).strip() for v in exclusions.values()):
        raise CoverageError('Every exclusion needs a documented reason')
    absent = set(exclusions) - set(games.game_id)
    if absent - {'2022_17_BUF_CIN'}:
        raise CoverageError('Unknown exclusion is absent from the schedule')
    # nflverse omits the officially cancelled 2022 game entirely. Keep its
    # documented exclusion in the manifest even when there is no schedule row.
    games = games.loc[~games.game_id.isin(exclusions)]
    completed = games.home_score.notna() & games.away_score.notna()
    expected = set(games.loc[completed, 'game_id'])
    stats = tables['player_stats']
    snaps = tables['snap_counts']
    stats_ids = set(stats.loc[stats.season.eq(season) & stats.season_type.eq('REG'), 'game_id'].dropna()) - set(exclusions)
    snap_ids = set(snaps.loc[snaps.season.eq(season) & snaps.game_type.eq('REG'), 'game_id'].dropna()) - set(exclusions)
    shared = stats_ids & snap_ids
    if shared - expected or (stats_ids | snap_ids) - set(games.game_id):
        raise CoverageError('Production contains unscheduled or uncompleted games')
    # Verify each source assigns every game to the same week as the schedule.
    week_by_game = games.set_index('game_id').week
    for frame in (stats.loc[stats.game_id.isin(stats_ids)], snaps.loc[snaps.game_id.isin(snap_ids)]):
        if not frame.week.eq(frame.game_id.map(week_by_game)).all():
            raise CoverageError('Source weeks disagree with the schedule')
    regression = set(previous_games) - shared
    if regression:
        raise CoverageError(f'Coverage regressed: {sorted(regression)}')
    missing_stats, missing_snaps = expected - stats_ids, expected - snap_ids
    missing = missing_stats | missing_snaps
    overdue, pending = [], []
    for row in games.loc[games.game_id.isin(missing)].itertuples():
        # Schedules have kickoff, not final whistle. Four hours is an explicit
        # conservative end estimate, followed by the publication grace period.
        local = pd.Timestamp(f'{row.gameday} {row.gametime}')
        if pd.isna(local):
            raise CoverageError('Missing kickoff time for a completed missing game')
        end = local.tz_localize('America/New_York').tz_convert('UTC') + pd.Timedelta(hours=4 + grace_hours)
        (overdue if now >= end else pending).append(row.game_id)
    cutoff = 0
    for week, group in games.groupby('week', sort=True):
        if int(week) != cutoff + 1 or not set(group.game_id) <= expected & shared:
            break
        cutoff = int(week)
    final_week = 18 if season >= 2021 else 17
    complete = cutoff == final_week and set(games.game_id) == expected == shared
    return dict(season=int(season), cutoff_week=cutoff, completed_games=sorted(expected),
                shared_games=sorted(shared), stats_games=sorted(stats_ids), snap_games=sorted(snap_ids),
                missing_stats=sorted(missing_stats), missing_snaps=sorted(missing_snaps),
                overdue_games=sorted(overdue), grace_games=sorted(pending),
                complete_season=complete, exclusions=exclusions, grace_hours=grace_hours,
                assessed_at=now.isoformat(),
                per_week=[dict(week=int(w), scheduled=len(g), completed=int((g.home_score.notna() & g.away_score.notna()).sum()),
                               shared=len(set(g.game_id) & shared)) for w, g in games.groupby('week', sort=True)])


def require_ready(coverage, *, historical=False):
    if coverage['overdue_games']:
        raise CoverageError(f"Production missing beyond grace: {coverage['overdue_games']}")
    if historical and not coverage['complete_season']:
        raise CoverageError('Historical inputs require a fully covered completed regular season')
    if not coverage['shared_games'] or not coverage['cutoff_week']:
        raise CoverageNotReady('No fully covered completed week is available yet')


def through_week(tables, week):
    """Return independent tables with production and weekly rosters at cutoff."""
    return {name: frame.loc[frame.week.le(week)].copy() if 'week' in frame else frame.copy()
            for name, frame in tables.items()}
