"""Game-level attributed production; exposure is specific to each component.

Exact model opportunities follow nflfastR PBP EPA filters, including two-point
attempts. Official attempts + sacks, carries and targets are retained as role
features and cannot substitute for these exposures. Neither EPA ratios nor
context controls isolate player skill or allocate additive team value.
"""
import numpy as np
import pandas as pd

from src.domain import OFFENSIVE_POSITIONS, POSITION_FAMILIES

COMPONENTS = {"pass": ("passing_epa", "pass_epa_opportunities"),
              "rush": ("rushing_epa", "rush_epa_opportunities"),
              "receive": ("receiving_epa", "receive_epa_opportunities")}
COUNTS = ("attempts", "sacks_suffered", "carries", "targets")


def shared_regular_games(season, player_stats, snap_counts):
    stats = player_stats.loc[player_stats.season.eq(season) & player_stats.season_type.eq("REG")].copy()
    snaps = snap_counts.loc[snap_counts.season.eq(season) & snap_counts.game_type.eq("REG")].copy()
    games = set(stats.game_id.dropna()) & set(snaps.game_id.dropna())
    if not games:
        raise ValueError(f"No common regular-season EPA and snap games for {season}")
    return stats.loc[stats.game_id.isin(games)], snaps.loc[snaps.game_id.isin(games)]


def game_production(season, *, players, player_stats, snap_counts, rosters=None, play_by_play=None):
    stats, snaps = shared_regular_games(season, player_stats, snap_counts)
    if stats.duplicated(["player_id", "game_id"]).any() or snaps.duplicated(["pfr_player_id", "game_id"]).any():
        raise ValueError("Duplicate player-game source rows")
    ids = players[['gsis_id', 'pfr_id', 'position']].copy()
    if rosters is not None and 'pfr_id' in rosters:
        # Roster releases contain known PFR mislabels (e.g. Conklin/Izzo).
        # The master identity table takes precedence; never overwrite it.
        extra = rosters.loc[~rosters.gsis_id.isin(ids.dropna(subset=['pfr_id']).gsis_id), ['gsis_id','pfr_id','position']]
        extra = extra.loc[~extra.pfr_id.isin(ids.pfr_id)]
        ids = pd.concat([ids, extra], ignore_index=True)
    ids = ids.dropna(subset=['gsis_id','pfr_id']).drop_duplicates(['gsis_id','pfr_id'], keep='first')
    if ids.pfr_id.duplicated().any() or ids.gsis_id.duplicated().any():
        raise ValueError("Conflicting PFR/GSIS identities")
    snap = snaps.merge(ids, left_on="pfr_player_id", right_on="pfr_id", how="inner", suffixes=("", "_player"), validate="many_to_one")
    if "position_player" in snap:
        snap["position"] = snap.position.where(snap.position.isin(OFFENSIVE_POSITIONS), snap.position_player)
    snap = snap.rename(columns={"offense_snaps": "offensive_snaps"})
    stat = stats.rename(columns={"player_id": "gsis_id"})
    stat["stats_present"] = True
    columns = ["gsis_id", "game_id", "stats_present"]
    columns += [c for c in [*(v[0] for v in COMPONENTS.values()), *COUNTS,
                            "passing_cpoe", "target_share", "air_yards_share", "opponent_team"] if c in stat]
    out = snap.merge(stat[columns], on=["gsis_id", "game_id"], how="outer", validate="one_to_one")
    # Stats-only players retain their identity/position; missing snap coverage is visible.
    out["position"] = out.position.fillna(out.gsis_id.map(players.drop_duplicates("gsis_id").set_index("gsis_id").position))
    out['role_position'] = out.position
    if rosters is not None:
        roster_position = rosters.dropna(subset=['gsis_id']).drop_duplicates('gsis_id',keep='last').set_index('gsis_id').position
        out['position'] = out.gsis_id.map(roster_position).fillna(out.position)
    # nflverse rosters include fullbacks in the RB family; preserve their raw
    # participation role while retaining every family member's EPA/snaps.
    out['position'] = out.position.map(POSITION_FAMILIES).fillna(out.position)
    for c in ["season", "week", "team"]:
        out[c] = out[c].fillna(out.game_id.map(stat.drop_duplicates("game_id").set_index("game_id")[c])) if c != "team" else out[c]
    stats_index = stat.set_index(["gsis_id", "game_id"])
    missing_team = out.team.isna()
    if missing_team.any():
        out.loc[missing_team, "team"] = [stats_index.loc[(r.gsis_id, r.game_id), "team"] for r in out.loc[missing_team].itertuples()]
    out["offensive_snaps"] = pd.to_numeric(out.offensive_snaps, errors="coerce")
    if "opponent_team" not in out:
        out["opponent_team"] = out.get("opponent", "Unknown")
    elif "opponent" in out:
        out["opponent_team"] = out.opponent_team.fillna(out.opponent)
    for col in COUNTS:
        # Absence from a covered stats game means no recorded offensive opportunities.
        # An absent source column is schema/coverage uncertainty, never invented zero.
        if col in stat:
            out[col] = pd.to_numeric(out[col], errors="coerce").mask(out.stats_present.isna(), 0)
        else:
            out[col] = np.nan
        if out[col].lt(0).any():
            raise ValueError(f"Negative opportunity count: {col}")
    out["pass_opportunities"] = out.attempts + out.sacks_suffered
    official_exposures = ['pass_opportunities', 'carries', 'targets']
    for (epa, _), exposure in zip(COMPONENTS.values(), official_exposures):
        out[epa] = pd.to_numeric(out[epa], errors="coerce")
        out[epa] = out[epa].mask(out[exposure].eq(0) | out.stats_present.isna(), out[epa].fillna(0))
    out["total_epa"] = out[[v[0] for v in COMPONENTS.values()]].sum(axis=1)
    out["opportunity_status"] = np.where(out[list(COUNTS)].notna().all(axis=1), "observed", "missing_source_counts")
    out = out.loc[out.position.isin(OFFENSIVE_POSITIONS)].sort_values(["game_id", "gsis_id"]).reset_index(drop=True)
    if play_by_play is not None:
        out = reconcile_epa_exposures(out, play_by_play)
    else:
        out['epa_exposure_status'] = 'unverified_official_counts_exclude_two_point_attempts'
        for _, exposure in COMPONENTS.values():
            out[exposure] = np.nan
    return out


def validate_game_aggregation(metrics, games, tolerance=1e-6):
    """Require game component EPA and offense exposure to reproduce season rows."""
    season = metrics.loc[metrics.position.isin(OFFENSIVE_POSITIONS)].set_index(['season','gsis_id'])
    fields = ['passing_epa','rushing_epa','receiving_epa','snaps']
    aggregate = games.groupby(['season','gsis_id'])[['passing_epa','rushing_epa','receiving_epa','offensive_snaps']].sum()
    aggregate = aggregate.rename(columns={'offensive_snaps':'snaps'}).reindex(season.index,fill_value=0)
    if (season[fields]-aggregate).abs().gt(tolerance).any().any():
        raise ValueError('Game components/offensive snaps do not reproduce player-season metrics')


def reconcile_epa_exposures(games, play_by_play, tolerance=.002):
    """Count exactly the source EPA play filters, including two-point attempts.

    Verify fresh PBP against archived weekly EPA before using it. The tolerance
    admits historical weekly EPA rounding to three decimals, not model drift.
    Plays with missing EPA do not become known rate opportunities.
    """
    pbp = play_by_play.loc[play_by_play.game_id.isin(games.game_id) & play_by_play.season_type.eq('REG')]
    definitions = [('pass', 'passer_player_id', 'qb_epa', pbp.play_type.isin(['pass','qb_spike'])),
                   ('rush', 'rusher_player_id', 'epa', pbp.play_type.isin(['run','qb_kneel'])),
                   ('receive', 'receiver_player_id', 'epa', pbp.receiver_player_id.notna())]
    out = games.copy()
    for name, player, value, mask in definitions:
        plays = pbp.loc[mask & pbp[player].notna()].copy()
        valid = plays.loc[plays[value].notna()]
        aggregate = valid.groupby(['game_id', player])[value].agg(['sum','count']).reset_index().rename(
            columns={player:'gsis_id', 'sum':name+'_pbp_epa', 'count':COMPONENTS[name][1]})
        out = out.merge(aggregate, on=['game_id','gsis_id'], how='left', validate='one_to_one')
        epa, exposure = COMPONENTS[name]
        out[exposure] = out[exposure].fillna(0).astype(int)
        out[name+'_pbp_epa'] = out[name+'_pbp_epa'].fillna(0)
        out[name+'_epa_reconciliation_error'] = out[epa] - out[name+'_pbp_epa']
        if out[name+'_epa_reconciliation_error'].abs().gt(tolerance).any():
            raise ValueError(f'{name} PBP/weekly EPA mismatch beyond historical rounding tolerance')
        if (out[exposure].eq(0) & out[epa].fillna(0).abs().gt(tolerance)).any():
            raise ValueError(f'Nonzero {epa} without a verified EPA opportunity')
        unknown = plays.loc[plays[value].isna()].groupby(['game_id',player]).size()
        out[name+'_missing_epa_plays'] = pd.MultiIndex.from_frame(out[['game_id','gsis_id']]).map(unknown).fillna(0).to_numpy()
    out['epa_exposure_status'] = 'PBP EPA filters reconciled; missing-EPA plays excluded'
    return out


def component_rows(games):
    if not set(v[1] for v in COMPONENTS.values()).issubset(games) or games[[v[1] for v in COMPONENTS.values()]].isna().any().any():
        raise ValueError('Play-by-play exposure reconciliation is required for contribution modeling')
    rows = []
    for name, (epa, exposure) in COMPONENTS.items():
        part = games[["season", "week", "game_id", "gsis_id", "position", "team", "opponent_team", epa, exposure]].copy()
        part = part.rename(columns={epa: "epa", exposure: "opportunities"})
        part["component"] = name
        rows.append(part)
    return pd.concat(rows, ignore_index=True).dropna(subset=["epa", "opportunities"])


def production_sensitivity(frame, games, min_snaps=100, top_n=10):
    """Leave-one-week/game stress tests, holding full-window eligibility fixed."""
    eligible = frame.loc[frame.position.isin(OFFENSIVE_POSITIONS) & frame.snaps.ge(min_snaps) & frame.contract_apy_m.gt(0)]
    result = []
    for pos, cohort in eligible.groupby("position"):
        base = cohort.loc[cohort.total_epa.gt(0)].sort_values("cost_per_epa").head(top_n)
        ranked = {p: [] for p in cohort.gsis_id}
        top_counts = {p: 0 for p in cohort.gsis_id}
        sub = games.loc[games.gsis_id.isin(cohort.gsis_id)]
        for week in sorted(sub.week.unique()):
            removed = sub.loc[sub.week.eq(week)].groupby("gsis_id").total_epa.sum()
            shortened = cohort.set_index("gsis_id").copy()
            shortened["remaining_epa"] = shortened.total_epa - removed.reindex(shortened.index, fill_value=0)
            costs = (shortened.contract_apy_m / shortened.remaining_epa).where(shortened.remaining_epa.gt(0)).dropna()
            ranks = costs.rank(method="min")
            for pid in ranked:
                ranked[pid].append(ranks.get(pid, np.nan))
                top_counts[pid] += int(pid in costs.nsmallest(top_n).index)
        for row in cohort.itertuples():
            player = sub.loc[sub.gsis_id.eq(row.gsis_id)]
            shares = player.total_epa.abs().sum()
            values = np.asarray(ranked[row.gsis_id]); finite = values[np.isfinite(values)]
            result.append(dict(season=row.season, gsis_id=row.gsis_id, position=pos,
                               observed_games=player.game_id.nunique(),
                               **{exposure:player[exposure].sum(min_count=1) for _,exposure in COMPONENTS.values()},
                               largest_game_absolute_epa_share=player.total_epa.abs().max()/shares if shares else np.nan,
                               original_top_member=row.gsis_id in set(base.gsis_id),
                               leave_week_top_fraction=top_counts[row.gsis_id]/len(ranked[row.gsis_id]) if ranked[row.gsis_id] else np.nan,
                               leave_week_rank_min=finite.min() if len(finite) else np.nan,
                               leave_week_rank_max=finite.max() if len(finite) else np.nan))
    return pd.DataFrame(result)
