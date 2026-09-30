"""Pure NFL roster joins and ROI calculations on supplied source tables."""

import datetime
import pandas as pd

from src.domain import SALARY_CAP_MILLIONS, numeric, rookie_contract_mask
from src.stats_helpers import compute_core_metrics, shrink_total_epa


def build_roster_roi(season, *, players, contracts, player_stats, snap_counts,
                     rosters, min_snaps=100, shrink_tau=200.0):
    """Return metrics, join audit, and unmatched contracts for one season.

    Use regular-season games present in BOTH EPA and snap sources. A newer
    roster release is not evidence that production data is equally current.
    """
    if season not in SALARY_CAP_MILLIONS:
        raise ValueError(f"Salary cap is not configured for season {season}")
    stats = player_stats.loc[
        player_stats["season"].eq(season) & player_stats["season_type"].eq("REG")
    ].copy()
    snaps = snap_counts.loc[
        snap_counts["season"].eq(season) & snap_counts["game_type"].eq("REG")
    ].copy()
    stats_games = set(stats["game_id"].dropna())
    snaps_games = set(snaps["game_id"].dropna())
    games = stats_games & snaps_games
    if not games:
        raise ValueError(f"No common regular-season EPA and snap games for {season}")
    stats = stats.loc[stats["game_id"].isin(games)].sort_values(["week", "game_id"])
    snaps = snaps.loc[snaps["game_id"].isin(games)].copy()
    stats = stats.rename(columns={"player_id": "gsis_id", "team": "recent_team"})
    season_stats = stats.groupby("gsis_id", as_index=False).agg(
        passing_epa=("passing_epa", "sum"), rushing_epa=("rushing_epa", "sum"),
        receiving_epa=("receiving_epa", "sum"), recent_team=("recent_team", "last"),
        games_played=("game_id", "nunique"), through_week=("week", "max"),
    )
    snaps["total_snaps"] = numeric(snaps["offense_snaps"]) + numeric(snaps["defense_snaps"])
    season_snaps = snaps.groupby("pfr_player_id", as_index=False)["total_snaps"].sum()

    players = players.copy().rename(columns={"display_name": "player_name"})
    players["otc_id"] = numeric(players["otc_id"], fill=float("nan")).astype("Int64")
    players = players.dropna(subset=["otc_id"]).drop_duplicates("otc_id")
    contracts = contracts.copy()
    contracts["otc_id"] = numeric(contracts["otc_id"], fill=float("nan")).astype("Int64")
    contracts["year_signed"] = numeric(contracts["year_signed"], fill=float("nan"))
    contracts = contracts.loc[contracts["year_signed"].between(1, season)].dropna(subset=["otc_id"])
    # Current active status breaks ties only; it is not historical active status.
    contracts = contracts.sort_values(["year_signed", "is_active", "apy"], kind="stable")
    contracts = contracts.drop_duplicates("otc_id", keep="last")
    player_cols = ["otc_id", "gsis_id", "pfr_id", "player_name", "position", "draft_year", "draft_round"]
    merged = contracts.merge(players[player_cols], on="otc_id", how="left", suffixes=("", "_ply"), validate="one_to_one")
    merged["gsis_id"] = merged["gsis_id"].fillna(merged["gsis_id_ply"])
    unmatched = merged.loc[merged["gsis_id"].isna(), ["player", "otc_id"]].rename(columns={"player": "player_name"})
    merged = merged.dropna(subset=["gsis_id"]).drop_duplicates("gsis_id", keep="last")
    merged = merged.merge(season_stats, on="gsis_id", how="left", validate="one_to_one")
    merged = merged.merge(season_snaps, left_on="pfr_id", right_on="pfr_player_id", how="left", validate="many_to_one")

    roster = rosters.dropna(subset=["gsis_id"]).copy()
    if "week" in roster:
        roster = roster.sort_values("week", kind="stable")
    roster = roster.drop_duplicates("gsis_id", keep="last")
    roster["age"] = (pd.Timestamp(f"{season}-09-01") - pd.to_datetime(roster["birth_date"], errors="coerce")).dt.days / 365.25
    if "entry_year" not in roster:
        roster["entry_year"] = float("nan")
    merged = merged.merge(
        roster[["gsis_id", "team", "position", "age", "years_exp", "entry_year"]],
        on="gsis_id", how="left", suffixes=("", "_roster"), validate="one_to_one",
    )
    merged = merged.loc[merged["gsis_id"].isin(roster["gsis_id"]) | merged["total_snaps"].gt(0)].copy()
    merged["team"] = merged["team_roster"].fillna(merged["recent_team"])
    pos = merged["position_roster"].fillna(merged["position_ply"]).fillna(merged["position"])
    fallback = merged["position_ply"].fillna(merged["position"])
    merged["position"] = pos.mask(pos.isin(["OL", "DB", "DL", "LB"]) & fallback.notna(), fallback)
    for field in ("draft_year", "draft_round"):
        merged[field] = merged[field].fillna(merged[field + "_ply"])

    out = pd.DataFrame(index=merged.index)
    out["season"] = season
    out["player_name"] = merged["player_name"].fillna(merged["player"])
    for column in ("gsis_id", "otc_id", "team", "position", "age", "years_exp"):
        out[column] = merged[column]
    out['years_exp'] = numeric(out['years_exp'], fill=float('nan')).astype('Int64')
    out["yearly_cap_hit"] = numeric(merged["apy"], fill=float("nan"))
    # Compatibility name: APY/current cap, not actual annual cap charge.
    out["cap_pct_of_team"] = out["yearly_cap_hit"] / SALARY_CAP_MILLIONS[season]
    for column in ("passing_epa", "rushing_epa", "receiving_epa"):
        out[column] = numeric(merged[column])
    out["snaps"] = numeric(merged["total_snaps"]).astype(int)
    out["is_rookie_deal"] = rookie_contract_mask(merged, season)
    out = compute_core_metrics(out)
    adjusted = shrink_total_epa(out, tau=shrink_tau)
    # Raw production remains consistent with the EPA composition chart.
    out["cost_per_epa_per_100_snaps"] = adjusted["cost_per_epa_per_100_snaps_shrunk"]
    out["sample_flag"] = "ok"
    out.loc[out["total_epa"].le(0), "sample_flag"] = "liability_or_zero"
    out.loc[out["snaps"].lt(min_snaps), "sample_flag"] = "low_sample"
    out.loc[out["yearly_cap_hit"].le(0) | out["yearly_cap_hit"].isna(), "sample_flag"] = "missing_contract"
    week = int(stats["week"].max())
    coverage = f"Regular season through week {week}; {len(games)} shared games"
    excluded = len(stats_games ^ snaps_games)
    out["notes"] = (
        f"{coverage}; {excluded} games excluded for source mismatch. "
        "APY in millions; latest contract signed by season year; historical approximation. "
        "Raw EPA; normalized cost uses position shrinkage; rookie cohort is a heuristic."
    )
    out["updated_at"] = datetime.datetime.now(datetime.UTC).isoformat()
    out["epai_lower"] = None
    out["epai_upper"] = None
    # The deployed schema requires a known non-null APY. Audit omissions.
    out = out.loc[out["yearly_cap_hit"].notna()].copy()
    return out.reset_index(drop=True), merged.reset_index(drop=True), unmatched
