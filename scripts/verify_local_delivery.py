"""Check the delivery against raw current sources and unchanged archived bytes.

No historical re-audit, estimator fit, network access or prospective evaluation.
"""
import argparse
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]


def sha(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def verify(out):
    out = Path(out).resolve()
    source = ROOT / "artifacts/model-research-source-resolution-v1"
    read = lambda name: pd.read_csv(out / name)
    checks = {}
    inputs = json.loads((out / "input_hashes.json").read_text())
    assert all(sha(path) == value for path, value in inputs.items())
    checks["unchanged_input_files"] = len(inputs)
    prior = read("prior_manifest_verification.csv")
    assert all(sha(row.path) == row.sha256 for row in prior.itertuples())
    checks["prior_manifest_entries"] = len(prior)
    schedule = pd.read_parquet(source / "current-sources-03/schedule.parquet")
    schedule = schedule.loc[schedule.season.eq(2026) & schedule.game_type.eq("REG")]
    stats = pd.read_parquet(source / "current-sources-03/2026_player_stats.parquet")
    snaps = pd.read_parquet(source / "current-sources-03/2026_snap_counts.parquet")
    pbp = pd.read_parquet(source / "current-sources-03/2026_pbp.parquet")
    completed = set(schedule.loc[schedule.home_score.notna() & schedule.away_score.notna(), "game_id"])
    sets = {"stats": set(stats.loc[stats.season.eq(2026) & stats.season_type.eq("REG"), "game_id"]),
            "snaps": set(snaps.loc[snaps.season.eq(2026) & snaps.game_type.eq("REG"), "game_id"]),
            "pbp": set(pbp.loc[pbp.season.eq(2026) & pbp.season_type.eq("REG"), "game_id"])}
    shared = completed.intersection(*sets.values())
    games = read("current_game_coverage.csv")
    assert set(games.game_id) == set(schedule.game_id)
    assert set(games.loc[games.completed, "game_id"]) == completed
    assert set(games.loc[games.included, "game_id"]) == shared
    for name, ids in sets.items():
        assert set(games.loc[games[name], "game_id"]) == ids
    checks["raw_game_coverage"] = {"completed": len(completed), "shared": len(shared),
                                  "completed_source_gaps": len(completed - shared),
                                  "not_completed_at_snapshot": len(schedule) - len(completed)}

    raw_roster = pd.read_parquet(source / "current-sources-02/2026_rosters.parquet")
    roster = read("current_roster_eligibility_usage.csv")
    assert len(roster) == len(raw_roster) and roster.source_row_0.tolist() == list(raw_roster.index)
    for column in ["gsis_id", "team", "status", "full_name"]:
        pd.testing.assert_series_equal(roster[column], raw_roster[column], check_names=False)
    assert not roster.loc[~roster.eligible_for_descriptive_observation, "used_for_recorded_observation"].any()
    assert roster.loc[roster.identity_status.ne("resolved"), [c for c in roster if c.startswith("recorded_")]].isna().all().all()
    assert roster.loc[~roster.used_for_recorded_observation, [c for c in roster if c.startswith("recorded_")]].isna().all().all()
    assert not roster.forecast_validated.any()
    rows = read("current_source_row_usage.csv")
    assert len(rows) == len(stats) + len(snaps)
    for name, raw in [("stats", stats), ("snaps", snaps)]:
        part = rows.loc[rows.table.eq(name)]
        assert set(part.source_row_0) == set(raw.index) and not part.source_row_0.duplicated().any()
        assert set(part.loc[part.used, "game_id"]) <= shared
        assert set(part.loc[part.used, "gsis_id"]) <= set(roster.loc[roster.eligible_for_descriptive_observation, "gsis_id"])
    checks["full_roster_and_source_row_accounting"] = {"roster": len(roster), "stats": len(stats), "snaps": len(snaps)}

    # Recompute only current recorded components from PBP, independently of the
    # saved audit totals. This is bounded to 49 shared games, never old seasons.
    definitions = [("pass", "passer_player_id", "qb_epa", pbp.play_type.isin(["pass", "qb_spike"])),
                   ("rush", "rusher_player_id", "epa", pbp.play_type.isin(["run", "qb_kneel"])),
                   ("receive", "receiver_player_id", "epa", pbp.receiver_player_id.notna())]
    components = read("current_component_usage.csv")
    reconstructed = []
    for component, id_col, value_col, mask in definitions:
        part = pbp.loc[mask & pbp.game_id.isin(shared) & pbp[id_col].notna() & pbp[value_col].notna()]
        assert not (part.groupby(["game_id", id_col]).posteam.nunique() > 1).any()
        aggregate = part.groupby(["game_id", id_col]).agg(epa=(value_col, "sum"), opportunities=(value_col, "size"), team=("posteam", "first")).reset_index().rename(columns={id_col: "gsis_id"})
        aggregate["component"] = component
        reconstructed.append(aggregate)
    reconstructed = pd.concat(reconstructed, ignore_index=True)
    joint = components.merge(reconstructed, on=["game_id", "gsis_id", "component"], how="outer", suffixes=("_saved", "_raw"), indicator=True, validate="one_to_one")
    assert joint._merge.eq("both").all()
    assert np.allclose(joint.epa_saved, joint.epa_raw, rtol=0, atol=1e-12)
    assert joint.opportunities_saved.eq(joint.opportunities_raw).all()
    assert joint.team_saved.eq(joint.team_raw).all()
    checks["current_components_reconciled_to_raw_pbp"] = len(joint)
    checks["maximum_component_epa_error"] = float((joint.epa_saved - joint.epa_raw).abs().max())
    teams = read("team_overview_32.csv")
    assert len(teams) == 32 and not teams.team.duplicated().any()
    assert teams.roster_rows.sum() == len(roster)
    assert teams.completed_games.sum() == 2 * len(completed)
    assert teams.shared_games.sum() == 2 * len(shared)
    for row in teams.itertuples():
        own = schedule.loc[schedule.home_team.eq(row.team) | schedule.away_team.eq(row.team)]
        assert row.shared_games == len(set(own.game_id) & shared)
        for component in ["pass", "rush", "receive"]:
            part = components.loc[components.used & components.team.eq(row.team) & components.component.eq(component)]
            assert np.isclose(getattr(row, component + "_recorded_epa"), part.epa.sum(min_count=1), equal_nan=True)
            assert getattr(row, component + "_recorded_opportunities") == part.opportunities.sum()
    checks["team_inventory_coverage_and_component_aggregation"] = 32

    old = pd.read_csv(ROOT / "artifacts/model-research-v3/extended/sample_ledger.csv")
    historical = read("historical_eligibility_usage.csv")
    pd.testing.assert_frame_equal(historical[old.columns], old, check_exact=False, rtol=1e-13, atol=1e-13)
    assert len(historical) == 13490 and historical.strict_label_conflict_annotation.sum() == 3
    assert historical.strict_feature_or_label_conflict_annotation.sum() == 5
    checks["historical_eligibility_and_original_values_preserved"] = len(historical)
    for name, original in [("action_obligations.csv", source / "cost-update-01/action_obligations.csv"),
                           ("action_cost_reconciliation.csv", source / "cost-update-01/action_cost_reconciliation.csv")]:
        assert sha(out / name) == sha(original)
    obligations = read("action_obligations.csv")
    costs = read("action_cost_reconciliation.csv")
    assert len(obligations) == 336 and obligations.status.eq("unknown").sum() == 270
    assert costs.incremental_k_m.isna().all() and costs.status.eq("incomplete_obligations").all()
    pit = read("pit_case_observations.csv")
    assert len(pit) == 4 and not pit.compatible_remaining_season_forecast.any()
    for column in ["deployment_contribution_difference_status", "lambda_status", "surplus_status"]:
        assert pit[column].eq("unknown").all()
    checks["pit_cost_bytes_and_unknown_value_inputs_preserved"] = {"obligations": len(obligations), "unknown": 270, "incomplete_comparisons": len(costs)}
    summary = json.loads((out / "summary.json").read_text())
    assert summary["model_fits"] == summary["annual_forecasts_generated"] == 0
    assert not summary["forecast_validated"] and not summary["new_prospective_validation"]
    result = dict(passed=True, verified_at=datetime.now(timezone.utc).isoformat(), checks=checks,
                  scope="Finite byte checks and new delivery checks; current PBP only; no historical re-audit or model fit")
    (out / "verification.json").write_text(json.dumps(result, indent=2, allow_nan=False) + "\n")
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", required=True)
    verify(parser.parse_args().output)
