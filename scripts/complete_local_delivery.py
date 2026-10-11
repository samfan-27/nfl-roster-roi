"""Package bounded observations and cached research; no fitting or network I/O.

Run with the existing Python 3.12 environment and a fresh, pre-created output
directory containing environment.json. Historical construction stays intact.
"""
import argparse
import hashlib
import json
from pathlib import Path
import shutil
import sys

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from src.domain import POSITION_FAMILIES

SOURCE = ROOT / "artifacts/model-research-source-resolution-v1"
AUDIT = SOURCE / "evidence-01/audit-2026"
IMPACT = ROOT / "artifacts/model-research-sample-impact-v1"
V3 = ROOT / "artifacts/model-research-v3"
INFO = ROOT / "artifacts/model-research-information-v1"
CORE = {"QB", "RB", "WR", "TE"}
TASKS = {("QB", "pass"), ("QB", "rush"), ("RB", "rush"),
         ("RB", "receive"), ("WR", "receive"), ("TE", "receive")}


def sha(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def write_json(path, value):
    path.write_text(json.dumps(value, indent=2, allow_nan=False) + "\n")


def main(output):
    out = Path(output).resolve()
    if not (out / "environment.json").is_file() or (out / "summary.json").exists():
        raise ValueError("Use a fresh output directory with captured environment")
    env = json.loads((out / "environment.json").read_text())
    expected = {"numpy": "2.4.2", "pandas": "2.3.3", "scikit-learn": "1.8.0",
                "scipy": "1.17.1", "pyarrow": "23.0.1", "xgboost": "3.2.0"}
    if sys.version_info[:2] != (3, 12) or any(env["dependencies"][k] != v for k, v in expected.items()):
        raise ValueError("Existing fixed Python 3.12 research environment required")
    inputs, integrity = {}, []

    def reference(path):
        path = Path(path).resolve()
        inputs[str(path)] = sha(path)
        return path

    def csv(path):
        return pd.read_csv(reference(path))

    def parquet(path):
        return pd.read_parquet(reference(path))

    def js(path):
        return json.loads(reference(path).read_text())

    def check(path, expected_hash, manifest):
        actual = sha(path)
        if actual != expected_hash:
            raise ValueError("Changed archived evidence: " + str(path))
        integrity.append(dict(path=str(path), sha256=actual, manifest=str(manifest), verified=True))

    # Verify finite saved evidence, not the historical measurement pipeline.
    for manifest in [SOURCE / "final-01/manifest.json", IMPACT / "final-01/manifest.json"]:
        document = js(manifest)
        for name, expected_hash in document["stage_artifacts"].items():
            check(manifest.parent.parent / name, expected_hash, manifest)
        for name, expected_hash in document["outputs"].items():
            check(manifest.parent / name, expected_hash, manifest)
    for manifest, names in [
        (V3 / "extended/baseline_manifest.json", ["sample_ledger.csv", "folds.json"]),
        (V3 / "checkpoint/manifest.json", ["price_reference_metrics.csv"]),
        (V3 / "challenger-checkpoint/manifest.json", ["comparisons.csv", "primary_metrics.csv"]),
    ]:
        document = js(manifest)
        for name in names:
            check(manifest.parent / name, document["outputs"][name], manifest)
    info_manifest = INFO / "final-stage-01/manifest.json"
    check(INFO / "experiment-01/comparisons.csv",
          js(info_manifest)["stage_artifacts"]["experiment-01/comparisons.csv"], info_manifest)
    pd.DataFrame(integrity).to_csv(out / "prior_manifest_verification.csv", index=False)

    receipts = []
    for folder, names in [(SOURCE / "current-sources-02", ["players.parquet", "2026_rosters.parquet"]),
                          (SOURCE / "current-sources-03", ["schedule.parquet", "2026_player_stats.parquet", "2026_snap_counts.parquet", "2026_pbp.parquet"])]:
        for row in js(folder / "manifest.json"):
            if row["name"] in names:
                check(Path(row["path"]), row["sha256"], folder / "manifest.json")
                reference(row["path"])
                receipts.append({k: row[k] for k in ["kind", "name", "path", "url", "fetched_at", "sha256", "bytes"]})
    pd.DataFrame(receipts).to_csv(out / "current_source_receipts.csv", index=False)

    roster = parquet(SOURCE / "current-sources-02/2026_rosters.parquet")
    schedule = parquet(SOURCE / "current-sources-03/schedule.parquet")
    stats = parquet(SOURCE / "current-sources-03/2026_player_stats.parquet")
    snaps = parquet(SOURCE / "current-sources-03/2026_snap_counts.parquet")
    games = csv(AUDIT / "game_checks.csv")
    pairs = csv(AUDIT / "identity_map.csv")
    spine = csv(AUDIT / "spine.csv")
    components = csv(AUDIT / "components.csv")
    if roster.gsis_id.dropna().duplicated().any() or spine.gsis_id.duplicated().any():
        raise ValueError("Nonunique roster inventory requires explicit handling")
    included = set(games.loc[games.included, "game_id"])
    current_schedule = schedule.loc[schedule.season.eq(2026) & schedule.game_type.eq("REG")].copy()
    current_schedule = current_schedule.merge(games, on=["season", "game_id"], validate="one_to_one")
    current_schedule.to_csv(out / "current_game_coverage.csv", index=False)

    ledger = roster[["season", "team", "gsis_id", "pfr_id", "full_name", "position", "status", "week"]].copy()
    ledger.insert(0, "source_row_0", roster.index)
    ledger["source_path"] = str(SOURCE / "current-sources-02/2026_rosters.parquet")
    ledger["source_sha256"] = sha(ledger.source_path.iloc[0])
    ledger["position"] = ledger.position.map(POSITION_FAMILIES).fillna(ledger.position)
    ledger = ledger.merge(spine[["gsis_id", "identity_status", "role_status"]], on="gsis_id", how="left", validate="one_to_one")
    ledger["identity_status"] = ledger.identity_status.fillna("missing_gsis")
    ledger["role_status"] = ledger.role_status.fillna("not_certified")
    ledger["core_scope"] = ledger.position.isin(CORE)
    ledger["eligible_for_descriptive_observation"] = ledger.core_scope & ledger.identity_status.eq("resolved") & ledger.role_status.eq("unique")
    good = set(ledger.loc[ledger.eligible_for_descriptive_observation, "gsis_id"])
    roster_ids = set(ledger.gsis_id.dropna())
    resolved_ids = set(ledger.loc[ledger.identity_status.eq("resolved"), "gsis_id"])
    core_ids = set(ledger.loc[ledger.core_scope, "gsis_id"].dropna())

    source_usage = []
    for name, frame, id_col in [("stats", stats, "player_id"), ("snaps", snaps, "pfr_player_id")]:
        frame = frame.copy()
        frame.insert(0, "source_row_0", frame.index)
        if name == "snaps":
            frame = frame.merge(pairs[["pfr_id", "gsis_id"]], left_on=id_col, right_on="pfr_id", how="left", validate="many_to_one")
        else:
            frame["gsis_id"] = frame[id_col]
        regular = frame.season.eq(2026) & frame["season_type" if name == "stats" else "game_type"].eq("REG")
        frame["used"] = regular & frame.game_id.isin(included) & frame.gsis_id.isin(good)
        frame["usage_reason"] = np.select(
            [~regular, ~frame.game_id.isin(included), frame.gsis_id.isna(),
             ~frame.gsis_id.isin(roster_ids), ~frame.gsis_id.isin(resolved_ids), ~frame.gsis_id.isin(core_ids)],
            ["outside_regular_season", "outside_shared_completed_games", "unresolved_provider_identity",
             "not_in_archived_roster", "identity_abstention", "outside_core_scope"],
            default="recorded_observation")
        source_path = SOURCE / ("current-sources-03/2026_player_stats.parquet" if name == "stats" else "current-sources-03/2026_snap_counts.parquet")
        keep = frame[["source_row_0", "game_id", id_col, "gsis_id", "team", "used", "usage_reason"]].rename(columns={id_col: "provider_id"})
        keep["source_path"] = str(source_path)
        keep["source_sha256"] = sha(source_path)
        keep["table"] = name
        source_usage.append(keep)
        used = frame.loc[frame.used]
        counts = used.groupby("gsis_id").game_id.nunique()
        ledger["recorded_" + name + "_game_rows"] = ledger.gsis_id.map(counts)
        if name == "snaps":
            ledger["recorded_offense_snaps"] = ledger.gsis_id.map(used.groupby("gsis_id").offense_snaps.sum(min_count=1))

    components.insert(0, "source_row_0", components.index)
    components = components.merge(stats[["game_id", "player_id", "team"]], left_on=["game_id", "gsis_id"], right_on=["game_id", "player_id"], how="left", validate="many_to_one")
    if components.team.isna().any():
        raise ValueError("Component game-team attribution is missing")
    components["position"] = components.gsis_id.map(ledger.set_index("gsis_id").position)
    components["used"] = components.gsis_id.isin(good) & pd.Series(
        [(p, c) in TASKS for p, c in zip(components.position, components.component)], index=components.index)
    components["usage_reason"] = np.where(components.used, "recorded_component_observation", "identity_or_component_scope_abstention")
    components["source_path"] = str(AUDIT / "components.csv")
    components["source_sha256"] = sha(AUDIT / "components.csv")
    components.to_csv(out / "current_component_usage.csv", index=False)
    pd.concat(source_usage, ignore_index=True).to_csv(out / "current_source_row_usage.csv", index=False)
    used_components = components.loc[components.used]
    ledger["recorded_component_game_rows"] = ledger.gsis_id.map(used_components.groupby("gsis_id").game_id.nunique())
    for component in ["pass", "rush", "receive"]:
        subset = used_components.loc[used_components.component.eq(component)]
        for column in ["epa", "opportunities"]:
            ledger["recorded_" + component + "_" + column] = ledger.gsis_id.map(subset.groupby("gsis_id")[column].sum(min_count=1))
    ledger["used_for_recorded_observation"] = ledger.eligible_for_descriptive_observation & ledger[["recorded_stats_game_rows", "recorded_snaps_game_rows", "recorded_component_game_rows"]].notna().any(axis=1)
    ledger["usage_reason"] = np.select(
        [~ledger.core_scope, ~ledger.identity_status.eq("resolved"), ~ledger.role_status.eq("unique"), ledger.used_for_recorded_observation],
        ["outside_core_scope", "identity_abstention", "ambiguous_role", "recorded_observation"],
        default="no_record_in_shared_games_unknown_outside_coverage")
    ledger["annual_forecast_status"] = "abstain_partial_season_input"
    ledger["forecast_validated"] = False
    ledger.to_csv(out / "current_roster_eligibility_usage.csv", index=False)

    team_rows = []
    for team in sorted(ledger.team.unique()):
        members = ledger.loc[ledger.team.eq(team)]
        active = members.loc[members.status.eq("ACT")]
        team_games = current_schedule.loc[current_schedule.home_team.eq(team) | current_schedule.away_team.eq(team)]
        row = dict(team=team, roster_rows=len(members), core_roster_rows=int(members.core_scope.sum()),
                   core_eligible=int(members.eligible_for_descriptive_observation.sum()),
                   core_observation_used=int(members.used_for_recorded_observation.sum()),
                   core_identity_abstentions=int((members.core_scope & ~members.identity_status.eq("resolved")).sum()),
                   active_core=int(active.core_scope.sum()), active_core_eligible=int(active.eligible_for_descriptive_observation.sum()),
                   active_core_observation_used=int(active.used_for_recorded_observation.sum()),
                   completed_games=int(team_games.completed.sum()), shared_games=int(team_games.included.sum()),
                   completed_source_gap_games=int((team_games.completed & ~team_games.included).sum()),
                   not_completed_at_snapshot=int((~team_games.completed).sum()),
                   result_type="descriptive_partial_observation", forecast_validated=False, roster_value_status="unknown")
        for component in ["pass", "rush", "receive"]:
            part = used_components.loc[used_components.team.eq(team) & used_components.component.eq(component)]
            row[component + "_recorded_epa"] = part.epa.sum(min_count=1)
            row[component + "_recorded_opportunities"] = part.opportunities.sum(min_count=1)
            row[component + "_recorded_players"] = part.gsis_id.nunique()
        team_rows.append(row)
    teams = pd.DataFrame(team_rows)
    teams.to_csv(out / "team_overview_32.csv", index=False)
    used_components.groupby(["team", "position", "component"]).agg(
        recorded_epa=("epa", "sum"), recorded_opportunities=("opportunities", "sum"),
        recorded_players=("gsis_id", "nunique"), recorded_player_game_rows=("game_id", "size")
    ).reset_index().to_csv(out / "team_position_component_observations.csv", index=False)

    # Complete saved eligibility, including unknown labels and unused members.
    historical = csv(V3 / "extended/sample_ledger.csv")
    folds = js(V3 / "extended/folds.json")
    for side in ["train", "test"]:
        uses = {}
        for fold in folds:
            for row_id in fold[side + "_ids"]:
                uses.setdefault(row_id, set()).add(fold["outer_year"])
        historical["archived_outer_" + side + "_years"] = historical.row_id.map(lambda x: ";".join(map(str, sorted(uses.get(x, set())))))
    annotations = csv(IMPACT / "audit-02/archived_observed_labels_with_unresolved_conflicts.csv")
    member_links = csv(IMPACT / "audit-02/saved_membership_links.csv")
    exceptions = csv(IMPACT / "audit-02/identity_sample_impact.csv")
    conflict_cases = set(exceptions.loc[exceptions.status.eq("provider_identity_conflict"), "case_id"])
    affected = set(member_links.loc[member_links.case_id.isin(conflict_cases) & member_links.dataset.eq("extended"), "row_id"])
    historical["archived_fit_or_test_used"] = historical.archived_outer_train_years.ne("") | historical.archived_outer_test_years.ne("")
    historical["archived_primary_scored"] = historical.archived_outer_test_years.ne("") & historical.test_season.between(2019, 2025)
    historical["strict_label_conflict_annotation"] = historical.row_id.isin(annotations.row_id)
    historical["strict_feature_or_label_conflict_annotation"] = historical.row_id.isin(affected)
    historical["current_use"] = "archived_evidence_only_no_new_fit_or_score"
    historical.to_csv(out / "historical_eligibility_usage.csv", index=False)
    for path in [IMPACT / "audit-02/identity_sample_impact.csv", IMPACT / "audit-02/identity_inventory.csv",
                 IMPACT / "audit-02/archived_observed_labels_with_unresolved_conflicts.csv",
                 SOURCE / "evidence-01/season_coverage.csv", SOURCE / "cost-update-01/action_obligations.csv",
                 SOURCE / "cost-update-01/action_cost_reconciliation.csv",
                 ROOT / "artifacts/model-research-decision-evidence-v1/case-selection-01/horizon-amendment.json"]:
        shutil.copy2(reference(path), out / path.name)
    historical.groupby(["test_season", "position", "component", "outcome_status"], dropna=False).agg(
        eligibility_rows=("row_id", "size"), archived_fit_or_test_used=("archived_fit_or_test_used", "sum"),
        archived_primary_scored=("archived_primary_scored", "sum")
    ).reset_index().to_csv(out / "historical_usage_counts.csv", index=False)
    comparisons = csv(V3 / "challenger-checkpoint/comparisons.csv")
    comparisons.loc[comparisons.experiment.eq("extended") & comparisons.baseline.eq("ridge_full")].to_csv(out / "cached_extended_contribution_comparisons.csv", index=False)
    csv(V3 / "challenger-checkpoint/primary_metrics.csv").to_csv(out / "cached_contribution_metrics.csv", index=False)
    csv(INFO / "experiment-01/comparisons.csv").to_csv(out / "cached_information_comparisons.csv", index=False)
    price = csv(V3 / "checkpoint/price_reference_metrics.csv")
    price.to_csv(out / "cached_price_metrics.csv", index=False)
    price_rows = []
    for (position, year), group in price.groupby(["position", "year"]):
        by_model = group.set_index("model")
        candidate, baseline = by_model.loc["ridge_no_mechanism"], by_model.loc["ridge_mean"]
        price_rows.append(dict(position=position, year=int(year), events=int(candidate.rows),
            ridge_mean_cap_share_rmse=baseline.cap_share_rmse, no_mechanism_cap_share_rmse=candidate.cap_share_rmse,
            descriptive_mse_skill=1-(candidate.cap_share_rmse/baseline.cap_share_rmse)**2,
            result_type="cached_retrospective_event_benchmark" if year == 2025 else "cached_observed_to_snapshot_event_benchmark"))
    pd.DataFrame(price_rows).to_csv(out / "cached_price_reference_contrasts.csv", index=False)

    alternatives = csv(ROOT / "artifacts/model-research-decision-evidence-v1/dossier-02/alternatives.csv")
    pit = alternatives.merge(ledger[["gsis_id", "identity_status", "used_for_recorded_observation", "recorded_receive_epa", "recorded_receive_opportunities", "recorded_offense_snaps", "recorded_snaps_game_rows"]], on="gsis_id", how="left", validate="one_to_one")
    pit["result_type"] = "recorded_observation_not_redeployment_effect"
    pit["incremental_cost_status"] = "incomplete_existing_ledger"
    pit["deployment_contribution_difference_status"] = "unknown"
    pit["lambda_status"] = "unknown"
    pit["surplus_status"] = "unknown"
    pit.to_csv(out / "pit_case_observations.csv", index=False)
    summary = dict(status="current_local_delivery_complete", source_snapshot_latest=max(r["fetched_at"] for r in receipts),
        roster_snapshot_at=next(r["fetched_at"] for r in receipts if r["kind"] == "roster"),
        regular_games=len(games), completed_games=int(games.completed.sum()), shared_games=len(included),
        completed_source_gaps=int((games.completed & ~games.included).sum()), not_completed_at_snapshot=int((~games.completed).sum()),
        roster_rows=len(ledger), core_rows=int(ledger.core_scope.sum()), core_eligible=int(ledger.eligible_for_descriptive_observation.sum()),
        core_observation_used=int(ledger.used_for_recorded_observation.sum()),
        core_unused=int((ledger.core_scope & ~ledger.used_for_recorded_observation).sum()),
        current_usage_reasons={str(k): int(v) for k, v in ledger.usage_reason.value_counts().items()},
        historical_rows=len(historical), historical_fit_or_test_used=int(historical.archived_fit_or_test_used.sum()),
        historical_primary_scored=int(historical.archived_primary_scored.sum()),
        historical_unknown_labels=int(historical.outcome_status.ne("observed").sum()),
        historical_label_conflict_rows=int(historical.strict_label_conflict_annotation.sum()),
        model_fits=0, new_retrieval=False, annual_forecasts_generated=0, forecast_validated=False,
        economic_surplus="unknown", new_prospective_validation=False,
        interpretation="Current observations and cached historical experiments; no annualization or deployment counterfactual")
    write_json(out / "summary.json", summary)
    write_json(out / "input_hashes.json", inputs)
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", required=True)
    main(parser.parse_args().output)
