# NFL Roster ROI

A Streamlit dashboard for NFL offensive skill-position production and contract APY efficiency. Supabase stores player-season metrics; nflverse supplies rosters, weekly player statistics, PFR snaps, and OTC contract history.

[Live dashboard](https://nfl-roster-roi-g3ufrcpghavld9jpvwpnvj.streamlit.app/)

## Quick start

Use Python 3.12 from the repository root:

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

Create an untracked `.env`:

```dotenv
SUPABASE_URL=https://your-project.supabase.co
SUPABASE_SERVICE_ROLE_KEY=your-etl-service-role-key
SUPABASE_ANON_KEY=your-dashboard-anon-key
```

Use the service-role key only for ETL. Streamlit reads with the anon key. On Streamlit Community Cloud, configure `SUPABASE_URL` and `SUPABASE_ANON_KEY` in app secrets. Apply `infra/supabase/ddl.sql` for a new database. Existing deployments must have the player-season unique index and read policies shown there.

```bash
# Inspect the freshest source data without writing to Supabase.
python -m etl.etl --fresh --dry-run

# Refresh the current NFL season; dates in January belong to the prior season.
python -m etl.etl --fresh

# Explicit refresh or historical backfill.
python -m etl.etl --seasons 2026 --fresh
python -m etl.etl --auto --fresh

# Updated same-week comparison and completed-season research model.
python -m etl.analyze

python -m pytest -q
streamlit run streamlit_app/app.py
```

`--auto` backfills from 2021. The regular refresh only writes the requested seasons and retains other years in the local combined CSV. Database updates are idempotent upserts on `(season, gsis_id)`; they do not delete old rows. Legacy mode writes batches: a failed write is reported and a rerun completes the update. When cloud mode is enabled, staged rows and success metadata commit in one database transaction. Back up production data before a historical rebuild. Raw snapshots, debug joins, unmatched IDs, and model outputs stay in ignored `artifacts/`.

## Keeping Supabase available

Supabase Free projects can pause after insufficient database activity over seven days. A paid plan is the supported guarantee against inactivity pausing. Periodic requests on Free reduce risk, but do not guarantee availability or resume an already paused database.

The repository includes daily refresh and database health workflows, plus gated weekly analysis and manual history initialization:

- `update.yml`: daily at 16:23 UTC, refreshes the current season after source updates. Manual runs can request a full backfill.
- `database-health.yml`: every six hours at minute 41, performs an actual database SELECT even when NFL extraction fails.

Set repository Actions secrets `SUPABASE_URL` and `SUPABASE_SERVICE_ROLE_KEY`. Keep failure notifications enabled. The schedules take effect only when the files are on the default branch and Actions is enabled.

**GitHub also disables scheduled workflows in public repositories after 60 days without repository activity.** This repository was found in `disabled_inactivity` state on September 29, 2026. Re-enable disabled workflows under Actions, and maintain normal repository activity. A cron change committed by an authorized user reactivates a disabled scheduled workflow. If the repository will be unattended longer than 60 days, use an independent, always-on scheduler for `python -m etl.healthcheck` and `python -m etl.etl --fresh`, and monitor their completion. A paid Supabase plan prevents database inactivity pausing but does not schedule Python refreshes. The health workflow shares GitHub's inactivity limitation; it is not an exemption.

If Supabase is already paused, resume it from the project dashboard first. Follow the restore deadline in the project's own email/dashboard; do not assume a newer documentation window retroactively changes an older project's deadline.

Sources: [Supabase project pausing](https://supabase.com/docs/guides/platform/free-project-pausing), [GitHub schedule behavior](https://docs.github.com/en/actions/reference/workflows-and-actions/events-that-trigger-workflows#schedule), [nflverse update schedule](https://nflreadr.nflverse.com/articles/nflverse_data_schedule.html).

## Analysis methodology

The dataset contains 2021 onward, including 2026 when published. UI season choices come from stored data. Each season uses **regular-season games present in both weekly EPA and PFR snap sources**. Missing releases, incompatible schemas, and empty shared production coverage fail the refresh instead of publishing fabricated zero production. Each row records the week and shared-game count in `notes`.

- `total_epa`: raw passing + rushing + receiving EPA. Raw components, total, and EPA per snap remain consistent.
- `yearly_cap_hit`: retained database name for **contract APY in millions**, not actual annual cap charge.
- `cap_pct_of_team`: APY / that season's league cap, a fraction. It is neither actual team cap usage nor OTC's cap share at signing.
- `cost_per_epa`: APY / positive raw EPA, in millions. Unknown/zero cost and nonpositive EPA are excluded from value rankings.
- `cost_per_epa_per_100_snaps`: APY / (100 × EPA rate shrunk toward the same-season positional mean). Default shrinkage strength is 200 snaps; this is a heuristic.
- `is_rookie_deal`: estimated from signing year, draft year/round, and a bounded rookie window. Roster experience is not CBA accrued seasons; this flag does not determine legal ERFA status.

Contract selection uses the latest signing year no later than the analyzed season, with current active status and APY as same-year tie-breakers. Exact historical transaction dates and extension effective dates are not resolved. Team charts assign season production to the latest roster team and sum player-attributed EPA. Passing and receiving EPA overlap; the result is not net team offensive EPA or actual team cap spending.

**In-season caveat:** annual APY divided by partial-season EPA is not comparable to a completed-season ratio. The analysis report compares the current and prior season through the same week. Missing matches and trades still limit interpretation; rankings are descriptive.

## Completed-season APY research

`src/valuation.py` contains one Ridge model per position, using EPA, snaps, EPA/snap, age, and experience. The target is `log1p(100 × APY / season cap)`. Training uses estimated veteran contracts. Incomplete seasons are excluded from both training and annual-volume scoring. No unsupported full-season valuation is produced from a few weeks of data.

Nested `GroupKFold` keeps each player's seasons together. Hyperparameters are tuned inside each outer training fold. MAE and R² are computed from held-out players. Final-model fitted surplus estimates are research artifacts, not independently held-out valuations for every training row or predictions of future offers. Predicted APY is bounded at 110% of the historical position's maximum observed APY, and feature extrapolation is flagged.

`python -m etl.analyze` writes `latest_analysis.md`, `matched_week_comparison.csv`, `model_diagnostics.csv`, and `roster_roi_scored.csv`. Both notebooks call the shared calculation modules instead of maintaining separate salary-cap maps or models. The dashboard displays descriptive ROI; cloud mode adds published weekly research reports and version history.

## Engineering boundaries

- `src/domain.py`: NFL salary caps, positions, numeric coercion, and cohort assumptions.
- `src/analysis.py`, `src/stats_helpers.py`, `src/valuation.py`, `src/reporting.py`: pure dataframe calculations without HTTP, credentials, or database writes.
- `etl/sources.py`: nflverse loading, cache freshness, and source schema checks.
- `etl/database.py`: Supabase clients, availability checks, serialization, and upserts.
- `etl/etl.py`, `etl/analyze.py`: orchestration and artifact writing.
- `streamlit_app/`: cached/paginated reads, season selection, freshness/coverage presentation, charts, and views.
- `tests/`: domain edge cases, write failures, pagination, UI smoke checks, and model validation boundaries.

The NFL salary-cap table is explicit in `src/domain.py` and cites its source. Add a verified cap for future years; unknown caps fail loudly instead of silently using a historical median.

## Versioned cloud weekly reports

The pipeline can archive validated inputs, historical snapshots, calculations,
audits and report versions in private Supabase Storage and Postgres. A new
**Weekly Reports** page exposes only fully published results. The selected free
execution plan uses GitHub Actions; no GCP or GitLab service is required.

Apply the additive migration and initialize canonical completed-season history
before setting `CLOUD_PIPELINE_ENABLED=true` in repository variables. The daily
workflow then uses atomic cloud ingestion, and the gated Thursday/Friday workflow
publishes complete-week reports. Annual models exclude incomplete seasons.
Deployment and activation are separate from committing these files.

See [cloud operations](docs/cloud-operations.md) for migration, backup, verification,
free quota limits, scheduling, rollback and the optional unactivated Cloud Run
container assets. Supabase Free pausing and GitHub schedule inactivity/delays
remain limitations of the free plan.
