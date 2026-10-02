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

Regular-season production uses games shared by weekly nflverse statistics and PFR snaps. Raw passing, rushing and receiving EPA are preserved; passing and receiving attributions overlap. Offensive snaps, official opportunities and verified PBP EPA opportunities have separate definitions.

Contract APY (`contract_apy_m`), annual cap charge (`season_cap_charge_m`) and annual cash (`season_cash_m`) are distinct, in millions. `yearly_cap_hit` is a deprecated APY alias. Unknown or ambiguous financial values remain unknown; traded-player costs require compatible team/production scope. Source contract mechanisms accompany estimated rookie labels.

Dashboard **Positive-EPA APY efficiency** ranks observed ratios within position and contract mechanism. Annual cap and cash options answer separate expenditure questions. Contribution volume and opportunities accompany efficiency. Negative EPA is retained in contribution research; it is not a replacement-level definition.

## Phase 1–3 research

The completed-season Ridge association saves whole-player held-out predictions, including historical rookies. Mean/median baselines, dollar errors, bias and chronological associations accompany the output. The inverse log target is a transformed geometric center, not an arithmetic mean or causal player value. Training-only cap-share bounds apply in both validation and scoring.

Verified game-level component models learn hierarchical pooling and compare later observed production. Acquisition-based replacement cohorts and game/week rank sensitivity remain explicit proxies. Event models compare mean-price Ridge/Gamma and median quantile predictions on later, player-purged contract events, using conservative pre-signing features. Preseason forecasts include future zero outcomes and benchmark rate/volume products against direct EPA prediction.

No incomplete season enters annual-volume fitting or scoring. Equivalent APY pricing discounts require verified compatible terms. Economic roster surplus requires validated replacement forecasts, an explicit contribution-price assumption and complete horizon-specific cap/cash/guarantee/exit schedules; unsupported quantities stay unknown.

Read [implementation and reproduction](docs/apy-methodology-implementation.md) and [validation results](docs/apy-methodology-validation.md). Before refreshing an existing deployment, apply the additive `20261002_apy_methodology.sql` migration after the existing cloud migrations. Core numerical dependencies are pinned; inputs, folds, seeds and dependency versions are archived.

```bash
# Read-only canonical downloads; then offline Phase 1–3 research.
PYTHONPATH=. python scripts/download_methodology_inputs.py
PYTHONPATH=. python scripts/download_epa_exposures.py
python -m etl.research

# Local annual analysis requires corrected history with offensive snap scope.
python -m etl.analyze --input artifacts/methodology-v2/results/historical_metrics.csv
```

## Engineering boundaries

- `src/domain.py`, `src/contracts.py`, `src/opportunities.py`: NFL definitions, identity/financial reconciliation and EPA exposure scope.
- `src/analysis.py`, `src/stats_helpers.py`, `src/valuation.py`, `src/contribution.py`, `src/pricing.py`, `src/forecasting.py`, `src/roster_decisions.py`, `src/reporting.py`: pure calculations and validation without credentials or database writes.
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
