# Cloud storage and weekly analysis operations

The selected execution plan is **free GitHub Actions + existing Supabase Free**.
The user declined GitLab and does not have a Google Cloud project. No paid
subscription, GCP project or independent scheduler is provisioned. GitHub
schedule delay/inactivity and Supabase Free pausing remain operational limits.

## Data lifecycle

`python -m etl.cloud refresh` fetches current releases and the schedule, checks
coverage, archives sources and audits, stages calculated player-season rows,
and atomically commits dashboard upserts, the ingestion pointer and success.
The existing `roster_roi` read contract is preserved, including retention of
omitted rows. Canonical research never trains on those retained dashboard rows.

`python -m etl.cloud bootstrap --season 2025 --exclusions infra/coverage/2025.json`
creates a separately validated completed-season historical snapshot. Run one
season at a time for 2021–2025. Initialize 2025 before weekly analysis. Historical
bootstrap does not replace dashboard history. Back up the retained dashboard
with `python -m etl.backup` before rebuilding canonical history. Backup manifests
are indexed in the private `pipeline_backups` table. For the first migration,
use `python -m etl.backup --local artifacts/backups` instead.

`python -m etl.cloud weekly` (also `python -m etl.analyze --cloud`) starts with no
local CSV/cache. It fetches current releases, selects the latest contiguous
fully completed and fully covered week, reads historical heads in stable pages,
verifies private object hashes, rebuilds last season at the same cutoff using
its archived inputs, and calls shared `src` reporting and valuation functions.
The cutoff also applies to weekly roster rows. Annual models use only validated
prior completed seasons. Historical scoring and diagnostics are research, not
future salary forecasts; no partial current season enters training or scoring.

Before publication, all source, result, report and manifest objects are uploaded
and downloaded to verify their SHA256 and byte length. The publication RPC
atomically inserts the complete public report and advances its season pointer.
Failure retains the previous report. A source correction produces an additional
immutable version; an identical input fingerprint reuses the existing version.
A duplicate older retry cannot roll the pointer back over a newer correction.

The **Weekly Reports** page reads only anonymous-access publication, pointer and
sanitized status tables. Users can inspect past versions and download reports
and comparisons. It never receives service-role credentials, private source
paths or signed Storage URLs. Private Storage has no anonymous read policies.
Unpublished model results, audits, manifests and backups remain private.

## Coverage rules

- Schedule rows count as completed only when both final scores are present.
- Every production game must agree with its schedule week. Each source and their
  intersection are compared against completed regular-season games.
- Default grace is 48 hours after kickoff plus a conservative four-hour game
  duration estimate. The schedule does not supply final-whistle timestamps.
- Missing games beyond grace fail. A refresh missing games within grace defers
  without replacing dashboard data. Weekly analysis may publish an earlier full
  week while later games remain inside grace; its metadata records them.
- A Thursday game does not advance the report cutoff while its week is partial.
  Postponed games block their week and later weeks. Bye weeks do not require a
  fixed number of games; matched-week counts are disclosed by season.
- Previously ingested games disappearing from either source fail as regression.
  Unchanged data with no newly completed game is valid.
- Completed-season inputs require coverage through the final regular-season
  week (18 since 2021) with no missing games. The NFL-cancelled 2022 BUF–CIN game
  is explicitly documented in `infra/coverage/2022.json`; current nflverse
  schedules omit it. No arbitrary missing game is silently waived.
- Missing source releases defer, schema/download/database failures fail, and
  no zero-filled substitute season is published.

## Migration and activation

1. Save a local backup. Review and apply
   `infra/supabase/migrations/20261001_cloud_pipeline.sql` as database owner in
   Supabase SQL Editor, followed by `20261001_snapshot_regression.sql`. These
   are one-time additive transactions, not the bootstrap
   DDL. They never drop the existing tables. Do not rerun after successful commit.
2. Verify the new private bucket, service-role RPCs and anonymous boundaries.
3. Initialize completed-season snapshots and run `python -m etl.backup` once.
4. Run a clean-container weekly job and verify the publication, private manifest
   hashes and anonymous report access. Run daily refresh and verify dashboard
   count/status; deliberately test failed writes only in an isolated database.
5. Merge the implementation PR into `main` after review. Set repository variable
   `CLOUD_PIPELINE_ENABLED=true` only after the successful manual run. Existing
   `update.yml` then delegates to the shared cloud runner. New weekly scheduled
   jobs are gated by that variable; manual dispatch remains available for setup.
6. Merge the separate deployment update into `feature/streamlit-skeleton` to
   publish the new UI. The entrypoint stays `streamlit_app/app.py`. Main-only edits
   do not deploy the Streamlit app; deployment-branch customizations are preserved.

Daily refresh keeps `23 16 * * *` UTC. Weekly attempts use Thursday 16:37 and
17:37 UTC to cover midday New York in both DST states; Friday 17:37 UTC is one
bounded later retry. Same-data repeats are idempotent. Database health retains
its independent six-hour Actions workflow and real SELECT query.

### Methodology v2 rollout on an active pipeline

Apply `infra/supabase/migrations/20261002_apy_methodology.sql` after the cloud
migrations and before a refresh or weekly analysis with v2 code. Back up the
retained dashboard rows and metadata first; inspect the current schema rather
than assuming a passing CI migration test applied anything to production.
The migration is additive and supports repeated application. It preserves the
existing fenced transaction and retained rows while allowing unknown APY and
adding explicit annual cap/cash and provenance fields.

The cloud runner checks the new schema before downloading or archiving source
data. Missing columns fail as `SchemaNotReady` with the exact migration path;
other database failures retain their own exception type. The availability-only
health check does not certify methodology readiness. If a rollout reaches an
enabled schedule before migration, apply the migration and retry the failed
run once, then verify the ingestion/publication head and saved financial fields.
Do not disable validation or turn missing contract costs into zeros to pass an
old NOT NULL constraint.

Before staging metrics, the runner serializes nullable signing years and counts
as integer JSON tokens. Pandas can represent a missing integer column as floats;
PostgreSQL rejects `2026.0` when populating an integer field from JSON. Missing
values stay null, fractional financial/rate values retain their precision, and
fractional/nonfinite/overflowing integer counts fail before the first staged write.
Failed database CLI results include only a recognized SQLSTATE/PostgREST code;
exception messages, request URLs and headers remain excluded.

## Locks, retries and diagnostics

All new writers/manual runs use the same Postgres lease. Each attempt receives
its own owner token, distinct from its stable provider execution run ID. Lease
TTL is 65 minutes; the executor task timeout must remain at most 55 minutes.
Run state, staging and final commit RPCs validate the token inside the transaction.
An expired worker cannot commit or release a new owner's lease. Contention and
source deferrals exit 75; failures exit 1. A successful execution ID retried by
its provider exits without downloading or republishing. A new scheduled attempt
has a new ID and can retry corrected sources. Limit external retries to two.

`pipeline_runs` separates ingestion/weekly/health/bootstrap results and stores
code revision, coverage, manifest and timestamps. Public `analysis_status` stores
only status and exception class; transport messages may contain credentials and
are not logged. A hard-killed worker may remain `running` until its lease expires;
check executor conclusion, timestamps, lock expiry and publication pointer.
A scheduler start response alone is not evidence of pipeline completion.

Keep GitHub failure notifications enabled. Do not automatically send alerts to
other people. Investigate an unsuccessful weekly run or no new complete-week
publication by Friday evening. A deferral with unchanged schedule may be expected
offseason; compare coverage instead of alarming on elapsed time alone.

## Free-tier storage budget

Sources and outputs use compressed Parquet and content-addressed immutable
objects; unchanged inputs are reused across runs. The default private archive
budget is **850 MB**, leaving headroom below the existing Free 1 GB file quota.
This is an application bucket budget, not a measurement of other buckets or
organization-wide usage. Failed uploads reserve bytes conservatively; reconcile
inventory before reclaiming reservations. No automatic deletion or paid upgrade
is performed. If the budget is reached, the job fails and preserves prior output.
Monitor the Supabase dashboard for total database, file storage and egress usage;
free retention is finite and indefinite daily correction archives cannot be
promised. Archive approved older data elsewhere before any deliberate deletion.

The Storage readback checks consume egress. Unchanged objects are verified too;
watch monthly usage when changing frequency or history size. See
[Supabase billing quotas](https://supabase.com/docs/guides/platform/billing-on-supabase).

## Optional independent Cloud Run deployment (not activated)

`Dockerfile` supplies the same Python 3.12 runner; `.dockerignore` excludes local
secrets, data, ignored notes and Git history. For a future explicitly approved
GCP deployment, choose a real project/region with billing and enable Run, Scheduler,
Secret Manager, Artifact Registry and Cloud Build APIs. Free quotas still require
billing; this option is not the selected free GitHub plan.

Build an **amd64** image, label it with its Git revision and push it to the chosen
private Artifact Registry repository. Pass its immutable digest to
`infra/cloud-run/deploy.sh`. Provision named secrets `nfl-roi-supabase-url` and
`nfl-roi-supabase-service-key` out of band; never pass secret values in CLI flags,
Docker build arguments or committed configuration. Pin numerical secret versions.
The script uses separate runtime and scheduler accounts, secret-specific reader
bindings and job-specific invoker bindings. Required configuration names are
listed in the script; no fictional project ID or account is supplied.

Deploy creates/updates paused schedules for daily, weekly, Friday retry and health
in `America/New_York`. Cloud Scheduler uses OAuth for the Google Run Jobs API.
A created schedule must be paused immediately; do provisioning outside its
scheduled minute to avoid an initial trigger before pause. Verify one manually
executed job with `gcloud run jobs execute ... --wait`, then one externally
scheduled execution, its `pipeline_runs` completion and publication. Configure
Cloud Monitoring alerts on failed Run executions and on stale ingestion/report
state; configure notification destinations only with owner authorization.

Only after that proof, deliberately remove/pause GitHub ETL, weekly and health
schedules, preserve CI/manual tools, and resume all chosen GCP schedules. The
shared cloud lock protects overlap only after all writers use the new runner.
Do not enable independent jobs while the legacy unguarded ETL is still writing.
To update code, build a new revision-labeled image and redeploy its digest. To
roll back, redeploy the prior digest; never rewind publication pointers blindly.

References checked September 30, 2026:
[Cloud Run scheduling](https://docs.cloud.google.com/run/docs/execute/jobs-on-schedule),
[Cloud Run secrets](https://docs.cloud.google.com/run/docs/configuring/jobs/secrets),
[Storage permissions](https://supabase.com/docs/guides/storage/security/access-control),
[nflverse publication schedule](https://nflreadr.nflverse.com/articles/nflverse_data_schedule.html),
[GitHub schedule limitations](https://docs.github.com/en/actions/reference/workflows-and-actions/events-that-trigger-workflows#schedule).
