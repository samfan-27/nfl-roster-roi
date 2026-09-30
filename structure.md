# Repository structure

```text
.github/workflows/
  update.yml                 Daily current-season refresh; manual backfill
  database-health.yml        Independent database SELECT every six hours
  tests.yml                  Pull-request and main-branch validation
etl/
  sources.py                 Source extraction, schema checks, and cache policy
  database.py                Supabase operations and serialization
  etl.py                     ETL orchestration
  analyze.py                 Analysis artifact orchestration
  healthcheck.py             Database availability CLI
  utils.py                   Artifact writing
  config.py                  Batch size
src/
  domain.py                  NFL assumptions and salary caps; no I/O
  analysis.py                Pure roster joins and season ROI
  stats_helpers.py           Pure metrics, shrinkage, and team aggregation
  valuation.py               Completed-season APY model and nested validation
  reporting.py               Same-week comparison and value candidates
streamlit_app/
  app.py                     Page router and pipeline status
  components/
    data_utils.py            Cached, paginated Supabase reads
    season_picker.py         Available seasons and coverage presentation
    charts.py                Plotly figures
  views/                     Home, position, team, and player pages
  utils/fmt.py               Display formatting
notebooks/                   Shared-model research entry points
infra/supabase/ddl.sql        Idempotent schema setup
artifacts/                   Ignored local datasets, backups, and reports
docs/                        Operational notes and dated analysis
tests/                       Domain, pipeline, and dashboard checks
pytest.ini                   Repository-root imports and test discovery
```

The domain package never imports ETL. The ETL and dashboard depend on domain calculations, with credentials and HTTP isolated in infrastructure modules. See README.md for operations and methodological limits.
