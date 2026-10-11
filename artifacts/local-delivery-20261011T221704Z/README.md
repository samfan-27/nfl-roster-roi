# Completed local delivery

Read FINAL_REPORT.md. summary.json contains machine-readable scope and counts.
current_source_receipts.csv identifies archived raw data by URL, receipt time and SHA256;
raw data remain at those immutable referenced paths. current_game_coverage.csv lists
all scheduled games. current_roster_eligibility_usage.csv retains every roster record;
current_source_row_usage.csv and current_component_usage.csv retain every measured
source/component row and its use or exclusion reason. Empty numeric cells are unknown
or unrecorded, not imputed zero. team_overview_32.csv separates roster inventory
from actual game-team component attribution. PIT observations are descriptive.

historical_eligibility_usage.csv retains the full extended transition inventory and
archived fold uses, primary-score flags and conflict annotations. The separate pinned
sample/folds and price eligibility/folds/predictions are byte copies. Price use flags
are membership in saved timing folds; training/test sets overlap across years.
No historical fitted result was recalculated or newly certified. Historical and
current data are not independent prospective forecasts.

Build: .venv/bin/python scripts/complete_local_delivery.py --output <fresh directory>
Verify: .venv/bin/python scripts/verify_local_delivery.py --output <directory>
The fresh directory needs environment.json captured from the existing Python 3.12
runtime. Supplementary pinned/price inventories are copied from the saved baseline
manifest after checking their hashes. Their use flags are set membership in the
saved folds, and price descriptive skill is one minus squared RMSE ratio. Final
reports/documents are packaged separately from the bounded builder. All file hashes,
input references and code/document snapshots are in manifest.json.

previous_documents/ preserves the complete incoming handoff/status documents;
workspace_snapshot/ contains final documents and the two reporting scripts.
The dated observation scope amendment preserves old protocols and failure records.
This package is the end of the authorized current delivery. Unknown future accuracy
and monetary value are limitations, not pending work or scheduled follow-ups.
