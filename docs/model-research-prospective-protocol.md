# Manual prospective confirmation protocol — October 3, 2026

## Estimand and proposed date

As of October 3, 2026 America/New_York, June 1, 2027 remains future. Proposed
cutoff: **June 1, 2027 23:59:59 America/New_York**. No real forecast is frozen,
no complete 2026/2027 labels are invented, and no automation is scheduled.
The local command must run on the actual cutoff day and rejects backdating.
Predictions created before the cutoff use only sources actually archived before
execution, with creation time separately recorded. A missed cutoff requires a
new genuinely future annual protocol, not a retrospective substitute.

Primary estimand: conditional arithmetic mean of one complete 2027 regular
season's signed attributed component EPA, equally weighted returning-player
transitions, separately QB/pass, QB/rush, RB/rush, RB/receive, WR/receive,
TE/receive. Eligibility is the complete prior-2026 roster/participation spine
with prior position fixed, including no prior component opportunities. No 2027
participation, team, contract, surviving identity or positive EPA filter. Entrants
and other positions are outside the claim. Prior-year ambiguous identity/role
requires explicit reconciliation or abstention, not unrecorded deletion. Unknown
future identity remains on the frozen spine and unknown at scoring.
Known covered absence means N=Y=0. N>0 may have negative Y; Y/N is undefined at
N=0. Passing/receiving attributions overlap and cannot be summed to team value.

## Procedures and claims fixed now

Use audited `ridge_full` with its existing FULL inputs, four-alpha grid
[1,10,100,1000], seed 20261002, earlier-only inner pooled MSE selection,
training median imputation/indicators/standardization and original numerical
failure logs. Generated pooled-rate histories must be built as of each feature
year with source and generator provenance; this verifier does not turn arbitrary
uploaded feature values into audited causal information.

References: original five-volume-feature alpha-10 multi-output `direct_ridge`
(extract its signed EPA prediction; do not clip EPA), and training mean. They
are named research references, not certified production models. No historical
per-position winner. The information protocol's joint inclusion rule excludes A:
no extended role reached 5%, and pinned WR skill is negative. B is unsupported.
No hurdle, neural, new information candidate or new search is silently added.

Six primary claims: audited versus original direct Ridge, one per core role.
Mean-reference comparisons are descriptive. New candidate inclusion requires a
separate amendment before any confirmation outcomes are observed. This design
confirms the information-rich reference's future predictive performance, not a
claim that A or complex algorithms already improved it.

## Manual input contract and archive

`src/prospective_research.py` exports exact `FEATURE_COLUMNS` and `TRAIN_COLUMNS`.
The feature CSV permits only row identity, prior-role/team/workload, decision and
measurement dates, FULL and original volume inputs; no future outcome columns.
Training CSV adds only earlier annual EPA/opportunity/game labels and their
availability dates. Each row ID must equal `year|gsis_id|component`. Folds and
transformations are fitted separately by role; future feature rows are given
unknown outcomes internally, never read from a label file. Minimum train support
is 20 rows/10 careers. A failed outer reference fit leaves a partial directory
and abstains; no favorable fallback confirmation is possible.

The evidence JSON supplies forecast_year, cutoff, `prior_spine_reconciled=true`,
`identity_map_verified=true`, and a `sources` ledger. Each record supplies absolute
path, SHA256, measurement_end, available_at, fetched_at (timezone-aware), provider
URL/name, version and kind. Both numerical CSVs must appear in this ledger.
For a real freeze require kinds: features, training, identity_map, schedule,
snaps, pbp, player_stats, roster. Include all underlying dependency inputs, exact
identity mapping, as-of rate-prior/generator evidence and prior full-spine
reconciliation. Raw files must already reside in a separately preserved immutable
local archive; no current provider URL substitutes for bytes. Historical
measurements fetched at this actual cutoff may legitimately use then-current
revisions for a *future* forecast; this does not certify past contemporaneity.

The command checks bytes and recorded times against cutoff and real current time;
manual source-release/spine assertions still require scientific review. It cannot
prove a provider's first publication, prove a user's non-access to outcomes or
independently certify an arbitrary supplied feature generator. It records code,
protocol, Python/library versions, seed/configuration, row memberships, fitted
states, separate input references and output checksums. Input references include
raw archives, while feature/training CSVs and evidence are copied into the freeze.
Do not delete referenced archives.

```bash
PYTHONPATH=. .venv/bin/python scripts/prospective_research.py freeze \
  --features /absolute/archive/2027/features.csv \
  --training /absolute/archive/2027/training.csv \
  --evidence /absolute/archive/2027/evidence.json \
  --output artifacts/model-research-information-v1/annual-2027-freeze-01
```

Existing output, including partial runs, is rejected. Original successful freeze
is the primary registered prediction set. Withdrawals and source corrections are
separate dated JSON notes with reason and original manifest hash; never rewrite
predictions. A pre-cutoff correction may create a separately numbered freeze with
parent hash and disclosure; all versions remain visible, and original scores are
reported. Post-cutoff prediction revision is a new exploratory forecast and cannot
replace primary confirmation. No automated scheduling/publication or database work.

## Evaluation and maturity gate

Earliest maturity is March 1, 2028 UTC, **plus** complete actual regular-season
schedule, reconciled all-position EPA/exposures and resolved/unknown identity
ledger. March 1 alone is insufficient. Label CSV schema: row_id, epa,
opportunities, status. Every frozen row must appear exactly once; status is
observed or unresolved_identity. Known labels must be finite, N nonnegative and
integer, Y=0 if N=0; unresolved labels must both be missing. Incomplete/uncovered
season status is fatal. All-position aggregation is essential after role changes.

A separate certificate supplies forecast_year, season_complete,
all_position_reconciled, labels_sha256, matured_at, source references and exact
expected/observed regular-game ID lists. Source hashes and schedule membership
are verified. The certificate is an explicit analyst attestation backed by raw
files, not automatic proof that PBP reconciliation was performed. Keep its
reconciliation tables with the source ledger. Scoring verifies original freeze
outputs and source hashes before reading labels; changed archives stop visibly.

```bash
PYTHONPATH=. .venv/bin/python scripts/prospective_research.py evaluate \
  --freeze artifacts/model-research-information-v1/annual-2027-freeze-01 \
  --labels /absolute/archive/2027/labels.csv \
  --certificate /absolute/archive/2027/label-certificate.json \
  --output artifacts/model-research-information-v1/annual-2027-evaluation-01
```

Primary equal-known-transition MSE and paired skill versus direct Ridge;
RMSE/MAE/bias/R² secondary. Save complete spine coverage and paired row losses.
A one-season screen needs >=5% skill, >=90% known label coverage in each claimed
role, >=30 known rows, and no >10% harm in prior-N strata with >=30 rows. All
unknowns remain in coverage. Report failed/abstained roles and missingness rather
than select a reduced easy population. No multiple-year/era guard is declared
passed from one season. Additional years require new annual freezes.

Use 10,000 fixed-prediction whole-player resamples, seed 20261002. Six primary
claims receive Bonferroni tail probability .05/(2*6); report conditional intervals
and require lower bound >0 for the screen. Also show unadjusted prior-team cluster
sensitivity. This multiplicity convention does not fix bootstrap assumptions,
shared games, cross-team or one-year calendar dependence. One season is one new
temporal realization; a pass is `one_season_screen_pass`, not broad transport,
causal economic usefulness or `forecast_validated=True`. No distribution or latent
talent intervals follow from point forecasts.

## Dry-run verification

`python scripts/exercise_prospective_research.py --output NEW_DIRECTORY` creates
an explicitly synthetic fixture, runs freeze and separate scoring, and exercises
future-label exclusion, overwrite protection, changed-source rejection, incomplete
labels and membership rejection. It writes evidence and predictions under
`dry_run_not_confirmation`. Synthetic future dates are allowed only with the
explicit dry-run flag. Tests additionally poison future labels, alter training
horizons, corrupt membership, preserve unknown labels and validate zero rules.
No process waits for 2027. What remains is actual 2026 completion, contemporaneous
source/spine preparation at the cutoff, real prediction freeze and mature 2027
labels. Preparation and successful tests do not constitute any of those events.

## October 4, 2026 amendment — computed source and membership boundary

Written before any genuine 2027 prediction/outcome. Original protocol and code
remain preserved in the information-stage final snapshot. The task, models,
thresholds, dates and six claims are unchanged. New source audits reveal
unresolved raw identities and incomplete 2026 coverage; no real annual freeze
is permitted on these inputs.

Real evidence now additionally requires `raw_seasons`, mapping each contiguous
prior year to six hash-ledger paths: schedule, roster, players, stats, snaps and
full PBP. `scripts/prepare_decision_evidence.py --recipe RECIPE --output NEW_DIR`
reconstructs measurements, identities, prior spine, as-of rates, features and
earlier labels without fitting contribution predictors. Real freeze reexecutes
this recipe and compares every numerical input/membership (absolute arithmetic
tolerance 1e-10, zero relative tolerance; exact identities). Manual boolean
assertions alone cannot pass. Provider first-publication and exhaustive source
collection remain human/provider evidence, not consequences of this calculation.

The strict recipe refuses unresolved snap/core identities, ambiguous prior
roles, source/game gaps and incomplete annual schedules. Historical preparation
using the original generator remains separate and cannot be promoted. Snapshot
availability is recorded independently of per-row measurement horizons. Missing
identity adjudications require verified source evidence, never a name-only join.

The real label certificate additionally supplies `raw_season` with the same six
source kinds, referenced hashes and actual receipt times. Evaluation computes
all-position EPA/exposures on the exact frozen spine and compares supplied labels;
booleans, equal game-ID lists or rehashed invented labels are insufficient.
Unresolved label identities stay unknown. No mature labels are available today.

Freeze records start and finalization separately, reruns cutoff/time/source
checks after fitting, saves the original direct Ridge's fitted transformations
and training mean, and includes new source-preparation code/dependencies in the
code archive. Read-only verification checks exact prediction membership and
metadata for all three procedures in addition to bytes. A cutoff rollover leaves
an unfinalized partial directory and cannot claim on-time creation. Local clocks
and hashes remain local evidence, without an independently trusted timestamp.

See `model-research-cutoff-evidence.md` for executable preparation, actual failed
gates and the separate synthetic exercises. No production flag is changed.


## October 4, 2026 source-resolution amendment — evidenced identity overlays

No estimand, model, membership rule, training window, cutoff or scoring date changes.
The full diagnostic recipe now preserves 2016–2026; it fails the 2016 identity gate.
New optional `identity_adjudications` maps a raw season to a version-1 JSON overlay.
Each overlay names that exact season and GSIS/old-roster-PFR/new-PFR records with
an explicit provider crosswalk path, hash and reason. The overlay and its evidence
must appear in the same source-time/hash ledger as the raw inputs. A pair must be
unambiguous in the provider evidence; names are not keys. Wrong seasons, collisions,
changed evidence and unsupported master overrides are rejected. Corrections needing
a retraction of a different master pair remain unsupported and must be resolved
with additional evidence, not forced through this additive/confirmatory mechanism.

Preparation and independent reconstruction feed the identical audited map into
participation calculations. Unadjudicated roster/master conflicts block strict
preparation, and corresponding label identities remain unknown. Exact duplicate
pairs are distinguished from real conflicts. Raw files and previous studies remain
unchanged; new overlays/results are versioned separately. A valid overlay is not
proof of exhaustive collection or contemporaneous availability. See the
[source-resolution results](model-research-source-resolution-results.md) for four
supported confirmations, outstanding gaps, tests and the unchanged freeze boundary.
