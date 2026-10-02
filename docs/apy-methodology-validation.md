# Methodology validation: October 1, 2026

The implementation was evaluated on pinned canonical 2021–2025 sources and the audit's 2026 week-3 snapshot. Current run ID: `71a8530e-6e1c-51e3-a57c-d5457a848f85`. Raw canonical files are hash checked against their stored manifests; PBP sources/subsets have independent hashes. Data and full output tables are in `artifacts/methodology-v2/results/`.

## Integrity and reproducibility

- Historical coverage: **1,359 regular-season games**; current coverage: **48 shared games**.
- Rebuilt dataset: **15,531 historical player-seasons** and **3,017 current player rows**, including unknown costs.
- Current offensive family population with at least 100 offensive snaps: **175 players**; APY known for **175**, annual cap/cash each known for **165**.
- Player/game PBP EPA matches archived weekly EPA to floating-point precision: maximum absolute difference **7.11e-15**. Whole-season component sums and offensive snaps reproduce the player-season rows within 1e-6.
- Raw participation roles include FB/HB while positions use the roster-compatible RB family. Master identities resolve documented historical PFR disagreements and reject incompatible homonymous contract GSIS mappings.
- Saved grouped folds hold every supported player's seasons together. Historical rookie predictions exclude that player's later veteran targets. Chronological and event-purged predictions were checked from the saved row-level provenance.
- **59 pytest tests passed**, including Streamlit financial-basis controls, units, ambiguity, two-point exposures, pooling mathematics, time cutoffs, unavailable outcomes and surplus gates. The additive migration passed isolated PostgreSQL 16 behavior tests, including repeat application, nullable APY, precision and atomic updates. The disposable container was removed.
- Numerical environment: numpy 2.4.2, pandas 2.3.3, scikit-learn 1.8.0, scipy 1.17.1. Versions, training signatures, seeds and input/output hashes are saved in `manifest.json`.

The larger rebuilt population includes missing-OTC/unknown-contract roster players omitted by the old contract-first join. Conservative same-year ambiguity exclusions and corrected snap eligibility change the salary-training population. These scores therefore are **not paired improvements over the audit's old rows**.

## Phase 1: salary association errors

Nested player-grouped results use estimated non-rookie contracts. Dollars are millions. Bias is prediction minus observed APY. The inverse log target is a transformed geometric center; no expected-dollar or economic-surplus interpretation is attached.

| Position | Veteran rows | MAE, $M | RMSE, $M | Bias, $M | R² |
| --- | --- | --- | --- | --- | --- |
| QB | 163 | 11.528 | 16.242 | -4.777 | 0.331 |
| RB | 212 | 2.366 | 3.517 | -0.634 | 0.388 |
| WR | 379 | 5.410 | 7.666 | -2.046 | 0.331 |
| TE | 226 | 2.664 | 3.630 | -0.605 | 0.427 |

The models beat their fold-trained mean/median baselines on MAE, but errors remain substantial. A small association gap is not evidence of a definitive bargain. Chronological completed-production results, baselines and safety-bound flags are in `apy_diagnostics.csv` and `chronological_apy_predictions.csv`.

## Phase 2: later observed component rates

The following 2025 rows compare identical returning players with positive exposure in both adjacent seasons. Rate RMSE is EPA per reconciled opportunity. Pooling/context selection and future innovation variance use earlier transitions. Coverage assesses an 80% **observed-rate reference band**, conditional on realized future opportunities, and does not establish latent-talent coverage.

| Position | Component | Players | Previous raw RMSE | Pooled RMSE | Observed coverage |
| --- | --- | --- | --- | --- | --- |
| QB | pass | 60 | 0.436 | 0.289 | 63.3% |
| QB | rush | 61 | 0.567 | 0.447 | 88.5% |
| RB | rush | 99 | 0.492 | 0.304 | 94.9% |
| RB | receive | 92 | 0.632 | 0.344 | 94.6% |
| WR | receive | 168 | 0.868 | 0.436 | 91.1% |
| TE | receive | 96 | 0.585 | 0.465 | 85.4% |

Pooling improves these rate errors relative to raw prior-season rates. Coverage is heterogeneous: QB passing remains materially below 80%, while several other components are overly wide. These bands **fail a general calibrated-interval claim** and remain research references. Sparse secondary roles have too little test support for individual valuation. Prior uncertainty, team/game dependence and temporal drift remain limitations.

One-year SFA and expanded SFA/UFA acquisitions produce distinct replacement sensitivity cases. Negative raw EPA can exceed an acquisition comparator, but acquisition-cohort rates do not prove that a particular player is currently available as a replacement. Three-week block-bootstrap rank stability is descriptive and has limited support.

## Phase 3: later negotiated-event prices

2025 event tests use chronological training, nested earlier-year tuning and whole-player target purging. All models within a position use identical test events. Columns report dollar MAE in millions; the target estimands and tuning losses differ. Realized signing-year cap converts normalized predictions back to dollars, so this is a retrospective price benchmark rather than a certified January-1 offer simulation.

| Position | Events | Mean Ridge MAE | Gamma mean MAE | Log Ridge MAE | Median quantile MAE |
| --- | --- | --- | --- | --- | --- |
| QB | 38 | 10.734 | 5.849 | 6.581 | 6.585 |
| RB | 52 | 1.415 | 1.473 | 1.071 | 1.482 |
| WR | 91 | 2.638 | 3.276 | 2.013 | 3.380 |
| TE | 53 | 1.596 | 1.471 | 1.589 | 1.868 |

There is no uniformly best alternative. Gamma helps some cohorts but is weaker for others; median regularization is not a general improvement. A smaller log-model MAE does not turn its geometric-center prediction into a conditional mean. Later 2026 event tests, RMSE/bias, pinball loss, mean/median baselines and market-specific counts/errors are saved separately. Retrospective label revisions, event ambiguity and missing pre-event histories limit generalization.

The following preseason 2025 tests include prior-roster players who have zero future opportunities. Direct joint EPA and pooled-rate × volume forecasts are research candidates, compared with repeating prior-season EPA. Errors are component EPA, not dollars or individual talent intervals.

| Position | Component | Prior-roster players | Previous EPA RMSE | Rate × volume RMSE | Direct EPA RMSE |
| --- | --- | --- | --- | --- | --- |
| QB | pass | 129 | 40.754 | 36.540 | 29.156 |
| RB | rush | 233 | 11.485 | 8.878 | 6.658 |
| WR | receive | 419 | 13.612 | 11.162 | 11.608 |
| TE | receive | 215 | 9.864 | 8.421 | 8.279 |

Improvements differ by role and loss. Forecasts explicitly remain unapproved (`forecast_validated=False`), and no partial 2026 production trains an annual model. Age/experience missingness uses training-only median imputation, not a fabricated age of zero. Independent prospective validation is still needed.

## What is ready and what stays unknown

The financial definitions, audited efficiency reporting, saved held-out predictions, component data, chronological validation, event benchmarks and explicit cost-scenario APIs are implemented and verified. Production migration/deployment was not performed.

No calibrated latent market-value or surplus distribution is established. Equivalent-APY discounts require verified compatible terms; economic scenarios require validated replacement forecasts, a stated contribution price and complete team/horizon cap/cash/guarantee/exit obligations. Missing inputs suppress surplus instead of being filled from APY or global MAE. Guarantees are not counted twice.

Numerical training/tuning excludes the declared outer years. Statistical designs were refined during this retrospective audit, so these results are not independent of all model-development decisions. Source/cutoff correctness, prediction accuracy and causal/economic identification are separate acceptance requirements.
