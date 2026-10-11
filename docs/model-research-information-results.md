# Information research results — October 3, 2026

One additional prior-role feature was tested in the same audited Ridge procedure. **No receiving role passes the extended incremental-information screen.** B was rejected before fitting. All historical evidence remains exploratory.

## Correct-reference results

Skill and interval values below are percentages of MSE improvement versus audited Ridge.

| experiment | position | skill | player_conditional_low | player_conditional_high | conclusion |
| --- | --- | --- | --- | --- | --- |
| extended | RB | -0.423 | -0.922 | -0.009 | no_retrospective_improvement |
| extended | TE | 0.520 | 0.103 | 0.935 | inconclusive_difference |
| extended | WR | 0.010 | -0.559 | 0.555 | inconclusive_difference |
| pinned | RB | 0.032 | -0.024 | 0.091 | inconclusive_difference |
| pinned | TE | 6.139 | 1.586 | 10.078 | promising_retrospective_improvement |
| pinned | WR | -0.159 | -0.618 | 0.256 | no_retrospective_improvement |

Extended TE improves only 0.520%, WR 0.010%, and RB worsens 0.423%. Pinned TE improves 6.139% and passes its two-year guards; the seven-year result does not support the same practical claim. Do not pick the shorter-history winner or lower the 5% threshold. The joint prospective inclusion rule therefore excludes A.

Against original direct Ridge, extended WR improves 8.285% and passes that distinct screen; extended TE improves 11.323% but fails its complete guards. Neither establishes incremental value over audited Ridge. Full contrasts, years, eras, workload guards and player/prior-team intervals remain in experiment-01/comparisons.csv.

## Coverage and execution

| experiment | position | component | spine_rows | known | predicted | share_known |
| --- | --- | --- | --- | --- | --- | --- |
| extended | RB | receive | 1680 | 1596 | 1596 | 1145 |
| extended | TE | receive | 1477 | 1396 | 1396 | 962 |
| extended | WR | receive | 2831 | 2637 | 2637 | 1710 |
| pinned | RB | receive | 462 | 434 | 434 | 308 |
| pinned | TE | receive | 421 | 393 | 393 | 277 |
| pinned | WR | receive | 834 | 764 | 764 | 486 |

All 5,629 extended and 1,591 pinned known-label receiving transitions received candidate predictions. Share-known counts include unknown-label rows in the full spine; missing shares never remove transitions. Per-year/role coverage, zero and undefined histories are in audit-04/feature_coverage.csv. Matched-feature-support sensitivity compares both predictors on identical rows.

27 outer fits and 372 inner trials (extended 21/336; pinned 6/36), zero numerical failures and zero estimator warnings. Every trial and fitted preprocessing state is saved. Cached references passed exact row, target and training-membership checks; no v3 fits repeated. Independent verifier recomputed alpha choices from saved inner losses, exact paired losses and output/input hashes without fitting.

## Diagnostics and sensitivity

The largest risk is not the many known future zeros: they contribute only 0.8–5.2% of audited full SSE. Training-defined extremes contribute 38.5–62.2%. These are descriptive outcome strata, not forecast-time features or an error-ceiling estimate. Audit tables retain prior workload/age/experience/missing-history/team/range strata with counts and small-stratum flags.

| position | sensitivity | rows | candidate_mse | reference_mse | skill |
| --- | --- | --- | --- | --- | --- |
| RB | matched_feature_support | 1145 | 37.643 | 37.476 | -0.004 |
| RB | missing_feature | 451 | 2.380 | 2.392 | 0.005 |
| RB | omit_extreme | 1518 | 11.015 | 10.961 | -0.005 |
| RB | omit_largest_player | 1589 | 25.182 | 25.058 | -0.005 |
| TE | matched_feature_support | 960 | 89.237 | 89.724 | 0.005 |
| TE | missing_feature | 436 | 4.343 | 4.321 | -0.005 |
| TE | omit_extreme | 1316 | 32.820 | 32.787 | -0.001 |
| TE | omit_largest_player | 1389 | 59.457 | 59.802 | 0.006 |
| WR | matched_feature_support | 1706 | 190.301 | 190.304 | 0.000 |
| WR | missing_feature | 931 | 9.667 | 9.697 | 0.003 |
| WR | omit_extreme | 2503 | 82.497 | 82.026 | -0.006 |
| WR | omit_largest_player | 2630 | 123.299 | 123.338 | 0.000 |

All leave-year results are retained. Conditional player intervals hold fitted predictors and calendar years fixed; prior-team clustering does not solve shared games, cross-team histories, source revisions or adaptive source selection. Sparse strata are weak evidence. No independently confirmed or economic claim follows.

## Sources and failed attempts

A uses an exposure-weighted observed-game target share, with exact all-position weekly denominator reconciliation across 2016–2025, game-specific team aliases and explicit unknown histories. It is not an average weekly share, full roster-tenure season share or EPA rate exposure. Provider versions were actually archived in October 2026: retrospective measurement only.

B uses a public 2024 weekly injury-file audit: 6,215 rows and two duplicate season/week/team/player keys. No row establishes June 1 status. Stale reports and modification timestamps cannot fill that gap. No B fit or A+B fit occurred. Air-yard ratios were excluded after the naive all-receiver denominator failed reconciliation, not after candidate scores.

Initial DNS/404 source attempts, the null-identity audit stop, a column-edit failure and the pre-alias audit are preserved in separate directories. No label defect was found; no v3 labels or inputs changed.

## Confirmation and economic status

The manual freeze/evaluation implementation and synthetic exercise are separate from real prospective evidence. No real June 1, 2027 forecast exists. See the prospective protocol for cutoff, source, membership, label maturity, multiplicity and abstention gates. See the decision-readiness matrix and saved hypothetical scenario tables for costs and break-even calculations. `forecast_validated=False`; supported economic surplus remains unknown.

## Final verification addendum

A fresh full local suite passed **119 tests in 9.51 seconds** (see
`validation-01/pytest.log`), distinct from the historical 94-test handoff.
The explicitly synthetic `prospective-dry-run-01` freeze/evaluation succeeded;
repeat freeze, incomplete labels, missing members and changed-source attempts
failed visibly. Original predictions were unchanged. No real annual freeze,
production change, new dependency, commit or push occurred.

A supplementary read-only hurdle diagnostic, added after candidate evaluation
without expanding any search, separates the saved conditional-total prediction
`m_hat = signed_total_prediction / probability_positive` from probability scores.
All saved probabilities are strictly positive. `hurdle-separation-01/` retains
row quantities, fixed-bin calibration, training-prevalence references and
positive/non-extreme/extreme magnitude scores. Extended positive-outcome magnitude
RMSE is QB/pass 40.125 (478 rows), QB/rush 10.345 (472), RB/receive 7.322 (825),
RB/rush 10.637 (850), TE/receive 10.895 (720), WR/receive 15.659 EPA (1,293).
These conditional errors cannot be added to Brier loss or full-population total
EPA MSE. Direct unconditional forecasts target a different quantity and are not
misrepresented as conditional-magnitude references. This is not an oracle or
prospectively attainable error bound.
