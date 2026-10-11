# Model research results: baselines and challenger evaluation

Updated October 3, 2026. Both frozen challenger experiments are complete. Baseline evidence below is preserved; measured challenger tables and diagnostics follow it.

**Main finding:** none of the six challenger families meets the prespecified improvement screen over audited Ridge on 2019–2025. Some qualify against the original direct Ridge, primarily WR receiving and Random Forest QB passing. This supports retaining the simpler audited reference for further research, without automatic model promotion. No role passes the earlier-only sequence-signal gate.

## Data and reproduction

The pinned 2021–2025 dataset and separately archived public 2016–2020 extension cover **2,639 regular-season games** (1,359 + 1,280). There are 30,994 all-position roster rows in the combined spine; this is not the supervised sample size. Only six QB/RB/WR/TE component tasks are evaluated. New sources are retrospective releases downloaded October 3 UTC; historical source availability is not established. PBP/game EPA reconciliation has maximum absolute discrepancy 7.11e-15. Earlier records contain one missing-EPA play in each pass/rush/receive component ledger; these are excluded from rate opportunities. The missing records may overlap the same play and must not be interpreted as three independent incidents.

v2 salary association, negotiated-price, rate-band and preseason predictions reproduced with maximum absolute numerical discrepancy below 5e-13, including identical selected association/price alpha values. All v2 output hashes remain unchanged. The initial reproduction failure was CSV empty-ID versus NaN serialization, now normalized only for comparison; that identity is not treated as resolved.

A subsequent all-position raw-source audit corrected future outcomes omitted by the skill-only game filter after position changes. Superseded baseline artifacts remain archived; `checkpoint/label_corrections.csv` identifies every changed target. No candidate grid changed after inspecting those outcomes. Outcomes now use all-position raw snaps/statistics/PBP; prior role remains fixed. Unresolved identities remain unknown, and partial 2026 is never scored as a full annual target.

The efficiency-family ablation was also corrected to remove indirect EPA information from recency/trend variables. Earlier incomplete-ablation runs are archived; only the rerun scores are final. The full and original direct Ridge definitions did not change.

Observed players reach 17 participation games in the 2019 16-game team-schedule era and 18 in the later era. The original games Ridge retains its inherited 17 cap solely as a reproduced reference; it is not a valid universal player support bound.

## Pinned primary forecast baseline

Test years: 2024–2025. Equal player-transition weights within each role. The warm-up year is excluded. EPA RMSE, signed totals including future zero outcomes:

| position | component | direct_ridge | mean | previous | ridge_full |
| --- | --- | --- | --- | --- | --- |
| QB | pass | 31.607 | 35.639 | 39.610 | 30.128 |
| QB | rush | 7.667 | 8.794 | 9.539 | 7.511 |
| RB | receive | 5.284 | 5.269 | 7.367 | 5.329 |
| RB | rush | 8.214 | 8.282 | 12.995 | 8.555 |
| TE | receive | 9.243 | 11.073 | 10.345 | 8.671 |
| WR | receive | 11.869 | 14.495 | 14.245 | 11.536 |

Audited feature/history Ridge relative to original direct EPA Ridge (skill is a fraction):

| position | component | skill | player_conditional_low | player_conditional_high | conclusion |
| --- | --- | --- | --- | --- | --- |
| QB | pass | 0.091 | -0.029 | 0.214 | inconclusive_difference |
| QB | rush | 0.040 | -0.093 | 0.157 | inconclusive_difference |
| RB | receive | -0.017 | -0.048 | 0.013 | no_retrospective_improvement |
| RB | rush | -0.085 | -0.202 | -0.017 | no_retrospective_improvement |
| TE | receive | 0.120 | 0.009 | 0.245 | promising_retrospective_improvement |
| WR | receive | 0.055 | -0.051 | 0.147 | inconclusive_difference |

Coverage ledger (distinct players here includes unknown outcomes):

| position | component | eligible_rows | distinct_players | known_outcomes | zero_opportunity_outcomes | negative_epa_outcomes | prediction_coverage |
| --- | --- | --- | --- | --- | --- | --- | --- |
| QB | pass | 252 | 150 | 243 | 106 | 69 | 0.964 |
| QB | rush | 252 | 150 | 243 | 106 | 66 | 0.964 |
| RB | receive | 462 | 290 | 434 | 222 | 113 | 0.939 |
| RB | rush | 462 | 290 | 434 | 206 | 160 | 0.939 |
| TE | receive | 421 | 255 | 393 | 184 | 67 | 0.933 |
| WR | receive | 834 | 529 | 764 | 394 | 104 | 0.916 |

## Extended primary forecast baseline

Test years: 2019–2025. Equal player-transition weights within each role. The warm-up year is excluded. EPA RMSE, signed totals including future zero outcomes:

| position | component | direct_ridge | mean | previous | ridge_full |
| --- | --- | --- | --- | --- | --- |
| QB | pass | 32.623 | 36.084 | 38.115 | 30.772 |
| QB | rush | 7.553 | 8.960 | 9.542 | 7.730 |
| RB | receive | 5.257 | 5.238 | 7.170 | 5.250 |
| RB | rush | 7.877 | 8.365 | 11.291 | 7.814 |
| TE | receive | 8.410 | 10.014 | 9.555 | 7.940 |
| WR | receive | 11.746 | 14.733 | 13.731 | 11.249 |

Audited feature/history Ridge relative to original direct EPA Ridge (skill is a fraction):

| position | component | skill | player_conditional_low | player_conditional_high | conclusion |
| --- | --- | --- | --- | --- | --- |
| QB | pass | 0.110 | 0.009 | 0.219 | inconclusive_difference |
| QB | rush | -0.047 | -0.108 | 0.003 | no_retrospective_improvement |
| RB | receive | 0.003 | -0.024 | 0.028 | inconclusive_difference |
| RB | rush | 0.016 | -0.015 | 0.051 | inconclusive_difference |
| TE | receive | 0.109 | -0.013 | 0.217 | inconclusive_difference |
| WR | receive | 0.083 | 0.030 | 0.131 | promising_retrospective_improvement |

Coverage ledger (distinct players here includes unknown outcomes):

| position | component | eligible_rows | distinct_players | known_outcomes | zero_opportunity_outcomes | negative_epa_outcomes | prediction_coverage |
| --- | --- | --- | --- | --- | --- | --- | --- |
| QB | pass | 882 | 260 | 850 | 372 | 258 | 0.964 |
| QB | rush | 882 | 260 | 850 | 378 | 227 | 0.964 |
| RB | receive | 1680 | 603 | 1596 | 771 | 431 | 0.950 |
| RB | rush | 1680 | 603 | 1596 | 746 | 607 | 0.950 |
| TE | receive | 1477 | 509 | 1396 | 676 | 225 | 0.945 |
| WR | receive | 2831 | 1028 | 2637 | 1344 | 387 | 0.931 |

## Does more training history help on identical recent outcomes?

| position | component | model | rows | short_rmse | long_rmse | longer_history_skill |
| --- | --- | --- | --- | --- | --- | --- |
| QB | pass | direct_ridge | 243 | 31.607 | 30.878 | 0.046 |
| QB | pass | ridge_full | 243 | 30.128 | 29.174 | 0.062 |
| QB | rush | direct_ridge | 243 | 7.667 | 7.557 | 0.028 |
| QB | rush | ridge_full | 243 | 7.511 | 7.517 | -0.002 |
| RB | receive | direct_ridge | 434 | 5.284 | 5.243 | 0.016 |
| RB | receive | ridge_full | 434 | 5.329 | 5.243 | 0.032 |
| RB | rush | direct_ridge | 434 | 8.214 | 8.065 | 0.036 |
| RB | rush | ridge_full | 434 | 8.555 | 8.104 | 0.103 |
| TE | receive | direct_ridge | 393 | 9.243 | 9.182 | 0.013 |
| TE | receive | ridge_full | 393 | 8.671 | 8.684 | -0.003 |
| WR | receive | direct_ridge | 764 | 11.869 | 11.763 | 0.018 |
| WR | receive | ridge_full | 764 | 11.536 | 11.126 | 0.070 |

The table compares identical 2024–2025 rows and targets. The longer pipeline also has more inner selection history and earlier rate-prior evidence; this is the effect of the complete longer-history learning procedure, not a pure sample-size causal effect. Negative skill is retained. No retrospective winner is independently confirmed.

## Price, rate and uncertainty boundaries

Price references preserve the same observed events and stricter player purge. The new Ridge removes eventual mechanism but does not solve retrospective signing-year cap conversion or source revision. 2026 event outcomes are observed-to-snapshot, not a completed annual market. Complete price-unit scores are in `checkpoint/price_reference_metrics.csv`; price cohorts are not comparable to salary association cohorts. Earlier-only log residual exponential factors and fitted-value bins are saved; no blanket smearing correction or calibrated latent-price distribution was fitted.

| position | component | test_season | rows | coverage | width | interval_score |
| --- | --- | --- | --- | --- | --- | --- |
| QB | pass | 2025 | 60 | 0.633 | 0.601 | 1.030 |
| QB | rush | 2025 | 61 | 0.885 | 1.215 | 1.575 |
| RB | receive | 2025 | 92 | 0.946 | 1.230 | 1.267 |
| RB | rush | 2025 | 99 | 0.949 | 0.799 | 0.900 |
| TE | receive | 2025 | 96 | 0.854 | 1.090 | 1.383 |
| WR | receive | 2025 | 168 | 0.911 | 1.295 | 1.469 |

The bands condition on future positive and realized exposure. They are observed-rate references, not talent intervals. Player-cluster paired intervals condition on fixed fits and observed years. Prior-team cluster sensitivity and per-year skills are saved, but neither resolves shared-game, cross-team, source-revision or calendar uncertainty. Multiplicity is exploratory; no formal adjusted significance claim is made.

## Baseline interpretation

Added historical efficiency/history is not uniformly beneficial, and additional seasons do not guarantee better transport. Measurement noise, context, selection, regime changes, limited model class and imperfect estimand alignment remain competing explanations; residual error does not identify their individual shares or an empirical performance ceiling. Ablations, training-player/history sensitivity, foldwise preprocessing and coefficients, row-level errors, extreme/zero/negative strata and all inner trials are saved.

Economic surplus remains **unknown**. Forecast ordering sensitivity is measured in EPA only; at a fixed assumed comparator and complete fixed cost, delta S=lambda*delta D. No supported feasible replacement, complete incremental cap/cash/exit schedules, contribution price or joint predictive covariance is supplied by this study. No player dollar-surplus judgment, latent-surplus probability or policy-regret claim is produced.

The finite challenger suite has now been evaluated below. RNN/LSTM did not pass its necessary information gate; a full dynamic state-space distribution remains unsupported by this experiment. RL lacks decision/action/reward/coverage data. No forecast has been validated, no future forecast freeze has occurred, and no production state was changed.


## Challenger evaluation: October 3, 2026

The frozen six-family suite has completed on both existing datasets. All comparisons are retrospective, with identical known-label rows, signed EPA targets and primary-year weights. No winner is automatically promoted.

## Execution and coverage

| experiment | outer_fits | inner_trials | outer_failures | inner_failures | outer_warnings | inner_warnings | summed_outer_fit_seconds | summed_inner_fit_seconds | configuration_count_per_role | candidate_families |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| pinned | 72 | 414 | 0 | 0 | 0 | 0 | 2.242 | 10.847 | 23 | 6 |
| extended | 252 | 3864 | 0 | 0 | 0 | 0 | 12.311 | 145.950 | 23 | 6 |

There are 23 configurations per role/origin across six families (6 Elastic Net, 3 spline, 2 hurdle, 4 forest, 4 XGBoost, 4 sequence Ridge). Two experiments × six roles × six families yield 72 family/role evaluations and 144 paired challenger comparisons against two references. Repeated inner origins are not independent tests. Baseline feature/algorithm searches add further exploratory multiplicity.

All known outcomes receive predictions; unresolved identities remain excluded from scores and retained in the declared spine. Per-model coverage is saved. No annual 2026 target is scored. The original pinned reporting interruption was CSV parsing, not an estimator failure; saved fits were reused after exact round-trip equality and checkpoint verification.

## Pinned primary results

EPA RMSE (primary years only):

| position | component | direct_ridge | elastic_net | hurdle | random_forest | ridge_full | ridge_sequence | spline | xgboost |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| QB | pass | 31.607 | 30.646 | 30.141 | 29.694 | 30.128 | 30.190 | 30.090 | 31.144 |
| QB | rush | 7.667 | 7.567 | 7.382 | 7.615 | 7.511 | 7.456 | 7.556 | 7.614 |
| RB | receive | 5.284 | 5.318 | 5.473 | 5.295 | 5.329 | 5.329 | 5.336 | 5.313 |
| RB | rush | 8.214 | 8.151 | 8.371 | 8.289 | 8.555 | 8.335 | 8.403 | 8.308 |
| TE | receive | 9.243 | 8.703 | 8.662 | 9.412 | 8.671 | 8.676 | 8.740 | 9.115 |
| WR | receive | 11.869 | 11.422 | 11.356 | 11.565 | 11.536 | 11.554 | 11.469 | 11.583 |

All candidates versus `direct_ridge`; skill is fractional MSE reduction:

| position | component | model | skill | player_conditional_low | player_conditional_high | conclusion |
| --- | --- | --- | --- | --- | --- | --- |
| QB | pass | elastic_net | 0.060 | -0.052 | 0.180 | inconclusive_difference |
| QB | pass | hurdle | 0.091 | -0.062 | 0.249 | inconclusive_difference |
| QB | pass | random_forest | 0.117 | -0.024 | 0.242 | inconclusive_difference |
| QB | pass | ridge_sequence | 0.088 | -0.036 | 0.211 | inconclusive_difference |
| QB | pass | spline | 0.094 | -0.041 | 0.232 | inconclusive_difference |
| QB | pass | xgboost | 0.029 | -0.112 | 0.167 | inconclusive_difference |
| QB | rush | elastic_net | 0.026 | -0.110 | 0.187 | inconclusive_difference |
| QB | rush | hurdle | 0.073 | -0.050 | 0.176 | inconclusive_difference |
| QB | rush | random_forest | 0.013 | -0.138 | 0.173 | inconclusive_difference |
| QB | rush | ridge_sequence | 0.054 | -0.069 | 0.204 | inconclusive_difference |
| QB | rush | spline | 0.029 | -0.120 | 0.160 | inconclusive_difference |
| QB | rush | xgboost | 0.014 | -0.135 | 0.200 | inconclusive_difference |
| RB | receive | elastic_net | -0.013 | -0.041 | 0.017 | no_retrospective_improvement |
| RB | receive | hurdle | -0.073 | -0.155 | -0.010 | no_retrospective_improvement |
| RB | receive | random_forest | -0.004 | -0.037 | 0.028 | no_retrospective_improvement |
| RB | receive | ridge_sequence | -0.017 | -0.047 | 0.011 | no_retrospective_improvement |
| RB | receive | spline | -0.020 | -0.051 | 0.011 | no_retrospective_improvement |
| RB | receive | xgboost | -0.011 | -0.039 | 0.018 | no_retrospective_improvement |
| RB | rush | elastic_net | 0.015 | -0.015 | 0.052 | inconclusive_difference |
| RB | rush | hurdle | -0.039 | -0.079 | 0.004 | no_retrospective_improvement |
| RB | rush | random_forest | -0.018 | -0.067 | 0.029 | no_retrospective_improvement |
| RB | rush | ridge_sequence | -0.030 | -0.057 | 0.003 | no_retrospective_improvement |
| RB | rush | spline | -0.047 | -0.085 | -0.009 | no_retrospective_improvement |
| RB | rush | xgboost | -0.023 | -0.096 | 0.062 | no_retrospective_improvement |
| TE | receive | elastic_net | 0.114 | 0.014 | 0.227 | promising_retrospective_improvement |
| TE | receive | hurdle | 0.122 | 0.002 | 0.259 | promising_retrospective_improvement |
| TE | receive | random_forest | -0.037 | -0.110 | 0.033 | no_retrospective_improvement |
| TE | receive | ridge_sequence | 0.119 | 0.014 | 0.235 | promising_retrospective_improvement |
| TE | receive | spline | 0.106 | -0.004 | 0.236 | inconclusive_difference |
| TE | receive | xgboost | 0.027 | -0.072 | 0.143 | inconclusive_difference |
| WR | receive | elastic_net | 0.074 | -0.020 | 0.157 | inconclusive_difference |
| WR | receive | hurdle | 0.085 | -0.023 | 0.177 | inconclusive_difference |
| WR | receive | random_forest | 0.051 | -0.023 | 0.116 | inconclusive_difference |
| WR | receive | ridge_sequence | 0.052 | -0.055 | 0.145 | inconclusive_difference |
| WR | receive | spline | 0.066 | -0.038 | 0.156 | inconclusive_difference |
| WR | receive | xgboost | 0.048 | -0.036 | 0.123 | inconclusive_difference |

All candidates versus `ridge_full`; skill is fractional MSE reduction:

| position | component | model | skill | player_conditional_low | player_conditional_high | conclusion |
| --- | --- | --- | --- | --- | --- | --- |
| QB | pass | elastic_net | -0.035 | -0.070 | -0.001 | no_retrospective_improvement |
| QB | pass | hurdle | -0.001 | -0.051 | 0.055 | no_retrospective_improvement |
| QB | pass | random_forest | 0.029 | -0.078 | 0.125 | inconclusive_difference |
| QB | pass | ridge_sequence | -0.004 | -0.010 | 0.001 | no_retrospective_improvement |
| QB | pass | spline | 0.002 | -0.021 | 0.030 | inconclusive_difference |
| QB | pass | xgboost | -0.069 | -0.188 | 0.044 | no_retrospective_improvement |
| QB | rush | elastic_net | -0.015 | -0.216 | 0.195 | no_retrospective_improvement |
| QB | rush | hurdle | 0.034 | -0.002 | 0.077 | inconclusive_difference |
| QB | rush | random_forest | -0.028 | -0.227 | 0.166 | no_retrospective_improvement |
| QB | rush | ridge_sequence | 0.015 | -0.149 | 0.189 | inconclusive_difference |
| QB | rush | spline | -0.012 | -0.133 | 0.115 | no_retrospective_improvement |
| QB | rush | xgboost | -0.028 | -0.261 | 0.200 | no_retrospective_improvement |
| RB | receive | elastic_net | 0.004 | -0.009 | 0.018 | inconclusive_difference |
| RB | receive | hurdle | -0.055 | -0.111 | -0.013 | no_retrospective_improvement |
| RB | receive | random_forest | 0.013 | -0.009 | 0.034 | inconclusive_difference |
| RB | receive | ridge_sequence | 0.000 | -0.004 | 0.003 | inconclusive_difference |
| RB | receive | spline | -0.003 | -0.005 | -0.000 | no_retrospective_improvement |
| RB | receive | xgboost | 0.006 | -0.006 | 0.017 | inconclusive_difference |
| RB | rush | elastic_net | 0.092 | 0.032 | 0.190 | promising_retrospective_improvement |
| RB | rush | hurdle | 0.042 | -0.011 | 0.141 | inconclusive_difference |
| RB | rush | random_forest | 0.061 | -0.010 | 0.170 | inconclusive_difference |
| RB | rush | ridge_sequence | 0.051 | 0.001 | 0.138 | promising_retrospective_improvement |
| RB | rush | spline | 0.035 | -0.008 | 0.111 | inconclusive_difference |
| RB | rush | xgboost | 0.057 | -0.033 | 0.178 | inconclusive_difference |
| TE | receive | elastic_net | -0.007 | -0.033 | 0.017 | no_retrospective_improvement |
| TE | receive | hurdle | 0.002 | -0.021 | 0.023 | inconclusive_difference |
| TE | receive | random_forest | -0.178 | -0.313 | -0.062 | no_retrospective_improvement |
| TE | receive | ridge_sequence | -0.001 | -0.021 | 0.019 | no_retrospective_improvement |
| TE | receive | spline | -0.016 | -0.032 | -0.000 | no_retrospective_improvement |
| TE | receive | xgboost | -0.105 | -0.174 | -0.037 | no_retrospective_improvement |
| WR | receive | elastic_net | 0.020 | 0.005 | 0.037 | inconclusive_difference |
| WR | receive | hurdle | 0.031 | 0.017 | 0.046 | inconclusive_difference |
| WR | receive | random_forest | -0.005 | -0.090 | 0.079 | no_retrospective_improvement |
| WR | receive | ridge_sequence | -0.003 | -0.009 | 0.002 | no_retrospective_improvement |
| WR | receive | spline | 0.012 | 0.001 | 0.023 | inconclusive_difference |
| WR | receive | xgboost | -0.008 | -0.082 | 0.070 | no_retrospective_improvement |

The promising screen requires >=5% primary skill, a positive conditional player interval, year/era guards and no >10% harm in sufficiently supported prior-opportunity strata. It does not establish multiplicity-adjusted significance, future-year uncertainty or independent confirmation.

## Extended primary results

EPA RMSE (primary years only):

| position | component | direct_ridge | elastic_net | hurdle | random_forest | ridge_full | ridge_sequence | spline | xgboost |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| QB | pass | 32.623 | 30.961 | 30.632 | 30.984 | 30.772 | 30.466 | 30.953 | 31.162 |
| QB | rush | 7.553 | 7.605 | 7.732 | 7.758 | 7.730 | 7.651 | 7.666 | 7.683 |
| RB | receive | 5.257 | 5.221 | 5.246 | 5.237 | 5.250 | 5.254 | 5.241 | 5.234 |
| RB | rush | 7.877 | 7.879 | 7.878 | 7.897 | 7.814 | 7.805 | 7.822 | 7.857 |
| TE | receive | 8.410 | 7.974 | 7.896 | 8.268 | 7.940 | 7.938 | 7.942 | 8.239 |
| WR | receive | 11.746 | 11.213 | 11.092 | 11.162 | 11.249 | 11.243 | 11.254 | 11.114 |

All candidates versus `direct_ridge`; skill is fractional MSE reduction:

| position | component | model | skill | player_conditional_low | player_conditional_high | conclusion |
| --- | --- | --- | --- | --- | --- | --- |
| QB | pass | elastic_net | 0.099 | 0.000 | 0.206 | inconclusive_difference |
| QB | pass | hurdle | 0.118 | 0.022 | 0.222 | inconclusive_difference |
| QB | pass | random_forest | 0.098 | 0.031 | 0.157 | promising_retrospective_improvement |
| QB | pass | ridge_sequence | 0.128 | 0.036 | 0.224 | inconclusive_difference |
| QB | pass | spline | 0.100 | 0.002 | 0.203 | inconclusive_difference |
| QB | pass | xgboost | 0.088 | 0.011 | 0.158 | inconclusive_difference |
| QB | rush | elastic_net | -0.014 | -0.055 | 0.017 | no_retrospective_improvement |
| QB | rush | hurdle | -0.048 | -0.113 | 0.007 | no_retrospective_improvement |
| QB | rush | random_forest | -0.055 | -0.152 | 0.030 | no_retrospective_improvement |
| QB | rush | ridge_sequence | -0.026 | -0.069 | 0.007 | no_retrospective_improvement |
| QB | rush | spline | -0.030 | -0.072 | 0.007 | no_retrospective_improvement |
| QB | rush | xgboost | -0.035 | -0.114 | 0.040 | no_retrospective_improvement |
| RB | receive | elastic_net | 0.013 | -0.012 | 0.037 | inconclusive_difference |
| RB | receive | hurdle | 0.004 | -0.024 | 0.029 | inconclusive_difference |
| RB | receive | random_forest | 0.007 | -0.022 | 0.035 | inconclusive_difference |
| RB | receive | ridge_sequence | 0.001 | -0.026 | 0.026 | inconclusive_difference |
| RB | receive | spline | 0.006 | -0.020 | 0.030 | inconclusive_difference |
| RB | receive | xgboost | 0.009 | -0.029 | 0.040 | inconclusive_difference |
| RB | rush | elastic_net | -0.001 | -0.033 | 0.031 | no_retrospective_improvement |
| RB | rush | hurdle | -0.000 | -0.022 | 0.023 | no_retrospective_improvement |
| RB | rush | random_forest | -0.005 | -0.026 | 0.016 | no_retrospective_improvement |
| RB | rush | ridge_sequence | 0.018 | -0.010 | 0.049 | inconclusive_difference |
| RB | rush | spline | 0.014 | -0.017 | 0.049 | inconclusive_difference |
| RB | rush | xgboost | 0.005 | -0.031 | 0.044 | inconclusive_difference |
| TE | receive | elastic_net | 0.101 | -0.016 | 0.205 | inconclusive_difference |
| TE | receive | hurdle | 0.119 | -0.008 | 0.233 | inconclusive_difference |
| TE | receive | random_forest | 0.034 | -0.055 | 0.120 | inconclusive_difference |
| TE | receive | ridge_sequence | 0.109 | -0.011 | 0.216 | inconclusive_difference |
| TE | receive | spline | 0.108 | -0.014 | 0.219 | inconclusive_difference |
| TE | receive | xgboost | 0.040 | -0.064 | 0.134 | inconclusive_difference |
| WR | receive | elastic_net | 0.089 | 0.039 | 0.134 | promising_retrospective_improvement |
| WR | receive | hurdle | 0.108 | 0.054 | 0.157 | promising_retrospective_improvement |
| WR | receive | random_forest | 0.097 | 0.047 | 0.145 | promising_retrospective_improvement |
| WR | receive | ridge_sequence | 0.084 | 0.033 | 0.132 | promising_retrospective_improvement |
| WR | receive | spline | 0.082 | 0.029 | 0.131 | inconclusive_difference |
| WR | receive | xgboost | 0.105 | 0.046 | 0.158 | promising_retrospective_improvement |

All candidates versus `ridge_full`; skill is fractional MSE reduction:

| position | component | model | skill | player_conditional_low | player_conditional_high | conclusion |
| --- | --- | --- | --- | --- | --- | --- |
| QB | pass | elastic_net | -0.012 | -0.029 | 0.005 | no_retrospective_improvement |
| QB | pass | hurdle | 0.009 | -0.004 | 0.023 | inconclusive_difference |
| QB | pass | random_forest | -0.014 | -0.102 | 0.051 | no_retrospective_improvement |
| QB | pass | ridge_sequence | 0.020 | 0.000 | 0.042 | inconclusive_difference |
| QB | pass | spline | -0.012 | -0.035 | 0.009 | no_retrospective_improvement |
| QB | pass | xgboost | -0.025 | -0.095 | 0.034 | no_retrospective_improvement |
| QB | rush | elastic_net | 0.032 | -0.004 | 0.077 | inconclusive_difference |
| QB | rush | hurdle | -0.001 | -0.008 | 0.009 | no_retrospective_improvement |
| QB | rush | random_forest | -0.007 | -0.087 | 0.085 | no_retrospective_improvement |
| QB | rush | ridge_sequence | 0.020 | -0.012 | 0.047 | inconclusive_difference |
| QB | rush | spline | 0.016 | -0.019 | 0.054 | inconclusive_difference |
| QB | rush | xgboost | 0.012 | -0.060 | 0.098 | inconclusive_difference |
| RB | receive | elastic_net | 0.011 | 0.003 | 0.018 | inconclusive_difference |
| RB | receive | hurdle | 0.001 | -0.009 | 0.012 | inconclusive_difference |
| RB | receive | random_forest | 0.005 | -0.021 | 0.029 | inconclusive_difference |
| RB | receive | ridge_sequence | -0.001 | -0.006 | 0.003 | no_retrospective_improvement |
| RB | receive | spline | 0.004 | -0.004 | 0.010 | inconclusive_difference |
| RB | receive | xgboost | 0.006 | -0.030 | 0.033 | inconclusive_difference |
| RB | rush | elastic_net | -0.017 | -0.032 | -0.003 | no_retrospective_improvement |
| RB | rush | hurdle | -0.017 | -0.041 | 0.006 | no_retrospective_improvement |
| RB | rush | random_forest | -0.021 | -0.048 | 0.003 | no_retrospective_improvement |
| RB | rush | ridge_sequence | 0.002 | -0.005 | 0.010 | inconclusive_difference |
| RB | rush | spline | -0.002 | -0.006 | 0.002 | no_retrospective_improvement |
| RB | rush | xgboost | -0.011 | -0.029 | 0.008 | no_retrospective_improvement |
| TE | receive | elastic_net | -0.009 | -0.022 | 0.003 | no_retrospective_improvement |
| TE | receive | hurdle | 0.011 | -0.003 | 0.026 | inconclusive_difference |
| TE | receive | random_forest | -0.084 | -0.206 | 0.010 | no_retrospective_improvement |
| TE | receive | ridge_sequence | 0.001 | -0.005 | 0.006 | inconclusive_difference |
| TE | receive | spline | -0.000 | -0.004 | 0.004 | no_retrospective_improvement |
| TE | receive | xgboost | -0.077 | -0.154 | -0.009 | no_retrospective_improvement |
| WR | receive | elastic_net | 0.006 | -0.001 | 0.015 | inconclusive_difference |
| WR | receive | hurdle | 0.028 | 0.018 | 0.038 | inconclusive_difference |
| WR | receive | random_forest | 0.015 | -0.018 | 0.050 | inconclusive_difference |
| WR | receive | ridge_sequence | 0.001 | -0.003 | 0.005 | inconclusive_difference |
| WR | receive | spline | -0.001 | -0.007 | 0.005 | no_retrospective_improvement |
| WR | receive | xgboost | 0.024 | -0.010 | 0.057 | inconclusive_difference |

The promising screen requires >=5% primary skill, a positive conditional player interval, year/era guards and no >10% harm in sufficiently supported prior-opportunity strata. It does not establish multiplicity-adjusted significance, future-year uncertainty or independent confirmation.

## Positive-opportunity probabilities

Hurdle probabilities target N>0, not injury status or all offensive participation. Log scores clip probabilities to [1e-12, 1−1e-12] for numerical scoring. Fixed calibration bins and per-year scores are saved.

| experiment | position | component | model | year | rows | brier | log_loss | mean_probability | observed_frequency |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| pinned | QB | pass | hurdle | primary | 243 | 0.147 | 0.462 | 0.580 | 0.564 |
| pinned | QB | pass | training_prevalence | primary | 243 | 0.247 | 0.686 | 0.589 | 0.564 |
| pinned | QB | rush | hurdle | primary | 243 | 0.150 | 0.461 | 0.594 | 0.564 |
| pinned | QB | rush | training_prevalence | primary | 243 | 0.246 | 0.686 | 0.586 | 0.564 |
| pinned | RB | receive | hurdle | primary | 434 | 0.142 | 0.441 | 0.511 | 0.488 |
| pinned | RB | receive | training_prevalence | primary | 434 | 0.251 | 0.696 | 0.526 | 0.488 |
| pinned | RB | rush | hurdle | primary | 434 | 0.153 | 0.475 | 0.505 | 0.525 |
| pinned | RB | rush | training_prevalence | primary | 434 | 0.249 | 0.692 | 0.529 | 0.525 |
| pinned | TE | receive | hurdle | primary | 393 | 0.116 | 0.380 | 0.483 | 0.532 |
| pinned | TE | receive | training_prevalence | primary | 393 | 0.250 | 0.694 | 0.494 | 0.532 |
| pinned | WR | receive | hurdle | primary | 764 | 0.125 | 0.403 | 0.461 | 0.484 |
| pinned | WR | receive | training_prevalence | primary | 764 | 0.250 | 0.693 | 0.486 | 0.484 |
| extended | QB | pass | hurdle | primary | 850 | 0.149 | 0.471 | 0.565 | 0.562 |
| extended | QB | pass | training_prevalence | primary | 850 | 0.247 | 0.686 | 0.541 | 0.562 |
| extended | QB | rush | hurdle | primary | 850 | 0.157 | 0.490 | 0.557 | 0.555 |
| extended | QB | rush | training_prevalence | primary | 850 | 0.247 | 0.688 | 0.532 | 0.555 |
| extended | RB | receive | hurdle | primary | 1596 | 0.146 | 0.452 | 0.523 | 0.517 |
| extended | RB | receive | training_prevalence | primary | 1596 | 0.250 | 0.693 | 0.507 | 0.517 |
| extended | RB | rush | hurdle | primary | 1596 | 0.150 | 0.458 | 0.543 | 0.533 |
| extended | RB | rush | training_prevalence | primary | 1596 | 0.249 | 0.692 | 0.517 | 0.533 |
| extended | TE | receive | hurdle | primary | 1396 | 0.135 | 0.422 | 0.526 | 0.516 |
| extended | TE | receive | training_prevalence | primary | 1396 | 0.250 | 0.693 | 0.518 | 0.516 |
| extended | WR | receive | hurdle | primary | 2637 | 0.136 | 0.423 | 0.492 | 0.490 |
| extended | WR | receive | training_prevalence | primary | 2637 | 0.250 | 0.693 | 0.486 | 0.490 |

## Sequence and more complex model feasibility

| position | component | outer_year | distinct_training_careers | earlier_origins | signal_origins | signal_gate | career_gate | gate_pass | experiment |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| RB | rush | 2025 | 593 | 7 | 0 | False | True | False | extended |
| WR | receive | 2025 | 529 | 2 | 0 | False | True | False | pinned |
| QB | pass | 2025 | 256 | 7 | 1 | False | False | False | extended |
| TE | receive | 2025 | 283 | 2 | 0 | False | False | False | pinned |
| RB | receive | 2025 | 593 | 7 | 0 | False | True | False | extended |
| RB | rush | 2025 | 316 | 2 | 1 | False | False | False | pinned |
| TE | receive | 2025 | 482 | 7 | 0 | False | False | False | extended |
| RB | receive | 2025 | 316 | 2 | 0 | False | False | False | pinned |
| QB | rush | 2025 | 256 | 7 | 1 | False | False | False | extended |
| QB | rush | 2025 | 163 | 2 | 0 | False | False | False | pinned |
| QB | pass | 2025 | 163 | 2 | 0 | False | False | False | pinned |
| WR | receive | 2025 | 924 | 7 | 0 | False | True | False | extended |

The gate uses distinct earlier inner origins; each alpha is selected from still earlier years, with alpha 10 for the first origin. The comparison uses identical cached baseline memberships. Passing 500 training careers alone does not supply sequence signal; 5% improvement is required in two origins. Ordered summaries use up to four observed games with missing indicators, not zero-padded scheduled weeks. Repeated games and roles do not create independent careers.

Dynamic recency and heterogeneous rate pooling remain baseline features. A full state-space distribution has not been specified or calibrated; point-forecast residuals do not identify its measurement, drift and context variances. RL remains unsupported: decision-time feasible actions, behavior policies, transitions and compatible rewards/costs are absent. No neural dependency, simulator or ensemble was introduced.

## Sensitivity and economic interpretation

Artifacts retain every paired loss, annual/era/subgroup guard, player/prior-team conditional interval, leave-one-year comparison, extreme/influential-row omission sensitivity, missing-history and position-change strata, fitted-value-bin errors, selected coefficients and common-recent-outcome history comparisons. Full-sample primary results remain the headline. Coefficient zeros are descriptive and not scientific variable selection. No distribution score or interval is assigned to these point predictors.

Scenario files measure changes in EPA forecasts and rank correlation against audited Ridge. At a fixed comparator and cost, delta S = lambda × delta D. This does not establish actual savings, feasible acquisition choices, policy regret or monetary surplus. Complete cap/cash/exit obligations, supported replacement forecasts, contribution prices and predictive covariance remain missing. `forecast_validated=False`; economic surplus stays unknown. Retrospective source revisions, shared games/teams and few calendar seasons remain unresolved. No prospective freeze has occurred.


## Reading the incremental evidence

On extended 2019–2025 outcomes, Random Forest QB passing has 9.8% MSE skill against original direct Ridge, but −1.4% against audited Ridge. WR Elastic Net, hurdle, Random Forest, XGBoost and sequence Ridge meet the original-baseline screen; their incremental skills over audited Ridge are only 0.6%, 2.8%, 1.6%, 2.4% and 0.1%, respectively. Spline WR fails the prior-zero-opportunity guard against the original baseline. No extended challenger reaches even the 5% practical threshold against audited Ridge. Improvements against the original baseline therefore do not establish that added model complexity beats the audited feature representation.

The WR hurdle increment is positive in all seven years, with conditional player-resampling interval [1.8%, 3.8%]. Its skill is 1.7% after omitting training-defined extreme outcomes and 2.5–3.1% under leave-one-year omission. These are useful exploratory signals but do not move the threshold. On the shorter pinned experiment, RB rushing Elastic Net and sequence Ridge pass the screen against audited Ridge, yet neither reaches 5% against original direct Ridge; the full historical comparison does not replicate those incremental claims.

Hurdle positive-opportunity Brier/log scores improve on training-prevalence references in all six extended roles. Extended Brier scores are 0.135–0.157 versus 0.247–0.250, but maximum absolute fixed-bin calibration gaps range from 2.8 to 8.7 percentage points. Better probability scores do not imply better signed-total EPA predictions or calibrated talent uncertainty. The numerical failure ledgers are empty: 324 outer fits and 4,278 inner trials completed with zero recorded estimator warnings. One descriptive rank correlation is undefined because pinned RB-receiving Elastic Net predicts a constant in 2025; the report retains NaN and its warning. Local Arrow CPU-discovery warnings are environmental, not estimator failures.

At the latest audited origin (2025), extended training includes 256 QB, 593 RB, 482 TE and 924 WR careers. Only one earlier QB passing and one QB rushing origin attain 5% sequence skill; RB, TE and WR have none. No role reaches two qualifying origins in either experiment. No RNN/LSTM fit is justified under the frozen gate. The sequence-support audit separately records absent/short histories, calendar gaps, undefined rates and missing second seasons.

WR hurdle forecasts differ from audited Ridge by mean absolute 0.86–1.25 EPA per year, with rank correlations 0.965–0.984. Those forecast changes cannot establish a feasible replacement, complete obligations, surplus sign or causal policy value. No dollar-surplus figure is inferred. Next work should address decision-time source/context/cost gaps or independently specified prospective confirmation; do not expand the historical grid to obtain a winner. All 94 local tests pass.
