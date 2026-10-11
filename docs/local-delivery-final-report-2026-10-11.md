# Final local research report — October 11, 2026

**Current delivery is complete.** Existing experiments did not establish qualifying long-history gains over audited Ridge or a prediction ceiling. Recorded 2026 observations add useful descriptions, with no independent prospective validation. `forecast_validated=False`.

## Findings

**Future contribution:** cached primary tests cover 2019–2025 using the full 2016–2025 measurement history. None of the six challenger families passes the fixed 5% incremental MSE screen over audited Ridge. Random Forest QB passing has about −1.4% incremental skill; WR hurdle has +2.77%. Added target-share Ridge has RB −0.423%, TE +0.520%, WR +0.010%. Shorter-history signals remain exploratory. No model, feature, tuning or search was added.

**Negotiated price:** the cached no-mechanism Ridge improves the 2025 QB cohort and worsens the other seven position/year contrasts. This does not establish a generally improved offer forecast. Event selection, realized signing-year cap, source revisions and limited market years restrict interpretation. The 2026 event cohort remains observed to its original archived price snapshot, separate from the game snapshot below. All other cached price models/scores are retained. Salary association, APY forecasts and unpaid cash obligations remain distinct.

Cap-share RMSE is in percentage points; skill is `1 − RMSE_candidate² / RMSE_reference²`. Here the price reference is `ridge_mean`, separate from contribution `ridge_full`.

| Position | Event year | Events | Mean Ridge RMSE | No-mechanism RMSE | MSE skill |
| --- | --- | --- | --- | --- | --- |
| QB | 2025 | 38 | 5.341 | 3.771 | +50.1% |
| QB | 2026 | 36 | 3.484 | 4.183 | -44.2% |
| RB | 2025 | 52 | 0.903 | 1.032 | -30.7% |
| RB | 2026 | 60 | 0.988 | 1.174 | -41.2% |
| TE | 2025 | 53 | 0.927 | 1.064 | -31.6% |
| TE | 2026 | 58 | 0.828 | 1.031 | -55.1% |
| WR | 2025 | 91 | 1.578 | 2.133 | -82.6% |
| WR | 2026 | 109 | 1.527 | 2.065 | -82.8% |

**Roster value:** recorded workload and component EPA can be compared descriptively. They do not identify deployment effects, complete incremental costs, lambda or surplus. The 32-team overview and single PIT case are delivered within that limit.

## Sources, eligibility and use

Execution is October 11, America/New_York. Reused game sources were received **October 4 at 22:56:10 UTC / 18:56:10 EDT**; roster/master sources were received **October 4 at 15:39 UTC / 11:39 EDT**. This is archived coverage, not an October 11 live check. No fetch was needed.

At the data snapshot, **58 of 272 scheduled games were completed**. Schedule/statistics/PBP cover 58; snaps cover 49. Observations use **49 shared completed games**, retaining nine completed source gaps and 214 games not completed at that snapshot. Game-set agreement does not prove provider exhaustiveness.

All **3,020 roster rows** remain. Of 944 core rows, 870 have resolved identity/role, 491 have recorded observations, 379 have no record in shared games, and 74 abstain for identity gaps (73 earlier core cases plus one missing GSIS). The other 2,076 records remain outside the six core tasks. ACT is a provider status, not game eligibility. Membership depends on identity/role/coverage, not performance.

| Position | Full inventory | Eligible | Recorded observation used | No shared-game record | Identity abstention |
| --- | --- | --- | --- | --- | --- |
| QB | 120 | 119 | 52 | 67 | 1 |
| RB | 219 | 202 | 123 | 79 | 17 |
| WR | 392 | 353 | 195 | 158 | 39 |
| TE | 213 | 196 | 121 | 75 | 17 |

Full source usage is retained: 4,024 stat rows (1,114 used), 4,579 snap rows (1,300 used), and 1,292 component rows (1,232 used). These overlapping units are not distinct people. Missing totals stay blank; missing games and rows never become annual zeros. No annualization or annual forecast was produced.

Historical scope remains intact: **13,490 extended transitions**, 11,448 used in archived fitting/testing, 8,925 primary scored, and 2,042 unknown labels. The separate **6,656 pinned transitions** include 5,042 used, 2,511 primary scored and 1,614 unknown labels; they are a subset, not additive. Prior years are 2016–2025 and target years 2017–2026; partial 2026 labels remain unknown.

The price ledger preserves **46,470 records with exclusion reasons**. Archived timing folds use 554 training and 497 test events: 887 unique events across overlapping folds; 45,583 have no timing-fold use. Complete lists and fold IDs are packaged.

Three archived observed labels remain uncertified under the conflict rule; five extended transitions have affected feature/label links, four used in training. Their numerical sensitivity is unknown. Original sources, labels, fits and scores remain unchanged. The dated [observation scope amendment](/Users/fanzirui/Desktop/samfan-27/nfl-roster-roi/docs/local-delivery-scope-amendment-2026-10-11.md) allows admitted rows' observations while preserving annual gates, old rules and the rejected preparation record.

## 32 teams and PIT

[team_overview_32.csv](/Users/fanzirui/Desktop/samfan-27/nfl-roster-roi/artifacts/local-delivery-20261011T221704Z/team_overview_32.csv) and [TEAM_OVERVIEW.md](/Users/fanzirui/Desktop/samfan-27/nfl-roster-roi/artifacts/local-delivery-20261011T221704Z/TEAM_OVERVIEW.md) cover every team. Totals use actual game-team attribution for supported position/component pairs; roster counts use roster-team attribution. Coverage varies between three and four shared games. Passing and receiving credits overlap and must not be summed into roster value. Defense, blocking, special teams and omitted roles are not valued.

The PIT action remains **October 3, 2026, 22:00 EDT**, retain Metcalf versus release and redeploy one existing teammate over the remaining 2026 regular season. Shared games have schedule dates through October 1; later receipts remain retrospective research evidence, not proof of decision-time availability.

| Player | Recorded snap games | Offensive snaps | Receiving opportunities | Receiving EPA |
| --- | --- | --- | --- | --- |
| DK Metcalf | 4 | 261 | 33 | -0.537 |
| Roman Wilson | 4 | 138 | 24 | 7.387 |
| Ben Skowronek | 4 | 34 | 4 | 1.737 |
| Michael Pittman Jr. | 3 | 156 | 12 | 2.382 |

Wilson, Skowronek and Pittman remain signed internal candidates subject to actual eligibility and role constraints. These recorded totals do not measure incremental redeployment or displaced contributions; their differences are not deployment `D`.

The cost ledger is byte-identical: **336 rows, 270 unknown, 54 provider-reported, 12 conditional; all six cap/cash comparisons incomplete**. No new payment/contract evidence exists. Complete `K`, remaining-season `D`, lambda and surplus remain unknown. APY/annual schedules are not remaining cash debt. Other teams' specific action costs remain uninvestigated. No private materials are requested.

## Verification and unknowns

Python 3.12.4 and existing dependencies were reused, with a saved environment lock. Targeted checks passed for **261 prior manifest entries**, unchanged inputs, full source/roster inventories, all 32 summaries, historical values and PIT ledger bytes. All **1,292 current component rows** reconcile independently to raw shared-game PBP; maximum EPA discrepancy is **3.55e−15**. Pinned/price copies and fold counts were also verified. No historical audit or fit was rerun. Logs retain environmental Arrow warnings and one pandas null-serialization FutureWarning; no numerical failure occurred.

True future accuracy, a prediction ceiling, exhaustive annual coverage, identity-error sensitivity, new-role transport, remaining obligations and counterfactual value remain unknown. The strict annual preparation rejection remains valid for that recipe. The prospective protocol is preserved and unexecuted; this completed delivery has no scheduled follow-up.

Evidence: [current_source_receipts.csv](/Users/fanzirui/Desktop/samfan-27/nfl-roster-roi/artifacts/local-delivery-20261011T221704Z/current_source_receipts.csv), [current_game_coverage.csv](/Users/fanzirui/Desktop/samfan-27/nfl-roster-roi/artifacts/local-delivery-20261011T221704Z/current_game_coverage.csv), [current_roster_eligibility_usage.csv](/Users/fanzirui/Desktop/samfan-27/nfl-roster-roi/artifacts/local-delivery-20261011T221704Z/current_roster_eligibility_usage.csv), [historical_eligibility_usage.csv](/Users/fanzirui/Desktop/samfan-27/nfl-roster-roi/artifacts/local-delivery-20261011T221704Z/historical_eligibility_usage.csv), [cached_price_eligibility_usage.csv](/Users/fanzirui/Desktop/samfan-27/nfl-roster-roi/artifacts/local-delivery-20261011T221704Z/cached_price_eligibility_usage.csv), [pit_case_observations.csv](/Users/fanzirui/Desktop/samfan-27/nfl-roster-roi/artifacts/local-delivery-20261011T221704Z/pit_case_observations.csv), [verification.json](/Users/fanzirui/Desktop/samfan-27/nfl-roster-roi/artifacts/local-delivery-20261011T221704Z/verification.json) and [manifest.json](/Users/fanzirui/Desktop/samfan-27/nfl-roster-roi/artifacts/local-delivery-20261011T221704Z/manifest.json). Additional tables, immutable copies and snapshots are indexed in [README.md](/Users/fanzirui/Desktop/samfan-27/nfl-roster-roi/artifacts/local-delivery-20261011T221704Z/README.md) and the manifest.

Local research and reversible reporting changes only; no external contact, payment, production change, deployment, automation, commit or push.
