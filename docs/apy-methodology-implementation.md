# APY methodology implementation: Phases 1–3

Implemented against the October 1, 2026 audit. All calculations are research on public attributed production and observed contract prices. The implementation separates NFL definitions (`domain`, `contracts`, `opportunities`), statistical models (`valuation`, `contribution`, `pricing`, `forecasting`) and I/O (`etl`, download scripts). It introduces no new service, optimizer, deployment or scheduling dependency.

## Research objective and necessary changes to the proposal

The project asks several different questions: observed cost efficiency, attributed contribution relative to an acquisition comparator, negotiated contract pricing, and future roster decisions. Each has its own numerator, financial denominator and information cutoff. A salary regression does not identify the causal value of production or an individual player's economic surplus.

Three implementation findings changed the approach:

1. **EPA and official opportunities have different coverage.** nflfastR's EPA filters include two-point attempts; official attempts, carries and targets do not. Historical source rows contain nonzero EPA with zero official opportunities. Rate models therefore require PBP counts using the same filters as the EPA numerator, with per-player/game reconciliation. Official counts remain useful role features. [nflfastR implementation](https://github.com/nflverse/nflfastR/blob/master/R/calculate_stats.R)
2. **Identity reconciliation needs more than an OTC join.** The source reuses GSIS IDs on some homonymous contract players whose master OTC IDs are missing. For example, an older Kenneth Walker WR deal shares the current RB's GSIS ID. Master OTC identity takes precedence; incompatible position families cannot establish a source GSIS identity. Surviving competing OTC identities remain unresolved. Some historical roster PFR IDs also disagree with the master identity table; master mappings take precedence and disagreements are recorded.
3. **Forecast products require assumptions.** Multiplying expected games by mean opportunities per game need not estimate expected total opportunities. Future games, total opportunities and total component EPA are directly supervised on historical transitions, including unavailable/zero outcomes. A separate pooled-rate × predicted-volume proxy is evaluated against the direct EPA benchmark; its independence approximation is explicit.

Observed contract prices can support retrospective market associations. Feasible replacement availability, verified equivalent new-money extension terms, complete horizon-specific obligations, and a causal dollars-per-contribution price are not established by these sources. The code produces explicit sensitivity scenarios and preserves unknown economic surplus rather than inventing those inputs.

## Phase 1: definitions and defensible reporting

`src/contracts.py` extracts `contract_apy_m`, `season_cap_charge_m`, `season_cash_m`, contract type, signing year, available signing/effective/transaction dates, provider URL and snapshot timestamp. All costs are in millions. `yearly_cap_hit` and `cap_pct_of_team` remain compatibility aliases for APY and APY/current league cap.

Annual histories are player-wide and repeated on several contract rows. Exact duplicate entries are removed. Competing annual records stay unresolved; costs are not summed across former and current teams. Team/production mismatches and multi-team production require allocation reconciliation. The join audit retains extracted provider numbers; the player-season financial fields remain unknown when their team scope is unresolved. Financial coverage includes unknown contracts.

Latest signing-year selection remains an explicitly approximate association. Competing historical same-year APYs are unknown. A uniquely active current-season price may be used when the source snapshot is from that season. No expiration date is inferred from signing year plus term. Contract types are matched to recorded terms rather than defined as the complement of a rookie heuristic. [Contract dictionary and units](https://nflreadr.nflverse.com/articles/dictionary_contracts.html)

Dashboard leaderboards are named **Positive-EPA APY efficiency**, with separate annual-cap and annual-cash options. Comparisons use positions and recorded mechanisms; rookie/non-rookie filters are explicitly estimated cohorts. Tables show raw production, snaps and available opportunities. Unknown costs remain unranked. Team aggregates disclose APY coverage and overlapping player attributions.

`src/valuation.py` saves a player-grouped out-of-fold prediction for every supported historical row, including rookies whose later veteran seasons would otherwise leak into their valuation. It saves outer fold, training-row signature, training years, regularization, bound, extrapolation flag and prediction provenance. The inverse log model estimates a geometric center of `1 + APY cap-share percentage`, not mean-dollar APY. `apy_association_gap_m` describes a difference from this center. `surplus_value` is retained only as a deprecated alias.

Validation and scoring share the same transformation and training-only cap-share safety bound. Diagnostics include mean/median training-fold baselines, dollar MAE, RMSE, bias and R². Chronological tests use strictly earlier training seasons but completed test-year production: they remain salary associations. Inputs are sorted deterministically; actual outer memberships and dependency versions are saved. Core numerical dependencies are pinned.

Offensive exposure replaces offense + defense snaps. Legacy annual CSVs that lack the new definitions are rejected by `etl.analyze`. Cloud weekly analysis recalculates historical metrics from the immutable, hash-verified source snapshots instead of silently reusing old metrics.

## Phase 2: contribution and uncertainty

`src/opportunities.py` preserves regular-season player/game EPA, offensive snaps, official attempts/sacks/carries/targets, available role shares and context. Model opportunities follow these exact PBP definitions:

| Component | Player identity and play filter | EPA numerator |
| --- | --- | --- |
| Pass | passer ID; `pass` or `qb_spike`; nonmissing `qb_epa` | passing EPA |
| Rush | rusher ID; `run` or `qb_kneel`; nonmissing `epa` | rushing EPA |
| Receive | receiver ID; nonmissing `epa` | receiving EPA |

These include recorded two-point attempts. Missing-EPA plays are counted separately and excluded from rate exposures. PBP/weekly differences beyond 0.002 EPA fail validation; this tolerance accommodates historical three-decimal source rounding. Incomplete-season regular-game coverage remains unchanged. FB/HB participation maps to the same RB family as the roster data while preserving the raw participation role; game totals must reproduce season metrics. Blocking remains unmeasured, especially for fullbacks. Routes, blocking and unobserved tracking quantities are not inferred.

`src/contribution.py` fits normal hierarchical component rates by position. Sampling variability uses player-season game-cluster residual sums rather than treating offensive snaps as independent plays. One-game observations use training-cohort noise. Heterogeneous-noise marginal likelihood estimates the prior mean and variance; an unweighted variance-minus-average-noise rule can collapse the prior because of a few very noisy low-opportunity observations. Optional regularized team/opponent associations are compared through earlier chronological transitions. They do not isolate player contribution.

Pooling scale and context choice use only earlier transitions. Future reference bands additionally learn an innovation variance from earlier prediction errors, allowing rates to change between seasons. The current latent-rate reference bands hold estimated hyperparameters fixed and do not include their estimation uncertainty. Future coverage is assessed for observed rates conditional on realized future exposure; zero-future-opportunity outcomes have no defined rate and remain in the separate volume/availability tests.

Replacement sensitivity uses identifiable one-year SFA acquisitions. An expanded one-year SFA/UFA cohort provides another comparator. Cohort means weight acquired player-seasons equally; minimum distinct-player coverage is disclosed. These are acquisition proxies, not certified available replacements. They support component-specific above-comparator EPA, including negative raw-EPA players. No positional percentile or constant EPA shift defines replacement level. No pooled cross-position contribution rank is produced.

Reports preserve leave-one-week sensitivity with original eligibility held fixed and whole-week bootstrap rank stability. All games/players in a sampled week move together, retaining shared-game dependence. This is a descriptive stress test, not a confidence interval. Three observed weeks provide especially limited resampling support.

## Phase 3: negotiated prices and roster decisions

`contract_events` collapses identical negotiated terms repeated across transferred-team histories. Current status and realized earnings do not define event identity. Competing same-player/year events, inconsistent APY/term/total conventions, missing identity and unconfigured signing caps are audited rather than silently weighted as separate negotiations.

`src/pricing.py` builds one row per usable event. Year-only signing precision implies the earliest possible date is January 1. The immediately preceding NFL season can finish after that date, so features conservatively use completed production **two seasons before signing**. Signing-year cap normalizes the observed stated APY. This is a time-valid feature window; retrospective source revisions still prevent a contemporaneous-offer claim. Low-volume players remain eligible when pre-event history exists; missing history is a reported selection boundary.

Models pool recorded contract mechanisms within each position using regularization. On identical later event rows, the implementation compares:

- Direct cap-share Ridge and Gamma/log-link models, targeting conditional means and tuned on squared cap-share error.
- Median quantile regression, tuned on median pinball loss.
- Log-target Ridge, tuned on its transformed loss and labeled with its different estimand.
- Fold-trained mean and median price baselines.

Inner tuning uses earlier event years. Outer price tests begin in 2025; each tested player's target history is purged from training. Saved event memberships and hashes document separation. Cohort-specific dollar errors and sample counts are reported. No winner is chosen from an outer test's R², and no price-prediction error is called latent market-value uncertainty. [Gamma model](https://scikit-learn.org/stable/modules/generated/sklearn.linear_model.GammaRegressor.html), [quantile loss](https://scikit-learn.org/stable/modules/generated/sklearn.metrics.mean_pinball_loss.html)

`apy_pricing_discount` requires an independently predicted equivalent new-contract APY, compatible terms verified by the caller, and held-out/prospective provenance. Otherwise the discount stays unknown. Historical salary-association gaps do not pass this gate automatically. Neither difference is current cap savings.

`src/forecasting.py` performs June 1 preseason returning-player tests using only completed earlier production/outcomes. Every prior-roster player remains in the population; a disappeared player has zero observed future participation, opportunities and EPA in a fully covered season. Offensive participation is not a health diagnosis. Forecasts separate games, total opportunities, rates and direct component EPA. The partial 2026 season never trains these models. Candidate forecasts are explicitly unapproved; annualizing current three-week EPA is not a forecast method.

`src/roster_decisions.py` accepts explicit per-team/year/horizon cap, cash, guaranteed cash and separate cap/cash exit schedules for player and replacement. Missing, duplicate or ambiguous obligations suppress surplus. Guarantees are disclosed and not added again to already-inclusive cash/cap costs. Scenario contribution price is a caller assumption, not a salary-regression coefficient. Forecast validation and compatible replacement contribution are required. Cap and cash objectives stay separate; latent-surplus lower quantiles remain unknown.

A discrete roster optimizer would require a supported contribution objective, defensible replacement choices and complete constraints. The current sources do not establish them; adding an optimizer would create precision without identification.

## Reproduce and inspect

Use Python 3.12 and the pinned dependencies in `requirements.txt`. The first command makes read-only canonical SELECTs and hash-verified Storage downloads; the second archives public PBP sources and their hashes. The research command runs offline and never modifies cloud state.

```bash
PYTHONPATH=. python scripts/download_methodology_inputs.py
PYTHONPATH=. python scripts/download_epa_exposures.py --seasons 2021 2022 2023 2024 2025 2026
python -m etl.research
python -m pytest -q
```

The downloader defaults to the tracked run IDs in `docs/methodology-inputs-2026-10-01.json`; `--provenance` can select another pinned snapshot set. No credentials or private object contents are stored in that file. Preserve the downloaded files for exact reproduction. Fresh PBP must reconcile with the pinned EPA; a source revision causes a failure rather than an unnoticed model comparison on different data.

`artifacts/methodology-v2/results/` contains source coverage, corrected metrics and games, held-out APY rows, chronological predictions, component priors, rate selections/errors/reference coverage, acquisition cohorts, contribution sensitivity, rank stability, event rows/exclusions/predictions, preseason forecasts/errors, and a validation report. `manifest.json` records dependencies, source/run IDs, actual input hashes, output hashes and seeds. Data files remain ignored by Git. See `docs/apy-methodology-validation.md` for the tested snapshot's results and limitations.

### Database rollout

Apply `infra/supabase/migrations/20261002_apy_methodology.sql` after the existing cloud migrations and before any refresh with this code. `ddl.sql` includes the new financial columns for fresh databases. The additive migration preserves atomic/fenced cloud ingestion and permits unknown APY. It retains legacy aliases, increases APY/EPA precision, and updates every new field on conflict. Old rows' newly added fields remain unknown until recalculated; the dashboard can read their legacy APY alias.

The migration was tested in disposable PostgreSQL, including repeated application, unknown APY and precise annual-cost updates. Production migration, ingestion, deployment and scheduling were not performed by this implementation.

### Validation boundary

Chronological target exclusion prevents numerical fitting/tuning leakage. It does not make repeated retrospective model development an untouched prospective experiment. Rate assumptions and identity handling were refined during this work; independent future-season validation is still required. Small cohorts, selection into observed contracts, source revisions, incomplete attribution and replacement availability remain substantive limits. The saved errors and uncertainty labels are part of the output, not optional interpretation footnotes.
