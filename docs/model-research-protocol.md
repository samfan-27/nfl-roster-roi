# NFL roster-value research protocol, v3

Initial specification: October 2, 2026 (America/New_York). Written before new
v3 baseline fits or challenger evaluation. Prior v2/audit results have already
been inspected. All historical evidence is exploratory retrospective evidence.
The approved execution prompt governs scope; this protocol fixes the finite
experiment, not a production approval. No cloud writes, deployment or publication.

## Scientific question

Given information genuinely available at a stated roster or contract decision,
can defensible models improve predictions of future attributed production and
negotiated price, and do the improvements support more stable roster-value
comparisons under explicit replacement and cost assumptions?

The initial contribution scope is one regular season, returning QB/pass,
QB/rush, RB/rush, RB/receive, WR/receive and TE/receive. Other components,
entrants without earlier roster history, other positions and multiple years are
outside the fitted claim. Passing and receiving credit overlap. No sum across
these players estimates team value, and skill-family EPA is not full roster value.

## Task definitions

| Task | Unit/population and information | Time/horizon and target/support | Missing outcomes, loss, weights and interpretation |
| --- | --- | --- | --- |
| Salary association | Completed player-season; v2 100-offensive-snap, known positive APY, complete five-feature cohort; veteran training, whole-player held-out scoring | Completed season, same-season APY in $M; positive | Original exclusions reproduced, explicit ledger/counts. Log squared loss tuning; dollar MSE/MAE diagnostics, equal player-season weights. Historical association, not an offer |
| Returning contribution | Prior-season roster/participation spine, prior position fixed; resolved GSIS/PFR identities, no future participation selection | June 1 of year t; full regular-season offensive-participation games G, exact opportunities N, signed component EPA Y; G,N nonnegative integers, Y real | Covered absence is zero only with identity/coverage checks; uncovered outcomes unknown. Equal player-transition squared loss per role, arithmetic conditional means |
| Future observed rate | Same returning cohort with prior and future positive component exposures, as in v2 rate reference | June 1; R=Y/N in future season, real and undefined at N=0 | Conditioning on future N>0 is evaluation selection, not forecast eligibility. Equal player-rate MSE; observed-rate reference intervals conditional on realized exposure |
| Negotiated price | Identifiable, positive-price event with reconciled stated APY convention and production history t-2; observed UFA/Extension/SFA/RFA/ERFA/Franchise/Transition | January 1 t conservative cutoff for year-only signing; stated APY/cap percentage points, positive | Unknown identity/terms/history excluded with reasons. Primary mean cap-share MSE, median pinball .5 separate; equal events. Conditional retrospective event benchmark; eventual mechanism is ex-post unless independently documented |
| Roster scenario | Specified player and feasible replacement, same team/component/horizon; complete avoidable cap or cash and additional exit obligations | Discounted contribution difference D and compatible incremental cost K; S(lambda)=lambda D-K | Missing costs/replacement/validated predictions imply unknown S. Sensitivity, not regression of synthetic surplus or observed counterfactual truth |

Production timing is a *necessary reconstructed* availability date: last included
regular-season game plus one day. This is not proof of provider release on that
day. Save both this date and actual source snapshot timestamp for every row.
All canonical historical snapshots were fetched October 1, 2026, with later PBP
fetches; revisions cannot be excluded. Age is measured from prior September 1
and advanced deterministically to June 1; experience remains prior recorded
experience, not inferred CBA accrued seasons. Last prior-season position/team
labels are retrospective roster reconstructions, never future team assignments.
For event targets use December 31 of signing year plus one day as conservative
event-label availability; the 2026 event test is explicitly observed-to-snapshot,
not complete-year event coverage. Realized signing-year cap is a retrospective
normalizer at January 1, not a known January price conversion.

## Mathematical foundations

For any information I and finite second moment, writing m=E[Y|I] gives
E[(Y-f)^2|I]=Var(Y|I)+(m-f)^2: the cross term vanishes since
E[Y-m|I]=0. Thus squared risk targets the arithmetic conditional mean. Absolute
risk is minimized by a conditional median (subgradient 2F(f)-1); pinball
rho_q(u)=u(q-1{u<0}) targets a q quantile (subgradient F(f)-q), possibly nonunique
at atoms. A collection of separate means is not a joint predictive distribution.

For known cap C in I, Z=log(1+100A/C), m_Z=E[Z|I], epsilon=Z-m_Z:
E[A|I]=C/100 {exp(m_Z) E[exp(epsilon)|I]-1}. Convexity gives
exp(E[Z|I]) <= E[exp(Z)|I], strictly for nondegenerate Z. Inverse log is a
geometric center of 1+100A/C, not generally mean A or median A. If C is random
and unknown, it must remain inside the conditional expectation. A residual
smearing factor requires finite exponential moments and transport of the
conditional residual law. Global smearing assumes away heteroskedasticity;
audit earlier out-of-fold residual exponential factors by fitted-value bins
before considering it. No smearing candidate is authorized merely by Jensen.
Clipping changes the predictor; reproduce old bounds and separately report
bounded versus raw errors. No target-dependent transformation uses outer labels.

For fixed observations and nonnegative weights, R²=1-SSE/SST_test, where
SST_test=sum w_i(y_i-ybar_w)^2. Its ordering agrees with SSE on identical rows
and weights. R² may be negative and is not squared correlation. Return unknown
with a degeneracy flag when variance <=1e-12 max(1, mean(y²)), including perfect
constant targets. Baseline skill=1-SSE/SSE_baseline has a different denominator;
zero baseline error yields unknown skill, not a favorable finite score.
By total variance, optimal population R²=Var(E[Y|I])/Var(Y)
=1-E[Var(Y|I)]/Var(Y), when Var(Y)>0. It is a property of I and the population,
not a ceiling estimated from a fitted model's residuals. Predictable coaching,
teammates or opportunity allocation do not become causal talent.

For r_i|theta_i~N(theta_i,v_i), theta_i~N(mu,tau²), completing the square gives
w=tau²/(tau²+v_i), posterior mean wr_i+(1-w)mu and variance tau²v_i/(tau²+v_i).
Game-cluster residual sums estimate v_i; one-game rates need training-cohort
noise. This approximation separates sampling noise from population rate
variation. Repeated players, shared teams/games, heavy tails and few games limit
the independent normal likelihood. Plug-in posterior variance omits prior
hyperparameter uncertainty; future rates also require temporal innovation and
future observation noise. v2 bands condition on realized future exposure and
do not establish latent-talent uncertainty. Preserve their failures.

At N=0, rate is undefined and total is zero. For positive exposure:
E[Nr|I,N>0]=E[N|I,N>0]E[r|I,N>0]+Cov(N,r|I,N>0).
Multiply by P(N>0|I) for the unconditional mean. The analogous games-times-usage
product needs its covariance too. A hurdle model instead fits P(N>0|I) and
E[Y|I,N>0] directly; its product follows total expectation and preserves signed
EPA. A product of separately forecast marginal rate and volume is only a proxy.

scikit-learn 1.8 Ridge minimizes SSE+alpha||beta||²; Elastic Net minimizes
SSE/(2n)+alpha*l1_ratio||beta||_1+alpha*(1-l1_ratio)||beta||²/2. Their alphas
are not numerically comparable. Elastic Net targets are standardized inside
training to make its fixed grid meaningful. QuantileRegressor minimizes mean
pinball loss plus alpha L1. GammaRegressor uses half mean Gamma deviance plus
alpha L2/2, with positive support and log link; its tuning here uses MSE.
Signed EPA cannot be a Gamma/Poisson/Tweedie-positive target. Additive splines
with Ridge penalize basis coefficients, not a conventional integrated-curvature
GAM penalty; the distinction and basis dimension must be recorded.

Dollar errors e_A=C e_Q/100 for Q=100A/C, so dollar squared risk weights
normalized squared errors by (C/100)². Primary price selection uses equal-event
cap-share risk, secondary dollar scores preserve these year weights. Log RMSE
does not optimize either arithmetic-mean risk. Never compare event R² with
salary-association R² as a paired improvement.

## Data freeze, eligibility and preprocessing

Use only pinned v2 canonical inputs and results. Hash all referenced files;
rebuild historical source joins/PBP exposures read-only into v3, reconcile to
saved v2 numeric fields, and preserve v2 bytes. Record schemas, source timestamps,
game coverage, row identities, distinct players, events, missingness and every
exclusion. Schedule files supply observed game dates; their hashes are newly
recorded local provenance, not represented as canonical source-manifest hashes.

Resolve identity using archived master/roster mappings. Never infer zero from a
failed join: require unique GSIS/PFR mapping for snap absence and full shared
game coverage. Keep unknown target rows in the ledger but not scored/trained.
Construct future totals directly from all covered game components regardless
of future roster position; multi-team totals remain one player-season. Freeze
prior position, flag changes for sensitivity, do not exclude them. This audit
may change the v3 cohort relative to v2; compare only paired common membership
and separately disclose full coverage. No candidate drops a difficult row.

Numeric median imputation with missing indicators, retained empty columns and
standardization is fitted within each training split. Constant columns remain
harmless under regularization; record training variance. Unseen market categories
map to all-zero one-hot encoding. Missing second history stays missing with an
indicator; no survivor restriction. Generated rate priors use only games through
each feature season, never that transition's future label. This as-of procedure
is fixed and rerun per historical origin; no full-data fitted prior enters X.
No target encoding is used. Player-specific random effects are not fitted.

## Validation and finite experiment

Returning-player primary tests: 2024 and 2025; 2023 is a saved baseline reference
and warm-up with no chronological tuning window. Training transitions start in
2022. For outer t, inner validation uses each available year 2023..t-1 with
strictly earlier target years. Selection minimizes pooled inner squared loss
(equal rows, not average fold R²); ties follow configuration order. Annual
label horizons do not overlap; require availability < cutoff and no extra
embargo. Save actual row IDs for outer/inner train/test, not just years. Own
earlier history is legal in returning-player fits.

Event reproduction preserves v2 player-purged chronological train/test and
original inner selection. New no-mechanism Ridge uses the same eligible event
rows, removes eventual contract type, and purges validation/test players from
supervised training. Available historical measurements may still be supplied
for those held players: this is new-player *target* generalization, not a
claim of no known NFL history. Mechanism-conditioned models remain conditional
retrospective references. No new complex price suite: very few event years and
market timing uncertainty justify concentrating new model search on production.

Baseline matrix, all saved: training mean and median, previous-season target,
original five-volume-feature alpha-10 multi-output Ridge, original pooled-rate
times predicted-volume proxy; feature-augmented fixed-alpha Ridge; nested-alpha
volume-only Ridge; nested-alpha audited Ridge. Reproduce salary/event/rate v2
references independently before challenger access. Primary contribution
baseline is original direct EPA Ridge on v3 eligible rows. Games/opportunity
baselines are its corresponding outputs. Primary price baseline is v2 mean Ridge
for algorithm comparisons and training mean for absolute usefulness; median
forecasts assessed separately by pinball, never declared mean winners from MAE.

Candidate order, frozen budgets per role/outer window (seed 20261002):

| Candidate | Hypothesis, objective and finite grid | Complexity/support/extrapolation |
| --- | --- | --- |
| Audited Ridge / ablations | Signed efficiency and history add information; alpha 1,10,100,1000. Full; without efficiency; without second history; volume-only | Linear signed mean; at least 20 rows/10 players in training; fixed-alpha intermediate isolates representation |
| Elastic Net | Correlated features benefit from shrinkage/sparsity; alpha .01,.1,1; l1 ratio .2,.8 | 6 fits per inner window; standardized target; max_iter 20000, tol 1e-5; coefficients descriptive |
| Additive spline Ridge | Limited nonlinear age/workload/rate response; 3 knots, degree 2; alpha 10,100,1000 | Splines only age, log1p opportunities, pooled rate, plus other linear features; <=12 added basis columns; constant extrapolation |
| Dynamic summaries + pooling | Persistence/drift beyond last-season volume; fixed recency .7 last/.3 previous; efficiency ablation and nested Ridge | As-of heterogeneous rate prior; no fully Bayesian state-space distribution without stronger temporal support |
| Hurdle signed total | Zero opportunity mass plus conditional signed mean; logistic C=1; positive-row Ridge alpha 10,100 | Direct P(N>0)E[Y|N>0] integration, Brier/log score; no unsupported 17-game binomial bound |
| Random Forest | Nonlinear thresholds; 200 trees; depth 2 or 4; leaf 20 or 40; max_features .7 | Four configs; bootstrap seed fixed; no OOB selection; terminal means limit extrapolation |
| XGBoost | Shallow boosted residual patterns; depth 1 or 2; rounds 50 or 150; eta .03, min_child_weight 20, lambda 10 | Four configs; squared error signed EPA, subsample/columns .8; rounds selected only by inner years, no outer early stopping; existing 3.2.0 research-only dependency |
| Ordered-game feasibility | Last four chronological games' rate/usage vs season summaries | One additional Ridge family, same alpha grid; retain all histories/missing indicators; investigate RNN only if this family adds >=5% inner skill in two origins and >=500 distinct training careers per role |
| RL feasibility | Need states, roster actions, transitions, rewards, horizon, constraints and action coverage | No simulator/reward invention. Audit trajectories/confounding; absent action/counterfactual data means unsupported |

Candidate fitting stops on invalid data/timing; failed numerical fits are logged,
with training-mean fallback and zero favorable credit. Warnings, every inner
trial, selection, fit seconds, training hashes, preprocessing statistics and
coefficients are saved. No outer-result-driven grid enlargement or ensemble.
Remaining models after diagnostic review may be rejected with evidence rather
than fitted for a leaderboard. No neural dependency is installed for feasibility.

## Evidence standards fixed before challenger errors

Primary endpoint per role: equal-transition EPA MSE aggregated over 2024–2025.
Report RMSE, MAE, signed bias, R², coverage and baseline squared-error skill.
Five percent lower MSE is a *predictive* minimum worth investigating; no economic
utility data calibrate it. Candidate/claim family includes all searched EPA
representations/algorithms across six roles. No formal significance claim from
this exploratory multiplicity. Save paired losses for every candidate vs direct
Ridge and vs audited Ridge to separate feature, tuning and algorithm effects.

Promising retrospective improvement requires >=5% primary skill, positive skill
in both years, a player-cluster conditional 95% skill interval above zero, and
no >10% harm in prespecified prior-N strata 0, 1–49, >=50 with >=30 test rows.
All other positive differences are inconclusive; negative/no support is retained.
These conservative screens are not multiplicity-adjusted confirmatory tests.
Independently confirmed improvement requires a separately frozen future test;
none is available here. Never automatically set forecast_validated=True.

Use 1000 paired whole-player bootstrap draws, fixed predictions and calendar
years: this estimates player sampling sensitivity conditional on observed years,
not new-season or retraining uncertainty. Shared team/game dependence remains;
also report whole-prior-team cluster resampling and per-season/leave-one-year
losses. Only two primary test years cannot estimate calendar-shock uncertainty
reliably. Neither folds nor repeated player-games are independent replications.
No pooled cross-position winner; equal rows within each role, no averaging R².

Report residuals by year, prior opportunity strata, zeros, negative outcomes,
extremes (training 95th absolute-EPA threshold), and rectangular training-range
flags. The latter is an incomplete joint-support diagnostic. Save residual
quantiles, squared-error concentration, fitted-value-bin bias/variance, and
learning curves with 50%/100% distinct training players and short/full windows.
Extreme-row omission is sensitivity only. Rate bands: coverage, width and
interval score IS=(u-l)+2/.2[(l-y)1{y<l}+(y-u)1{y>u}]. No CRPS or likelihood
is invented for point models. Probability models: Brier/log loss and fixed-bin
calibration. No conformal guarantees under unverified dependence.

## Economic decision scope

Let d_h=(1+r)^-(h-h0), D=sum_h d_h(Y_player,h-Y_replacement,h),
K_b=sum_h d_h(avoidable player cost_b - replacement cost_b + additional net
exit obligations_b), b in {cap,cash}. Aggregate each obligation once, not per
component. Guarantees already included in schedules are disclosures, not added
again. APY/term cannot reconstruct annual obligations, sunk costs or exit charges.

For fixed lambda, S_b=lambda D-K_b. Var(D) includes player/replacement and
cross-year/component covariance; Var(S|I)=lambda²Var(D|I)+Var(K|I)
-2lambda Cov(D,K|I). Known scenario costs are fixed; absent obligations are
unknown, not a probability distribution. Lambda/replacement feasibility are
scenario uncertainty unless independently supported probabilities exist.
For D>0, S>=0 iff lambda>=K/D; D<0 reverses the inequality; D=0 gives S=-K.
Restrict scenario lambda>=0, allowing negative incremental K. Prediction changes
imply delta S=lambda delta D at fixed K, not validation of economic surplus.
Inspect ordering sensitivity over forecast/comparator choices without manufacturing
actual dollar judgments. Acquisition cohorts are not certified feasible alternatives.

## Test access, confirmation and amendments

v2 and the original audit are already seen. Baseline and challenger stages write
separate access logs and protocol hashes. A baseline checkpoint is saved and
reported before any challenger outer errors are inspected. Any later substantive
change states reason and already accessed outcomes, retaining the initial freeze.
The planned next prospective freeze is June 1, 2027, before its season, with
contemporaneous inputs and a predeclared eligible spine; it has **not occurred**.
A forecast made now cannot be relabeled June 1, 2026. No scheduled task is created.

Primary methodological references consulted for this protocol:
[Cawley and Talbot](https://jmlr.org/papers/v11/cawley10a.html),
[R² edge cases](https://scikit-learn.org/1.8/modules/generated/sklearn.metrics.r2_score.html),
[Ridge](https://scikit-learn.org/1.8/modules/generated/sklearn.linear_model.Ridge.html),
[Elastic Net](https://scikit-learn.org/1.8/modules/generated/sklearn.linear_model.ElasticNet.html),
[spline basis](https://scikit-learn.org/1.8/modules/generated/sklearn.preprocessing.SplineTransformer.html),
[proper scoring rules](https://sites.stat.washington.edu/people/raftery/Research/PDF/Gneiting2007jasa.pdf),
[XGBoost](https://arxiv.org/abs/1603.02754), and
[off-policy evaluation](https://jmlr.org/beta/papers/v21/19-827.html).
The project-specific equations above follow from the stated assumptions.

### Amendment 1 — user-requested longer history, before any v3 fits

The user requested earlier data during implementation. Audit public 2016–2020
PBP, player statistics, PFR snaps, season rosters and schedules in a separate
`earlier-inputs` archive with URLs/fetch timestamps/hashes. Preserve the original
2021–2025 experiment. If coverage/identity/EPA reconciliation pass, add a separate
2016–2025 contribution experiment, with 2017 first training transition, 2018
warm-up, and 2019–2025 primary tests. Use the identical candidate budgets and
nested procedure; actual inner windows grow chronologically. No economic or
price population is silently extended. The scope adds production history, not
unsupported old contract snapshots or historical cap assumptions.

Report 2019–2020 and 2021–2025 eras separately (16- versus 17-game schedules),
and 2024–2025 common outcomes with short/full training histories. Pandemic and
rule/context changes are possible transport failures, not automatically corrected
by more rows. Acceptance requires positive season skill in at least 75% of
extended primary years (at least 6 of 7), positive aggregate skill in each era,
plus the original >=5%, conditional interval and prior-N subgroup guards. The
original two-year rule is unchanged. These extra tests are historical, not
untouched. Frozen protocol copies and separate manifests identify the populations.
No additional data source or larger model search is justified by favorable scores.

### Amendment 2 — all-position label reconciliation, October 3, 2026

After inspecting the first baseline runs (before challenger outcomes), an audit
found that the existing skill-family game table can omit future offensive snaps
and EPA when a prior skill player is subsequently labeled DB/LB/DL. Future
eligibility must not depend on that label. v3 now constructs outcomes directly
from all-position raw PFR snaps, weekly statistics and exact PBP filters, with
per-player/game EPA reconciliation. Prior position still defines the prediction
role; identity and complete-game coverage still gate known zeros. This corrects
labels without selecting a new feature or grid. Original v2 predictions are
reproduced unchanged for traceability; affected first v3 baseline runs are retained
under `superseded/` and are not the final checkpoint. Rerun both baseline datasets.

### Amendment 3 — complete efficiency-family ablation, October 3, 2026

Review after baseline access found that the first `ridge_no_efficiency` feature
list still contained second-season EPA, recency EPA and EPA trends. Those can
carry or even reconstruct the excluded information. The corrected ablation
retains volume/career variables and lagged opportunities/games only. This is an
implementation correction to the prespecified family question, not an expanded
search. Preserve the previous runs under `superseded/*-incomplete-ablation` and
rerun the same finite baseline grids; the main full-Ridge and direct-Ridge
specifications do not change. The next session must not use superseded ablation
scores to conclude efficiency has no incremental information.

### Amendment 4 — challenger execution audit, October 3, 2026

Written after final baseline/residual access and before any real-data challenger
fit. The finite grids, candidate order, frozen data/folds, losses and evidence
thresholds are unchanged. Complete the previously specified numerical-failure
behavior: validate data/timing outside exception handling; log estimator errors
and nonfinite predictions; retain all rows with a training-target-mean fallback
(and training positive-opportunity frequency for the hurdle probability). A
configuration with any failed inner fit cannot win selection; if every choice
fails, use the fallback without an outer estimator fit. An outer failure cannot
earn a promising conclusion even if its fallback scores well. Unexpected coding,
missing-dependency and data/timing errors still stop execution. Record warnings
without treating convergence warnings as independent evidence of convergence.

Save fitted imputation, scaling, target scaling, spline knots and hurdle classifier
state, every trial, and per-outer-fit predictions immediately. Preserve partial
runs and refuse silent restart/overwrite. Baseline code/protocol/manifests remain
intact; challenger code and this amended protocol get separate frozen copies.

For the sequence information gate, retain every fixed-grid inner prediction.
At each unique inner validation year choose alpha using strictly earlier inner
years, defaulting to alpha 10 when none exist. Compare sequence Ridge with the
existing baseline Ridge prediction for that same origin, whose alpha was also
chosen from strictly earlier years. Deduplicate repeated inner origins across
outer windows; verify identical predictions. Thus neither the current inner-year
outcome nor any later outer outcome selects its alpha. A role passes the signal
gate only with >=5% skill at two distinct inner origins, and requires >=500
distinct training players at the intended neural forecast origin. Save origin
membership, alpha, training-career counts and skills. This is a necessary
exploratory screen, not sufficient evidence for a neural architecture or calibrated
state-space distribution; any such experiment requires a separately justified,
pre-outcome specification. Do not count repeated appearances of an origin twice.

### Amendment 5 — exact CSV round trips, October 3, 2026

All 72 pinned challenger outer fits and 414 inner trials completed before report
assembly stopped: the default CSV parser perturbed 153 baseline EPA targets by
at most 1.4211e-14 relative to the frozen Parquet values. Challenger scores had
not been inspected. Reading with `float_precision='round_trip'` restores exact
equality for every row; saved baseline and challenger CSV targets also agree.
Use round-trip parsing rather than relaxing paired-target equality. Preserve the
partial execution, log and diagnosis; finalize its saved predictions without
refitting. The fit code, grids, memberships and thresholds do not change. Keep
the original challenger protocol/code freeze and a separate reporting-repair
snapshot. The extended run includes this correction before fitting.
