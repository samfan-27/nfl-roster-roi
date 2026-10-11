# Identity and sample impact — October 5, 2026

The remaining gaps have now been linked to saved research members. No identity
was newly resolved; no sample, feature, label, fitted value, score, cost or
research rule was changed. Strict preparation remains unavailable. Three archived
`observed` labels have unresolved provider conflicts under the already-established
strict standard; their values are preserved and explicitly flagged below.

The audit began October 4 at 23:18:06 UTC / 19:18:06 EDT. After the usage-limit
interruption, work resumed October 6 at 01:25:53 UTC / October 5 21:25:53 EDT.
Python 3.12.4 and the existing fixed research dependencies were reused. No applicable
AGENTS.md was found. Existing Git changes, raw evidence and failed runs were preserved.

## Traceable deliverables

- [Identity/sample impact table](../artifacts/model-research-sample-impact-v1/audit-02/identity_sample_impact.csv): one row per provider identity key and season; includes teams, positions, participation, coverage, prior-feature/label/training links and provisional 2027 spine membership.
- [Deduplicated identity inventory](../artifacts/model-research-sample-impact-v1/audit-02/identity_inventory.csv): all seasons and case IDs for each key. GSIS and unlinked PFR namespaces remain separate; they cannot be added as proven distinct people.
- [Original row links](../artifacts/model-research-sample-impact-v1/audit-02/source_row_links.csv): path, SHA256, zero-based source row and game key. [Saved member links](../artifacts/model-research-sample-impact-v1/audit-02/saved_membership_links.csv) retain exact transition IDs, dataset, feature/label relation, original values and fold uses.
- [Blocker/evidence matrix](../artifacts/model-research-sample-impact-v1/final-01/blocker_evidence_matrix.csv) and [independent linkage verification](../artifacts/model-research-sample-impact-v1/final-01/verification.json).

The table contains 3,622 identity-season rows, including 2,884 resolved-absence
controls and four already-adjudicated controls. Controls are not new exceptions.
Names are display metadata only. Provider assertion edges are retained separately;
conflicting identities are never unioned or used to transfer measurements.

## Measured impact

Counts below are archived identity keys, seasonal cases and player-game rows,
not necessarily independently verified people. The core-gap row overlaps the
missing-PFR and conflict rows. Game counts refer to the common covered games;
PFR-only rows have no reliable GSIS sample attribution. Recorded snaps are observed
source subtotals, not estimates of unknown total participation.

| Group | Identity keys | Season rows | Snap game rows | Stats game rows | Distinct player-game rows | Recorded offensive snaps |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| All core identity gaps | 615 GSIS | 688 | 1 | 47 | 47 | 5, disputed attribution |
| GSIS without resolved PFR | 613 GSIS | 684 | 0 | 46 | 46 | 0 linked; total unknown |
| Provider conflicts | 4 GSIS | 8 | 30 | 5 | 33 | 340, disputed attribution |
| PFR participants without GSIS | 24 PFR | 40 | 212 | Not linkable | 212 | 1,441 |
| Participants absent from annual roster | 2 provider GSIS-column keys | 2 | 8 | 0 | 8 | 0 recorded |
| Resolved covered-absence controls | 1,703 GSIS | 2,884 | 0 | 0 | 0 | 0 recorded |

Of the 688 core cases, 601 have no participation record in complete game sets,
72 have no record in incomplete 2026 sources, and 15 have recorded participation
rows. The 684 missing-PFR cases include nine GSIS keys over 14 seasons with 46
weekly statistical rows. Those facts refute an automatic zero classification.
The other recorded core case is the disputed 2023 Kwamie Lassiter pair (five
snaps). Complete game sets alone do not certify the unresolved cases' absences.

| Relation of the 688 cases to saved transitions | Extended rows / GSIS keys | Pinned rows / GSIS keys |
| --- | ---: | ---: |
| Prior-year feature membership | 789 / 553 | 359 / 262 |
| Same-component second calendar history | 65 / 41 | 21 / 14 |
| Target-season label membership | 80 / 51 | 36 / 24 |

Both v3 and information-stage saved membership files give these counts; they are
not additive across stages or feature/label relations. Relation-specific identity,
season, game and recorded-snap counts are also saved in
[impact relation counts](../artifacts/model-research-sample-impact-v1/final-01/impact_relation_counts.csv). In the extended sample,
the 789 prior-feature links have 707 unresolved future identities, 78 uncovered
future outcomes and four observed outcomes. The 80 label links have 63 unresolved
identities, 14 uncovered outcomes and three observed outcomes. Missing outcomes
remain in the saved ledgers; they are not equivalent to fitted/scored members.

The core cases connect to four distinct saved training transition IDs, all via
the two unresolved core conflict identities. The 684 missing-PFR cases connect
to **zero** saved training members. They still affect historical feature/label
availability and sample coverage. All 73 core gaps in the archived 2026 roster
are provisional 2027 eligibility-spine cases (39 WR, 17 TE, 16 RB, one QB).
None of the 688 is only unrelated raw roster inventory under these broad saved
membership/eligibility definitions. One other conflict case, 2018 T.J. Johnson
(OL), has no recorded participation or direct saved contribution membership and
is inventory-only within this contribution-membership audit. This flag is not a
claim about every price-event or salary-association inventory. His 2016–2017 disputed selected pair has 335 recorded offensive
snaps but no direct core transition membership.

The two missing-roster keys are `00-0020895` (2024, provider display Chris Smith,
DT, five rows) and `WOO547869` (2026, provider display Peter Woods, DL, three rows).
Neither has direct saved core contribution membership. The latter is literally
a provider GSIS-column value, not a newly verified canonical GSIS identity; do not
merge it with the conflicting `00-0041545` by name. Noncore participants do not
alone establish missing core EPA or authorize inventing a prior core role.

No reliable direct sample linkage is possible for the 24 unmatched PFR keys.
Their 1,441 snaps include linemen and skill positions; zero offensive snaps in
some rows does not resolve the person. No 2026 source-only snap/stat rows match
the enumerated cases under the available key mappings. That limited negative
result does not repair the nine missing-game sources or establish exhaustive
player-level coverage.

## Archived labels that cannot be newly certified

| Transition ID | Preserved EPA / opportunities / offensive games | Saved use |
| --- | --- | --- |
| `2017\|00-0029435\|receive` | 0 / 0 / 0 | Extended training |
| `2023\|00-0037420\|receive` | 0.22231479996116832 / 1 / 1 | Training and test |
| `2024\|00-0037420\|receive` | 0 / 0 / 0 | Training and test |

Exact floating-point values and dataset-specific memberships remain in the
[annotation table](../artifacts/model-research-sample-impact-v1/audit-02/archived_observed_labels_with_unresolved_conflicts.csv).
This is an audit of saved labels, not replacement labels. Damaris Johnson's
2016–2017 and Kwamie Lassiter's 2023–2024 feature seasons also propagate through
prior/second-history features; across feature and label links there are five
unique affected transition IDs, four used in training. A training discrepancy
can affect other predictions through shared fitted parameters; this audit does
not bound that effect or claim the few rows are numerically negligible. Rate
prior effects are likewise not quantified without resolved measurements.

The previous source-resolution stage already made these conflicts block strict
preparation and remain unknown in strict label verification. No new implementation
repair is warranted merely because older historical artifacts used master precedence.
Their original metrics remain conditional on their archived construction and are
not newly certified as a strict future-ready study.

## Scientific gate assessment and required evidence

| State | What is established / still missing | Required action under the existing protocol |
| --- | --- | --- |
| Known GSIS, unresolved PFR | Roster key/role can be known; recorded stats may exist; full snap identity or verified absence is not established | Block strict core prior preparation. Obtain a season-correct explicit provider crosswalk or authoritative correction. GSIS presence alone cannot justify zeros. An alternative absence-certification rule would require a dated scientific amendment; none is adopted. |
| PFR participant not linked to GSIS | Player-game row and recorded snaps exist; GSIS attribution and sample relationship are unknown | Block complete strict preparation under the fixed unresolved-snap gate. Require direct cross-provider identity evidence, including collision checks. Do not discard noncore-looking or zero-offense rows to pass. |
| Provider conflict | Incompatible assertions are explicit; selected master measurements are not an adjudication | Block the current strict preparation recipe; corresponding strict labels stay unknown. Require provider confirmation/retraction resolving both conflicting pairs and season scope. |
| Participant missing annual roster | A participating provider key lacks an annual-spine member; the two actual cases are noncore | Preserve the gap and seek corrected roster/role evidence. A core participant must block the explicit preparation membership guard. These two cases alone are not evidence of a core estimand omission; all-source completeness remains unproven. |
| Resolved identity, no recorded participation, complete game sets | Unique archived pair and coverage permit the protocol's covered-absence convention; provider exhaustiveness is not independently proved | No identity blocker for that row; component totals may be zero under existing rules, while rates at N=0 remain undefined. This cannot certify the entire dataset while other gates fail. |
| Incomplete sources or season | Nine completed games lacked snaps in the archive and 214 were not completed | Stop annual preparation. Require actual completed season and all required reconciled sources; never turn unobserved games into zero. |

The strict all-position unresolved-snap/conflict conditions are conservative dataset
certification rules, stronger than proving every exception changes a particular
core component. They are explicit in the October 4 protocol, not an accidental
requirement that can be removed as a coding fix. Core no-PFR cases and noncore
inventory cases therefore have different scientific impacts even where both
prevent the frozen full-source recipe from passing. No eligibility, history,
coverage or zero/unknown rule was relaxed, and no method amendment was made.

## Verification, preparation and stopping point

The source-resolution manifest verifies **197 stage files relative to its stage
root and 13 outputs relative to final-01**. All 4,734 original row links were
checked against source identity/game keys. Independent merges reproduce the
core feature/label memberships; lag support and saved train/test flags agree.
All audit inputs and previously frozen bytes remained unchanged. This is finite
linkage verification, not a new forecast-preparation or model-numerical validation.
No pytest suite was rerun because no implementation changed.

The first local table (`audit-01`) is retained. Table QA found that a calendar-only
lag link could reference a component absent from that dataset's preceding year.
`audit-02` requires the actual prior component member and also records source-only
rows. This removes five distinct control lag links and two control training links;
all exception/core counts are unchanged. This is a correction to this session's
reporting table, not to model code, scientific eligibility or historical values.
The scripts and both logs are preserved; neither invocation runs a season audit,
preparation recipe, rate fit or prediction fit.

No new direct identity evidence, source revision, or preparation implementation
defect was established. The retained full-2016–2026
[preparation rejection](../artifacts/model-research-source-resolution-v1/preparation-rejected-01/failure.json)
remains `2016: unresolved identities/roles require adjudication; no fabricated zeros`.
It was not repeated, and no new successful preparation/independent numerical
verification is claimed. The [2026 missing-game list](../artifacts/model-research-sample-impact-v1/audit-02/coverage_2026_missing_games.csv)
retains **58 completed / 49 shared, nine source gaps, 214 not completed** as of
October 4 22:56:10 UTC receipts. This is archived coverage, not live coverage on
October 5. The interruption's elapsed time alone does not constitute evidence of
a corrected provider file. No retrieval or polling was performed.

The original scientific conclusion is unchanged: completed model and information
experiments did not establish a qualifying long-history gain over audited Ridge;
this is not a prediction ceiling. This audit adds concrete limitations on the
certification of existing samples, not evidence of predictive improvement.
Negotiated APY remains distinct from an executable offer or remaining cash debt.
The 32-team overview and single PIT case retain their original scope and decision
horizon. PIT's 336 ledger rows, 270 unknown amounts, incomplete K, deployment D,
lambda and surplus are unchanged; no private materials were requested again.

Remaining identity/roster gaps require external authoritative evidence; 2026
requires genuinely complete sources. No current preparation coding defect was
identified to repair. `forecast_validated=False`. A real freeze remains permitted
only June 1, 2027, completed by 23:59:59 New York with every gate passing. A missed
deadline requires a genuinely future amendment. Earliest label maturity remains
March 1, 2028 UTC plus actual completeness. No freeze, automation, deployment,
external contact, payment, commit or push occurred.
