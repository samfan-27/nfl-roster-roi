# NFL roster ROI analysis: 2026, regular season through week 3

Generated at 2026-09-30T02:09:25.528567+00:00.

Regular season through week 3; 48 shared games; 0 games excluded for source mismatch. APY in millions; latest contract signed by season year; historical approximation. Raw EPA; normalized cost uses position shrinkage; rookie cohort is a heuristic.

## Interpretation

- Production is season-to-date, not a completed season or forecast.
- Annual APY divided by partial-season EPA rises mechanically when fewer games are recorded. Compare within the same season and position.
- All financial values are millions of dollars unless explicitly labeled as dollars. APY is not the season's actual cap charge.
- Team and position totals sum player-attributed EPA; passing and receiving EPA overlap and must not be interpreted as net offensive team EPA.
- Contract selection is a historical approximation using signing year. Same-year transactions and extension effective dates are not fully resolved.
- Rookie labels are estimates, not CBA legal classifications.

## Current position totals

```text
position  players  snaps  player_attributed_epa  annual_apy_m  epa_per_snap
      QB       51   6229                259.562     1,053.746         0.042
      RB      111   6653               -193.294       447.502        -0.029
      TE      116   8824                168.232       502.605         0.019
      WR      174  14663                468.081     1,285.962         0.032
```

## Matched coverage: 2026 versus 2025, through week 3

```text
position  players_current  snaps_current  player_attributed_epa_current  annual_apy_m_current  epa_per_snap_current  players_previous  snaps_previous  player_attributed_epa_previous  annual_apy_m_previous  epa_per_snap_previous  epa_per_snap_change
      QB               51           6229                        259.562             1,053.746                 0.042                55            6212                         171.631              1,080.791                  0.028                0.014
      RB              111           6653                       -193.294               447.502                -0.029               108            6625                        -199.068                353.038                 -0.030                0.001
      TE              116           8824                        168.232               502.605                 0.019               105            8321                         139.185                425.784                  0.017                0.002
      WR              174          14663                        468.081             1,285.962                 0.032               175           15969                         463.773              1,078.383                  0.029                0.003
```

## Positive-EPA value candidates: minimum 100 snaps

These are descriptive rankings, not definitive contract valuations.

```text
      player_name team position  is_rookie_deal  snaps  total_epa  yearly_cap_hit  cost_per_epa_dollars
Parker Washington  JAX       WR            True    133     13.038           1.008            77,320.254
    Devaughn Vele   NO       WR            True    212     10.153           1.031           101,558.263
        Drew Lock  SEA       QB           False    115     24.268           2.500           103,017.078
       Josh Downs  IND       WR            True    150     13.089           1.380           105,443.265
       Tre Tucker   LV       WR            True    125      9.702           1.326           136,675.382
    Ladd McConkey  LAC       WR            True    119     16.919           2.499           147,693.228
 Konata Mumpfield   LA       WR            True    122      6.132           1.077           175,661.361
Harold Fannin Jr.  CLE       TE            True    152      9.180           1.686           183,637.213
    Kalif Raymond  CHI       WR           False    145     18.667           3.500           187,493.124
       Geno Smith  NYJ       QB           False    207     16.593           3.300           198,875.505
```

## Completed-season APY research model

The incomplete season is excluded from both model training and annual-volume scoring. Validation uses nested cross-validation grouped by player. These metrics are out-of-fold; historical contract approximation still limits interpretation.

```text
position  veteran_rows  veteran_players  best_alpha  grouped_oof_mae_m  grouped_oof_r2
      QB           185               73      10.000             11.376           0.344
      RB           268              124      17.783              2.189           0.397
      WR           485              203       3.162              4.664           0.393
      TE           294              121      17.783              2.336           0.484
```

## Artifacts

- `roster_roi_scored.csv`: completed-season expected APY and surplus estimates.
- `model_diagnostics.csv`: nested grouped validation.
- `matched_week_comparison.csv`: prior/current production at matched week coverage.
