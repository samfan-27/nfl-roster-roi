import math
import pandas as pd
from etl.utils import safe_numeric

def compute_core_metrics(df: pd.DataFrame) -> pd.DataFrame:
    """Given df with numeric columns for passing_epa/rushing_epa/receiving_epa/snaps/yearly_cap_hit,
    compute total_epa, epa_per_snap, cost_per_epa (only for total_epa > 0), and normalized cost per 100 snaps.
    """
    out = df.copy()
    out['passing_epa'] = safe_numeric(out.get('passing_epa', 0.0))
    out['rushing_epa'] = safe_numeric(out.get('rushing_epa', 0.0))
    out['receiving_epa'] = safe_numeric(out.get('receiving_epa', 0.0))
    out['total_epa'] = out['passing_epa'] + out['rushing_epa'] + out['receiving_epa']

    out['snaps'] = pd.to_numeric(out.get('snaps', 0), errors='coerce').fillna(0).astype(int)

    # EPA per snap (0 when snaps==0)
    out['epa_per_snap'] = out.apply(lambda r: (r['total_epa'] / r['snaps']) if r['snaps'] > 0 else 0.0, axis=1)

    # yearly_cap_hit numeric
    out['yearly_cap_hit'] = safe_numeric(out.get('yearly_cap_hit', 0.0))

    def cost_per_epa(row):
        te = row['total_epa']
        if te is None or math.isclose(te, 0.0) or te <= 0:
            return None
        return row['yearly_cap_hit'] / te

    out['cost_per_epa'] = out.apply(cost_per_epa, axis=1)

    def cost_per_100(row):
        te = row['total_epa']
        snaps = row['snaps']
        if snaps <= 0 or te is None:
            return None
        te_per_100 = te * (100.0 / snaps)
        if te_per_100 <= 0:
            return None
        return row['yearly_cap_hit'] / te_per_100

    out['cost_per_epa_per_100_snaps'] = out.apply(cost_per_100, axis=1)

    return out

def shrink_total_epa(df: pd.DataFrame, tau: float = 200.0) -> pd.DataFrame:
    """
    Empirical-Bayes-like shrinkage of EPA per snap toward position mean.
    tau: prior strength — essentially 'dummy snaps' at the positional average.
    We shrink the rate, then re-multiply by actual snaps for volume.
    """
    out = df.copy()
    
    # positional EPA/snap averages
    pos_totals = out.groupby('position')[['total_epa', 'snaps']].sum()
    pos_rates = (pos_totals['total_epa'] / pos_totals['snaps'].replace(0, 1)).to_dict()
    
    global_total_epa = out['total_epa'].sum()
    global_snaps = out['snaps'].sum()
    global_rate = global_total_epa / global_snaps if global_snaps > 0 else 0.0

    def shrink_row(r):
        pos = r.get('position')
        prior_rate = pos_rates.get(pos, global_rate)
        
        actual_epa = r.get('total_epa', 0.0)
        n = max(float(r.get('snaps', 0.0)), 0.0)
        
        denom = n + tau
        if denom <= 0:
            return actual_epa, 0.0
            
        # Shrunk Rate = (Actual EPA + (Prior Rate * Tau)) / (Actual Snaps + Tau)
        shrunk_rate = (actual_epa + prior_rate * tau) / denom
        
        # Shrunk Total = Shrunk Rate * Actual Snaps
        shrunk_total = shrunk_rate * n
        
        return shrunk_total, shrunk_rate

    # Apply the shrinkage
    shrunk_results = out.apply(shrink_row, axis=1, result_type='expand')
    out['total_epa_shrunk'] = shrunk_results[0]
    out['epa_per_snap_shrunk'] = shrunk_results[1]

    def cost_per_epa_shrunk(r):
        te = r['total_epa_shrunk']
        if te is None or math.isclose(te, 0.0) or te <= 0:
            return None
        return r['yearly_cap_hit'] / te

    out['cost_per_epa_shrunk'] = out.apply(cost_per_epa_shrunk, axis=1)

    def cost_per_100_shrunk(r):
        te = r['total_epa_shrunk']
        snaps = r['snaps']
        if snaps <= 0 or te is None:
            return None
        te_per_100 = te * (100.0 / snaps)
        if te_per_100 <= 0:
            return None
        return r['yearly_cap_hit'] / te_per_100

    out['cost_per_epa_per_100_snaps_shrunk'] = out.apply(cost_per_100_shrunk, axis=1)
    
    return out
