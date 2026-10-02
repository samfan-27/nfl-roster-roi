"""Contract definitions and conservative reconciliation of OTC source records.

All monetary fields use millions of dollars. Nested histories are player-wide
and repeated on multiple deals; neither current status nor term arithmetic
establishes a historical effective date. No network or persistence belongs here.
"""
from hashlib import sha256
import json

import numpy as np
import pandas as pd

from src.domain import SALARY_CAP_MILLIONS, POSITION_FAMILIES

APY_CONVENTION = "OTC stated contract APY; not realized effective APY"
FINANCIAL_FIELDS = ['contract_apy_m','contract_year_signed','contract_years','contract_type',
                    'contract_selection_status','season_cap_charge_m','season_cash_m','season_financial_team',
                    'annual_financial_status','annual_entry_count','contract_signed_date','contract_effective_date',
                    'contract_transaction_date','contract_snapshot_at','contract_source_url','contract_apy_convention']



def nested_records(value):
    if isinstance(value, str):
        value = json.loads(value)
    return list(value) if isinstance(value, (list, tuple, np.ndarray)) else []


def unique_records(records):
    """Remove exact duplicates, retaining competing records for an audit."""
    return list({json.dumps(r, sort_keys=True, default=str): r for r in records}.values())


def _number(value):
    return pd.to_numeric(value, errors="coerce")


def reconcile_contract_identities(contracts, players):
    """Resolve OTC identity first; audit reused GSIS IDs on homonymous players.

    Several contract GSIS mappings are reused on differently positioned names
    when the master OTC ID is missing. A conflicting position family cannot
    establish that identity. Never choose the first, cheapest, or latest homonym.
    """
    master = players.dropna(subset=['gsis_id']).drop_duplicates('gsis_id').copy()
    master['otc_id'] = pd.to_numeric(master.otc_id, errors='coerce')
    known = master.dropna(subset=['otc_id'])
    if known.groupby('otc_id').gsis_id.nunique().gt(1).any():
        raise ValueError('Conflicting OTC/GSIS player identities')
    otc_to_gsis = known.drop_duplicates('otc_id').set_index('otc_id').gsis_id
    families = master.set_index('gsis_id').position.map(POSITION_FAMILIES)
    out = contracts.copy()
    out['otc_id'] = pd.to_numeric(out.otc_id, errors='coerce').astype('Int64')
    mapped = out.otc_id.map(otc_to_gsis)
    conflict = mapped.notna() & out.gsis_id.notna() & mapped.ne(out.gsis_id)
    if conflict.any():
        raise ValueError('Contract/player GSIS identity conflict')
    source_family = out.position.map(POSITION_FAMILIES)
    master_family = out.gsis_id.map(families)
    incompatible = mapped.isna() & source_family.notna() & master_family.notna() & source_family.ne(master_family)
    out['contract_identity_status'] = np.where(incompatible, 'source_gsis_position_conflict',
                                              np.where(mapped.notna(), 'master_otc_identity', 'source_gsis_identity'))
    out['gsis_id'] = mapped.fillna(out.gsis_id).mask(incompatible)
    return out


def select_contracts(contracts, season, snapshot_at=None):
    """Latest signed-year association, explicitly approximate, never expiration."""
    data = contracts.copy()
    data["otc_id"] = pd.to_numeric(data.otc_id, errors="coerce").astype("Int64")
    data["year_signed"] = pd.to_numeric(data.year_signed, errors="coerce")
    data = data.loc[data.year_signed.between(1, season)].dropna(subset=["otc_id"])
    data["apy"] = pd.to_numeric(data.apy, errors="coerce")
    data = data.sort_values(["year_signed", "is_active", "apy"], kind="stable")
    selected = data.drop_duplicates("otc_id", keep="last").copy()
    copies_by_player = {pid:g for pid,g in data.groupby('otc_id', sort=False)}
    details = []
    for _, row in selected.iterrows():
        # Read all player histories, not just the selected deal's copy.
        copies = copies_by_player[row.otc_id]
        annual = unique_records([r for v in copies.get("season_history", [])
                                 for r in nested_records(v) if _number(r.get("year")) == season])
        history = unique_records([r for v in copies.get("contract_history", [])
                                  for r in nested_records(v)])
        matching = [r for r in history if _number(r.get("year_signed")) == row.year_signed
                    and np.isclose(_number(r.get("apy")), row.apy, equal_nan=False)]
        kinds = {r.get("contract_type") for r in matching if r.get("contract_type")}
        # Different team records may represent a trade or dead money; no sums.
        entry = annual[0] if len(annual) == 1 else {}
        latest = copies.loc[copies.year_signed.eq(row.year_signed)]
        terms = latest[["apy", "team"]].drop_duplicates()
        competing_prices = latest.apy.dropna().nunique() > 1
        snapshot_year = pd.Timestamp(snapshot_at).year if snapshot_at is not None else None
        current_active_price = (snapshot_year == season and bool(row.get('is_active', False))
                                and latest.loc[latest.is_active.eq(True)].apy.dropna().nunique() == 1)
        associated_apy = row.apy if not competing_prices or current_active_price else np.nan
        details.append(dict(
            contract_apy_m=associated_apy, contract_year_signed=row.year_signed,
            contract_years=row.get("years"), contract_type=next(iter(kinds)) if len(kinds) == 1 else "Unknown",
            contract_selection_status="year_only_approximation" if len(terms) == 1 else "ambiguous_same_year",
            season_cap_charge_m=_number(entry.get("cap_number")),
            season_cash_m=_number(entry.get("cash_paid")), season_financial_team=entry.get("team"),
            annual_financial_status="resolved" if len(annual) == 1 else ("ambiguous" if annual else "missing"),
            annual_entry_count=len(annual), contract_signed_date=row.get("signed_date"),
            contract_effective_date=row.get("effective_date"), contract_transaction_date=row.get("transaction_date"),
            contract_snapshot_at=snapshot_at, contract_source_url=row.get("player_page"),
            contract_apy_convention=APY_CONVENTION,
        ))
    for col in FINANCIAL_FIELDS:
        selected[col] = [r[col] for r in details]
    return selected


def contract_events(contracts, players, snapshot_at=None):
    """One event per distinguishable set of negotiated terms, plus exclusions.

    Same terms on traded-team copies collapse to one event. Multiple distinct
    deals in the same player/year cannot be ordered from year-only metadata.
    Mutable realized earnings and current status never define the event ID.
    """
    contracts = reconcile_contract_identities(contracts, players)
    identities = contracts.dropna(subset=['otc_id','gsis_id']).drop_duplicates('otc_id').set_index('otc_id')
    master_positions = players.drop_duplicates('gsis_id').set_index('gsis_id').position
    events = {}
    for _, row in contracts.iterrows():
        for record in nested_records(row.get("contract_history")):
            terms = dict(otc_id=_number(row.otc_id), year_signed=_number(record.get("year_signed")),
                         contract_type=record.get("contract_type", "Unknown"),
                         contract_apy_m=_number(record.get("apy")), contract_years=_number(record.get("yrs")),
                         contract_total_m=_number(record.get("total")), guarantees_m=_number(record.get("guarantees")))
            key = json.dumps(terms, sort_keys=True)
            if key not in events:
                events[key] = dict(**terms, event_id=sha256(key.encode()).hexdigest()[:24],
                                   teams=set(), player_name=row.player, contract_source_url=row.get("player_page"),
                                   contract_snapshot_at=snapshot_at, signed_date=None, effective_date=None,
                                   transaction_date=None, apy_convention=APY_CONVENTION)
            events[key]["teams"].add(str(record.get("team")))
    out = pd.DataFrame(events.values())
    if out.empty:
        return out
    out["teams"] = out.teams.map(lambda x: "|".join(sorted(x)))
    out["gsis_id"] = out.otc_id.map(identities.gsis_id)
    out['role_position'] = out.gsis_id.map(master_positions)
    out["position"] = out.role_position.map(POSITION_FAMILIES).fillna(out.role_position)
    out["signing_cap_m"] = out.year_signed.map(SALARY_CAP_MILLIONS)
    out["signing_cap_share_pct"] = 100 * out.contract_apy_m / out.signing_cap_m
    out["timing_precision"] = "year_only"
    out["label_provenance"] = "retrospective_source_history"
    ambiguous = out.groupby(["otc_id", "year_signed"]).event_id.transform("size").gt(1)
    out["event_status"] = np.select(
        [out.gsis_id.isna(), ambiguous, out.contract_apy_m.le(0) | out.contract_apy_m.isna(), out.signing_cap_m.isna()],
        ["missing_identity", "ambiguous_player_year", "invalid_price", "unconfigured_signing_cap"], default="usable_year_only")
    return out.sort_values(["year_signed", "event_id"]).reset_index(drop=True)


def financial_coverage(frame):
    """Coverage denominators include unknown contracts and nonpositive EPA."""
    return pd.DataFrame([dict(
        season=year, position=pos, rows=len(g),
        apy_known=int(g.contract_apy_m.notna().sum()),
        annual_cap_known=int(g.season_cap_charge_m.notna().sum()),
        annual_cash_known=int(g.season_cash_m.notna().sum()),
        annual_ambiguous=int(g.annual_financial_status.eq("ambiguous").sum()),
        type_unknown=int(g.contract_type.eq("Unknown").sum()),
    ) for (year, pos), g in frame.groupby(["season", "position"])])
