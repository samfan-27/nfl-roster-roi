"""Presentation controls for explicit financial definitions and cohorts."""
import pandas as pd
import streamlit as st
from src.reporting import top_value_players

FINANCES = {'Contract APY': 'contract_apy_m', 'Season cap charge': 'season_cap_charge_m', 'Season cash': 'season_cash_m'}


def prepare_finances(frame):
    out = frame.copy()
    if 'contract_apy_m' not in out:
        out['contract_apy_m'] = out.yearly_cap_hit
    else:
        out['contract_apy_m'] = out.contract_apy_m.fillna(out.yearly_cap_hit)
    for field in ['season_cap_charge_m', 'season_cash_m']:
        if field not in out:
            out[field] = float('nan')
    if 'contract_type' not in out:
        out['contract_type'] = 'Unknown'
    return out


def cohort_filter(frame, key):
    cohort = st.radio('Contract cohort', ['All', 'Rookie (estimated)', 'Non-rookie (estimated)'], horizontal=True, key=key+'_cohort')
    if cohort != 'All':
        frame = frame.loc[frame.is_rookie_deal.eq(cohort=='Rookie (estimated)')]
    kinds = sorted(frame.contract_type.dropna().unique())
    mechanism = st.selectbox('Contract mechanism', ['All'] + kinds, key=key+'_mechanism')
    return frame.loc[frame.contract_type.eq(mechanism)] if mechanism != 'All' else frame


def efficiency_table(frame, financial_label, limit=10):
    field = FINANCES[financial_label]
    known = int(frame[field].notna().sum())
    st.caption(f'{financial_label} coverage: {known}/{len(frame)} eligible players. Unknown costs remain unranked.')
    title = 'APY' if field == 'contract_apy_m' else ('annual-cap' if field == 'season_cap_charge_m' else 'annual-cash')
    st.subheader(f'Positive-EPA {title} efficiency')
    st.caption('Compare within position and contract mechanism. Attributed EPA overlaps between passers and receivers. These ratios describe observed production and costs.')
    table = top_value_players(frame, min_snaps=0, limit=limit, financial_field=field)
    if table.empty:
        st.info('No positive-EPA players with known positive costs meet these filters.')
    else:
        st.dataframe(table, hide_index=True, width='stretch')
