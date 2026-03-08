import os
import pandas as pd
from supabase import create_client
import streamlit as st

@st.cache_resource
def _get_client():
    url = st.secrets.get('SUPABASE_URL', os.getenv('SUPABASE_URL'))
    key = st.secrets.get('SUPABASE_ANON_KEY', os.getenv('SUPABASE_ANON_KEY'))
    if not url or not key:
        raise RuntimeError('Supabase URL or anon key not found in secrets/environment')
    return create_client(url, key)

@st.cache_data(ttl=300)
def load_offense_roster(season: int):
    """
    Returns a DataFrame filtered to QB, RB, WR, TE for the given season.
    """
    sup = _get_client()
    res = sup.table('roster_roi').select('*').eq('season', season).in_('position', ['QB', 'RB', 'WR', 'TE']).execute()
    return pd.DataFrame(res.data)

@st.cache_data(ttl=300)
def load_team_efficiency(season: int):
    sup = _get_client()
    res = sup.table('roster_roi').select('team,total_epa,yearly_cap_hit').eq('season', season).in_('position', ['QB', 'RB', 'WR', 'TE']).execute()
    df = pd.DataFrame(res.data)
    
    if df.empty:
        return df

    df['yearly_cap_hit'] = pd.to_numeric(df['yearly_cap_hit'], errors='coerce').fillna(0)
    df['total_epa'] = pd.to_numeric(df['total_epa'], errors='coerce').fillna(0)

    out = df.groupby('team', as_index=False).agg(
        team_total_epa=('total_epa', 'sum'),
        team_total_cap_dollars=('yearly_cap_hit', lambda s: s.sum() * 1_000_000)
    )
    return out

@st.cache_data(ttl=60)
def load_pipeline_meta():
    sup = _get_client()
    res = sup.table('pipeline_meta').select('*').execute()
    return pd.DataFrame(res.data)

@st.cache_data(ttl=300)
def load_player_history(gsis_id: str):
    sup = _get_client()
    res = sup.table('roster_roi').select('*').eq('gsis_id', gsis_id).execute()
    return pd.DataFrame(res.data)
