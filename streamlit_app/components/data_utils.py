"""Cached Supabase reads; calculations remain in the domain package."""

import os
import pandas as pd
from supabase import create_client
import streamlit as st
from src.domain import OFFENSIVE_POSITIONS


@st.cache_resource
def _get_client():
    url = os.getenv('SUPABASE_URL')
    key = os.getenv('SUPABASE_ANON_KEY')
    try:
        url = st.secrets.get('SUPABASE_URL', url)
        key = st.secrets.get('SUPABASE_ANON_KEY', key)
    except st.errors.StreamlitSecretNotFoundError:
        pass
    if not url or not key:
        raise RuntimeError('Supabase URL or anon key not found in secrets/environment')
    return create_client(url, key)


def _read_pages(query, page_size=1000):
    """Supabase caps responses; fetch every page in a stable query order."""
    records = []
    offset = 0
    while True:
        page = query.range(offset, offset + page_size - 1).execute().data
        records.extend(page)
        if len(page) < page_size:
            return pd.DataFrame(records)
        offset += page_size


@st.cache_data(ttl=300)
def load_available_seasons():
    query = _get_client().table('roster_roi').select('season').order('season', desc=True).order('gsis_id')
    frame = _read_pages(query)
    return sorted(frame['season'].dropna().astype(int).unique().tolist(), reverse=True) if not frame.empty else []


@st.cache_data(ttl=300)
def load_offense_roster(season):
    query = _get_client().table('roster_roi').select('*').eq('season', season).in_('position', list(OFFENSIVE_POSITIONS)).order('gsis_id')
    return _read_pages(query)


@st.cache_data(ttl=300)
def load_team_efficiency(season):
    frame = load_offense_roster(season)
    if frame.empty:
        return frame
    from src.stats_helpers import aggregate_team_efficiency
    return aggregate_team_efficiency(frame)


@st.cache_data(ttl=60)
def load_pipeline_meta():
    response = _get_client().table('pipeline_meta').select('*').eq('id', 1).execute()
    return pd.DataFrame(response.data)


@st.cache_data(ttl=300)
def load_player_history(gsis_id):
    query = _get_client().table('roster_roi').select('*').eq('gsis_id', gsis_id).order('season')
    return _read_pages(query)


@st.cache_data(ttl=60)
def load_analysis_status():
    return _read_pages(_get_client().table('analysis_status').select('*').order('season', desc=True))


@st.cache_data(ttl=60)
def load_report_versions():
    columns = 'run_id,season,week,published_at,coverage'
    return _read_pages(_get_client().table('analysis_publications').select(columns)
                       .order('season', desc=True).order('week', desc=True)
                       .order('published_at', desc=True).order('run_id'))


@st.cache_data(ttl=60)
def load_report_heads():
    return _read_pages(_get_client().table('analysis_heads').select('*').order('season', desc=True))


@st.cache_data(ttl=300)
def load_published_report(run_id):
    rows = _get_client().table('analysis_publications').select('*').eq('run_id', run_id).execute().data
    return rows[0] if rows else None
