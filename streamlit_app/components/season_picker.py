"""Shared season selection and data freshness presentation."""

import pandas as pd
import streamlit as st
from components.data_utils import load_available_seasons


def select_season():
    seasons = load_available_seasons()
    if not seasons:
        st.warning('No seasons are loaded. Run the data refresh before using the dashboard.')
        st.stop()
    return st.selectbox('Season', seasons, index=0)


def show_coverage(frame):
    if frame.empty or 'notes' not in frame:
        return
    notes = frame['notes'].dropna()
    if not notes.empty:
        coverage = str(notes.iloc[0]).split('. ')[0]
        st.caption(coverage)
        if 'through week' in coverage and 'through week 18;' not in coverage:
            st.info('Season-to-date data: full annual APY is divided by production recorded so far. Cost per EPA and total EPA are not directly comparable with completed seasons.')


def show_pipeline_status(meta):
    if meta.empty:
        st.warning('Pipeline status is unavailable.')
        return
    row = meta.iloc[0]
    st.caption(f"Last pipeline run (UTC): {row['last_run']} | Status: {row['last_status']}")
    if row['last_status'] != 'success':
        st.warning('The latest refresh did not succeed. Displayed data may be from an earlier run.')
    timestamp = pd.to_datetime(row['last_run'], utc=True, errors='coerce')
    if pd.notna(timestamp) and pd.Timestamp.now(tz='UTC') - timestamp > pd.Timedelta(days=3):
        st.warning('The pipeline has not run in over three days. Check GitHub Actions and Supabase availability.')
