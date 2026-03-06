import streamlit as st
from pages import home, by_position, team, player

PAGES = {
    'Home': home,
    'By Position': by_position,
    'Team Efficiency': team,
    'Player Detail': player,
    'About / Methodology': None,
}

st.set_page_config(page_title='Offensive Skill Position ROI', layout='wide')

st.sidebar.title('Navigation')
page = st.sidebar.radio('Go to', list(PAGES.keys()), index=0)

if page == 'About / Methodology':
    st.title('About & Methodology')
    st.markdown("""
    - `yearly_cap_hit` uses APY (AAV) in millions. UI displays as $XM.
    - Shrinkage applied: empirical Bayes toward position mean.
    - Rankings by cost_per_epa exclude negative/zero total_epa.
    """)
else:
    page_module = PAGES[page]
    page_module.render()
