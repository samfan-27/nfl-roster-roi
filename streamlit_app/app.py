import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
sys.path.insert(0, str(Path(__file__).resolve().parent))
from dotenv import load_dotenv
load_dotenv(Path(__file__).resolve().parents[1] / '.env')

import streamlit as st
from views import home, by_position, team, player, reports
from components.data_utils import load_pipeline_meta
from components.season_picker import show_pipeline_status

PAGES = {
    'Home': home,
    'By Position': by_position,
    'Team Efficiency': team,
    'Player Detail': player,
    'Weekly Reports': reports,
    'About / Methodology': None,
}

st.set_page_config(page_title='Offensive Skill Position ROI', layout='wide')

st.sidebar.title('Navigation')
try:
    show_pipeline_status(load_pipeline_meta())
except Exception:
    st.error('Cannot query Supabase. Check project availability and application credentials.')
    st.stop()
page = st.sidebar.radio('Go to', list(PAGES.keys()), index=0)

if page == 'About / Methodology':
    st.title('About & Methodology')
    st.markdown("""
    This project studies observed offensive production, contract efficiency and
    retrospective market-price associations for QB, RB, WR and TE players.

    **Financial definitions.** Contract APY, the season's cap charge and the
    season's cash payment are distinct amounts, stored in millions. Unknown or
    conflicting costs remain unknown. Annual costs on trades require compatible
    team and production scope. The older `yearly_cap_hit` field is an APY alias.

    **Production.** Raw passing, rushing and receiving EPA use regular-season
    games shared by nflverse statistics and PFR snap data. Snaps cover offense.
    Passing and receiving EPA overlap; their sum does not allocate team value.
    Attempts, carries and targets describe role volume. Research rate exposures
    additionally reconcile the EPA play filters, including two-point attempts.
    Routes and blocking are not measured by these sources.

    **Efficiency.** Positive-EPA APY efficiency compares APY per observed EPA
    within position and recorded contract mechanism. Separate cap and cash
    options answer different expenditure questions. Negative EPA is retained
    elsewhere and does not establish that a player is below replacement level.
    Rookie labels are estimates; non-rookie deals include multiple markets.

    **Reliability.** A snap threshold alone does not establish precision. The
    offline research reports game/week sensitivity, opportunity-specific rate
    pooling and future observed-rate coverage. Conditional normal reference
    bands are not validated confidence intervals for individual talent.
    Acquisition-based replacement cohorts are sensitivity proxies.

    **Contract research.** Completed-season historical associations hold out
    each player's whole history, including later veteran seasons when scoring
    rookies. The inverse log model estimates a transformed geometric center.
    Separate event models compare conditional means and medians using only
    pre-event production. Exact signing/effective dates are often unavailable.
    Retrospective labels do not reconstruct contemporaneous offer information.

    **Roster decisions.** Future participation, opportunity allocation and
    component rates are evaluated separately using chronological preseason
    tests. In-season EPA is not automatically annualized. APY differences do
    not establish current cap savings. Economic surplus requires compatible
    replacement forecasts, a contribution-price assumption, and complete
    cap/cash/guarantee/exit schedules over an explicit horizon.

    **Sources.** Production: nflverse/nflfastR; snaps: PFR; contracts: OTC via
    nflverse. Team charts describe grouped player-attributed EPA and known APY;
    they do not establish total franchise obligations or front-office quality.
    """)

else:
    page_module = PAGES[page]
    page_module.render()
