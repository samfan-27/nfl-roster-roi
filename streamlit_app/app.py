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
    st.markdown('### Executive Summary')
    st.markdown('''
    This dashboard calculates the **Financial Return on Investment (ROI)** for NFL offensive skill positions (QB, RB, WR, TE).
    It compares recorded production with contract APY to describe position-level cost efficiency. Results are descriptive and sensitive to coverage and contract assumptions.
    ''')

    st.divider()

    st.markdown('### Data Sources')
    st.markdown('''
    - **Production Data:** Play-by-play Expected Points Added (EPA) sourced via `nflreadpy` (nflfastR).
    - **Usage Data:** Snap counts tracked via Pro-Football-Reference (PFR).
    - **Financial Data:** Contract metadata and salary cap figures sourced via OverTheCap (OTC).
    ''')

    st.divider()

    st.markdown('### Core Metrics & Mathematical Adjustments')
    st.markdown('''
    *   **`total_epa`**: The raw sum of a player's Passing, Rushing, and Receiving Expected Points Added from regular-season games shared by both statistics and snap-count sources.
    *   **`yearly_cap_hit` (APY)**: Average Per Year contract value (in millions). APY describes average annual contract value; it does not measure the current season cap charge or independently establish market value.
    *   **`cost_per_epa`**: Calculated as `yearly_cap_hit / total_epa`. Stored in millions of dollars per EPA. Players with nonpositive EPA or unknown/zero APY are excluded from value rankings; that exclusion is not a complete assessment of player value.
    *   **Position Shrinkage**: The normalized cost-per-100-snaps metric uses a rate shrunk toward the same season and position mean. Raw total EPA, EPA per snap, and cost per EPA remain unadjusted. Shrinkage is a heuristic, not a fitted Bayesian model.
    *   **Contract Cohorts**: Rookie status is an estimate using draft year, contract signing year, and a bounded rookie window. Roster experience is not CBA accrued seasons; the flag does not determine legal ERFA status.
    *   **Financial Scope**: APY is in millions of dollars. `cap_pct_of_team` is APY divided by that season's league cap, not an actual team cap charge. Contract selection uses the latest signing year no later than the season; historical timing and same-year transactions are approximate.
    *   **Team Scope**: Team charts sum player-attributed EPA and APY by current/latest roster team. Passing and receiving EPA overlap; the sum is not net team offensive EPA or actual cap spending.
    *   **In-Season Scope**: Production is season-to-date. Full annual APY divided by partial-season EPA is not comparable to a completed season. No automatic full-season valuation is inferred.
    ''')

    st.divider()

    st.markdown('### Page Architecture')
    st.markdown('''
    1.  **Home (Macro View)**: A league-wide scatter plot showing the raw relationship between capital spent and points generated. Used to spot absolute outliers.
    2.  **By Position (Micro View)**: Positional stratification. Because QBs inherently generate significantly more EPA than RBs, plotting them together obscures relative skill. This page solves the "Apples to Oranges" problem and includes Usage vs. Efficiency (Snaps vs. EPA/Snap) quadrants.
    3.  **Team Efficiency (Macro Aggregation)**: Evaluates Front Office performance. Aggregates total positional spending versus total offensive output to visualize which GMs are operating in the optimal "Moneyball" quadrant (low spend, high production).
    4.  **Player Detail (Dossier)**: A micro-level search engine isolating how a specific player generated their EPA (Passing vs. Rushing vs. Receiving) and flagging their CBA constraint status.
    ''')

    st.divider()

    st.markdown('### Completed-Season APY Research')
    st.markdown('''
    The analysis notebooks call a shared position-specific Ridge model using completed-season EPA, snaps, EPA per snap, age, and experience. The target is APY as a share of the season's league salary cap.

    Estimated veteran contracts form the training cohort. Nested cross-validation holds out all years of each player together, with tuning inside each training fold. The incomplete season is excluded from annual-volume training and scoring. Historical surplus estimates are research outputs, not predictions of future offers.
    ''')

else:
    page_module = PAGES[page]
    page_module.render()
