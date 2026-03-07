import streamlit as st
from views import home, by_position, team, player

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
    st.markdown('### Executive Summary')
    st.markdown('''
    This dashboard calculates the **Financial Return on Investment (ROI)** for NFL offensive skill positions (QB, RB, WR, TE). 
    By merging play-by-play production data with salary cap capmetrics, it identifies market inefficiencies, surplus value, and front-office capital allocation strategies.
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
    *   **`total_epa`**: The sum of a player's Passing, Rushing, and Receiving Expected Points Added.
    *   **`yearly_cap_hit` (APY)**: Average Per Year contract value (in millions). APY is strictly used over "current year cap hit" to normalize front-office accounting tricks (like void years and signing bonus prorations) and reflect the true market value of the contract.
    *   **`cost_per_epa`**: Calculated as `yearly_cap_hit / total_epa`. Represents the dollar cost for a single unit of scoring value. *(Note: Players with zero or negative EPA are excluded from efficiency rankings as they operate as financial liabilities).*
    *   **Empirical Bayes Shrinkage**: Applied to `epa_per_snap` to regress low-sample, high-variance outliers toward the positional mean. This prevents players with 2 snaps and 1 long touchdown from breaking the efficiency models.
    *   **CBA Cohort Splitting**: The NFL CBA artificially constrains rookie salaries. Comparing a $900k rookie to a $35M veteran mathematically distorts cost-per-EPA. We programmatically isolate "Rookie Scale Deals" from "Veteran / Open Market Deals" using strict rules:
        *   *Drafted Players*: Must still be on the contract signed in their `draft_year`.
        *   *Undrafted Free Agents (UDFAs)*: Must have fewer than 3 accrued seasons (`years_exp < 3`), legally restricting them to Exclusive Rights Free Agent (ERFA) minimums.
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

    st.markdown('### Phase 2 Roadmap: Expected APY Model')
    st.markdown('''
    The current `cost_per_epa` metric evaluates historical efficiency but assumes a linear price curve. In reality, elite NFL production scales exponentially due to roster scarcity (11 players on the field). 
    
    **Next Steps:**
    *   Train a Generalized Linear Model (GLM) / Ridge Regression / Non-linear models exclusively on the unconstrained **Veteran** cohort.
    *   Learn the true open-market price of a unit of EPA (controlling for snaps, position, and age).
    *   Score all players (including rookies) against this model to calculate absolute **Surplus Value** (`Expected APY - Actual APY`).
    ''')

else:
    page_module = PAGES[page]
    page_module.render()
