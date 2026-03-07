import streamlit as st
import pandas as pd
import plotly.graph_objects as go
from components.data_utils import load_offense_roster
from utils.fmt import dollars_to_str

def build_epa_composition_chart(row):
    """Builds a simple bar chart showing the breakdown of a player's EPA."""
    categories = ['Passing EPA', 'Rushing EPA', 'Receiving EPA']
    values = [
        float(row.get('passing_epa', 0) or 0),
        float(row.get('rushing_epa', 0) or 0),
        float(row.get('receiving_epa', 0) or 0)
    ]
    
    plot_cats, plot_vals = [], []
    for c, v in zip(categories, values):
        if v != 0:
            plot_cats.append(c)
            plot_vals.append(v)
            
    if not plot_cats:
        return go.Figure().update_layout(title='No EPA components recorded')
        
    colors = ['#2ca02c' if v > 0 else '#d62728' for v in plot_vals]
            
    fig = go.Figure(data=[go.Bar(x=plot_cats, y=plot_vals, marker_color=colors)])
    fig.update_layout(
        title='EPA Composition',
        yaxis_title='Expected Points Added',
        margin=dict(l=20, r=20, t=40, b=20),
        height=300
    )
    return fig

def render():
    st.title('Player Dossier')
    st.markdown('Micro-level breakdown of individual player contract efficiency.')
    
    season = st.selectbox('Season', [2025])
    df = load_offense_roster(season)
    
    if df.empty:
        st.warning('No roster data available.')
        return
    
    col1, col2 = st.columns(2)
    with col1:
        teams = sorted(df['team'].dropna().unique().tolist())
        selected_team = st.selectbox('Select Team', ['All'] + teams)
    
    with col2:
        if selected_team != 'All':
            player_pool = df[df['team'] == selected_team]
        else:
            player_pool = df
            
        player_names = sorted(player_pool['player_name'].dropna().unique().tolist())
        selected_player = st.selectbox('Select Player', player_names)

    if selected_player:
        row = df[df['player_name'] == selected_player].iloc[0]
        
        st.divider()
        
        # Header
        st.subheader(f"{row['player_name']} | {row['position']} - {row['team']}")
        
        is_rookie = row.get("is_rookie_deal", False)
        if is_rookie:
            st.info('🟢 **Contract Status: Rookie Scale Deal (CBA Constrained)**')
        else:
            st.info('🔵 **Contract Status: Veteran / Open Market Deal**')
        
        # Top level metrics
        m1, m2, m3, m4 = st.columns(4)
        m1.metric('APY (Cost)', dollars_to_str(row['yearly_cap_hit']))
        m2.metric('Total EPA', f"{row['total_epa']:.1f}")
        m3.metric('Snaps', f"{int(row['snaps'])}")
        
        cost_per_epa = row.get('cost_per_epa')
        if pd.isna(cost_per_epa) or cost_per_epa is None:
            cpe_display = 'Liability (Negative EPA)'
        else:
            cpe_display = f"${(cost_per_epa * 1_000_000):,.0f}"
            
        m4.metric('Cost per EPA', cpe_display)
        
        st.divider()
        
        col_chart, col_context = st.columns([3, 2])
        
        with col_chart:
            fig = build_epa_composition_chart(row)
            st.plotly_chart(fig, width='stretch')
            
        with col_context:
            st.markdown('### Positional Context')
            st.markdown('How this player compares to the rest of the league at their position.')
            
            pos_df = df[df['position'] == row['position']]
            median_epa = pos_df['total_epa'].median()
            median_cost = pos_df['yearly_cap_hit'].median()
            
            st.write(f'**Positional Median EPA:** {median_epa:.1f}')
            st.write(f'**Positional Median APY:** {dollars_to_str(median_cost)}')
            
            if row['total_epa'] > median_epa and row['yearly_cap_hit'] < median_cost:
                st.success('  Elite Value (Above average production, below average cost)')
            elif row['total_epa'] < median_epa and row['yearly_cap_hit'] > median_cost:
                st.error('  Roster Liability (Below average production, above average cost)')
            else:
                st.info('  Market Value (Production aligns with cost)')

        st.info('Historical time-series tracking will be implemented in Phase 2.')
        