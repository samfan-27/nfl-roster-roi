import streamlit as st
from components.data_utils import load_team_efficiency
from components.charts import build_team_heatmap, build_team_scatter
from utils.fmt import dollars_to_str

def render():
    st.title('Team Efficiency')
    st.markdown('Macro-level analysis of positional spending vs. offensive production.')
    
    season = st.selectbox('Season', [2025, 2024, 2023, 2022, 2021])
    df = load_team_efficiency(season)
    
    if df.empty:
        st.warning('No team data available for this season.')
        return
    
    tab1, tab2 = st.tabs(['ROI Scatter (Efficiency)', 'Treemap (Magnitude)'])
    
    with tab1:
        st.subheader('Team Spending Efficiency')
        st.markdown("Are teams getting what they pay for? (Top-Left is the optimal 'Moneyball' quadrant).")
        fig_scatter = build_team_scatter(df)
        st.plotly_chart(fig_scatter, width='stretch')
        
    with tab2:
        st.subheader('League Spending Landscape')
        st.markdown('Size = Cap Space Spent. Color = EPA Generated (Green is good, Red is bad).')
        fig_heat = build_team_heatmap(df)
        st.plotly_chart(fig_heat, width='stretch')

    st.divider()

    st.subheader('Raw Team Data')
    df_display = df.sort_values('team_total_epa', ascending=False).copy()
    
    df_display['Total Cap ($M)'] = (df_display['team_total_cap_dollars'] / 1_000_000).apply(dollars_to_str)
    
    df_display['Team Cost per EPA'] = df_display.apply(
        lambda r: f"${(r['team_total_cap_dollars'] / r['team_total_epa']):,.0f}" if r['team_total_epa'] > 0 else 'N/A', 
        axis=1
    )
    
    display_cols = {
        'team': 'Team', 
        'Total Cap ($M)': 'Total Cap ($M)', 
        'team_total_epa': 'Total EPA', 
        'Team Cost per EPA': 'Cost per EPA'
    }
    
    st.dataframe(
        df_display.rename(columns=display_cols)[list(display_cols.values())], 
        hide_index=True, 
        width='stretch'
    )
    