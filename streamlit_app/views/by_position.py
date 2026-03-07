import streamlit as st
from components.data_utils import load_offense_roster
from components.charts import build_steal_scatter, build_efficiency_scatter
from utils.fmt import dollars_to_str

def render():
    st.title('Positional Market Deep Dives')
    st.markdown('Analyze cost vs. production scaled to specific positional markets.')
    
    col1, col2, col3 = st.columns([1, 2, 1])
    with col1:
        season = st.selectbox('Season', [2025], index=0)
    with col2:
        cohort = st.radio(
            'Contract Cohort', 
            ['All', 'Veterans (APY ≥ $4M)', 'Rookie Scale (APY < $4M)'],
            horizontal=True
        )
    with col3:
        st.write("")
        use_log = st.checkbox('Log X-Axis', value=False, key='pos_log')
        
    df_all = load_offense_roster(season)
    
    if df_all.empty:
        st.warning(f'No data available for {season}.')
        return

    if cohort == 'Veterans (APY ≥ $4M)':
        df_all = df_all[df_all['yearly_cap_hit'] >= 4.0]
    elif cohort == 'Rookie Scale (APY < $4M)':
        df_all = df_all[df_all['yearly_cap_hit'] < 4.0]

    tabs = st.tabs(['Quarterbacks', 'Running Backs', 'Wide Receivers', 'Tight Ends'])
    positions = ['QB', 'RB', 'WR', 'TE']

    for tab, pos in zip(tabs, positions):
        with tab:
            df_pos = df_all[df_all['position'] == pos].copy()
            
            if df_pos.empty:
                st.info(f'No {pos} data found for the {cohort} cohort.')
                continue
                
            # Financial Arbitrage Chart
            st.subheader(f'{pos} Market — Cost vs Total EPA')
            fig_roi = build_steal_scatter(df_pos, x_col='yearly_cap_hit', y_col='total_epa', log_x=use_log)
            st.plotly_chart(fig_roi, width='stretch')
            
            st.divider()
            
            # Volume vs Efficiency Chart
            st.subheader(f'{pos} Usage — Snaps vs EPA per Snap')
            fig_eff = build_efficiency_scatter(df_pos)
            st.plotly_chart(fig_eff, width='stretch')
            
            st.divider()

            # Data Table
            st.markdown(f'### Top {pos} Steals ({cohort})')
            top = df_pos[df_pos['total_epa'] > 0].sort_values('cost_per_epa', ascending=True).head(15)
            
            if not top.empty:
                top['apy_str'] = top['yearly_cap_hit'].apply(dollars_to_str)
                display_cols = ['player_name', 'team', 'apy_str', 'total_epa', 'cost_per_epa', 'snaps']
                st.dataframe(top[display_cols], hide_index=True, width='stretch')
            else:
                st.write('No positive-EPA players found for this filter.')
                