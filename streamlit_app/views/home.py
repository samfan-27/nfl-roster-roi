import streamlit as st
from components.data_utils import load_offense_roster, load_pipeline_meta
from components.charts import build_steal_scatter
from utils.fmt import dollars_to_str

def render():
    st.title('Offensive Skill Position ROI — Overview')

    col1, col2, col3, col4 = st.columns([1, 1.5, 2, 1])
    with col1:
        seasons = [2025]  
        season = st.selectbox('Season', seasons, index=0)
    with col2:
        min_snaps = st.slider('Min snaps', 0, 1000, 100, 50)
    with col3:
        cohort = st.radio(
            "Contract Tier", 
            ["All", "Premium Deals (APY ≥ $4M)", "Value Deals (APY < $4M)"],
            horizontal=True
        )
    with col4:
        st.write('')
        use_log = st.checkbox('Log X-Axis', value=False)

    df = load_offense_roster(season)
    
    if not df.empty:
        if min_snaps and 'snaps' in df.columns:
            df = df[df['snaps'].fillna(0) >= min_snaps]
            
        if cohort == "Premium Deals (APY ≥ $4M)":
            df = df[df['yearly_cap_hit'] >= 4.0]
        elif cohort == "Value Deals (APY < $4M)":
            df = df[df['yearly_cap_hit'] < 4.0]

        # Chart
        fig = build_steal_scatter(df, x_col="yearly_cap_hit", y_col="total_epa", log_x=use_log)
        st.plotly_chart(fig, width="stretch")

        st.markdown(f'### Top Steals: {cohort}')
        steals = df[df['total_epa'] > 0].sort_values('cost_per_epa', ascending=True).head(10)
        
        if not steals.empty:
            steals = steals.copy() 
            steals['apy_str'] = steals['yearly_cap_hit'].apply(dollars_to_str)
            display_cols = ['player_name', 'team', 'position', 'apy_str', 'total_epa', 'cost_per_epa', 'snaps']
            st.dataframe(steals[display_cols], hide_index=True, width="stretch")
        else:
            st.info('No players meet the filter criteria for steals.')
    else:
        st.warning(f'No roster data found for the {season} season.')
        
    meta = load_pipeline_meta()
    if not meta.empty and 'last_run' in meta.columns:
        last_run_time = meta['last_run'].iloc[0]
        st.caption(f'Data last updated: {last_run_time}')
        