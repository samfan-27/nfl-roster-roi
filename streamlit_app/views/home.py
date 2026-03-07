import streamlit as st
from components.data_utils import load_offense_roster, load_pipeline_meta
from components.charts import build_steal_scatter
from utils.fmt import dollars_to_str

def render():
    st.title('Offensive Skill Position ROI — Overview')

    col1, col2, col3 = st.columns(3)
    with col1:
        seasons = [2025]  
        season = st.selectbox('Season', seasons, index=0)
    with col2:
        min_snaps = st.slider('Min snaps', 0, 1000, 100, 50)
    with col3:
        st.write('')
        hide_low = st.checkbox('Hide low_sample', value=True)

    df = load_offense_roster(season)
    
    if not df.empty:
        if hide_low and 'sample_flag' in df.columns:
            df = df[df['sample_flag'] != 'low_sample']
        if min_snaps and 'snaps' in df.columns:
            df = df[df['snaps'].fillna(0) >= min_snaps]

        # Chart
        fig = build_steal_scatter(df, x_col='yearly_cap_hit', y_col='total_epa', log_x=False)
        st.plotly_chart(fig, use_container_width=True)

        st.markdown('### Top Steals (Highest EPA per Dollar)')
        steals = df[df['total_epa'] > 0].sort_values('cost_per_epa', ascending=True).head(10)
        
        if not steals.empty:
            steals = steals.copy() 
            steals['apy_str'] = steals['yearly_cap_hit'].apply(dollars_to_str)
            display_cols = ['player_name', 'team', 'position', 'apy_str', 'total_epa', 'cost_per_epa', 'snaps']
            st.dataframe(steals[display_cols], hide_index=True, use_container_width=True)
        else:
            st.info('No players meet the filter criteria for steals.')
    else:
        st.warning(f'No roster data found for the {season} season.')
        
    meta = load_pipeline_meta()
    if not meta.empty and 'last_run' in meta.columns:
        last_run_time = meta['last_run'].iloc[0]
        st.caption(f'Data last updated: {last_run_time}')
        