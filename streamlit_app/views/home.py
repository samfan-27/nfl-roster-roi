from components.season_picker import select_season, show_coverage
import streamlit as st
from components.data_utils import load_offense_roster, load_pipeline_meta
from components.charts import build_steal_scatter
from components.efficiency import prepare_finances, cohort_filter, efficiency_table, FINANCES


def render():
    st.title('Offensive production and contract efficiency')
    season = select_season()
    min_snaps = st.slider('Minimum offensive snaps', 0, 1000, 100, 50)
    df = load_offense_roster(season)
    show_coverage(df)
    if df.empty:
        st.warning(f'No roster data found for the {season} season.')
        return
    df = prepare_finances(df)
    position = st.selectbox('Position', sorted(df.position.unique()))
    df = df.loc[df.position.eq(position) & df.snaps.ge(min_snaps)]
    df = cohort_filter(df, 'home')
    financial = st.selectbox('Financial measure', list(FINANCES))
    use_log = st.checkbox('Log cost axis', value=False)
    st.plotly_chart(build_steal_scatter(df, x_col=FINANCES[financial], log_x=use_log), width='stretch')
    efficiency_table(df, financial)
    meta = load_pipeline_meta()
    if not meta.empty and 'last_run' in meta:
        st.caption(f"Data last updated: {meta['last_run'].iloc[0]}")
