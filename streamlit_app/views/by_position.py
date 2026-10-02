from components.season_picker import select_season, show_coverage
import streamlit as st
from components.data_utils import load_offense_roster
from components.charts import build_steal_scatter, build_efficiency_scatter
from components.efficiency import prepare_finances, cohort_filter, efficiency_table, FINANCES


def render():
    st.title('Production and efficiency by position')
    season = select_season()
    min_snaps = st.slider('Minimum offensive snaps', 0, 1000, 100, 50, key='pos_snaps')
    df = load_offense_roster(season)
    show_coverage(df)
    if df.empty:
        st.warning(f'No data available for {season}.')
        return
    df = prepare_finances(df)
    df = cohort_filter(df.loc[df.snaps.ge(min_snaps)], 'position')
    financial = st.selectbox('Financial measure', list(FINANCES))
    for tab, pos in zip(st.tabs(['Quarterbacks', 'Running Backs', 'Wide Receivers', 'Tight Ends']), ['QB', 'RB', 'WR', 'TE']):
        with tab:
            cohort = df.loc[df.position.eq(pos)].copy()
            if cohort.empty:
                st.info(f'No {pos} players meet these filters.')
                continue
            st.plotly_chart(build_steal_scatter(cohort, x_col=FINANCES[financial]), width='stretch')
            st.plotly_chart(build_efficiency_scatter(cohort), width='stretch')
            efficiency_table(cohort, financial, limit=15)
