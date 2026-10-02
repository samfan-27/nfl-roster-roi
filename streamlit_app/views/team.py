from components.season_picker import select_season, show_coverage
import streamlit as st
from components.data_utils import load_team_efficiency, load_offense_roster
from components.charts import build_team_heatmap, build_team_scatter
from utils.fmt import dollars_to_str

def render():
    st.title('Team production and known APY')
    st.markdown('Annual offensive skill-position APY versus player-attributed EPA.')
    st.caption('Passing and receiving EPA overlap. Totals are attributed to current/latest roster teams, not net team offensive EPA or actual cap spending.')

    season = select_season()
    show_coverage(load_offense_roster(season))
    df = load_team_efficiency(season)

    if df.empty:
        st.warning('No team data available for this season.')
        return

    tab1, tab2 = st.tabs(['APY and attributed EPA', 'Treemap (Magnitude)'])

    with tab1:
        st.subheader('Known APY and observed production')
        st.markdown("APY totals cover known player contracts. Unmeasured obligations and overlapping EPA prevent an economic roster-value interpretation.")
        fig_scatter = build_team_scatter(df)
        st.plotly_chart(fig_scatter, width='stretch')

    with tab2:
        st.subheader('League Spending Landscape')
        st.markdown('Size = known contract APY. Color = overlapping player-attributed EPA.')
        fig_heat = build_team_heatmap(df)
        st.plotly_chart(fig_heat, width='stretch')

    st.divider()

    st.subheader('Raw Team Data')
    df_display = df.sort_values('team_total_epa', ascending=False).copy()

    df_display['Total APY ($M)'] = (df_display['team_total_cap_dollars'] / 1_000_000).apply(dollars_to_str)

    df_display['Team Cost per EPA'] = df_display.apply(
        lambda r: f"${(r['team_total_cap_dollars'] / r['team_total_epa']):,.0f}" if r['team_total_epa'] > 0 else 'N/A',
        axis=1
    )

    display_cols = {
        'team': 'Team',
        'Total APY ($M)': 'Total APY ($M)',
        'team_total_epa': 'Total EPA',
        'Team Cost per EPA': 'Cost per EPA'
    }

    for field in ['known_apy_players','unknown_apy_players']:
        if field in df_display:
            display_cols[field] = field.replace('_',' ').capitalize()
    st.dataframe(
        df_display.rename(columns=display_cols)[list(display_cols.values())],
        hide_index=True,
        width='stretch'
    )

