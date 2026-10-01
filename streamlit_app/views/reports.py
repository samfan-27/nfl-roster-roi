"""Published weekly reports; only the anon-readable publication tables are used."""
import pandas as pd
import streamlit as st
from components.data_utils import (load_analysis_status,load_report_versions,
                                   load_report_heads,load_published_report)


def render():
    st.title('Weekly Reports')
    try:
        versions,status,heads=load_report_versions(),load_analysis_status(),load_report_heads()
    except Exception:
        st.info('Weekly report storage has not been enabled yet. Player dashboards remain available.')
        return
    if not status.empty:
        latest=status.iloc[0]
        st.caption(f"Latest analysis attempt ({latest['season']}): {latest['status']} · {latest['updated_at']}")
        if latest['status'] in ('failed','deferred','running'):
            st.info('The previous successful report remains available while a new report is pending.')
    if versions.empty:
        st.info('No weekly report has been published yet.')
        return
    seasons=sorted(versions.season.astype(int).unique(),reverse=True)
    season=st.selectbox('Report season',seasons)
    available=versions.loc[versions.season.eq(season)]
    choices=available.run_id.tolist()
    head=heads.loc[heads.season.eq(season)] if not heads.empty else pd.DataFrame()
    default=head.iloc[0].run_id if not head.empty else choices[0]
    by_id=available.set_index('run_id')
    def label(key):
        row=by_id.loc[key]
        return f"Week {row.week} · {row.published_at} · {key[:8]}"
    selected=st.selectbox('Published version',choices,index=choices.index(default) if default in choices else 0,format_func=label)
    try:
        report=load_published_report(selected)
    except Exception:
        st.error('Could not load the selected report. Please retry later.')
        return
    if report is None:
        st.warning('This report is unavailable.')
        return
    coverage=report['coverage']
    st.caption(f"Completed coverage through week {report['week']} · {len(coverage['shared_games'])} source games at generation")
    with st.expander('Schedule coverage and publication grace'):
        st.dataframe(pd.DataFrame(coverage['per_week']),hide_index=True)
        st.write(f"Source publication grace: {coverage['grace_hours']} hours after estimated game end.")
        if coverage['grace_games']:
            st.write('Later completed games awaiting sources:',coverage['grace_games'])
    st.markdown(report['report_markdown'])
    st.download_button('Download report',report['report_markdown'],
                       file_name=f"nfl-roi-{season}-week-{report['week']:02d}-{selected[:8]}.md",mime='text/markdown')
    comparison=pd.DataFrame(report['comparison'])
    st.download_button('Download matched comparison',comparison.to_csv(index=False),
                       file_name=f"comparison-{season}-week-{report['week']:02d}-{selected[:8]}.csv",mime='text/csv')
