"""Generate shared research locally, or publish via validated cloud inputs."""
import argparse
from pathlib import Path
import pandas as pd
from dotenv import load_dotenv
from etl.sources import configure_sources,current_season,load_reference_tables,load_season_tables,load_schedule
from etl.coverage import assess_coverage,require_ready,through_week
from src.analysis import build_roster_roi
from src.reporting import analysis_outputs


def main(argv=None):
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--cloud',action='store_true')
    parser.add_argument('--input',default='artifacts/roster_roi_combined.csv')
    parser.add_argument('--output',default='artifacts/latest_analysis.md')
    args=parser.parse_args(argv)
    load_dotenv()
    if args.cloud:
        from etl.cloud import main as cloud_main
        return cloud_main(['weekly'])
    frame=pd.read_csv(args.input)
    latest=current_season()
    configure_sources(fresh=True)
    references=load_reference_tables()
    now_tables=load_season_tables(latest)
    coverage=assess_coverage(latest,load_schedule(latest),now_tables)
    require_ready(coverage)
    week=coverage['cutoff_week']
    before_tables=load_season_tables(latest-1)
    before_coverage=assess_coverage(latest-1,load_schedule(latest-1),before_tables,grace_hours=0)
    require_ready(before_coverage,historical=True)
    now,_,_=build_roster_roi(latest,**references,**through_week(now_tables,week))
    before,_,_=build_roster_roi(latest-1,**references,**through_week(before_tables,week))
    # Local CSV mode remains an explicit research convenience. Canonical cloud
    # execution never treats arbitrary retained dashboard rows as training data.
    report,outputs=analysis_outputs(frame.loc[frame.season.lt(latest)],now,before,latest,week)
    output=Path(args.output)
    output.parent.mkdir(parents=True,exist_ok=True)
    output.write_text(report)
    filenames=dict(scored='roster_roi_scored.csv',diagnostics='model_diagnostics.csv',comparison='matched_week_comparison.csv',summary='position_summary.csv',candidates='value_candidates.csv')
    for name,data in outputs.items():
        data.to_csv(output.parent/filenames[name],index=False)
    print(f'Wrote {output}')
    return 0


if __name__=='__main__':
    raise SystemExit(main())
