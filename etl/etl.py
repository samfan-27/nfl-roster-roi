"""Extract NFL data, calculate season ROI, write artifacts, then upsert.

Examples:
  python -m etl.etl --fresh --dry-run
  python -m etl.etl --seasons 2026 --fresh
  python -m etl.etl --auto --fresh
"""

import argparse
from pathlib import Path
import pandas as pd
from dotenv import load_dotenv
from loguru import logger

from etl.database import check_connection, get_supabase_client, upsert_supabase, update_pipeline_meta
from etl.sources import configure_sources, current_season, load_reference_tables, load_season_tables, SourceNotReadyError
from etl.utils import write_artifacts
from src.analysis import build_roster_roi
from src.domain import FIRST_SEASON


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument('--seasons', nargs='+', type=int, help='Explicit seasons to refresh')
    mode.add_argument('--auto', action='store_true', help='Backfill from 2021 through the current season')
    parser.add_argument('--min-snaps', type=int, default=100)
    parser.add_argument('--shrink-tau', type=float, default=200.0)
    parser.add_argument('--output', default='./artifacts/roster_roi_combined.csv')
    parser.add_argument('--dry-run', action='store_true', help='Write local artifacts only')
    parser.add_argument('--fresh', action='store_true', help='Bypass cached source downloads')
    args = parser.parse_args(argv)
    if args.min_snaps < 0 or args.shrink_tau < 0:
        parser.error('Minimum snaps and shrinkage strength must be non-negative')
    if args.seasons and any(s < FIRST_SEASON or s > current_season() for s in args.seasons):
        parser.error(f'Seasons must be between {FIRST_SEASON} and {current_season()}')
    return args


def merge_local_history(new_metrics, output):
    """Replace refreshed seasons in the local artifact, retaining other years."""
    path = Path(output)
    if not path.exists():
        return new_metrics
    old = pd.read_csv(path)
    old = old.loc[~old['season'].isin(new_metrics['season'].unique())]
    return pd.concat([old, new_metrics], ignore_index=True).sort_values(['season', 'gsis_id'])


def main(argv=None):
    load_dotenv()
    args = parse_args(argv)
    latest = current_season()
    seasons = list(range(FIRST_SEASON, latest + 1)) if args.auto else sorted(set(args.seasons or [latest]))
    sup = None
    if not args.dry_run:
        sup = get_supabase_client()
        check_connection(sup)
    try:
        configure_sources(fresh=args.fresh)
        references = load_reference_tables()
        metrics, audits, unmatched, coverage = [], [], [], []
        for season in seasons:
            logger.info('Building regular-season ROI for {}', season)
            try:
                tables = load_season_tables(season)
            except SourceNotReadyError:
                if args.auto and season == latest:
                    logger.warning('Current season is not published yet; backfilling completed seasons only')
                    continue
                raise
            frame, audit, missing = build_roster_roi(
                season, **references, **tables, min_snaps=args.min_snaps,
                shrink_tau=args.shrink_tau,
            )
            if frame.empty or frame.duplicated(['season', 'gsis_id']).any():
                raise ValueError(f'Empty or duplicate player-season metrics for {season}')
            metrics.append(frame)
            audits.append(audit)
            missing = missing.assign(season=season)
            unmatched.append(missing)
            coverage.append(f'{season}: {frame["notes"].iloc[0].split(". ")[0]}')
        if not metrics:
            raise SourceNotReadyError('No season data is ready; previous database rows were retained')
        combined = pd.concat(metrics, ignore_index=True)
        history = merge_local_history(combined, args.output)
        write_artifacts(history, pd.concat(audits, ignore_index=True), pd.concat(unmatched, ignore_index=True), args.output)
        rows_written = 0
        if sup is not None:
            rows_written = upsert_supabase(sup, combined)
            update_pipeline_meta(sup, 'success', rows_written, '; '.join(coverage))
        logger.success('ETL completed: produced={}, written={}; {}', len(combined), rows_written, '; '.join(coverage))
        return combined
    except Exception as exc:
        if sup is not None:
            try:
                update_pipeline_meta(sup, 'failed', message=f'{type(exc).__name__}: {exc}')
            except Exception:
                logger.warning('Could not record pipeline failure in Supabase')
        raise


if __name__ == '__main__':
    main()
