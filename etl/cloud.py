"""Portable cloud runner: refresh, weekly, bootstrap and database health.

Durable inputs and next-run state live in Supabase; the runner needs no cache
or local historical files. The same entrypoint runs in any Python 3.12 executor.
"""
import argparse
from hashlib import sha256
from importlib.metadata import version
import json
import os
import re
from pathlib import Path
import uuid
import pandas as pd
from dotenv import load_dotenv
from postgrest.exceptions import APIError
from etl.cloud_store import CloudStore, PipelineBusy, SchemaNotReady, utcnow, validate_metrics
from etl.coverage import assess_coverage, require_ready, through_week, CoverageNotReady
from etl.database import get_supabase_client, check_connection
from etl.sources import (configure_sources, current_season, load_reference_tables,
                         load_season_tables, load_schedule, SourceNotReadyError)
from src.analysis import build_roster_roi
from src.opportunities import game_production
from src.reporting import analysis_outputs
from src.domain import FIRST_SEASON

SOURCE_BASE = 'https://github.com/nflverse/nflverse-data/releases/download/'


def source_urls(season):
    return {name: SOURCE_BASE + path for name, path in dict(
        players='players/players.parquet', contracts='contracts/historical_contracts.parquet',
        player_stats=f'stats_player/stats_player_week_{season}.parquet',
        snap_counts=f'snap_counts/snap_counts_{season}.parquet',
        rosters=f'rosters/roster_{season}.parquet', schedule='schedules/games.parquet',
    ).items()}


def archive_inputs(store, season, tables, references, schedule, coverage, revision, parameters):
    artifacts = {name: store.put_frame(frame) for name, frame in {**tables, **references, 'schedule':schedule}.items()}
    return dict(schema_version=2, methodology_version=2, season=season, fetched_at=utcnow(), code_revision=revision,
                coverage=coverage, parameters=parameters, artifacts=artifacts, source_urls=source_urls(season),
                dependencies={name:version(name) for name in ('nflreadpy','pandas','numpy','scikit-learn','scipy','pyarrow','supabase')})


def get_run_id(kind, season):
    execution = os.getenv('CLOUD_RUN_EXECUTION') or os.getenv('GITHUB_RUN_ID')
    if not execution:
        return str(uuid.uuid4())
    provider = 'cloud-run' if os.getenv('CLOUD_RUN_EXECUTION') else 'github'
    return str(uuid.uuid5(uuid.NAMESPACE_URL, f'nfl-roi/{provider}/{execution}/{kind}/{season}'))


def revision():
    value = os.getenv('CODE_REVISION') or os.getenv('GITHUB_SHA')
    if not value:
        import subprocess
        value = subprocess.check_output(['git','rev-parse','HEAD'], text=True).strip()
        if subprocess.check_output(['git','status','--porcelain'], text=True).strip():
            value += '-dirty'
    return value


def rebuild_historical_metrics(store, snapshots, *, min_snaps=100, shrink_tau=200):
    """Recalculate old canonical sources with the current financial definitions.

    Frozen v1 metrics contain combined defensive snaps and discarded financial
    details. Reusing those rows would silently bypass the methodology migration.
    Source snapshots remain immutable and every download is hash checked.
    """
    rebuilt = []
    for snapshot, _ in snapshots:
        manifest = snapshot['manifest']
        names = ['players','contracts','player_stats','snap_counts','rosters']
        tables = {name:store.frame(manifest['artifacts'][name]) for name in names}
        frame, _, _ = build_roster_roi(snapshot['season'], **tables, min_snaps=min_snaps,
                                       shrink_tau=shrink_tau, snapshot_at=manifest['fetched_at'])
        validate_metrics(frame, snapshot['season'])
        shared = set(tables['player_stats'].loc[tables['player_stats'].season_type.eq('REG')].game_id) & set(
            tables['snap_counts'].loc[tables['snap_counts'].game_type.eq('REG')].game_id)
        if shared != set(snapshot['coverage']['shared_games']):
            raise ValueError('Recalculated history differs from canonical game coverage')
        rebuilt.append(frame)
    return pd.concat(rebuilt,ignore_index=True)


def execute(store, kind, season, *, grace_hours=48, min_snaps=100, shrink_tau=200,
            exclusions=None, code_revision=None, now=None):
    rev = code_revision or revision()
    run_id = get_run_id(kind, season)
    coverage = None
    with store.lock():
        if not store.begin(run_id, kind, season, rev):
            return dict(run_id=run_id, status='already_succeeded')
        try:
            if kind == 'health':
                check_connection(store.client)
                store.finish(run_id, 'succeeded')
                return dict(run_id=run_id, status='succeeded')
            store.require_methodology_schema()
            configure_sources(fresh=True)
            references = load_reference_tables()
            tables, schedule = load_season_tables(season), load_schedule(season)
            raw_tables = tables
            fetched_at = utcnow()
            tables = {name:frame.loc[~frame.game_id.isin(exclusions or {})].copy() if name in ('player_stats','snap_counts') else frame.copy() for name,frame in tables.items()}
            coverage = assess_coverage(season, schedule, tables, now=now, grace_hours=grace_hours,
                                       previous_games=store.previous_games(season), exclusions=exclusions)
            require_ready(coverage, historical=kind == 'bootstrap' or season < current_season())
            # Daily ingestion can retain an already-completed partial next week,
            # but refuses ANY missing source game (inside grace is a deferral).
            if kind != 'weekly' and coverage['grace_games']:
                raise CoverageNotReady('Completed games are awaiting upstream publication')
            if kind == 'weekly':
                tables = through_week(tables, coverage['cutoff_week'])
            parameters = dict(min_snaps=min_snaps, shrink_tau=shrink_tau, grace_hours=grace_hours,
                              exclusions=exclusions or {},analysis_cutoff=coverage['cutoff_week'] if kind=='weekly' else None)
            manifest = archive_inputs(store, season, raw_tables, references, schedule, coverage, rev, parameters)
            manifest['fetched_at'] = fetched_at
            if kind == 'weekly':
                manifest['analysis_inputs'] = {name:store.put_frame(frame) for name,frame in {**tables,'schedule':schedule.loc[schedule.week.le(coverage['cutoff_week'])]}.items()}
            metrics, audit, unmatched = build_roster_roi(season, **tables, **references,
                                                        min_snaps=min_snaps, shrink_tau=shrink_tau, snapshot_at=fetched_at)
            validate_metrics(metrics, season)
            for name, frame in dict(metrics=metrics, join_audit=audit, unmatched_contracts=unmatched,
                                    game_components=game_production(season,players=references['players'],**tables)).items():
                manifest['artifacts'][name] = store.put_frame(frame)
            if kind in ('refresh','bootstrap'):
                manifest['manifest_object'] = store.put_json(manifest)
                store.ingest(run_id, season, metrics, coverage, manifest, historical=kind=='bootstrap')
            else:
                history = store.histories(season)
                previous_snapshot = next(s for s, _ in history if s['season'] == season-1)
                before_manifest = previous_snapshot['manifest']
                previous_tables = {name:store.frame(before_manifest['artifacts'][name])
                                   for name in ('player_stats','snap_counts','rosters')}
                previous_references = {name:store.frame(before_manifest['artifacts'][name]) for name in ('players','contracts')}
                previous_schedule = store.frame(before_manifest['artifacts']['schedule'])
                previous_coverage = assess_coverage(season-1, previous_schedule, previous_tables,
                    grace_hours=0, exclusions=previous_snapshot['coverage'].get('exclusions'))
                require_ready(previous_coverage, historical=True)
                previous_tables = through_week(previous_tables, coverage['cutoff_week'])
                before, _, _ = build_roster_roi(season-1, **previous_tables, **previous_references,
                                                min_snaps=min_snaps, shrink_tau=shrink_tau, snapshot_at=before_manifest['fetched_at'])
                historical_metrics = rebuild_historical_metrics(store,history,min_snaps=min_snaps,shrink_tau=shrink_tau)
                report, outputs = analysis_outputs(historical_metrics, metrics, before, season,
                                                    coverage['cutoff_week'], min_snaps=min_snaps)
                outputs['historical_metrics_recalculated'] = historical_metrics
                manifest['historical_run_ids'] = [s['run_id'] for s, _ in history]
                manifest['previous_coverage'] = previous_snapshot['coverage']
                fingerprint_input = dict(inputs={name:d['sha256'] for name,d in {**manifest['analysis_inputs'],**{n:manifest['artifacts'][n] for n in ('players','contracts')}}.items()},
                                         historical_run_ids=manifest['historical_run_ids'], parameters=parameters, revision=rev)
                fingerprint = sha256(json.dumps(fingerprint_input, sort_keys=True).encode()).hexdigest()
                manifest['artifacts']['report'] = store.put(report.encode(), 'md', 'text/markdown')
                for name, frame in outputs.items():
                    manifest['artifacts'][name] = store.put_frame(frame)
                manifest['fingerprint'] = fingerprint
                manifest['manifest_object'] = store.put_json(manifest)
                store.publish(run_id, season, coverage, fingerprint, manifest, report, outputs)
            return dict(run_id=run_id, status='succeeded', cutoff_week=coverage['cutoff_week'])
        except (CoverageNotReady, SourceNotReadyError) as exc:
            store.finish(run_id, 'deferred', coverage, type(exc).__name__)
            return dict(run_id=run_id, status='deferred', error_type=type(exc).__name__)
        except Exception as exc:
            # Persist only the type. Transport exception messages can contain URLs
            # or request headers; never send them to frontend status or logs.
            try:
                store.finish(run_id, 'failed', coverage, type(exc).__name__)
            except Exception:
                pass
            raise


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('kind', choices=['refresh','weekly','bootstrap','health'])
    parser.add_argument('--season', type=int)
    parser.add_argument('--grace-hours', type=float, default=48)
    parser.add_argument('--min-snaps', type=int, default=100)
    parser.add_argument('--shrink-tau', type=float, default=200)
    parser.add_argument('--exclusions', help='JSON game IDs mapped to justified cancellation reasons')
    parser.add_argument('--storage-budget-mb', type=int, default=850)
    args = parser.parse_args(argv)
    latest = current_season()
    args.season = args.season or latest
    if not FIRST_SEASON <= args.season <= latest or min(args.grace_hours,args.min_snaps,args.shrink_tau)<0 or args.storage_budget_mb<1:
        parser.error('Invalid season, calculation parameter or storage budget')
    if args.kind == 'bootstrap' and args.season >= latest:
        parser.error('Bootstrap accepts completed prior seasons only')
    return args


def main(argv=None):
    load_dotenv()
    args = parse_args(argv)
    exclusions = json.loads(Path(args.exclusions).read_text()) if args.exclusions else {}
    if not isinstance(exclusions, dict):
        raise ValueError('Exclusions must map game IDs to reasons')
    # One bounded task; external executors retry at most twice. A deferred run
    # exits nonzero so its completion is visible to the executor and monitoring.
    try:
        result = execute(CloudStore(get_supabase_client(), storage_limit=args.storage_budget_mb*1_000_000),
                         args.kind, args.season, grace_hours=args.grace_hours, min_snaps=args.min_snaps,
                         shrink_tau=args.shrink_tau, exclusions=exclusions)
        print(json.dumps(result))
        return 75 if result['status']=='deferred' else 0
    except PipelineBusy:
        print(json.dumps(dict(status='busy', error_type='PipelineBusy')))
        return 75
    except SchemaNotReady as exc:
        # This message is authored locally; it contains no transport details.
        print(json.dumps(dict(status='failed', error_type='SchemaNotReady', action=str(exc))))
        return 1
    except Exception as exc:
        result = dict(status='failed', error_type=type(exc).__name__)
        if isinstance(exc, APIError) and re.fullmatch(r'(?:[0-9A-Z]{5}|PGRST[0-9]{3})', str(exc.code)):
            # SQLSTATE/PostgREST codes are bounded identifiers, not messages.
            result['database_code'] = exc.code
        print(json.dumps(result))
        return 1


if __name__ == '__main__':
    raise SystemExit(main())
