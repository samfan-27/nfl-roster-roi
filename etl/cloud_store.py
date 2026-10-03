"""Private immutable blobs, paginated metadata and fenced transactional writes."""
from contextlib import contextmanager
from datetime import datetime, timezone
from hashlib import sha256
from io import BytesIO
import json
import uuid
import pandas as pd
from postgrest.exceptions import APIError


METHODOLOGY_MIGRATION = 'infra/supabase/migrations/20261002_apy_methodology.sql'
METHODOLOGY_COLUMNS = (
    'contract_apy_m', 'season_cap_charge_m', 'season_cash_m', 'offensive_snaps',
    'contract_identity_status', 'pfr_identity_status',
)


class SchemaNotReady(RuntimeError):
    def __init__(self):
        super().__init__(f'Apply {METHODOLOGY_MIGRATION} before cloud ingestion or analysis.')


class PipelineBusy(RuntimeError):
    pass


def records(frame):
    return json.loads(frame.to_json(orient='records', date_format='iso'))


def read_pages(query, page_size=500):
    if not 1 <= page_size <= 1000:
        raise ValueError('Page size must be between 1 and 1000')
    rows, start = [], 0
    while True:
        data = query.range(start, start + page_size - 1).execute().data
        if data is None:
            raise RuntimeError('Database returned no page data')
        rows.extend(data)
        if len(data) < page_size:
            return rows
        start += page_size


def utcnow():
    return datetime.now(timezone.utc).isoformat()


class CloudStore:
    def __init__(self, client, bucket='nfl-pipeline', storage_limit=850_000_000):
        self.client = client
        self.bucket = client.storage.from_(bucket)
        self.storage_limit = storage_limit
        self.owner = None

    def rpc(self, name, **params):
        return self.client.rpc(name, params).execute().data

    @contextmanager
    def lock(self):
        # An attempt has its own fencing token, even when retrying the same run.
        self.owner = str(uuid.uuid4())
        if not self.rpc('acquire_pipeline_lock', p_owner=self.owner):
            self.owner = None
            raise PipelineBusy('Another pipeline owns the cloud lease')
        try:
            yield self
        finally:
            try:
                self.rpc('release_pipeline_lock', p_owner=self.owner)
            finally:
                self.owner = None

    def assert_lock(self):
        self.rpc('assert_pipeline_lock', p_owner=self.owner)

    def require_methodology_schema(self):
        """Check the additive migration before downloading or archiving inputs.

        Selecting zero rows validates column availability without retrieving
        player data. Only missing-column errors become migration guidance;
        authentication, connectivity and other database failures retain their
        original exception type. Never expose transport exception messages.
        """
        try:
            self.client.table('roster_roi').select(','.join(METHODOLOGY_COLUMNS)).limit(0).execute()
        except APIError as exc:
            if exc.code in ('42703', 'PGRST204'):
                raise SchemaNotReady() from None
            raise

    def begin(self, run_id, kind, season, revision):
        self.active = (run_id, kind, season, revision)
        return self.rpc('pipeline_run_event', p_owner=self.owner, p_run=run_id,
                        p_kind=kind, p_season=season, p_revision=revision,
                        p_status='running', p_coverage=None, p_error=None)

    def finish(self, run_id, status, coverage=None, error_type=None):
        active_id, kind, season, revision = self.active
        if run_id != active_id:
            raise ValueError('Run identity mismatch')
        self.rpc('pipeline_run_event', p_owner=self.owner, p_run=run_id,
                 p_kind=kind, p_season=season, p_revision=revision,
                 p_status=status, p_coverage=coverage, p_error=error_type)

    def put(self, data, extension, content_type):
        self.assert_lock()
        digest = sha256(data).hexdigest()
        path = f'objects/{digest}.{extension}'
        old = self.client.table('artifact_objects').select('path,bytes').eq('path', path).execute().data
        if not old:
            # Reserve accounting BEFORE upload. Failed uploads conservatively
            # consume quota until an operator reconciles private object inventory.
            used = self.rpc('pipeline_storage_bytes')
            if used + len(data) > self.storage_limit:
                raise RuntimeError('Private storage budget exceeded; publication retained')
            self.client.table('artifact_objects').insert(dict(path=path, bytes=len(data), sha256=digest)).execute()
        try:
            existing = self.bucket.download(path) if old else None
        except Exception:
            existing = None
        if existing is None:
            # No overwrite, ever. Retry first verifies the existing immutable blob.
            self.bucket.upload(path, data, file_options={'content-type':content_type, 'upsert':'false'})
            existing = self.bucket.download(path)
        if sha256(existing).hexdigest() != digest:
            raise RuntimeError('Cloud artifact hash mismatch')
        return dict(path=path, sha256=digest, bytes=len(data))

    def put_frame(self, frame):
        stream = BytesIO()
        frame.to_parquet(stream, index=False, compression='zstd')
        return self.put(stream.getvalue(), 'parquet', 'application/octet-stream')

    def put_json(self, value):
        return self.put(json.dumps(value, sort_keys=True, allow_nan=False).encode(), 'json', 'application/json')

    def get(self, descriptor):
        data = self.bucket.download(descriptor['path'])
        if len(data) != descriptor['bytes'] or sha256(data).hexdigest() != descriptor['sha256']:
            raise RuntimeError('Cloud input hash mismatch')
        return data

    def frame(self, descriptor):
        return pd.read_parquet(BytesIO(self.get(descriptor)))

    def snapshot(self, run_id):
        rows = self.client.table('season_snapshots').select('*').eq('run_id', run_id).execute().data
        if len(rows) != 1:
            raise RuntimeError('Canonical snapshot is missing')
        return rows[0]

    def previous_games(self, season):
        games = set()
        for table in ('ingestion_heads', 'history_heads'):
            rows = self.client.table(table).select('run_id').eq('season', season).execute().data
            if rows:
                games.update(self.snapshot(rows[0]['run_id'])['coverage']['shared_games'])
        return sorted(games)

    def histories(self, current):
        heads = read_pages(self.client.table('history_heads').select('*').lt('season', current).order('season'))
        if not heads or current - 1 not in {r['season'] for r in heads}:
            raise ValueError('Validated cloud history, including the previous season, must be initialized')
        result = []
        for head in heads:
            snapshot = self.snapshot(head['run_id'])
            if not snapshot['complete_season'] or not snapshot['coverage']['complete_season']:
                raise ValueError('Cloud historical snapshot is incomplete')
            if snapshot['season'] != head['season']:
                raise ValueError('Cloud historical season pointer mismatch')
            metrics = self.frame(snapshot['manifest']['artifacts']['metrics'])
            validate_metrics(metrics, head['season'])
            result.append((snapshot, metrics))
        return result

    def ingest(self, run_id, season, metrics, coverage, manifest, historical=False):
        validate_metrics(metrics, season)
        payloads = records(metrics)
        for start in range(0, len(payloads), 200):
            self.rpc('stage_pipeline_metrics', p_owner=self.owner, p_run=run_id,
                     p_rows=payloads[start:start+200], p_reset=start==0)
        self.rpc('commit_ingestion', p_owner=self.owner, p_run=run_id, p_season=season,
                 p_count=len(metrics), p_coverage=coverage, p_manifest=manifest, p_history=historical)

    def publish(self, run_id, season, coverage, fingerprint, manifest, report, outputs):
        return self.rpc('publish_analysis', p_owner=self.owner, p_run=run_id, p_season=season,
                        p_week=coverage['cutoff_week'], p_fingerprint=fingerprint,
                        p_manifest=manifest, p_coverage=coverage, p_report=report,
                        p_comparison=records(outputs['comparison']), p_summary=records(outputs['summary']),
                        p_candidates=records(outputs['candidates']), p_diagnostics=records(outputs['diagnostics']))


def validate_metrics(frame, season):
    required = {'season','gsis_id','snaps','total_epa','yearly_cap_hit','position','notes'}
    if required - set(frame) or frame.empty or frame.gsis_id.isna().any() or frame.duplicated(['season','gsis_id']).any():
        raise ValueError('Empty, duplicate or invalid cloud metrics')
    if not frame.season.eq(season).all():
        raise ValueError('Cloud metrics contain an unexpected season')
