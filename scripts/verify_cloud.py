"""Read-only end-to-end checks of cloud publication and public access boundaries."""
import os
import sys
from pathlib import Path
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from dotenv import load_dotenv
from supabase import create_client
from etl.cloud_store import CloudStore
from etl.sources import current_season
from etl.database import get_supabase_client

load_dotenv()
client=get_supabase_client()
store=CloudStore(client)
season=current_season()
heads=client.table('analysis_heads').select('*').eq('season',season).execute().data
if not heads:raise RuntimeError('No published weekly report')
run_id=heads[0]['run_id']
run=client.table('pipeline_runs').select('*').eq('id',run_id).execute().data[0]
assert run['status']=='succeeded' and run['kind']=='weekly'
manifest=run['manifest']
assert store.get(manifest['manifest_object'])
report=store.get(manifest['artifacts']['report']).decode()
scored=store.frame(manifest['artifacts']['scored'])
assert not scored.empty and scored.season.lt(season).all()
public=create_client(os.environ['SUPABASE_URL'],os.environ['SUPABASE_ANON_KEY'])
versions=public.table('analysis_publications').select('run_id,week').eq('season',season).execute().data
for row in versions:
    result=public.table('analysis_publications').select('report_markdown,comparison').eq('run_id',row['run_id']).execute().data
    assert result and result[0]['report_markdown'] and result[0]['comparison']
latest=public.table('analysis_publications').select('report_markdown,week').eq('run_id',run_id).execute().data[0]
assert latest['report_markdown']==report
for table in ('pipeline_runs','season_snapshots','roster_roi_stage','artifact_objects','pipeline_backups'):
    try:
        response=public.table(table).select('*').limit(1).execute().data
    except Exception:
        continue
    if response:raise RuntimeError(f'Private table exposed: {table}')
try:
    public.storage.from_('nfl-pipeline').download(manifest['artifacts']['report']['path'])
except Exception:
    pass
else:
    raise RuntimeError('Private Storage object exposed to anonymous client')
print(dict(season=season,week=latest['week'],published_versions=len(versions),
           scored_seasons=sorted(scored.season.unique().tolist()),scored_rows=len(scored),
           archive_bytes=store.rpc('pipeline_storage_bytes'),public_reports='verified',private_sources='denied'))
