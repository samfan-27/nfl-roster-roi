"""Run SQL behavior checks against an isolated postgres:16 Docker container.

Usage: python scripts/test_migration.py [container-name]
The target must be disposable. Never supply a production connection here.
"""
from pathlib import Path
import subprocess
import sys
import json
import uuid
import time

container=sys.argv[1] if len(sys.argv)>1 else 'nfl-roi-migration-test'
def sql(statement,ok=True):
    result=subprocess.run(['docker','exec','-i',container,'psql','-U','postgres','-v','ON_ERROR_STOP=1','-At'],
                          input=statement,text=True,capture_output=True)
    if ok and result.returncode:
        raise RuntimeError(result.stderr)
    if not ok and result.returncode==0:
        raise AssertionError('Expected database rejection')
    return result.stdout.strip()

for attempt in range(40):
    ready=subprocess.run(['docker','exec',container,'pg_isready','-U','postgres'],capture_output=True)
    if ready.returncode==0:break
    time.sleep(.5)
else:raise RuntimeError('Test database did not start')

sql('create role anon;create role authenticated;create role service_role bypassrls;create schema storage;create table storage.buckets(id text primary key,name text,public boolean,file_size_limit bigint);')
sql('grant usage on schema public to public;')
sql(Path('infra/supabase/ddl.sql').read_text())
sql(Path('infra/supabase/migrations/20261001_cloud_pipeline.sql').read_text())
owner,other=str(uuid.uuid4()),str(uuid.uuid4())
assert sql(f"select acquire_pipeline_lock('{owner}')")=='t'
assert sql(f"select acquire_pipeline_lock('{other}')")=='f'
sql(f"select assert_pipeline_lock('{other}')",ok=False)

def run(kind='refresh'):
    r=str(uuid.uuid4())
    sql(f"insert into pipeline_runs(id,kind,season,status,code_revision) values('{r}','{kind}',2026,'running','test')")
    return r

coverage=json.dumps(dict(shared_games=['g1'],cutoff_week=1,complete_season=False))
r=run()
payload=json.dumps(dict(season=2026,gsis_id='p1',player_name='Test',yearly_cap_hit=1,snaps=100,total_epa=5))
sql(f"insert into roster_roi_stage values('{r}',2026,'p1','{payload}')")
# Failed commit cannot mutate dashboard or latest-success pointer.
sql(f"select commit_ingestion('{owner}','{r}',2026,'{coverage}','{{}}',false,2)",ok=False)
assert sql('select count(*) from roster_roi')=='0'
assert sql('select count(*) from ingestion_heads')=='0'
sql(f"select commit_ingestion('{owner}','{r}',2026,'{coverage}','{{}}',false,1)")
assert sql('select count(*) from roster_roi')=='1'
assert sql('select last_row_count from pipeline_meta')=='1'
assert sql(f"select pipeline_run_event('{owner}','{r}','refresh',2026,'test','running',null,null)")=='f'
# Reuse existing row id on next upsert; retain omitted player-season rows.
r2=run()
sql(f"insert into roster_roi_stage values('{r2}',2026,'p1','{payload}')")
sql(f"select commit_ingestion('{owner}','{r2}',2026,'{coverage}','{{}}',false,1)")
assert sql('select count(*) from roster_roi')=='1'

r3=run()
sql(f"insert into roster_roi_stage values('{r3}',2026,'p1','{payload}')")
regressed=json.dumps(dict(shared_games=[],cutoff_week=0,complete_season=False))
sql(f"select commit_ingestion('{owner}','{r3}',2026,'{regressed}','{{}}',false,1)",ok=False)
sql(f"select commit_ingestion('{owner}','{r3}',2026,'{coverage}','{{}}',true,1)",ok=False)
assert sql('select run_id from ingestion_heads')==r2
assert sql('select count(*) from history_heads')=='0'

report='A validated weekly report. '*10
w1=run('weekly')
def publish(r,week,fingerprint):
    cov=json.dumps(dict(shared_games=['g1'],cutoff_week=week,complete_season=False))
    return f"select publish_analysis('{owner}','{r}',2026,{week},'{fingerprint}','{cov}','{{}}','{report}','[]','[]','[]','[{{\"position\":\"QB\"}}]')"
assert sql(publish(w1,1,'version1'))==w1
w2=run('weekly');assert sql(publish(w2,1,'correction'))==w2
assert sql('select run_id from analysis_heads')==w2
# Duplicate version1 retry does not revert correction.
w3=run('weekly');assert sql(publish(w3,1,'version1'))==w1
assert sql('select run_id from analysis_heads')==w2
assert sql('select count(*) from analysis_publications')=='2'
# Fail a publication and retain prior result.
w4=run('weekly')
sql(publish(w4,0,'invalid'),ok=False)
assert sql('select run_id from analysis_heads')==w2
# Public client can read only completed publications; no source/private table/RPC.
assert sql('set role anon;select count(*) from analysis_publications')=='SET\n2'
sql('set role anon;select * from pipeline_runs',ok=False)
sql(f"set role anon;select acquire_pipeline_lock('{other}')",ok=False)
# Expired owner cannot commit; a new owner can acquire the lock.
sql("update pipeline_locks set expires_at=now()-interval '1 second'")
assert sql(f"select acquire_pipeline_lock('{other}')")=='t'
sql(f"select assert_pipeline_lock('{owner}')",ok=False)
sql(f"select stage_pipeline_metrics('{owner}','{r3}','[]',true)",ok=False)
sql(f"select release_pipeline_lock('{owner}')")
assert sql('select owner from pipeline_locks')==other
print('Migration verified: atomic ingestion/publication, correction versions, duplicate retry, anon boundaries and fenced lease.')
