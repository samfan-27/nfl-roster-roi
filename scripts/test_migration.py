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
    result=subprocess.run(['docker','exec','-i',container,'psql','-h','127.0.0.1','-U','postgres','-v','ON_ERROR_STOP=1','-At'],
                          input=statement,text=True,capture_output=True)
    if ok and result.returncode:
        raise RuntimeError(result.stderr)
    if not ok and result.returncode==0:
        raise AssertionError('Expected database rejection')
    return result.stdout.strip()

# The entrypoint temporarily starts a Unix-socket-only server during initdb.
# Wait on TCP so that only the final server can satisfy readiness.
for attempt in range(40):
    ready=subprocess.run(['docker','exec',container,'pg_isready','-h','127.0.0.1','-U','postgres'],capture_output=True)
    if ready.returncode==0:break
    time.sleep(.5)
else:raise RuntimeError('Test database did not start')

sql('create role anon;create role authenticated;create role service_role bypassrls;create schema storage;create table storage.buckets(id text primary key,name text,public boolean,file_size_limit bigint);')
sql('grant usage on schema public to public;')
# Start from the actual legacy bootstrap, without its appended v2 alterations.
# This verifies an upgrade of the old NOT NULL/precision schema, not only a
# migration applied to an already-updated empty database.
ddl=Path('infra/supabase/ddl.sql').read_text()
legacy_ddl,separator,_=ddl.partition('-- Explicit financial definitions and offensive exposure; apply before refreshing.')
assert separator, 'Legacy bootstrap boundary is missing'
sql(legacy_ddl)
sql("insert into roster_roi(season,gsis_id,player_name,yearly_cap_hit) values(2025,'legacy','Existing player',20.01)")
sql("insert into roster_roi(season,gsis_id,player_name,yearly_cap_hit) values(2025,'unknown','Unknown price',null)",ok=False)
sql(Path('infra/supabase/migrations/20261001_cloud_pipeline.sql').read_text())
sql(Path('infra/supabase/migrations/20261001_snapshot_regression.sql').read_text())
sql(Path('infra/supabase/migrations/20261002_apy_methodology.sql').read_text())
sql(Path('infra/supabase/migrations/20261002_apy_methodology.sql').read_text())
sql(Path('infra/supabase/migrations/20261002_single_pass_ingestion.sql').read_text())
sql(Path('infra/supabase/migrations/20261002_single_pass_ingestion.sql').read_text())
sql("select (jsonb_populate_record(null::roster_roi,'{\"contract_year_signed\":2026.0}'::jsonb)).contract_year_signed",ok=False)
assert sql("select (jsonb_populate_record(null::roster_roi,'{\"contract_year_signed\":2026}'::jsonb)).contract_year_signed")=='2026'
assert sql("select yearly_cap_hit from roster_roi where gsis_id='legacy'")=='20.010000'
assert sql("select contract_apy_m is null from roster_roi where gsis_id='legacy'")=='t'
# Remove only the setup fixture in this disposable test database.
sql("delete from roster_roi where gsis_id='legacy'")
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
stored_id=sql("select id from roster_roi where gsis_id='p1'")
assert sql(f"select pipeline_run_event('{owner}','{r}','refresh',2026,'test','running',null,null)")=='f'
# Reuse existing row id on next upsert; retain omitted player-season rows.
r2=run()
sql(f"insert into roster_roi_stage values('{r2}',2026,'p1','{payload}')")
sql(f"select commit_ingestion('{owner}','{r2}',2026,'{coverage}','{{}}',false,1)")
assert sql('select count(*) from roster_roi')=='1'
assert sql("select id from roster_roi where gsis_id='p1'")==stored_id

r3=run()
sql(f"insert into roster_roi_stage values('{r3}',2026,'p1','{payload}')")
regressed=json.dumps(dict(shared_games=[],cutoff_week=0,complete_season=False))
sql(f"select commit_ingestion('{owner}','{r3}',2026,'{regressed}','{{}}',false,1)",ok=False)
sql(f"select commit_ingestion('{owner}','{r3}',2026,'{coverage}','{{}}',true,1)",ok=False)
assert sql('select run_id from ingestion_heads')==r2
assert sql('select count(*) from history_heads')=='0'

# Unknown APY is a missing value, not zero; new fields update atomically.
r4=run()
unknown=json.dumps(dict(season=2026,gsis_id='p1',player_name='Test',yearly_cap_hit=None,
    contract_apy_m=None,season_cap_charge_m=3.722066,season_cash_m=3.674,
    contract_year_signed=2026,annual_entry_count=1,attempts=153,sacks_suffered=13,
    contract_type='Drafted',contract_identity_status='master_otc_identity',
    pfr_identity_status='master_mapping',production_team_count=1,snaps=100,total_epa=5))
sql(f"insert into roster_roi_stage values('{r4}',2026,'p1','{unknown}')")
sql(f"select commit_ingestion('{owner}','{r4}',2026,'{coverage}','{{}}',false,1)")
assert sql('select yearly_cap_hit is null from roster_roi')=='t'
assert sql('select season_cap_charge_m from roster_roi')=='3.722066'
assert sql('select season_cash_m from roster_roi')=='3.674'
assert sql('select contract_type from roster_roi')=='Drafted'
assert sql('select contract_year_signed from roster_roi')=='2026'
assert sql('select annual_entry_count from roster_roi')=='1'
assert sql('select attempts from roster_roi')=='153'

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
# The trigger protects canonical history even without an ingestion head.
h1,h2=str(uuid.uuid4()),str(uuid.uuid4())
for key in (h1,h2):
    sql(f"insert into pipeline_runs(id,kind,season,status,code_revision) values('{key}','bootstrap',2025,'running','test')")
hcov=json.dumps(dict(shared_games=['h1','h2'],cutoff_week=18,complete_season=True))
sql(f"insert into season_snapshots values('{h1}',2025,true,'{hcov}','{{}}')")
hlost=json.dumps(dict(shared_games=['h1'],cutoff_week=18,complete_season=True))
sql(f"insert into season_snapshots values('{h2}',2025,true,'{hlost}','{{}}')",ok=False)
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
