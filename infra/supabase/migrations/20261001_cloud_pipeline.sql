-- Additive migration. Run as database owner; never rerun the bootstrap DDL.
begin;
create table public.pipeline_runs (
  id uuid primary key, kind text not null check (kind in ('refresh','weekly','bootstrap','health')),
  season integer not null, status text not null check (status in ('running','succeeded','failed','deferred')),
  started_at timestamptz not null default now(), finished_at timestamptz,
  code_revision text not null, coverage jsonb, manifest jsonb, error_type text
);
create table public.pipeline_locks (
  name text primary key, owner uuid not null, expires_at timestamptz not null
);
create table public.season_snapshots (
  run_id uuid primary key references public.pipeline_runs(id), season integer not null,
  complete_season boolean not null, coverage jsonb not null, manifest jsonb not null
);
create table public.ingestion_heads (
  season integer primary key, run_id uuid not null references public.season_snapshots(run_id)
);
create table public.history_heads (
  season integer primary key, run_id uuid not null references public.season_snapshots(run_id)
);
create table public.analysis_publications (
  run_id uuid primary key references public.pipeline_runs(id), season integer not null,
  week integer not null check (week between 1 and 18), fingerprint text not null,
  published_at timestamptz not null default now(), coverage jsonb not null,
  report_markdown text not null, comparison jsonb not null, summary jsonb not null,
  candidates jsonb not null, diagnostics jsonb not null,
  unique(season, week, fingerprint)
);
create table public.analysis_heads (
  season integer primary key, run_id uuid not null references public.analysis_publications(run_id)
);
-- Narrow operational status: no source paths, secrets, exception messages or manifests.
create table public.analysis_status (
  season integer primary key, run_id uuid not null, status text not null,
  updated_at timestamptz not null default now(), cutoff_week integer, error_type text
);
create table public.pipeline_backups (
  id uuid primary key default gen_random_uuid(), created_at timestamptz not null default now(),
  manifest jsonb not null, row_count integer not null
);
alter table public.pipeline_backups enable row level security;
revoke all on public.pipeline_backups from anon,authenticated;
grant all on public.pipeline_backups to service_role;
create table public.artifact_objects (
  path text primary key, bytes bigint not null check (bytes>0), sha256 text not null,
  created_at timestamptz not null default now()
);
alter table public.artifact_objects enable row level security;
revoke all on public.artifact_objects from anon,authenticated;
grant all on public.artifact_objects to service_role;
create function public.pipeline_storage_bytes() returns bigint language sql
 security definer set search_path=public,pg_temp as $$ select coalesce(sum(bytes),0)::bigint from artifact_objects $$;
revoke all on function public.pipeline_storage_bytes() from public,anon,authenticated;
grant execute on function public.pipeline_storage_bytes() to service_role;
create table public.roster_roi_stage (
  run_id uuid not null references public.pipeline_runs(id), season integer not null,
  gsis_id text not null, payload jsonb not null, primary key(run_id,season,gsis_id)
);
create index on public.analysis_publications(season, week desc, published_at desc, run_id);

alter table public.pipeline_runs enable row level security;
alter table public.pipeline_locks enable row level security;
alter table public.season_snapshots enable row level security;
alter table public.ingestion_heads enable row level security;
alter table public.history_heads enable row level security;
alter table public.roster_roi_stage enable row level security;
alter table public.analysis_publications enable row level security;
alter table public.analysis_heads enable row level security;
alter table public.analysis_status enable row level security;
revoke all on public.pipeline_runs,public.pipeline_locks,public.season_snapshots,
  public.ingestion_heads,public.history_heads,public.roster_roi_stage,
  public.analysis_publications,public.analysis_heads,public.analysis_status from anon,authenticated;
grant all on public.pipeline_runs,public.pipeline_locks,public.season_snapshots,
  public.ingestion_heads,public.history_heads,public.roster_roi_stage,
  public.analysis_publications,public.analysis_heads,public.analysis_status to service_role;
grant select on public.analysis_publications,public.analysis_heads,public.analysis_status to anon,authenticated;
create policy published_reports on public.analysis_publications for select to anon,authenticated using (true);
create policy published_heads on public.analysis_heads for select to anon,authenticated using (true);
create policy report_status on public.analysis_status for select to anon,authenticated using (true);
insert into storage.buckets(id,name,public,file_size_limit)
 values ('nfl-pipeline','nfl-pipeline',false,104857600)
 on conflict(id) do nothing;
-- No anon/authenticated Storage policies. The app reads published report bodies
-- from Postgres; source snapshots, manifests, model outputs and backups stay private.

create function public.acquire_pipeline_lock(p_owner uuid) returns boolean
 language plpgsql security definer set search_path = public,pg_temp as $$
declare n integer;
begin
 insert into pipeline_locks(name,owner,expires_at) values('pipeline',p_owner,now()+interval '65 minutes')
 on conflict(name) do update set owner=excluded.owner,expires_at=excluded.expires_at
 where pipeline_locks.expires_at < now();
 get diagnostics n = row_count;
 return n=1;
end $$;
create function public.assert_pipeline_lock(p_owner uuid) returns void
 language plpgsql security definer set search_path = public,pg_temp as $$
begin
 perform 1 from pipeline_locks where name='pipeline' and owner=p_owner and expires_at>now() for update;
 if not found then raise exception 'Pipeline lease lost'; end if;
end $$;
create function public.release_pipeline_lock(p_owner uuid) returns void
 language sql security definer set search_path = public,pg_temp as $$
 delete from pipeline_locks where name='pipeline' and owner=p_owner
$$;

create function public.pipeline_run_event(p_owner uuid,p_run uuid,p_kind text,p_season integer,
 p_revision text,p_status text,p_coverage jsonb,p_error text) returns boolean
 language plpgsql security definer set search_path=public,pg_temp as $$
begin
 perform assert_pipeline_lock(p_owner);
 if exists(select 1 from pipeline_runs where id=p_run and status='succeeded') then return false; end if;
 if p_status='running' then
  insert into pipeline_runs(id,kind,season,status,code_revision) values(p_run,p_kind,p_season,'running',p_revision)
  on conflict(id) do update set status='running',started_at=now(),finished_at=null,error_type=null;
 else
  update pipeline_runs set status=p_status,finished_at=now(),coverage=p_coverage,error_type=p_error where id=p_run;
 end if;
 if p_kind='weekly' then
  insert into analysis_status values(p_season,p_run,p_status,now(),(p_coverage->>'cutoff_week')::integer,p_error)
  on conflict(season) do update set run_id=excluded.run_id,status=excluded.status,updated_at=excluded.updated_at,
   cutoff_week=excluded.cutoff_week,error_type=excluded.error_type;
 end if;
 return true;
end $$;
create function public.stage_pipeline_metrics(p_owner uuid,p_run uuid,p_rows jsonb,p_reset boolean)
 returns void language plpgsql security definer set search_path=public,pg_temp as $$
begin
 perform assert_pipeline_lock(p_owner);
 if p_reset then delete from roster_roi_stage where run_id=p_run; end if;
 insert into roster_roi_stage(run_id,season,gsis_id,payload)
 select p_run,(r->>'season')::integer,r->>'gsis_id',r from jsonb_array_elements(p_rows) r;
end $$;
revoke all on function public.pipeline_run_event(uuid,uuid,text,integer,text,text,jsonb,text),
 public.stage_pipeline_metrics(uuid,uuid,jsonb,boolean) from public,anon,authenticated;
grant execute on function public.pipeline_run_event(uuid,uuid,text,integer,text,text,jsonb,text),
 public.stage_pipeline_metrics(uuid,uuid,jsonb,boolean) to service_role;

-- All required objects are uploaded and hash-checked before this RPC. Dashboard
-- rows, coverage pointer and run success change together, or none of them do.
create function public.commit_ingestion(p_owner uuid,p_run uuid,p_season integer,
 p_coverage jsonb,p_manifest jsonb,p_history boolean,p_count integer) returns void
 language plpgsql security definer set search_path = public,pg_temp as $$
declare old_games jsonb;
begin
 perform assert_pipeline_lock(p_owner);
 if not exists(select 1 from pipeline_runs where id=p_run and status='running' and season=p_season) then
  raise exception 'Run is not active'; end if;
 if p_count<1 or (select count(*) from roster_roi_stage where run_id=p_run and season=p_season)<>p_count then
  raise exception 'Staged row count mismatch'; end if;
 select s.coverage->'shared_games' into old_games from ingestion_heads h join season_snapshots s on s.run_id=h.run_id where h.season=p_season;
 if old_games is not null and not (p_coverage->'shared_games' @> old_games) then raise exception 'Coverage regression'; end if;
 if p_history and not coalesce((p_coverage->>'complete_season')::boolean,false) then raise exception 'Incomplete history'; end if;
 insert into season_snapshots values(p_run,p_season,(p_coverage->>'complete_season')::boolean,p_coverage,p_manifest);
 if p_history then
  insert into history_heads values(p_season,p_run) on conflict(season) do update set run_id=excluded.run_id;
 else
  -- Match the existing player-season keys, retaining rows omitted by extraction.
  -- Payload columns are generated from the existing table type, preserving schema.
  insert into roster_roi
  select (jsonb_populate_record(null::roster_roi,payload || jsonb_build_object('id',coalesce(r.id,gen_random_uuid())))).*
  from roster_roi_stage t left join roster_roi r on r.season=t.season and r.gsis_id=t.gsis_id
  where t.run_id=p_run
  on conflict(season,gsis_id) do update set
   player_name=excluded.player_name,otc_id=excluded.otc_id,team=excluded.team,position=excluded.position,
   age=excluded.age,years_exp=excluded.years_exp,is_rookie_deal=excluded.is_rookie_deal,
   yearly_cap_hit=excluded.yearly_cap_hit,cap_pct_of_team=excluded.cap_pct_of_team,
   passing_epa=excluded.passing_epa,rushing_epa=excluded.rushing_epa,receiving_epa=excluded.receiving_epa,
   total_epa=excluded.total_epa,snaps=excluded.snaps,epa_per_snap=excluded.epa_per_snap,
   cost_per_epa=excluded.cost_per_epa,cost_per_epa_per_100_snaps=excluded.cost_per_epa_per_100_snaps,
   epai_lower=excluded.epai_lower,epai_upper=excluded.epai_upper,sample_flag=excluded.sample_flag,
   notes=excluded.notes,updated_at=excluded.updated_at;
  insert into ingestion_heads values(p_season,p_run) on conflict(season) do update set run_id=excluded.run_id;
  update pipeline_meta set last_run=now(),last_status='success',last_row_count=p_count,
   last_message=format('%s: schedule validated; through complete week %s; %s shared games',p_season,p_coverage->>'cutoff_week',jsonb_array_length(p_coverage->'shared_games')) where id=1;
 end if;
 update pipeline_runs set status='succeeded',finished_at=now(),coverage=p_coverage,manifest=p_manifest where id=p_run;
 delete from roster_roi_stage where run_id=p_run;
end $$;

create function public.publish_analysis(p_owner uuid,p_run uuid,p_season integer,p_week integer,
 p_fingerprint text,p_coverage jsonb,p_manifest jsonb,p_report text,p_comparison jsonb,
 p_summary jsonb,p_candidates jsonb,p_diagnostics jsonb) returns uuid
 language plpgsql security definer set search_path = public,pg_temp as $$
declare published uuid; old_week integer;
begin
 perform assert_pipeline_lock(p_owner);
 if not exists(select 1 from pipeline_runs where id=p_run and status='running' and kind='weekly' and season=p_season) then raise exception 'Run is not active'; end if;
 if p_week<1 or p_week<>(p_coverage->>'cutoff_week')::integer or length(p_report)<100 or jsonb_array_length(p_diagnostics)<1 then raise exception 'Invalid report'; end if;
 select a.week into old_week from analysis_heads h join analysis_publications a on h.run_id=a.run_id where h.season=p_season;
 if old_week>p_week then raise exception 'Weekly coverage regression'; end if;
 select run_id into published from analysis_publications where season=p_season and week=p_week and fingerprint=p_fingerprint;
 if published is null then
  published:=p_run;
  insert into analysis_publications(run_id,season,week,fingerprint,coverage,report_markdown,comparison,summary,candidates,diagnostics)
   values(p_run,p_season,p_week,p_fingerprint,p_coverage,p_report,p_comparison,p_summary,p_candidates,p_diagnostics);
 end if;
 -- A duplicate retry never rolls a newer correction back to an older version.
 if published=p_run or not exists(select 1 from analysis_heads where season=p_season) then
  insert into analysis_heads values(p_season,published) on conflict(season) do update set run_id=excluded.run_id;
 end if;
 update pipeline_runs set status='succeeded',finished_at=now(),coverage=p_coverage,manifest=p_manifest where id=p_run;
 insert into analysis_status values(p_season,p_run,'succeeded',now(),p_week,null)
  on conflict(season) do update set run_id=excluded.run_id,status=excluded.status,updated_at=excluded.updated_at,cutoff_week=excluded.cutoff_week,error_type=null;
 return published;
end $$;

revoke all on function public.acquire_pipeline_lock(uuid),public.assert_pipeline_lock(uuid),
 public.release_pipeline_lock(uuid),public.commit_ingestion(uuid,uuid,integer,jsonb,jsonb,boolean,integer),
 public.publish_analysis(uuid,uuid,integer,integer,text,jsonb,jsonb,text,jsonb,jsonb,jsonb,jsonb) from public,anon,authenticated;
grant execute on function public.acquire_pipeline_lock(uuid),public.assert_pipeline_lock(uuid),
 public.release_pipeline_lock(uuid),public.commit_ingestion(uuid,uuid,integer,jsonb,jsonb,boolean,integer),
 public.publish_analysis(uuid,uuid,integer,integer,text,jsonb,jsonb,text,jsonb,jsonb,jsonb,jsonb) to service_role;
commit;
