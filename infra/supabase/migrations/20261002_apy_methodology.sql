-- Explicit financial definitions and offensive exposure; apply before refreshing.
-- This migration does not synthesize old annual costs or historical contract dates.
alter table public.roster_roi alter column yearly_cap_hit drop not null;
alter table public.roster_roi alter column yearly_cap_hit type numeric(16,6);
alter table public.roster_roi alter column passing_epa type numeric(18,9);
alter table public.roster_roi alter column rushing_epa type numeric(18,9);
alter table public.roster_roi alter column receiving_epa type numeric(18,9);
alter table public.roster_roi alter column total_epa type numeric(18,9);
alter table public.roster_roi add column if not exists contract_apy_m numeric;
alter table public.roster_roi add column if not exists contract_identity_status text;
alter table public.roster_roi add column if not exists pfr_identity_status text;
alter table public.roster_roi add column if not exists production_team_count integer;
alter table public.roster_roi add column if not exists contract_year_signed integer;
alter table public.roster_roi add column if not exists contract_years numeric;
alter table public.roster_roi add column if not exists contract_type text;
alter table public.roster_roi add column if not exists contract_selection_status text;
alter table public.roster_roi add column if not exists season_cap_charge_m numeric;
alter table public.roster_roi add column if not exists season_cash_m numeric;
alter table public.roster_roi add column if not exists season_financial_team text;
alter table public.roster_roi add column if not exists annual_financial_status text;
alter table public.roster_roi add column if not exists annual_entry_count integer;
alter table public.roster_roi add column if not exists contract_signed_date date;
alter table public.roster_roi add column if not exists contract_effective_date date;
alter table public.roster_roi add column if not exists contract_transaction_date date;
alter table public.roster_roi add column if not exists contract_snapshot_at timestamptz;
alter table public.roster_roi add column if not exists contract_source_url text;
alter table public.roster_roi add column if not exists contract_apy_convention text;
alter table public.roster_roi add column if not exists games_played integer;
alter table public.roster_roi add column if not exists attempts integer;
alter table public.roster_roi add column if not exists sacks_suffered integer;
alter table public.roster_roi add column if not exists carries integer;
alter table public.roster_roi add column if not exists targets integer;
alter table public.roster_roi add column if not exists pass_opportunities integer;
alter table public.roster_roi add column if not exists offensive_snaps integer;
alter table public.roster_roi add column if not exists passing_cpoe_game_mean numeric;
alter table public.roster_roi add column if not exists target_share_game_mean numeric;
alter table public.roster_roi add column if not exists air_yards_share_game_mean numeric;
alter table public.roster_roi add column if not exists cap_cost_per_epa numeric;
alter table public.roster_roi add column if not exists cash_cost_per_epa numeric;
alter table public.roster_roi add column if not exists through_week integer;
alter table public.roster_roi add column if not exists shared_games integer;

-- Preserve atomic/fenced updates while refreshing every new field.
create or replace function public.commit_ingestion(p_owner uuid,p_run uuid,p_season integer,
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
   notes=excluded.notes,updated_at=excluded.updated_at,
   contract_apy_m=excluded.contract_apy_m,
   contract_identity_status=excluded.contract_identity_status,
   pfr_identity_status=excluded.pfr_identity_status,
   production_team_count=excluded.production_team_count,
   contract_year_signed=excluded.contract_year_signed,
   contract_years=excluded.contract_years,
   contract_type=excluded.contract_type,
   contract_selection_status=excluded.contract_selection_status,
   season_cap_charge_m=excluded.season_cap_charge_m,
   season_cash_m=excluded.season_cash_m,
   season_financial_team=excluded.season_financial_team,
   annual_financial_status=excluded.annual_financial_status,
   annual_entry_count=excluded.annual_entry_count,
   contract_signed_date=excluded.contract_signed_date,
   contract_effective_date=excluded.contract_effective_date,
   contract_transaction_date=excluded.contract_transaction_date,
   contract_snapshot_at=excluded.contract_snapshot_at,
   contract_source_url=excluded.contract_source_url,
   contract_apy_convention=excluded.contract_apy_convention,
   games_played=excluded.games_played,
   attempts=excluded.attempts,
   sacks_suffered=excluded.sacks_suffered,
   carries=excluded.carries,
   targets=excluded.targets,
   pass_opportunities=excluded.pass_opportunities,
   offensive_snaps=excluded.offensive_snaps,
   passing_cpoe_game_mean=excluded.passing_cpoe_game_mean,
   target_share_game_mean=excluded.target_share_game_mean,
   air_yards_share_game_mean=excluded.air_yards_share_game_mean,
   cap_cost_per_epa=excluded.cap_cost_per_epa,
   cash_cost_per_epa=excluded.cash_cost_per_epa,
   through_week=excluded.through_week,
   shared_games=excluded.shared_games;
  insert into ingestion_heads values(p_season,p_run) on conflict(season) do update set run_id=excluded.run_id;
  update pipeline_meta set last_run=now(),last_status='success',last_row_count=p_count,
   last_message=format('%s: schedule validated; through complete week %s; %s shared games',p_season,p_coverage->>'cutoff_week',jsonb_array_length(p_coverage->'shared_games')) where id=1;
 end if;
 update pipeline_runs set status='succeeded',finished_at=now(),coverage=p_coverage,manifest=p_manifest where id=p_run;
 delete from roster_roi_stage where run_id=p_run;
end $$;
