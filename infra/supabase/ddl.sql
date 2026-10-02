-- Idempotent setup for a new database. Existing deployments already have
-- these additional columns and the player-season uniqueness constraint.
create table if not exists public.roster_roi (
  id uuid primary key default gen_random_uuid(),
  season integer not null,
  player_name text not null,
  gsis_id text,
  otc_id text,
  team text,
  position text,
  age numeric,
  years_exp integer,
  is_rookie_deal boolean,
  yearly_cap_hit numeric(12,2) not null,
  cap_pct_of_team numeric(6,4),
  passing_epa numeric(10,3),
  rushing_epa numeric(10,3),
  receiving_epa numeric(10,3),
  total_epa numeric(10,3),
  snaps integer,
  epa_per_snap numeric(12,6),
  cost_per_epa numeric(12,6),
  cost_per_epa_per_100_snaps numeric(12,6),
  epai_lower numeric(10,3),
  epai_upper numeric(10,3),
  sample_flag text,
  notes text,
  updated_at timestamptz default now()
);

alter table public.roster_roi add column if not exists age numeric;
alter table public.roster_roi add column if not exists years_exp integer;
alter table public.roster_roi add column if not exists is_rookie_deal boolean;
create unique index if not exists idx_roster_roi_season_gsis
  on public.roster_roi (season, gsis_id);
create index if not exists idx_roster_roi_season_position
  on public.roster_roi (season, position);
create index if not exists idx_roster_roi_team on public.roster_roi (team);

alter table public.roster_roi enable row level security;
DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_policies WHERE schemaname = 'public'
      AND tablename = 'roster_roi' AND policyname = 'allow_public_select'
  ) THEN
    CREATE POLICY allow_public_select ON public.roster_roi
      FOR SELECT USING (true);
  END IF;
END
$$ LANGUAGE plpgsql;

create table if not exists public.pipeline_meta (
  id integer primary key default 1,
  last_run timestamptz,
  last_row_count integer,
  last_status text,
  last_message text
);
insert into public.pipeline_meta (id, last_row_count, last_status)
  values (1, 0, 'initialized') on conflict (id) do nothing;

alter table public.pipeline_meta enable row level security;
DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_policies WHERE schemaname = 'public'
      AND tablename = 'pipeline_meta' AND policyname = 'allow_public_select'
  ) THEN
    CREATE POLICY allow_public_select ON public.pipeline_meta
      FOR SELECT USING (true);
  END IF;
END
$$ LANGUAGE plpgsql;

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
