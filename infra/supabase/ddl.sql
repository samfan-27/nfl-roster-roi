create table if not exists public.roster_roi (
  id uuid primary key default gen_random_uuid(),
  season integer not null,
  player_name text not null,
  gsis_id text,
  otc_id text,
  team text,
  position text,
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

create index if not exists idx_roster_roi_season_position
on public.roster_roi (season, position);

create index if not exists idx_roster_roi_team
on public.roster_roi (team);

alter table public.roster_roi enable row level security;

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM pg_policies
    WHERE schemaname = 'public'
      AND tablename  = 'roster_roi'
      AND policyname = 'allow_public_select'
  ) THEN
    CREATE POLICY allow_public_select
      ON public.roster_roi
      FOR SELECT
      USING (true);
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

insert into public.pipeline_meta (id, last_run, last_row_count, last_status)
values (1, now(), 0, 'initialized')
on conflict (id)
do update set 
  last_run = excluded.last_run,
  last_status = excluded.last_status;
  