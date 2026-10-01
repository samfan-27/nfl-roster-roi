-- Protect historical initialization as well as current ingestion from game loss.
-- Apply after 20261001_cloud_pipeline.sql. This does not rewrite existing data.
begin;
create function public.reject_snapshot_regression() returns trigger
 language plpgsql security definer set search_path=public,pg_temp as $$
begin
 if exists(select 1 from season_snapshots s where s.season=new.season
           and not ((new.coverage->'shared_games') @> (s.coverage->'shared_games'))) then
  raise exception 'Snapshot coverage regression';
 end if;
 return new;
end $$;
revoke all on function public.reject_snapshot_regression() from public,anon,authenticated;
create trigger preserve_snapshot_games before insert on public.season_snapshots
 for each row execute function public.reject_snapshot_regression();
commit;
