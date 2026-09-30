-- Dashboard freshness metadata only. Run in the project's Supabase SQL Editor.
-- No roster data, credentials, or write permissions are exposed.
grant select on public.pipeline_meta to anon, authenticated;
alter table public.pipeline_meta enable row level security;
DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_policies WHERE schemaname = 'public'
      AND tablename = 'pipeline_meta' AND policyname = 'allow_public_select'
  ) THEN
    CREATE POLICY allow_public_select ON public.pipeline_meta
      FOR SELECT TO anon, authenticated USING (true);
  END IF;
END
$$ LANGUAGE plpgsql;
