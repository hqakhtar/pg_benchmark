-- hammerdb_maintenance_citus.sql
-- "No data removal" maintenance for HammerDB runs on Citus/PostgreSQL.
-- - DOES NOT drop any tables/views/data
-- - Resets stats (pg_stat_reset) and resets pg_stat_statements IF installed
-- - VACUUM (ANALYZE) all tables in public (table-by-table)
-- - Best-effort CHECKPOINT
-- Run on the COORDINATOR (psql -d postgres -f hammerdb_maintenance_citus.sql)

\set ON_ERROR_STOP on
\timing on

SELECT current_database() AS db, current_schema() AS schema;
SET search_path = public;

-- Best-effort: enable coordinator->worker DDL propagation (not strictly needed here)
DO $$
BEGIN
  BEGIN
    EXECUTE 'SET citus.enable_ddl_propagation = on';
  EXCEPTION
    WHEN undefined_object THEN
      RAISE NOTICE 'Skipping SET citus.enable_ddl_propagation: not a Citus GUC here';
    WHEN insufficient_privilege THEN
      RAISE NOTICE 'Skipping SET citus.enable_ddl_propagation: insufficient privilege';
  END;
END $$;

-- ------------------------------------------------------------
-- A) Reset stats (best-effort)
-- ------------------------------------------------------------
DO $$
DECLARE
  has_pgs boolean;
  reset_oid oid;
BEGIN
  -- Per-db stats reset
  BEGIN
    PERFORM pg_stat_reset();
    RAISE NOTICE 'pg_stat_reset() executed';
  EXCEPTION
    WHEN insufficient_privilege THEN
      RAISE NOTICE 'Skipping pg_stat_reset(): insufficient privilege';
  END;

  -- pg_stat_statements reset if installed
  SELECT EXISTS (SELECT 1 FROM pg_extension WHERE extname = 'pg_stat_statements')
    INTO has_pgs;

  IF NOT has_pgs THEN
    RAISE NOTICE 'pg_stat_statements not installed; skipping reset';
  ELSE
    SELECT to_regproc('pg_stat_statements_reset()') INTO reset_oid;

    IF reset_oid IS NULL THEN
      RAISE NOTICE 'pg_stat_statements installed but pg_stat_statements_reset() not found; skipping';
    ELSE
      BEGIN
        PERFORM pg_stat_statements_reset();
        RAISE NOTICE 'pg_stat_statements_reset() executed';
      EXCEPTION
        WHEN insufficient_privilege THEN
          RAISE NOTICE 'Skipping pg_stat_statements_reset(): insufficient privilege';
        WHEN others THEN
          RAISE NOTICE 'pg_stat_statements_reset() failed: %', SQLERRM;
      END;
    END IF;
  END IF;

  -- Shared stats (often superuser-only; safe to skip)
  BEGIN
    PERFORM pg_stat_reset_shared('bgwriter');
  EXCEPTION WHEN insufficient_privilege THEN
    RAISE NOTICE 'Skipping pg_stat_reset_shared(bgwriter): insufficient privilege';
  END;

  BEGIN
    PERFORM pg_stat_reset_shared('archiver');
  EXCEPTION WHEN insufficient_privilege THEN
    RAISE NOTICE 'Skipping pg_stat_reset_shared(archiver): insufficient privilege';
  END;

  -- SLRU stats (often blocked on managed services)
  BEGIN
    PERFORM pg_stat_reset_slru('all');
  EXCEPTION
    WHEN undefined_function THEN
      RAISE NOTICE 'Skipping pg_stat_reset_slru(all): function not available on this version';
    WHEN insufficient_privilege THEN
      RAISE NOTICE 'Skipping pg_stat_reset_slru(all): insufficient privilege';
  END;
END $$;

-- ------------------------------------------------------------
-- B) VACUUM / ANALYZE (does not remove data)
-- ------------------------------------------------------------
DO $$
DECLARE r record;
BEGIN
  FOR r IN
    SELECT n.nspname AS schemaname, c.relname AS relname
    FROM pg_class c
    JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE n.nspname = 'public'
      AND c.relkind IN ('r','p')  -- ordinary + partitioned tables
  LOOP
    BEGIN
      EXECUTE format('VACUUM (ANALYZE) %I.%I', r.schemaname, r.relname);
    EXCEPTION
      WHEN insufficient_privilege THEN
        RAISE NOTICE 'Skipping VACUUM on %.%: insufficient privilege', r.schemaname, r.relname;
      WHEN others THEN
        RAISE NOTICE 'Skipping VACUUM on %.%: %', r.schemaname, r.relname, SQLERRM;
    END;
  END LOOP;
END $$;

-- Optional: update planner stats more broadly (no data changes)
-- ANALYZE;

-- ------------------------------------------------------------
-- C) Best-effort CHECKPOINT (may be blocked on managed services)
-- ------------------------------------------------------------
DO $$
BEGIN
  BEGIN
    CHECKPOINT;
    RAISE NOTICE 'CHECKPOINT executed';
  EXCEPTION WHEN insufficient_privilege THEN
    RAISE NOTICE 'Skipping CHECKPOINT: insufficient privilege';
  END;
END $$;

-- ------------------------------------------------------------
-- D) Optional: show top tables by size (handy after runs)
-- ------------------------------------------------------------
SELECT
  n.nspname AS schema,
  c.relname AS table,
  pg_size_pretty(pg_total_relation_size(c.oid)) AS total_size
FROM pg_class c
JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE n.nspname = 'public'
  AND c.relkind IN ('r','p')
ORDER BY pg_total_relation_size(c.oid) DESC
LIMIT 20;

