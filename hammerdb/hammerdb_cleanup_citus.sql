-- hammerdb_cleanup_citus.sql
-- Cleanup HammerDB objects from postgres DB, public schema, in a Citus-safe way.
-- - Drops known TPCC/TPROC-C + TPCH tables (CASCADE)
-- - Drops ONLY HammerDB-ish views/matviews (won't touch Citus extension views like citus_tables)
-- - Drops leftover sequences/functions by conservative patterns
-- - Resets pg_stat* and pg_stat_statements (only if installed + reset function exists)
-- - VACUUM (ANALYZE) public
-- - Best-effort CHECKPOINT (skips if not permitted)

\set ON_ERROR_STOP on
\timing on

-- Safety / visibility
SELECT current_database() AS db, current_schema() AS schema;

-- Target public schema explicitly
SET search_path = public;

-- Best-effort: enable coordinator->worker DDL propagation (skip if not present/allowed)
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

BEGIN;

-- ------------------------------------------------------------
-- A) Drop ONLY HammerDB-ish views/matviews (avoid extension views)
--    (Optional, but safe; table drops below would cascade anyway.)
-- ------------------------------------------------------------
DO $$
DECLARE
  obj text;
  r record;
BEGIN
  -- Materialized views
  FOR r IN
    SELECT schemaname, matviewname
    FROM pg_matviews
    WHERE schemaname = 'public'
      AND (
        matviewname ILIKE 'tpcc%' OR
        matviewname ILIKE 'tpch%' OR
        matviewname ILIKE 'hammerdb%' OR
        matviewname ILIKE '%neword%' OR
        matviewname ILIKE '%payment%' OR
        matviewname ILIKE '%delivery%' OR
        matviewname ILIKE '%ostat%' OR
        matviewname ILIKE '%slev%'
      )
  LOOP
    obj := format('%I.%I', r.schemaname, r.matviewname);
    BEGIN
      EXECUTE 'DROP MATERIALIZED VIEW IF EXISTS ' || obj || ' CASCADE';
    EXCEPTION
      WHEN insufficient_privilege THEN
        RAISE NOTICE 'Skipping drop of matview %: insufficient privilege', obj;
      WHEN others THEN
        RAISE NOTICE 'Skipping drop of matview %: %', obj, SQLERRM;
    END;
  END LOOP;

  -- Views
  FOR r IN
    SELECT schemaname, viewname
    FROM pg_views
    WHERE schemaname = 'public'
      AND (
        viewname ILIKE 'tpcc%' OR
        viewname ILIKE 'tpch%' OR
        viewname ILIKE 'hammerdb%' OR
        viewname ILIKE '%neword%' OR
        viewname ILIKE '%payment%' OR
        viewname ILIKE '%delivery%' OR
        viewname ILIKE '%ostat%' OR
        viewname ILIKE '%slev%'
      )
  LOOP
    obj := format('%I.%I', r.schemaname, r.viewname);
    BEGIN
      EXECUTE 'DROP VIEW IF EXISTS ' || obj || ' CASCADE';
    EXCEPTION
      WHEN insufficient_privilege THEN
        RAISE NOTICE 'Skipping drop of view %: insufficient privilege', obj;
      WHEN others THEN
        RAISE NOTICE 'Skipping drop of view %: %', obj, SQLERRM;
    END;
  END LOOP;
END $$;

-- ------------------------------------------------------------
-- B) Drop known HammerDB tables (TPROC-C / TPCC-like and TPCH)
--    In Citus: DROP TABLE on coordinator should drop shards too
--    when DDL propagation is enabled.
-- ------------------------------------------------------------
DO $$
DECLARE
  obj text;

  tpcc_tables text[] := ARRAY[
    'warehouse','district','customer','history','new_order',
    'oorder','order_line','item','stock'
  ];

  tpch_tables text[] := ARRAY[
    'region','nation','supplier','part','partsupp','customer','orders','lineitem'
  ];

BEGIN
  -- Drop TPCC tables
  FOREACH obj IN ARRAY tpcc_tables LOOP
    EXECUTE 'DROP TABLE IF EXISTS public.' || quote_ident(obj) || ' CASCADE';
  END LOOP;

  -- Drop TPCH tables
  FOREACH obj IN ARRAY tpch_tables LOOP
    EXECUTE 'DROP TABLE IF EXISTS public.' || quote_ident(obj) || ' CASCADE';
  END LOOP;
END $$;

-- ------------------------------------------------------------
-- C) Drop leftover HammerDB-ish objects by conservative patterns
--    (Only in public)
-- ------------------------------------------------------------

-- Sequences
DO $$
DECLARE r record;
BEGIN
  FOR r IN
    SELECT n.nspname AS schemaname, c.relname AS seqname
    FROM pg_class c
    JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE n.nspname = 'public'
      AND c.relkind = 'S'
      AND (
        c.relname ILIKE 'warehouse%' OR
        c.relname ILIKE 'district%' OR
        c.relname ILIKE 'customer%' OR
        c.relname ILIKE 'history%' OR
        c.relname ILIKE 'new_order%' OR
        c.relname ILIKE 'oorder%' OR
        c.relname ILIKE 'order_line%' OR
        c.relname ILIKE 'item%' OR
        c.relname ILIKE 'stock%' OR
        c.relname ILIKE 'region%' OR
        c.relname ILIKE 'nation%' OR
        c.relname ILIKE 'supplier%' OR
        c.relname ILIKE 'part%' OR
        c.relname ILIKE 'partsupp%' OR
        c.relname ILIKE 'orders%' OR
        c.relname ILIKE 'lineitem%' OR
        c.relname ILIKE 'tpcc%' OR
        c.relname ILIKE 'tpch%' OR
        c.relname ILIKE 'hammerdb%'
      )
  LOOP
    EXECUTE format('DROP SEQUENCE IF EXISTS %I.%I CASCADE', r.schemaname, r.seqname);
  END LOOP;
END $$;

-- Functions (rare, but safe)
DO $$
DECLARE r record;
BEGIN
  FOR r IN
    SELECT n.nspname AS schemaname, p.proname, pg_get_function_identity_arguments(p.oid) AS args
    FROM pg_proc p
    JOIN pg_namespace n ON n.oid = p.pronamespace
    WHERE n.nspname = 'public'
      AND (
        p.proname ILIKE 'tpcc%' OR
        p.proname ILIKE 'tpch%' OR
        p.proname ILIKE 'hammerdb%' OR
        p.proname ILIKE '%neword%' OR
        p.proname ILIKE '%payment%' OR
        p.proname ILIKE '%delivery%' OR
        p.proname ILIKE '%slev%' OR
        p.proname ILIKE '%ostat%'
      )
  LOOP
    EXECUTE format('DROP FUNCTION IF EXISTS %I.%I(%s) CASCADE', r.schemaname, r.proname, r.args);
  END LOOP;
END $$;

COMMIT;

-- ------------------------------------------------------------
-- D) Reset stats (core + pg_stat_statements if installed)
-- ------------------------------------------------------------
DO $$
DECLARE
  has_pgs boolean;
  reset_oid oid;
BEGIN
  -- Per-db stats reset
  PERFORM pg_stat_reset();

  -- Detect pg_stat_statements extension
  SELECT EXISTS (
    SELECT 1 FROM pg_extension WHERE extname = 'pg_stat_statements'
  ) INTO has_pgs;

  IF NOT has_pgs THEN
    RAISE NOTICE 'pg_stat_statements not installed; skipping reset';
  ELSE
    -- Ensure reset function exists
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

  -- Shared stats (often superuser-only)
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

  -- Optional on newer PG
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
-- E) Maintenance
-- ------------------------------------------------------------
VACUUM (ANALYZE);
-- VACUUM (FULL, ANALYZE) public;  -- optional, expensive

-- Optional: checkpoint (often restricted on managed services)
DO $$
BEGIN
  BEGIN
    CHECKPOINT;
  EXCEPTION WHEN insufficient_privilege THEN
    RAISE NOTICE 'Skipping CHECKPOINT: insufficient privilege';
  END;
END $$;

-- ------------------------------------------------------------
-- F) Sanity report: remaining benchmark-looking relations in public
-- ------------------------------------------------------------
SELECT c.relkind, c.relname
FROM pg_class c
JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE n.nspname = 'public'
  AND c.relkind IN ('r','p','v','m','S')
  AND (
    c.relname ILIKE 'tpcc%' OR c.relname ILIKE 'tpch%' OR c.relname ILIKE 'hammerdb%' OR
    c.relname IN (
      'warehouse','district','customer','history','new_order','oorder','order_line','item','stock',
      'region','nation','supplier','part','partsupp','orders','lineitem'
    )
  )
ORDER BY c.relkind, c.relname;

