-- Run on the Citus query endpoint, not an individual shard.
-- psql -X -v started_at=2026-11-09T07:59:42Z -v watch_interval=5 -f monitor_data_loading.sql
\set ON_ERROR_STOP on
\pset pager off

\if :{?started_at}
\else
DO $$
BEGIN
    RAISE EXCEPTION 'Specify started_at with psql -v started_at=YYYY-MM-DDTHH:MM:SSZ';
END
$$;
\endif

\if :{?watch_interval}
\else
\set watch_interval 10
\endif

SELECT :'watch_interval'::bigint BETWEEN 1 AND 2147483647 AS valid_interval
\gset watch_
\if :watch_valid_interval
\else
DO $$
BEGIN
    RAISE EXCEPTION 'watch_interval must be a positive integer between 1 and 2147483647';
END
$$;
\endif

SET default_transaction_read_only = on;

SELECT now() AS checked_at,
       now() - :'started_at'::timestamptz AS elapsed,
       count(*) AS warehouse_count,
       min(w_id) AS min_w_id,
       max(w_id) AS max_w_id,
       (SELECT pg_size_pretty(sum(shard_size)) FROM citus_shards) AS total_shard_size
FROM public.warehouse
\watch :watch_interval
