-- Run against the logical warehouse table on the cluster's query endpoint.
-- psql -X -v expected_warehouses=100000 -v first_warehouse=1 -f verify_tpcc_warehouses.sql
-- Checks warehouse assignment coverage only, not the contents of other tables.
\set ON_ERROR_STOP on
\pset pager off

\if :{?expected_warehouses}
\else
DO $$
BEGIN
    RAISE EXCEPTION 'Specify expected_warehouses with psql -v expected_warehouses=N';
END
$$;
\endif

\if :{?first_warehouse}
\else
\set first_warehouse 1
\endif

BEGIN READ ONLY;

SELECT :'first_warehouse'::bigint > 0
       AND :'expected_warehouses'::bigint > 0 AS valid_range
\gset verify_

\if :verify_valid_range
\else
DO $$
BEGIN
    RAISE EXCEPTION 'first_warehouse and expected_warehouses must be positive integers';
END
$$;
\endif

WITH warehouse_counts AS
(
    SELECT w_id, count(*) AS copies
    FROM public.warehouse
    GROUP BY w_id
),
coverage AS
(
    SELECT
        coalesce(sum(copies), 0) AS actual_rows,
        :'expected_warehouses'::bigint - count(*) FILTER
        (
            WHERE w_id BETWEEN :'first_warehouse'::bigint
                AND :'first_warehouse'::bigint + :'expected_warehouses'::bigint - 1
        ) AS missing_ids,
        coalesce(sum(copies - 1), 0) AS duplicate_rows,
        coalesce(sum(copies) FILTER
        (
            WHERE w_id IS NULL
               OR w_id < :'first_warehouse'::bigint
               OR w_id > :'first_warehouse'::bigint + :'expected_warehouses'::bigint - 1
        ), 0) AS unexpected_rows
    FROM warehouse_counts
)
SELECT *,
       missing_ids = 0 AND duplicate_rows = 0 AND unexpected_rows = 0 AS ok
FROM coverage
\gset verify_

SELECT
    :'first_warehouse'::bigint AS first_warehouse,
    :'first_warehouse'::bigint + :'expected_warehouses'::bigint - 1 AS last_warehouse,
    :'expected_warehouses'::bigint AS expected_warehouses,
    :'verify_actual_rows'::bigint AS actual_rows,
    :'verify_missing_ids'::bigint AS missing_ids,
    :'verify_duplicate_rows'::bigint AS duplicate_rows,
    :'verify_unexpected_rows'::bigint AS unexpected_rows,
    CASE WHEN :'verify_ok'::boolean THEN 'PASS' ELSE 'FAIL' END AS status;

\if :verify_ok
COMMIT;
\echo 'Warehouse coverage verification passed (warehouse table only).'
\else
DO $$
BEGIN
    RAISE EXCEPTION 'Warehouse coverage verification failed: missing, duplicate, or unexpected warehouse IDs';
END
$$;
\endif
