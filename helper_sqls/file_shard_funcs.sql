-- Citus helpers for inspecting and balancing pgbench or HammerDB shards.
-- Run on the coordinator as a role allowed to move shards.

CREATE SCHEMA IF NOT EXISTS benchmark_utils;
DO
$$
BEGIN
    RAISE NOTICE 'benchmark_utils schema created or already exists.';
END
$$ LANGUAGE plpgsql;


-- Detect the active benchmark, preferring pgbench when both are present.
CREATE OR REPLACE FUNCTION benchmark_utils.detect_benchmark_schema
()
RETURNS TABLE
(
    benchmark_name text,
    dist_table text,
    dist_column text
)
AS
$$
BEGIN
    IF to_regclass('pgbench_accounts') IS NOT NULL
       AND EXISTS
       (
           SELECT 1
           FROM pg_attribute
           WHERE attrelid = to_regclass('pgbench_accounts')
             AND attname = 'aid'
             AND attnum > 0
             AND NOT attisdropped
       )
    THEN
        benchmark_name := 'pgbench';
        dist_table := 'pgbench_accounts';
        dist_column := 'aid';
    ELSIF to_regclass('warehouse') IS NOT NULL
          AND EXISTS
          (
              SELECT 1
              FROM pg_attribute
              WHERE attrelid = to_regclass('warehouse')
                AND attname = 'w_id'
                AND attnum > 0
                AND NOT attisdropped
          )
    THEN
        benchmark_name := 'HammerDB';
        dist_table := 'warehouse';
        dist_column := 'w_id';
    ELSE
        RAISE EXCEPTION
            'Could not detect a supported benchmark schema. Expected pgbench_accounts(aid) or warehouse(w_id)';
    END IF;

    RAISE NOTICE
        'Detected % workload: table %, distribution column %',
        benchmark_name,
        dist_table,
        dist_column;

    RETURN NEXT;
END;
$$ LANGUAGE plpgsql STABLE;

DO
$$
BEGIN
    RAISE NOTICE 'benchmark_utils.detect_benchmark_schema function created or replaced.';
END
$$ LANGUAGE plpgsql;


-- Count distinct distribution values on each primary shard placement.
CREATE OR REPLACE FUNCTION benchmark_utils.get_dist_column_count_per_shard
(
    table_name text,
    distribution_column text
)
RETURNS TABLE
(
    nodename text,
    nodeport integer,
    shardid bigint,
    dist_value_count bigint
)
AS
$$
BEGIN
    RETURN QUERY EXECUTE format
    (
        '
        WITH shard_table AS
        (
            SELECT
                n.nodename::text AS nodename,
                n.nodeport::integer AS nodeport,
                get_shard_id_for_distribution_column(%2$L, t.%1$I) AS shardid,
                t.%1$I AS dist_value
            FROM %2$I t
            JOIN pg_dist_placement p
              ON p.shardid = get_shard_id_for_distribution_column(%2$L, t.%1$I)
            JOIN pg_dist_node n
              ON n.groupid = p.groupid
            WHERE n.noderole = ''primary''
        )
        SELECT
            st.nodename,
            st.nodeport,
            st.shardid,
            COUNT(DISTINCT st.dist_value)::bigint AS dist_value_count
        FROM shard_table st
        GROUP BY st.nodename, st.nodeport, st.shardid
        ORDER BY st.nodename, st.nodeport, st.shardid
        ',
        distribution_column,
        table_name
    );
END;
$$ LANGUAGE plpgsql;

DO
$$
BEGIN
    RAISE NOTICE 'benchmark_utils.get_dist_column_count_per_shard function created or replaced.';
END
$$ LANGUAGE plpgsql;


-- Aggregate distribution-value counts by worker node.
CREATE OR REPLACE FUNCTION benchmark_utils.get_dist_column_count_per_node
(
    table_name text,
    distribution_column text
)
RETURNS TABLE
(
    nodename text,
    nodeport integer,
    dist_value_count bigint
)
AS
$$
BEGIN
    RETURN QUERY
    SELECT
        st.nodename,
        st.nodeport,
        SUM(st.dist_value_count)::bigint AS total_dist_value_count
    FROM benchmark_utils.get_dist_column_count_per_shard(table_name, distribution_column) st
    GROUP BY st.nodename, st.nodeport
    ORDER BY st.nodename, st.nodeport;
END;
$$ LANGUAGE plpgsql;

DO
$$
BEGIN
    RAISE NOTICE 'benchmark_utils.get_dist_column_count_per_node function created or replaced.';
END
$$ LANGUAGE plpgsql;


-- Report shard sizes for one table or an entire colocation group.
CREATE OR REPLACE FUNCTION benchmark_utils.get_shard_size
(
    p_table_name text DEFAULT NULL,
    p_colocation_id int DEFAULT 1,
    p_include_shard_id boolean DEFAULT true
)
RETURNS TABLE
(
    nodename text,
    nodeport integer,
    shardid bigint,
    table_name text,
    shard_size bigint,
    shard_nblocks bigint,
    shard_size_pretty text
)
AS
$$
BEGIN
    RETURN QUERY
    SELECT
        s.nodename::text,
        s.nodeport::integer,
        CASE
            WHEN p_include_shard_id THEN s.shardid::bigint
            ELSE NULL::bigint
        END AS shardid,
        CASE
            WHEN p_table_name IS NOT NULL THEN s.table_name::text
            ELSE NULL::text
        END AS table_name,
        SUM(s.shard_size)::bigint AS shard_size,
        (SUM(s.shard_size) / 8192)::bigint AS shard_nblocks,
        pg_size_pretty(SUM(s.shard_size))::text AS shard_size_pretty
    FROM citus_shards s
    WHERE s.colocation_id = p_colocation_id
      AND
      (
          p_table_name IS NULL
          OR s.table_name = p_table_name::regclass
      )
    GROUP BY
        s.nodename,
        s.nodeport,
        CASE
            WHEN p_include_shard_id THEN s.shardid::bigint
            ELSE NULL::bigint
        END,
        CASE
            WHEN p_table_name IS NOT NULL THEN s.table_name::text
            ELSE NULL::text
        END
    ORDER BY
        s.nodename,
        s.nodeport,
        CASE
            WHEN p_table_name IS NOT NULL THEN s.table_name::text
            ELSE NULL::text
        END;
END;
$$ LANGUAGE plpgsql;

DO
$$
BEGIN
    RAISE NOTICE 'benchmark_utils.get_shard_size function created or replaced.';
END
$$ LANGUAGE plpgsql;


-- Locate the primary shard placement for a distribution value.
CREATE OR REPLACE FUNCTION benchmark_utils.get_shard_location_for_key
(
    p_table_name text,
    p_distribution_column text,
    p_distribution_value anyelement
)
RETURNS TABLE
(
    nodename text,
    nodeport integer,
    table_name text,
    shard_table_name text,
    shard_id bigint
)
AS
$$
DECLARE
    v_actual_dist_col text;
    v_shard_id bigint;
BEGIN
    SELECT column_to_column_name(logicalrelid, partkey)
    INTO v_actual_dist_col
    FROM pg_dist_partition
    WHERE logicalrelid = p_table_name::regclass;

    IF v_actual_dist_col IS NULL
    THEN
        RAISE EXCEPTION 'Table % is not a distributed table', p_table_name;
    END IF;

    IF v_actual_dist_col <> p_distribution_column
    THEN
        RAISE EXCEPTION
            'Column % is not the distribution column for table %. Actual distribution column is %',
            p_distribution_column,
            p_table_name,
            v_actual_dist_col;
    END IF;

    v_shard_id :=
        get_shard_id_for_distribution_column
        (
            p_table_name,
            p_distribution_value
        );

    RETURN QUERY
    SELECT
        n.nodename::text,
        n.nodeport::integer,
        p_table_name::text,
        FORMAT('%s_%s', p_table_name, v_shard_id)::text,
        v_shard_id
    FROM pg_dist_placement p
    JOIN pg_dist_node n
      ON n.groupid = p.groupid
    WHERE p.shardid = v_shard_id
      AND n.noderole = 'primary'
    ORDER BY n.nodename, n.nodeport;
END;
$$ LANGUAGE plpgsql STABLE;

DO
$$
BEGIN
    RAISE NOTICE 'benchmark_utils.get_shard_location_for_key function created or replaced.';
END
$$ LANGUAGE plpgsql;


-- Move distribution values until node counts differ by at most one.
CREATE OR REPLACE PROCEDURE benchmark_utils.rebalance_distribution_values_evenly
(
    p_table_name text DEFAULT NULL,
    p_distribution_column text DEFAULT NULL,
    p_max_moves integer DEFAULT NULL,
    p_cascade_option text DEFAULT 'CASCADE',
    p_shard_transfer_mode text DEFAULT 'block_writes',
    p_rebalance_at_end boolean DEFAULT false,
    p_analyze_at_end boolean DEFAULT true
)
LANGUAGE plpgsql
AS
$$
DECLARE
    v_detected_benchmark text;
    dist_table text;
    dist_column text;

    v_table_regclass regclass;
    v_table_sql text;
    v_actual_dist_col text;
    v_dist_type_sql text;
    v_colocation_id integer;

    v_source_node text;
    v_source_port integer;
    v_source_count bigint;

    v_target_node text;
    v_target_port integer;
    v_target_count bigint;

    v_candidate_value_text text;
    v_candidate_shard_id bigint;
    v_candidate_shard_dist_value_count bigint;

    v_move_shard_id bigint;
    v_moves_done integer := 0;

    rec record;
BEGIN
    -- Select a supported benchmark when no table is supplied.
    IF p_table_name IS NULL
    THEN
        SELECT
            config.benchmark_name,
            config.dist_table,
            config.dist_column
        INTO
            v_detected_benchmark,
            dist_table,
            dist_column
        FROM benchmark_utils.detect_benchmark_schema() config;

        p_table_name := dist_table;
        p_distribution_column := dist_column;

        RAISE NOTICE
            'Automatically selected % workload: table %, distribution column %',
            v_detected_benchmark,
            dist_table,
            dist_column;
    END IF;

    v_table_regclass := p_table_name::regclass;
    v_table_sql := v_table_regclass::text;

    -- Read the authoritative distribution metadata from Citus.
    SELECT
        column_to_column_name(p.logicalrelid, p.partkey),
        p.colocationid
    INTO
        v_actual_dist_col,
        v_colocation_id
    FROM pg_dist_partition p
    WHERE p.logicalrelid = v_table_regclass;

    IF v_actual_dist_col IS NULL
    THEN
        RAISE EXCEPTION 'Table % is not a distributed table', p_table_name;
    END IF;

    IF p_distribution_column IS NOT NULL
       AND p_distribution_column <> v_actual_dist_col
    THEN
        RAISE NOTICE
            'Ignoring provided distribution column %, using actual distribution column % for table %',
            p_distribution_column,
            v_actual_dist_col,
            p_table_name;
    END IF;

    p_distribution_column := v_actual_dist_col;

    SELECT format_type(a.atttypid, a.atttypmod)
    INTO v_dist_type_sql
    FROM pg_attribute a
    WHERE a.attrelid = v_table_regclass
      AND a.attname = p_distribution_column
      AND NOT a.attisdropped;

    IF v_dist_type_sql IS NULL
    THEN
        RAISE EXCEPTION
            'Could not determine data type of %.%',
            p_table_name,
            p_distribution_column;
    END IF;

    RAISE NOTICE
        'Starting rebalance for table %, distribution column %, colocation id %',
        v_table_sql,
        p_distribution_column,
        v_colocation_id;

    LOOP
        EXIT WHEN p_max_moves IS NOT NULL AND v_moves_done >= p_max_moves;

        -- Pick the most- and least-loaded primary nodes.
        SELECT t.nodename, t.nodeport, t.dist_value_count
        INTO v_source_node, v_source_port, v_source_count
        FROM benchmark_utils.get_dist_column_count_per_node
        (
            v_table_sql,
            p_distribution_column
        ) AS t
        ORDER BY t.dist_value_count DESC, t.nodename, t.nodeport
        LIMIT 1;

        SELECT t.nodename, t.nodeport, t.dist_value_count
        INTO v_target_node, v_target_port, v_target_count
        FROM benchmark_utils.get_dist_column_count_per_node
        (
            v_table_sql,
            p_distribution_column
        ) AS t
        ORDER BY t.dist_value_count ASC, t.nodename, t.nodeport
        LIMIT 1;

        IF v_source_node IS NULL OR v_target_node IS NULL
        THEN
            RAISE NOTICE 'Could not determine source/target nodes. Stopping.';
            EXIT;
        END IF;

        IF v_source_count - v_target_count <= 1
        THEN
            RAISE NOTICE
                'Balanced enough: max node has %, min node has %. Stopping.',
                v_source_count,
                v_target_count;
            EXIT;
        END IF;

        IF v_source_node = v_target_node
           AND v_source_port = v_target_port
        THEN
            RAISE EXCEPTION
                'Source and target resolved to same node %.%',
                v_source_node,
                v_source_port;
        END IF;

        -- Prefer a value already isolated on its own shard.
        EXECUTE FORMAT
        (
            $sql$

            WITH dist_value_shards AS
            (
                SELECT DISTINCT
                    t.%1$I::text AS dist_value_text,
                    get_shard_id_for_distribution_column(%2$L, t.%1$I) AS shard_id
                FROM %3$s t
                JOIN pg_dist_placement p
                  ON p.shardid =
                     get_shard_id_for_distribution_column(%2$L, t.%1$I)
                JOIN pg_dist_node n
                  ON n.groupid = p.groupid
                 AND n.noderole = 'primary'
                WHERE n.nodename = %4$L
                  AND n.nodeport = %5$s
            ),
            shard_dist_value_counts AS
            (
                SELECT
                    get_shard_id_for_distribution_column(%2$L, t.%1$I) AS shard_id,
                    COUNT(DISTINCT t.%1$I)::bigint AS dist_value_count
                FROM %3$s t
                GROUP BY 1
            )
            SELECT
                dvs.dist_value_text,
                dvs.shard_id,
                sdvc.dist_value_count
            FROM dist_value_shards dvs
            JOIN shard_dist_value_counts sdvc USING (shard_id)
            ORDER BY
                CASE WHEN sdvc.dist_value_count = 1 THEN 0 ELSE 1 END,
                dvs.dist_value_text
            LIMIT 1

            $sql$,
            p_distribution_column,
            v_table_sql,
            v_table_sql,
            v_source_node,
            v_source_port
        )
        INTO
            v_candidate_value_text,
            v_candidate_shard_id,
            v_candidate_shard_dist_value_count;

        IF v_candidate_value_text IS NULL
        THEN
            RAISE NOTICE
                'No candidate distribution value found on %.%. Stopping.',
                v_source_node,
                v_source_port;
            EXIT;
        END IF;

        IF v_candidate_shard_dist_value_count = 1
        THEN
            v_move_shard_id := v_candidate_shard_id;
        ELSE
            -- Isolate a shared value before moving its shard.
            EXECUTE FORMAT
            (
                'SELECT isolate_tenant_to_new_shard(%L, %L::%s, %L, shard_transfer_mode := %L)',
                v_table_sql,
                v_candidate_value_text,
                v_dist_type_sql,
                p_cascade_option,
                p_shard_transfer_mode
            )
            INTO v_move_shard_id;
        END IF;

        RAISE NOTICE
            'Move %: value % shard % from %.% to %.% (counts % -> %)',
            v_moves_done + 1,
            v_candidate_value_text,
            v_move_shard_id,
            v_source_node,
            v_source_port,
            v_target_node,
            v_target_port,
            v_source_count,
            v_target_count;

        -- Move the selected shard to the least-loaded node.
        EXECUTE FORMAT
        (
            'SELECT citus_move_shard_placement(%s, %L, %s, %L, %s,
                    shard_transfer_mode := %L)',
            v_move_shard_id,
            v_source_node,
            v_source_port,
            v_target_node,
            v_target_port,
            p_shard_transfer_mode
        );

        v_moves_done := v_moves_done + 1;

        COMMIT AND CHAIN;
    END LOOP;

    COMMIT;

    IF p_rebalance_at_end
    THEN
        PERFORM citus_rebalance_start();
        PERFORM citus_rebalance_wait();
        COMMIT;
    END IF;

    IF p_analyze_at_end
    THEN
        -- Refresh statistics for every table in the colocation group.
        SET LOCAL citus.enable_local_execution TO OFF;

        FOR rec IN
            SELECT logicalrelid::regclass AS relname
            FROM pg_dist_partition
            WHERE colocationid = v_colocation_id
            ORDER BY 1
        LOOP
            EXECUTE FORMAT('ANALYZE %s', rec.relname);
        END LOOP;

        COMMIT;
    END IF;

    RAISE NOTICE 'Done. Total moves committed: %', v_moves_done;
END;
$$;

DO
$$
BEGIN
    RAISE NOTICE 'benchmark_utils.rebalance_distribution_values_evenly procedure created or replaced.';
END
$$ LANGUAGE plpgsql;


-- Show the current distribution and shard sizes.
SELECT shard_counts.*
FROM benchmark_utils.detect_benchmark_schema() config
CROSS JOIN LATERAL benchmark_utils.get_dist_column_count_per_shard
(
    config.dist_table,
    config.dist_column
) shard_counts
ORDER BY
    shard_counts.dist_value_count DESC,
    shard_counts.nodename,
    shard_counts.nodeport,
    shard_counts.shardid;

SELECT node_counts.*
FROM benchmark_utils.detect_benchmark_schema() config
CROSS JOIN LATERAL benchmark_utils.get_dist_column_count_per_node
(
    config.dist_table,
    config.dist_column
) node_counts
ORDER BY
    node_counts.dist_value_count DESC,
    node_counts.nodename,
    node_counts.nodeport;

SELECT shard_sizes.*
FROM benchmark_utils.detect_benchmark_schema() config
JOIN pg_dist_partition partition_info
  ON partition_info.logicalrelid = config.dist_table::regclass
CROSS JOIN LATERAL benchmark_utils.get_shard_size
(
    NULL,
    partition_info.colocationid
) shard_sizes
ORDER BY
    shard_sizes.shard_size DESC,
    shard_sizes.nodename,
    shard_sizes.nodeport,
    shard_sizes.table_name;

DO
$$
BEGIN
    RAISE NOTICE 'Rebalance was not executed.';
    RAISE NOTICE 'Run CALL benchmark_utils.rebalance_distribution_values_evenly() to rebalance distribution values.';
END
$$ LANGUAGE plpgsql;
