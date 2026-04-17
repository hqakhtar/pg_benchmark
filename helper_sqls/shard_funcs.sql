CREATE SCHEMA IF NOT EXISTS citus_utils;
Do $$
BEGIN
RAISE NOTICE 'citus_utils schema created or already exists.';
END $$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION citus_utils.get_dist_column_count_per_shard(
    table_name text,
    distribution_column text
)
RETURNS TABLE (
    nodename text,
    nodeport integer,
    shardid bigint,
    dist_value_count bigint
)
AS $$
BEGIN
    RETURN QUERY EXECUTE format(
        '
        WITH shard_table AS (
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
        ORDER BY dist_value DESC, st.nodename, st.nodeport, st.shardid
        ',
        distribution_column,
        table_name
    );
END;
$$ LANGUAGE plpgsql;
Do $$
BEGIN
RAISE NOTICE 'citus_utils.get_dist_column_count_per_shard function created or replaced.';
END $$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION citus_utils.get_dist_column_count_per_node(
    table_name text,
    distribution_column text
)
RETURNS TABLE (
    nodename text,
    nodeport integer,
    dist_value_count bigint
)
AS $$
BEGIN
    RETURN QUERY
    SELECT
        st.nodename,
        st.nodeport,
        SUM(st.dist_value_count)::bigint AS total_dist_value_count
    FROM citus_utils.get_dist_column_count_per_shard(table_name, distribution_column) st
    GROUP BY st.nodename, st.nodeport
    ORDER BY total_dist_value_count DESC, st.nodename, st.nodeport;
END;
$$ LANGUAGE plpgsql;
Do $$
BEGIN
RAISE NOTICE 'citus_utils.get_dist_column_count_per_node function created or replaced.';
END $$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION citus_utils.get_shard_size(
    p_table_name text DEFAULT NULL,
    p_colocation_id int DEFAULT 1
)
RETURNS TABLE (
    nodename text,
    nodeport integer,
    table_name text,
    shard_size bigint,
    shard_nblocks bigint,
    shard_size_pretty text
)
AS $$
BEGIN
    RETURN QUERY
    SELECT
        s.nodename::text,
        s.nodeport::integer,
        CASE
            WHEN p_table_name IS NOT NULL THEN s.table_name::text
            ELSE NULL::text
        END AS table_name,
        SUM(s.shard_size)::bigint AS shard_size,
        (SUM(s.shard_size) / 8192)::bigint AS shard_nblocks,
        pg_size_pretty(SUM(s.shard_size))::text AS shard_size_pretty
    FROM citus_shards s
    WHERE s.colocation_id = p_colocation_id
      AND (
          p_table_name IS NULL
          OR s.table_name = p_table_name::regclass
      )
    GROUP BY
        s.nodename,
        s.nodeport,
        CASE
            WHEN p_table_name IS NOT NULL THEN s.table_name::text
            ELSE NULL::text
        END
    ORDER BY
        shard_size desc,
        s.nodename,
        s.nodeport,
        CASE
            WHEN p_table_name IS NOT NULL THEN s.table_name::text
            ELSE NULL::text
        END;
END;
$$ LANGUAGE plpgsql;
Do $$
BEGIN
RAISE NOTICE 'citus_utils.get_shard_size function created or replaced.';
END $$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION citus_utils.get_shard_location_for_key(
    p_table_name text,
    p_distribution_column text,
    p_distribution_value anyelement
)
RETURNS TABLE (
    nodename text,
    nodeport integer,
    table_name text,
    shard_table_name text,
    shard_id bigint
)
AS $$
DECLARE
    v_actual_dist_col text;
    v_shard_id bigint;
BEGIN
    -- Validate distributed table + distribution column
    SELECT column_to_column_name(logicalrelid, partkey)
    INTO v_actual_dist_col
    FROM pg_dist_partition
    WHERE logicalrelid = p_table_name::regclass;

    IF v_actual_dist_col IS NULL THEN
        RAISE EXCEPTION 'Table % is not a distributed table', p_table_name;
    END IF;

    IF v_actual_dist_col <> p_distribution_column THEN
        RAISE EXCEPTION
            'Column % is not the distribution column for table %. Actual distribution column is %',
            p_distribution_column,
            p_table_name,
            v_actual_dist_col;
    END IF;

    -- Determine shard id for the provided distribution key value
    v_shard_id :=
        get_shard_id_for_distribution_column(
            p_table_name,
            p_distribution_value
        );

    -- Return shard placement
    RETURN QUERY
    SELECT
        n.nodename::text,
        n.nodeport::integer,
        p_table_name::text,
        format('%s_%s', p_table_name, v_shard_id)::text,
        v_shard_id
    FROM pg_dist_placement p
    JOIN pg_dist_node n
      ON n.groupid = p.groupid
    WHERE p.shardid = v_shard_id
      AND n.noderole = 'primary'
    ORDER BY n.nodename, n.nodeport;

END;
$$ LANGUAGE plpgsql STABLE;
Do $$
BEGIN
RAISE NOTICE 'citus_utils.get_shard_location_for_key function created or replaced.';
END $$ LANGUAGE plpgsql;


SELECT * FROM citus_utils.get_dist_column_count_per_shard('warehouse', 'w_id');
SELECT * FROM citus_utils.get_dist_column_count_per_node('warehouse', 'w_id');
SELECT * FROM citus_utils.get_shard_size();
