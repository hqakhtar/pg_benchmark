CREATE SCHEMA IF NOT EXISTS citus_utils;

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
                get_shard_id_for_distribution_column(%3$L, t.%1$I) AS shardid,
                t.%1$I AS dist_value
            FROM %2$I t
            JOIN pg_dist_placement p
              ON p.shardid = get_shard_id_for_distribution_column(%3$L, t.%1$I)
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
        table_name,
        table_name
    );
END;
$$ LANGUAGE plpgsql;


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
    ORDER BY st.nodename, st.nodeport;
END;
$$ LANGUAGE plpgsql;
