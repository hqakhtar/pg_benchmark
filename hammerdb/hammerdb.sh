#!/usr/bin/env bash

HDB_MODULE_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"

# Keep the field list explicit so credentials never enter this metadata.
benchmark_describe()
{
    printf '{\n'
    json_field workload "$HDB_WORKLOAD"
    json_field installation "$HAMMERDB_HOME"
    json_field superuser "$HDB_SUPERUSER"
    printf '  "warehouses": %s,\n' "$HDB_WAREHOUSES"
    printf '  "first_warehouse": %s,\n' "$HDB_FIRST_WAREHOUSE"
    printf '  "build_vus": %s,\n' "$HDB_BUILD_VUS"
    printf '  "run_vus": %s,\n' "$HDB_RUN_VUS"
    printf '  "rampup_minutes": %s,\n' "$HDB_RAMPUP_MINUTES"
    printf '  "duration_minutes": %s,\n' "$HDB_DURATION_MINUTES"
    printf '  "citus": %s,\n' "$HDB_CITUS_COMPAT"
    printf '  "citus_loadbalancer_port": %s,\n' "$HDB_CITUS_LOADBALANCER_PORT"
    printf '  "citus_direct_workers": %s,\n' "$HDB_CITUS_DIRECT_WORKERS"
    printf '  "distributed_load": %s,\n' "$HDB_DISTRIBUTED_LOAD"
    printf '  "reset_schema": %s,\n' "$HDB_RESET_SCHEMA"
    printf '  "maintenance": %s,\n' "$HDB_MAINTENANCE"
    printf '  "vacuum": %s\n}\n' "$HDB_VACUUM"
}

hammerdb_write_workload()
{
    local phase="$1" destination="$2"
    cat >"$destination" <<'TCL'
dbset db pg
dbset bm TPC-C
diset connection pg_host $::env(PGHOST)
diset connection pg_port $::env(PGPORT)
diset connection pg_sslmode $::env(PGSSLMODE)
diset connection pg_azure_citus $::env(HDB_CITUS_COMPAT)
diset connection pg_citus_loadbalancer $::env(HDB_CITUS_LOADBALANCER_PORT)
diset connection pg_citus_direct_workers $::env(HDB_CITUS_DIRECT_WORKERS)
diset tpcc pg_dbase $::env(PGDATABASE)
diset tpcc pg_defaultdbase $::env(PGMAINTENANCE_DB)
diset tpcc pg_user $::env(PGUSER)
diset tpcc pg_pass $::env(PGPASSWORD)
diset tpcc pg_superuser $::env(HDB_SUPERUSER)
diset tpcc pg_superuserpass $::env(HDB_SUPERUSER_PASSWORD)
diset tpcc pg_num_vu $::env(HDB_BUILD_VUS)
diset tpcc pg_count_ware $::env(HDB_WAREHOUSES)
diset tpcc pg_first_ware $::env(HDB_FIRST_WAREHOUSE)
diset tpcc pg_cituscompat $::env(HDB_CITUS_COMPAT)
TCL

    if [[ "$phase" == prepare ]]
    then
        cat >>"$destination" <<'TCL'
giset virtual_user_options virtual_users $::env(HDB_BUILD_VUS)
giset virtual_user_options user_delay 1
vuset delay 1
buildschema
vudestroy
TCL
    else
        cat >>"$destination" <<'TCL'
diset tpcc pg_driver timed
diset tpcc pg_rampup $::env(HDB_RAMPUP_MINUTES)
diset tpcc pg_duration $::env(HDB_DURATION_MINUTES)
diset tpcc pg_vacuum $::env(HDB_VACUUM)
giset virtual_user_options virtual_users $::env(HDB_RUN_VUS)
giset virtual_user_options user_delay 1
vuset delay 1
vuset logtotemp 1
loadscript
vuset vu $::env(HDB_RUN_VUS)
vucreate
vurun
vudestroy
TCL
    fi
}

hammerdb_execute()
{
    local phase="$1" directory="$2" status=0
    hammerdb_write_workload "$phase" "$directory/workload.tcl"
    export TMPDIR="$directory" TMP="$directory" TEMP="$directory"
    (
        cd -- "$HAMMERDB_HOME" || exit 1
        exec ./hammerdbcli auto "$directory/workload.tcl"
    ) 2>&1 | tee "$directory/hammerdb.log" || status=$?
    if ((status != 0));
    then
        printf 'ERROR: HammerDB %s failed with exit %s\n' "$phase" "$status" >&2
        return "$status"
    fi
}

benchmark_cleanup()
{
    local directory="$1"
    [[ "$RUN_ALLOW_DESTRUCTIVE" == true ]] ||
    {
        fail "Refusing schema cleanup without RUN_ALLOW_DESTRUCTIVE=true"
        return 1
    }

    # Use the administrative role without changing the benchmark database.
    PGPASSWORD="$HDB_SUPERUSER_PASSWORD" \
        postgresql_exec "$PGDATABASE" "$HDB_SUPERUSER" \
        -f "$HDB_MODULE_DIR/hammerdb_cleanup_citus.sql" \
        >"$directory/cleanup.log" 2>&1 ||
        {
            fail "Schema cleanup failed; see $directory/cleanup.log"
            return 1
        }
}

benchmark_prepare()
{
    local directory="$1"
    if [[ "$HDB_RESET_SCHEMA" == true ]];
    then
        benchmark_cleanup "$directory"
    fi

    hammerdb_execute prepare "$directory"
}

benchmark_run()
{
    local directory="$1"
    if [[ "$HDB_MAINTENANCE" == true ]];
    then
        PGPASSWORD="$HDB_SUPERUSER_PASSWORD" \
            postgresql_exec "$PGDATABASE" "$HDB_SUPERUSER" \
            -f "$HDB_MODULE_DIR/hammerdb_maintenance_citus.sql" \
            >"$directory/maintenance.log" 2>&1 ||
            {
                fail "Maintenance failed; see $directory/maintenance.log"
                return 1
            }

    fi

    hammerdb_execute run "$directory"

    # Accept one unambiguous result and retain HammerDB's native metric units.
    local line count=0 nopm="" tpm="" version=""
    local result_pattern='TEST RESULT[[:space:]]*:[[:space:]]*System achieved ((0|[1-9][0-9]*)([.][0-9]+)?) NOPM from ((0|[1-9][0-9]*)([.][0-9]+)?) PostgreSQL TPM'
    while IFS= read -r line; do
        if [[ "$line" =~ $result_pattern ]];
        then
            nopm="${BASH_REMATCH[1]}"
            tpm="${BASH_REMATCH[4]}"
            count=$((count + 1))
        fi

        if [[ "$line" == "HammerDB CLI "* ]];
        then
            version="${line#HammerDB CLI }"
        fi

    done <"$directory/hammerdb.log"
    [[ "$count" == 1 && -n "$version" ]] ||
    {
        fail "Expected one valid NOPM/TPM result and a HammerDB version; found $count result(s)"
        return 1
    }

    {
        printf '{\n'
        json_field benchmark hammerdb
        json_field workload "$HDB_WORKLOAD"
        json_field tool_version "$version"
        printf '  "iteration": %s,\n' "$RUN_ITERATION"
        printf '  "metrics": {\n'
        printf '    "nopm": {"value": %s, "unit": "new_orders_per_minute"},\n' "$nopm"
        printf '    "tpm": {"value": %s, "unit": "transactions_per_minute"}\n' "$tpm"
        printf '  }\n}\n'
    } >"$directory/result.json"

    printf 'Iteration %s: %s NOPM, %s PostgreSQL TPM\n' "$RUN_ITERATION" "$nopm" "$tpm"
}

if [[ "${BASH_SOURCE[0]}" == "$0" ]];
then
    printf 'Use wrapper.sh hammerdb; this file defines the benchmark adapter.\n' >&2
    exit 2
fi
