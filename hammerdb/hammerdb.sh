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
    printf '  "raise_error": %s,\n' "$HDB_RAISEERROR"
    printf '  "citus": %s,\n' "$HDB_CITUS_COMPAT"
    printf '  "citus_azure_elastic_cluster": %s,\n' "$HDB_CITUS_AZURE_ELASTIC_CLUSTER"
    printf '  "citus_loadbalancer_port": %s,\n' "$HDB_CITUS_LOADBALANCER_PORT"
    printf '  "citus_direct_workers": %s,\n' "$HDB_CITUS_DIRECT_WORKERS"
    printf '  "stored_procedures": %s,\n' "$HDB_STOREDPROCS"
    printf '  "distributed_load": %s,\n' "$HDB_DISTRIBUTED_LOAD"
    printf '  "reset_schema": %s,\n' "$HDB_RESET_SCHEMA"
    printf '  "maintenance": %s,\n' "$HDB_MAINTENANCE"
    printf '  "vacuum": %s\n}\n' "$HDB_VACUUM"
}

tcl_literal()
{
    local value="$1"
    value="${value//\\/\\\\}"
    value="${value//\"/\\\"}"
    value="${value//\$/\\\$}"
    value="${value//\[/\\[}"
    value="${value//\]/\\]}"
    value="${value//$'\n'/\\n}"
    value="${value//$'\r'/\\r}"
    value="${value//$'\t'/\\t}"
    printf '"%s"' "$value"
}

hammerdb_write_workload()
{
    local phase="$1" destination="$2"
    cat >"$destination" <<TCL
dbset db pg
dbset bm TPC-C
diset connection pg_host $(tcl_literal "$PGHOST")
diset connection pg_port $(tcl_literal "$PGPORT")
diset connection pg_sslmode $(tcl_literal "$PGSSLMODE")
# diset connection pg_citus_direct_workers $(tcl_literal "$HDB_CITUS_DIRECT_WORKERS")
diset tpcc pg_dbase $(tcl_literal "$PGDATABASE")
diset tpcc pg_defaultdbase $(tcl_literal "$PGMAINTENANCE_DB")
diset tpcc pg_user $(tcl_literal "$PGUSER")
diset tpcc pg_pass $::env(PGPASSWORD)
diset tpcc pg_superuser $(tcl_literal "$HDB_SUPERUSER")
diset tpcc pg_superuserpass $::env(HDB_SUPERUSER_PASSWORD)
diset tpcc pg_num_vu $(tcl_literal "$HDB_BUILD_VUS")
diset tpcc pg_count_ware $(tcl_literal "$HDB_WAREHOUSES")
# diset tpcc pg_first_ware $(tcl_literal "$HDB_FIRST_WAREHOUSE")
diset tpcc pg_cituscompat $(tcl_literal "$HDB_CITUS_COMPAT")
diset tpcc pg_citus_azure_elastic_cluster $(tcl_literal "$HDB_CITUS_AZURE_ELASTIC_CLUSTER")
diset tpcc pg_citus_loadbalancer $(tcl_literal "$HDB_CITUS_LOADBALANCER_PORT")
diset tpcc pg_storedprocs $(tcl_literal "$HDB_STOREDPROCS")
TCL

    if [[ "$phase" == prepare ]]
    then
        cat >>"$destination" <<TCL
giset virtual_user_options virtual_users $(tcl_literal "$HDB_BUILD_VUS")
giset virtual_user_options user_delay 1
vuset delay 1
buildschema
vudestroy
TCL
    else
        cat >>"$destination" <<TCL
diset tpcc pg_driver timed
diset tpcc pg_rampup $(tcl_literal "$HDB_RAMPUP_MINUTES")
diset tpcc pg_duration $(tcl_literal "$HDB_DURATION_MINUTES")
diset tpcc pg_raiseerror $(tcl_literal "$HDB_RAISEERROR")
diset tpcc pg_vacuum $(tcl_literal "$HDB_VACUUM")
giset virtual_user_options virtual_users $(tcl_literal "$HDB_RUN_VUS")
giset virtual_user_options user_delay 1
vuset delay 1
vuset logtotemp 1
loadscript
vuset vu $(tcl_literal "$HDB_RUN_VUS")
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
