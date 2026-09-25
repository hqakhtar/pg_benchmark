#!/usr/bin/env bash

HDB_MODULE_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"

benchmark_validate()
{
    [[ "$HDB_WORKLOAD" == tpcc ]] ||
    {
        fail "HammerDB currently supports HDB_WORKLOAD=tpcc only"
        return 1
    }

    HAMMERDB_HOME="$(absolute_path "$HAMMERDB_HOME")"
    export HAMMERDB_HOME
    [[ -x "$HAMMERDB_HOME/hammerdbcli" ]] ||
    {
        fail "HAMMERDB_HOME must contain an executable hammerdbcli: $HAMMERDB_HOME"
        return 1
    }

    [[ -n "$HDB_SUPERUSER" ]] || { fail "HDB_SUPERUSER must not be empty"; return 1; }

    validate_integer HDB_WAREHOUSES "$HDB_WAREHOUSES" 0
    validate_integer HDB_FIRST_WAREHOUSE "$HDB_FIRST_WAREHOUSE" 1
    validate_integer HDB_BUILD_VUS "$HDB_BUILD_VUS" 1
    validate_integer HDB_RUN_VUS "$HDB_RUN_VUS" 1
    validate_integer HDB_RAMPUP_MINUTES "$HDB_RAMPUP_MINUTES" 0
    validate_integer HDB_DURATION_MINUTES "$HDB_DURATION_MINUTES" 1
    validate_integer HDB_CITUS_LOADBALANCER_PORT "$HDB_CITUS_LOADBALANCER_PORT" 1 65535
    validate_boolean HDB_CITUS_COMPAT "$HDB_CITUS_COMPAT"
    validate_boolean HDB_CITUS_DIRECT_WORKERS "$HDB_CITUS_DIRECT_WORKERS"
    validate_boolean HDB_DISTRIBUTED_LOAD "$HDB_DISTRIBUTED_LOAD"
    validate_boolean HDB_RESET_SCHEMA "$HDB_RESET_SCHEMA"
    validate_boolean HDB_MAINTENANCE "$HDB_MAINTENANCE"
    validate_boolean HDB_VACUUM "$HDB_VACUUM"

    if ((HDB_WAREHOUSES > 0 && HDB_BUILD_VUS > HDB_WAREHOUSES));
    then
        fail "HDB_BUILD_VUS cannot exceed HDB_WAREHOUSES"
        return 1
    fi

    # Zero warehouses selects post-data DDL in the distributed load protocol.
    if ((HDB_WAREHOUSES == 0));
    then
        [[ "$HDB_DISTRIBUTED_LOAD" == true &&
           ( "$RUN_ACTION" == prepare || "$RUN_ACTION" == check ) &&
           "$HDB_FIRST_WAREHOUSE" == 1 && "$HDB_BUILD_VUS" == 1 ]] ||
        {
            fail "Zero warehouses are only valid for the distributed prepare finalization phase (first warehouse/build VUs = 1)"
            return 1
        }

    fi

    if ((HDB_FIRST_WAREHOUSE != 1)) && [[ "$HDB_DISTRIBUTED_LOAD" != true ]];
    then
        fail "Warehouse ranges require HDB_DISTRIBUTED_LOAD=true"
        return 1
    fi

    if [[ "$HDB_DISTRIBUTED_LOAD" == true && "$RUN_ACTION" == run ]];
    then
        fail "HDB_DISTRIBUTED_LOAD is a prepare-only protocol, not a benchmark run mode"
        return 1
    fi

    if [[ "$HDB_RESET_SCHEMA" == true && "$RUN_ALLOW_DESTRUCTIVE" != true ]];
    then
        fail "HDB_RESET_SCHEMA=true requires RUN_ALLOW_DESTRUCTIVE=true"
        return 1
    fi

    if [[ "$RUN_PREPARE_MODE" == each && "$HDB_RESET_SCHEMA" != true ]];
    then
        fail "HammerDB preparation for each iteration requires HDB_RESET_SCHEMA=true"
        return 1
    fi

    if [[ "$HDB_DISTRIBUTED_LOAD" == true && "$HDB_RESET_SCHEMA" == true ]];
    then
        fail "Distributed preparation must not reset the schema on individual runners"
        return 1
    fi

    local limit required
    limit="$(ulimit -n)"
    required=$((5 * (HDB_BUILD_VUS > HDB_RUN_VUS ? HDB_BUILD_VUS : HDB_RUN_VUS)))
    if [[ "$limit" != unlimited ]] && ((limit <= required));
    then
        fail "Open-file limit ($limit) must exceed 5 times the largest VU count ($required)"
        return 1
    fi

    limit="$(ulimit -u)"
    if [[ "$limit" != unlimited ]] && ((limit < 4096));
    then
        fail "Process limit ($limit) must be at least 4096"
        return 1
    fi

    local file
    for file in tpcc.tcl hammerdb_cleanup_citus.sql hammerdb_maintenance_citus.sql; do
        [[ -r "$HDB_MODULE_DIR/$file" ]] ||
        {
            fail "Missing HammerDB asset: $HDB_MODULE_DIR/$file"
            return 1
        }

    done
}

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

hammerdb_execute()
{
    local phase="$1" directory="$2" status=0
    # Runtime values stay in the environment rather than generated Tcl literals.
    cp -- "$HDB_MODULE_DIR/tpcc.tcl" "$directory/workload.tcl"
    export HDB_PHASE="$phase" TMPDIR="$directory" TMP="$directory" TEMP="$directory"
    (
        cd -- "$HAMMERDB_HOME" || exit 1
        exec ./hammerdbcli auto "$directory/workload.tcl"
    ) 2>&1 | tee "$directory/hammerdb.log" || status=$?
    if ((status != 0));
    then
        printf 'ERROR: HammerDB %s failed with exit %s\n' "$phase" "$status" >&2
        return "$status"
    fi

    # A clean CLI exit alone does not prove that the virtual users completed.
    local marker="BENCHMARK_${phase^^}_COMPLETE"
    grep -Fxq "$marker" "$directory/hammerdb.log" ||
    {
        fail "HammerDB did not confirm successful completion of $phase"
        return 1
    }
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
