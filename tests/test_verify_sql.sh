#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
mkdir -p -- "$ROOT/tests/.work"
TEST_ROOT="$(mktemp -d "$ROOT/tests/.work/verify-sql-XXXXXXXX")"
server_started=false
cleanup()
{
    local status="$?"
    if [[ "$server_started" == true ]]
    then
        "$PG_BIN_DIR/pg_ctl" -D "$TEST_ROOT/data" -m fast -w stop \
            >"$TEST_ROOT/stop.log" 2>&1 || status=1
    fi

    if ((status == 0))
    then
        rm -r -- "$TEST_ROOT"
    else
        printf 'Fixtures preserved: %s\n' "$TEST_ROOT" >&2
    fi

    exit "$status"
}
trap cleanup EXIT
umask 077
# Keep instrumented PostgreSQL builds from writing profiles into their source tree.
export GCOV_PREFIX="$TEST_ROOT/coverage" GCOV_PREFIX_STRIP=0
PG_CONFIG="${PG_CONFIG:-pg_config}"
command -v "$PG_CONFIG" >/dev/null ||
    { printf 'Missing test dependency: %s\n' "$PG_CONFIG" >&2; exit 1; }
PG_BIN_DIR="$("$PG_CONFIG" --bindir)"
for command in initdb pg_ctl psql
do
    [[ -x "$PG_BIN_DIR/$command" ]] ||
        { printf 'Missing test dependency: %s/%s\n' "$PG_BIN_DIR" "$command" >&2; exit 1; }
done

mkdir -- "$TEST_ROOT/socket"
"$PG_BIN_DIR/initdb" -D "$TEST_ROOT/data" -U verifier -A trust --no-locale \
    >"$TEST_ROOT/initdb.log" 2>&1
server_started=true
"$PG_BIN_DIR/pg_ctl" -D "$TEST_ROOT/data" -l "$TEST_ROOT/server.log" \
    -o "-F -c listen_addresses='' -k '$TEST_ROOT/socket' -p 5432" -w start \
    >"$TEST_ROOT/start.log" 2>&1
PSQL=("$PG_BIN_DIR/psql" -X -w --host="$TEST_ROOT/socket" --port=5432
    --dbname=postgres -v ON_ERROR_STOP=1)
"${PSQL[@]}" --username=verifier -q <<'SQL'
CREATE TABLE public.warehouse (w_id integer);
CREATE ROLE warehouse_reader LOGIN;
GRANT SELECT ON public.warehouse TO warehouse_reader;
INSERT INTO public.warehouse SELECT generate_series(1, 5);
SQL

passed=0
assert()
{
    if ! "$@"
    then
        printf 'Assertion failed: ' >&2
        printf '%q ' "$@" >&2
        printf '\nSee %s/current.log\n' "$TEST_ROOT" >&2
        exit 1
    fi
}

pass()
{
    passed=$((passed + 1))
    printf 'PASS: %s\n' "$1"
}

invoke()
{
    status=0
    "${PSQL[@]}" --username=warehouse_reader "$@" \
        -f "$ROOT/helper_sqls/verify_tpcc_warehouses.sql" \
        >"$TEST_ROOT/current.log" 2>&1 || status=$?
}

invoke -v expected_warehouses=5
assert test "$status" == 0
assert grep -Fq 'Warehouse coverage verification passed' "$TEST_ROOT/current.log"
assert test "$("${PSQL[@]}" --username=verifier -Atc 'SELECT count(*) FROM public.warehouse')" == 5
pass 'complete coverage passes with a SELECT-only role and the default first warehouse'

"${PSQL[@]}" --username=verifier -qc 'UPDATE public.warehouse SET w_id = w_id + 100'
invoke -v expected_warehouses=5 -v first_warehouse=101
assert test "$status" == 0
pass 'non-default warehouse ranges pass'

"${PSQL[@]}" --username=verifier -qc \
    'TRUNCATE public.warehouse; INSERT INTO public.warehouse VALUES (1), (2), (2), (4), (5)'
invoke -v expected_warehouses=5
assert test "$status" == 3
assert grep -Fq 'Warehouse coverage verification failed' "$TEST_ROOT/current.log"
assert grep -Eq '5[[:space:]]*\|[[:space:]]*1[[:space:]]*\|[[:space:]]*1[[:space:]]*\|[[:space:]]*0[[:space:]]*\|[[:space:]]*FAIL' \
    "$TEST_ROOT/current.log"
pass 'a missing ID masked by a duplicate fails even when count and endpoints match'

"${PSQL[@]}" --username=verifier -qc 'UPDATE public.warehouse SET w_id = 6 WHERE w_id = 5'
invoke -v expected_warehouses=5
assert test "$status" == 3
assert grep -Eq '5[[:space:]]*\|[[:space:]]*2[[:space:]]*\|[[:space:]]*1[[:space:]]*\|[[:space:]]*1[[:space:]]*\|[[:space:]]*FAIL' \
    "$TEST_ROOT/current.log"
pass 'unexpected warehouse IDs are reported along with missing IDs'

"${PSQL[@]}" --username=verifier -qc \
    'TRUNCATE public.warehouse; INSERT INTO public.warehouse SELECT generate_series(1, 5); INSERT INTO public.warehouse VALUES (NULL)'
invoke -v expected_warehouses=5
assert test "$status" == 3
assert grep -Eq '6[[:space:]]*\|[[:space:]]*0[[:space:]]*\|[[:space:]]*0[[:space:]]*\|[[:space:]]*1[[:space:]]*\|[[:space:]]*FAIL' \
    "$TEST_ROOT/current.log"
pass 'null warehouse IDs fail verification'

"${PSQL[@]}" --username=verifier -qc 'TRUNCATE public.warehouse'
invoke -v expected_warehouses=5
assert test "$status" == 3
assert grep -Eq '0[[:space:]]*\|[[:space:]]*5[[:space:]]*\|[[:space:]]*0[[:space:]]*\|[[:space:]]*0[[:space:]]*\|[[:space:]]*FAIL' \
    "$TEST_ROOT/current.log"
invoke -v expected_warehouses=2147483647 -v first_warehouse=2147483647
assert test "$status" == 3
assert grep -Fq '4294967293' "$TEST_ROOT/current.log"
pass 'empty data and large ranges fail without generating every expected ID or overflowing'

invoke
assert test "$status" == 3
assert grep -Fq 'Specify expected_warehouses' "$TEST_ROOT/current.log"
for value in 0 -1 invalid
do
    invoke -v expected_warehouses="$value"
    assert test "$status" == 3
    invoke -v expected_warehouses=5 -v first_warehouse="$value"
    assert test "$status" == 3
done
pass 'standalone SQL rejects missing or invalid range parameters'

"${PSQL[@]}" --username=verifier -qc 'INSERT INTO public.warehouse SELECT generate_series(1, 5)'
cp -- "$ROOT/connection.env.sample" "$TEST_ROOT/connection.env"
cp -- "$ROOT/hammerdb/hammerdb.env.sample" "$TEST_ROOT/benchmark.env"
cp -- "$ROOT/run.env.sample" "$TEST_ROOT/run.env"
VERIFY_ENV=("PGHOST=$TEST_ROOT/socket" "PGPORT=5432" "PGDATABASE=postgres"
    "PGUSER=warehouse_reader" "PGPASSWORD=" "PGSSLMODE=disable" "PG_TARGET_MODE=existing"
    "PG_PSQL=$PG_BIN_DIR/psql" "HAMMERDB_HOME=/deliberately/missing/HammerDB"
    "HDB_WAREHOUSES=5" "HDB_FIRST_WAREHOUSE=1" "RUN_LABEL=coverage"
    "RUN_PREPARE_MODE=each" "HDB_RESET_SCHEMA=true" "RUN_ALLOW_DESTRUCTIVE=false")
invoke_wrapper()
{
    status=0
    env "${VERIFY_ENV[@]}" RUN_OUTPUT_ROOT="$1" \
        bash "$ROOT/wrapper.sh" --verify --connection-env "$TEST_ROOT/connection.env" \
        --benchmark-env "$TEST_ROOT/benchmark.env" --run-env "$TEST_ROOT/run.env" \
        >"$TEST_ROOT/current.log" 2>&1 || status=$?
}

invoke_wrapper "$TEST_ROOT/success-results"
assert test "$status" == 0
manifest="$(find "$TEST_ROOT/success-results" -name run.json -print -quit)"
assert grep -Fq '"status": "succeeded"' "$manifest"
assert grep -Fq 'Warehouse coverage verification passed' "$TEST_ROOT/current.log"
pass 'wrapper verifies real PostgreSQL data without HammerDB, maintenance, or destructive permission'

"${PSQL[@]}" --username=verifier -qc 'DELETE FROM public.warehouse WHERE w_id = 3'
invoke_wrapper "$TEST_ROOT/failure-results"
assert test "$status" == 3
manifest="$(find "$TEST_ROOT/failure-results" -name run.json -print -quit)"
assert grep -Fq '"status": "failed"' "$manifest"
assert grep -Fq '"exit_code": 3' "$manifest"
assert grep -Fq 'Warehouse coverage verification failed' "$TEST_ROOT/current.log"
pass 'a real coverage failure returns nonzero and persists a failed wrapper run'

printf '%s verification integration tests passed.\n' "$passed"
