#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
[[ -x "$ROOT/tests/fixtures/fake-command.sh" ]] ||
{
    printf 'Test fixture must be executable; refusing to fall back to native commands\n' >&2
    exit 1
}

for command in tclsh jq setsid timeout; do
    command -v "$command" >/dev/null ||
    {
        printf 'Missing test dependency: %s\n' "$command" >&2
        exit 1
    }

done
mkdir -p -- "$ROOT/tests/.work"
TEST_ROOT="$ROOT/tests/.work/runner-$$-$RANDOM"
mkdir -- "$TEST_ROOT"
trap '
    if [[ "$?" == 0 ]];
    then
        rm -r -- "$TEST_ROOT";
    else
        printf "Fixtures preserved: %s\n" "$TEST_ROOT" >&2;
    fi

' EXIT
mkdir -p "$TEST_ROOT/bin" "$TEST_ROOT/hammer db" "$TEST_ROOT/home"
cp -- "$ROOT/connection.env.sample" "$TEST_ROOT/connection.env"
cp -- "$ROOT/hammerdb/hammerdb.env.sample" "$TEST_ROOT/benchmark.env"
cp -- "$ROOT/run.env.sample" "$TEST_ROOT/run.env"
for command in psql pg_config initdb pg_ctl tee; do
    ln -s "$ROOT/tests/fixtures/fake-command.sh" "$TEST_ROOT/bin/$command"
done
ln -s "$ROOT/tests/fixtures/fake-command.sh" "$TEST_ROOT/hammer db/hammerdbcli"
BASE_ENV=(
    "HOME=$TEST_ROOT/home" "PATH=$TEST_ROOT/bin:/usr/bin:/bin"
    "TEST_ROOT=$TEST_ROOT" "TEST_BIN=$TEST_ROOT/bin" "TEST_PROJECT_ROOT=$ROOT"
    "TEST_TCLSH=$(command -v tclsh)" "TEST_REAL_TEE=$(command -v tee)"
    "TEST_TRACE=$TEST_ROOT/trace"
    "CONNECTION_ENV_FILE=$TEST_ROOT/connection.env"
    "BENCHMARK_ENV_FILE=$TEST_ROOT/benchmark.env" "RUN_ENV_FILE=$TEST_ROOT/run.env"
    "PGHOST=mock-db.invalid" "PGPORT=5432" "PGDATABASE=benchmark_db" "PGUSER=benchmark_user"
    "PGPASSWORD="
    "PG_CONFIG=/deliberately/missing/pg_config"
    "HAMMERDB_HOME=$TEST_ROOT/hammer db" "HDB_WAREHOUSES=2" "HDB_BUILD_VUS=1" "HDB_RUN_VUS=1"
    "HDB_RAMPUP_MINUTES=0" "HDB_DURATION_MINUTES=1"
    "RUN_OUTPUT_ROOT=$TEST_ROOT/results" "RUN_ITERATIONS=2"
)
EXTRA_ENV=()
WRAPPER="$ROOT/wrapper.sh"
passed=0

assert()
{
    if ! "$@";
    then
        printf 'Assertion failed: ' >&2
        printf '%q ' "$@" >&2
        printf '\nSee %s\n' "$TEST_ROOT/current.log" >&2
        exit 1
    fi
}

invoke()
{
    : >"$TEST_ROOT/trace"
    set +e
    (
        cd "$TEST_ROOT"
        env -i "${BASE_ENV[@]}" "${EXTRA_ENV[@]}" \
            bash "$WRAPPER" "$@"
    ) >"$TEST_ROOT/current.log" 2>&1
    status=$?
    set -e
}

last_run()
{
    local line directory="" pattern='^Run [a-z]+: (.*) \(exit [0-9]+\)$'
    while IFS= read -r line; do
        if [[ "$line" =~ $pattern ]];
        then
            directory="${BASH_REMATCH[1]}";
        fi

    done <"$TEST_ROOT/current.log"
    [[ -n "$directory" ]] || { printf 'No run directory was reported\n' >&2; return 1; }

    printf '%s\n' "$directory"
}

trace_count()
{
    grep -c -- "$1" "$TEST_ROOT/trace" || [[ "$?" == 1 ]]
}

pass()
{
    passed=$((passed + 1))
    printf 'PASS: %s\n' "$1"
}

invoke hammerdb --check
assert test "$status" == 0
assert test ! -s "$TEST_ROOT/trace"
assert test ! -e "$TEST_ROOT/results"
pass 'check is offline and existing targets do not need pg_config'

set +e
env -i "${BASE_ENV[@]}" env -u PGPASSWORD \
    bash "$WRAPPER" hammerdb --check >"$TEST_ROOT/current.log" 2>&1
status=$?
set -e
assert test "$status" != 0
assert grep -Fq 'PGPASSWORD: Your password goes here' "$TEST_ROOT/current.log"
assert test ! -s "$TEST_ROOT/trace"
assert test ! -e "$TEST_ROOT/results"
pass 'the connection password guard rejects unset values but accepts explicitly empty values'

for group in connection benchmark run
do
    EXTRA_ENV=()
    invoke hammerdb --check "--$group-env" "$TEST_ROOT/missing.env"
    assert test "$status" != 0
    assert grep -q 'Copy the matching.*template' "$TEST_ROOT/current.log"
    assert test ! -s "$TEST_ROOT/trace"
done
mkdir -- "$TEST_ROOT/directory.env"
invoke hammerdb --check --connection-env "$TEST_ROOT/directory.env"
assert test "$status" != 0
assert grep -q 'readable regular file' "$TEST_ROOT/current.log"
pass 'all three private configs are required and directories cannot be sourced'

# shellcheck disable=SC2016
printf 'printf "sample executed\\n" >>"$TEST_TRACE"\n' >"$TEST_ROOT/trap.sample"
cp -- "$TEST_ROOT/trap.sample" "$TEST_ROOT/trap.SAMPLE"
ln -s "$TEST_ROOT/trap.sample" "$TEST_ROOT/alias.env"
ln -s "$ROOT/connection.env.sample" "$TEST_ROOT/connection-alias.env"
for group in connection benchmark run
do
    for file in "$TEST_ROOT/trap.sample" "$TEST_ROOT/trap.SAMPLE" \
        "$TEST_ROOT/alias.env" "$TEST_ROOT/connection-alias.env"
    do
        EXTRA_ENV=()
        invoke hammerdb --check "--$group-env" "$file"
        assert test "$status" != 0
        assert grep -iq 'sample' "$TEST_ROOT/current.log"
        assert test ! -s "$TEST_ROOT/trace"
        EXTRA_ENV=("${group^^}_ENV_FILE=$file")
        invoke hammerdb --check
        assert test "$status" != 0
        assert test ! -s "$TEST_ROOT/trace"
    done
done
pass 'CLI and environment selectors reject samples and sample symlink targets before sourcing'

EXTRA_ENV=("CONNECTION_ENV_FILE=$ROOT/connection.env.sample"
    "BENCHMARK_ENV_FILE=$ROOT/hammerdb/hammerdb.env.sample" "RUN_ENV_FILE=$ROOT/run.env.sample")
invoke hammerdb --check --connection-env ./connection.env \
    --benchmark-env ./benchmark.env --run-env ./run.env
assert test "$status" == 0
assert test ! -s "$TEST_ROOT/trace"
pass 'CLI private selectors override sample-valued selectors and resolve relative to the caller'

for file in "$ROOT/initdb.env.sample" "$TEST_ROOT/trap.sample" "$TEST_ROOT/alias.env"
do
    for mode in existing temporary
    do
        EXTRA_ENV=("PG_SERVER_ENV_FILE=$file" "PG_TARGET_MODE=$mode"
            "PGHOST=127.0.0.1" "PG_CONFIG=$TEST_ROOT/bin/pg_config" "RUN_PREPARE_MODE=once")
        invoke hammerdb --check
        assert test "$status" != 0
        assert grep -iq 'sample' "$TEST_ROOT/current.log"
        assert test ! -s "$TEST_ROOT/trace"
    done
done
pass 'PG_SERVER_ENV_FILE rejects templates in either target mode before native commands'

TEST_REPO="$TEST_ROOT/checkout"
mkdir -p -- "$TEST_REPO/lib" "$TEST_REPO/hammerdb"
cp -- "$ROOT/wrapper.sh" "$ROOT/connection.env.sample" "$ROOT/run.env.sample" "$TEST_REPO/"
cp -- "$ROOT/lib/"*.sh "$TEST_REPO/lib/"
cp -- "$ROOT/hammerdb/"*.sh "$ROOT/hammerdb/"*.tcl "$ROOT/hammerdb/"*.sql \
    "$ROOT/hammerdb/hammerdb.env.sample" "$TEST_REPO/hammerdb/"
WRAPPER="$TEST_REPO/wrapper.sh"
EXTRA_ENV=("CONNECTION_ENV_FILE=" "BENCHMARK_ENV_FILE=" "RUN_ENV_FILE=")
invoke hammerdb --check
assert test "$status" != 0
assert grep -q 'connection.env' "$TEST_ROOT/current.log"
assert test ! -s "$TEST_ROOT/trace"
cp -- "$TEST_REPO/connection.env.sample" "$TEST_REPO/connection.env"
invoke hammerdb --check
assert test "$status" != 0
assert grep -q 'hammerdb.env' "$TEST_ROOT/current.log"
cp -- "$TEST_REPO/hammerdb/hammerdb.env.sample" "$TEST_REPO/hammerdb/hammerdb.env"
invoke hammerdb --check
assert test "$status" != 0
assert grep -q 'run.env' "$TEST_ROOT/current.log"
cp -- "$TEST_REPO/run.env.sample" "$TEST_REPO/run.env"
invoke hammerdb --check
assert test "$status" == 0
assert test ! -s "$TEST_ROOT/trace"
pass 'default paths require private copies and never fall back to shipped templates'

rm -- "$TEST_REPO/run.env"
ln -s run.env.sample "$TEST_REPO/run.env"
invoke hammerdb --check
assert test "$status" != 0
assert grep -q 'sample file' "$TEST_ROOT/current.log"
assert test ! -s "$TEST_ROOT/trace"
EXTRA_ENV=()
# shellcheck disable=SC2016
printf 'printf "unselected default loaded\\n" >>"$TEST_TRACE"\n' >"$TEST_REPO/connection.env"
invoke hammerdb --check
assert test "$status" == 0
assert test ! -s "$TEST_ROOT/trace"
WRAPPER="$ROOT/wrapper.sh"
pass 'default sample aliases are rejected and selected configs never layer unselected defaults'

for group in connection benchmark run
do
    cp -- "$TEST_ROOT/$group.env" "$TEST_ROOT/ordered-$group.env"
    # shellcheck disable=SC2016
    printf 'printf "%s\\n" >>"$TEST_TRACE"\n' "$group" >>"$TEST_ROOT/ordered-$group.env"
done
printf 'PGUSER=ordered_user\n' >>"$TEST_ROOT/ordered-connection.env"
invoke hammerdb --check --connection-env "$TEST_ROOT/ordered-connection.env" \
    --benchmark-env "$TEST_ROOT/ordered-benchmark.env" --run-env "$TEST_ROOT/ordered-run.env"
assert test "$status" == 0
assert test "$(cat "$TEST_ROOT/trace")" == $'connection\nbenchmark\nrun'
assert grep -q '"superuser": "ordered_user"' "$TEST_ROOT/current.log"
pass 'configs are sourced exactly once in connection-benchmark-run order with exported plain assignments'

for body in 'return 27' $'false\ntrue'
do
    cp -- "$TEST_ROOT/connection.env" "$TEST_ROOT/failing.env"
    printf '%s\n' "$body" >>"$TEST_ROOT/failing.env"
    invoke hammerdb --check --connection-env "$TEST_ROOT/failing.env"
    assert test "$status" != 0
    assert test ! -s "$TEST_ROOT/trace"
done
ln -s connection.env "$TEST_ROOT/private-alias.env"
invoke hammerdb --check --connection-env "$TEST_ROOT/private-alias.env"
assert test "$status" == 0
assert test ! -s "$TEST_ROOT/trace"
pass 'configuration failures stop immediately and aliases to real private files remain valid'

invoke --help
assert test "$status" == 0
if grep -Eiq 'legacy|old flags|migration' "$TEST_ROOT/current.log";
then
    printf 'Help contains an obsolete interface notice\n' >&2
    exit 1
fi

pass 'help describes only the current interface'

invoke sysbench --check
assert test "$status" != 0
assert grep -q 'Unsupported benchmark type' "$TEST_ROOT/current.log"
for option in -b -c -C -e -E -H -I -i -l -n -O -P -r -S -Z -t --unknown; do
    invoke hammerdb "$option"
    assert test "$status" == 2
    assert grep -Fxq "ERROR: Unknown option '$option' (see --help)" "$TEST_ROOT/current.log"
    assert test ! -s "$TEST_ROOT/trace"
done
pass 'unsupported tools and options fail explicitly without special handling'

EXTRA_ENV=("RUN_ITERATIONS=0")
invoke hammerdb --check
assert test "$status" != 0
EXTRA_ENV=("PGDATABASE=postgresql://elsewhere/wrong")
invoke hammerdb --check
assert test "$status" != 0
pass 'invalid settings and connection-string targets fail'

EXTRA_ENV=("PG_DBASE=ignored_database" "PG_USER=ignored_user" "PG_SUPERUSER=ignored_admin"
    "PG_DEFAULTDBASE=ignored_database" "PG_COUNT_WARE=0" "PG_FIRST_WARE=0"
    "PG_NUM_VU=0" "PG_VU=0" "PG_RAMPUP=-1" "PG_DURATION=0"
    "ENABLE_CITUS=invalid" "CITUS_LB_PORT=0")
invoke hammerdb --check
assert test "$status" == 0
assert grep -Fxq 'Target: benchmark_user@mock-db.invalid:5432/benchmark_db' "$TEST_ROOT/current.log"
assert test ! -s "$TEST_ROOT/trace"
invoke hammerdb
assert test "$status" == 0
directory="$(last_run)"
assert jq -e '.database == "benchmark_db" and .user == "benchmark_user"' "$directory/target.json"
assert jq -e '.warehouses == 2 and .build_vus == 1 and .run_vus == 1 and .citus == false' "$directory/benchmark.json"
assert grep -q 'psql|database=benchmark_db|user=benchmark_user' "$TEST_ROOT/trace"
pass 'obsolete environment names do not affect validation, target, or workload'

cp -- "$TEST_ROOT/connection.env" "$TEST_ROOT/selected-connection.env"
cp -- "$TEST_ROOT/benchmark.env" "$TEST_ROOT/selected-benchmark.env"
cp -- "$TEST_ROOT/run.env" "$TEST_ROOT/selected-run.env"
printf 'PGDATABASE=selected_db\nPGUSER=selected_user\n' >>"$TEST_ROOT/selected-connection.env"
printf 'HDB_RUN_VUS=2\n' >>"$TEST_ROOT/selected-benchmark.env"
printf 'RUN_ITERATIONS=1\nRUN_LABEL=selected\n' >>"$TEST_ROOT/selected-run.env"
EXTRA_ENV=("CONNECTION_ENV_FILE=/missing" "BENCHMARK_ENV_FILE=/missing" "RUN_ENV_FILE=/missing")
invoke hammerdb --connection-env "$TEST_ROOT/selected-connection.env" \
    --benchmark-env "$TEST_ROOT/selected-benchmark.env" --run-env "$TEST_ROOT/selected-run.env"
assert test "$status" == 0
directory="$(last_run)"
assert jq -e '.iterations == 1 and .completed_iterations == 1 and .status == "succeeded"' "$directory/run.json"
assert jq -e '.database == "selected_db" and .server_version == "17.42-test-server"' "$directory/target.json"
assert grep -q 'psql|database=selected_db|user=selected_user' "$TEST_ROOT/trace"
assert grep -q 'run|warehouses=2|first=1|vus=2' "$TEST_ROOT/trace"
assert jq -e '.metrics.nopm.value == 123.5 and .metrics.tpm.value == 456' "$directory/iteration-1/result.json"
pass 'three config overrides, CLI selector precedence, canonical target, and native metrics'

EXTRA_ENV=("HAMMERDB_HOME=./hammer db" "RUN_OUTPUT_ROOT=./relative results" "RUN_ITERATIONS=01"
    "HDB_WAREHOUSES=08" "HDB_BUILD_VUS=02" "HDB_RUN_VUS=02")
invoke hammerdb
assert test "$status" == 0
assert test -d "$TEST_ROOT/relative results"
assert grep -q 'run|warehouses=8|first=1|vus=2' "$TEST_ROOT/trace"
pass 'relative paths with spaces and decimal normalization'

EXTRA_ENV=("RUN_PREPARE_MODE=once" "TEST_STOCK_HAMMERDB=1")
invoke hammerdb
assert test "$status" == 0
assert test "$(trace_count '^prepare|')" == 1
assert test "$(trace_count '^run|')" == 2
pass 'prepare once and run twice using stock non-Citus dictionaries'

EXTRA_ENV=("HDB_CITUS_COMPAT=true" "RUN_PREPARE_MODE=once" "RUN_ITERATIONS=1")
invoke hammerdb
assert test "$status" == 0
directory="$(last_run)"
assert jq -e '.citus == true' "$directory/benchmark.json"
assert test "$(trace_count '^prepare|')" == 1
assert test "$(trace_count '^run|')" == 1
pass 'HDB_CITUS_COMPAT configures both preparation and execution while preserving the metadata schema'

EXTRA_ENV=("HDB_CITUS_COMPAT=invalid")
invoke hammerdb --check
assert test "$status" != 0
assert grep -q 'HDB_CITUS_COMPAT' "$TEST_ROOT/current.log"
assert test ! -s "$TEST_ROOT/trace"
EXTRA_ENV=("HDB_CITUS_COMPAT=true" "TEST_STOCK_HAMMERDB=1")
invoke hammerdb --prepare
assert test "$status" != 0
assert grep -q 'Citus mode requires.*pg_azure_citus' "$TEST_ROOT/current.log"
pass 'Citus compatibility rejects invalid settings and unsupported HammerDB dictionaries'

EXTRA_ENV=("RUN_PREPARE_MODE=each")
invoke hammerdb --check
assert test "$status" != 0
EXTRA_ENV=("RUN_PREPARE_MODE=each" "HDB_RESET_SCHEMA=true" "RUN_ALLOW_DESTRUCTIVE=true")
invoke hammerdb
assert test "$status" == 0
assert test "$(trace_count '^prepare|')" == 2
assert test "$(trace_count 'file=hammerdb_cleanup_citus.sql')" == 2
assert test "$(trace_count '^run|')" == 2
pass 'each-iteration preparation requires opt-in and resets/rebuilds twice'

EXTRA_ENV=()
invoke hammerdb --cleanup
assert test "$status" != 0
assert test ! -s "$TEST_ROOT/trace"
EXTRA_ENV=("RUN_ALLOW_DESTRUCTIVE=true" "HDB_SUPERUSER=maintenance_user")
invoke hammerdb --cleanup
assert test "$status" == 0
assert grep -q 'psql|database=benchmark_db|user=maintenance_user|file=hammerdb_cleanup_citus.sql' "$TEST_ROOT/trace"
assert test "$(trace_count '^run|')" == 0
pass 'explicit cleanup requires permission and keeps the database target with a distinct admin role'

EXTRA_ENV=("RUN_ITERATIONS=1" "TEST_HDB_FAIL=73")
invoke hammerdb --prepare
assert test "$status" == 73
directory="$(last_run)"
assert jq -e '.status == "failed" and .exit_code == 73' "$directory/run.json"
EXTRA_ENV=("TEST_HDB_FAIL=37")
invoke hammerdb
assert test "$status" == 37
assert test "$(trace_count '^run|')" == 1
pass 'native prepare/run failures preserve status and stop subsequent iterations'

for setting in TEST_MISSING_COMPLETION TEST_NO_METRICS TEST_DUPLICATE_METRICS TEST_VU_FAIL; do
    EXTRA_ENV=("$setting=1")
    invoke hammerdb
    assert test "$status" != 0
    directory="$(last_run)"
    assert jq -e '.status == "failed" and .completed_iterations == 0' "$directory/run.json"
done
pass 'missing completion, missing/ambiguous metrics, and failed virtual users cannot report success'

EXTRA_ENV=("TEST_PSQL_FAIL=true")
invoke hammerdb
assert test "$status" != 0
assert test "$(trace_count '^run|')" == 0
EXTRA_ENV=("TEST_LOGGER_FAIL=true")
invoke hammerdb
assert test "$status" == 74
directory="$(last_run)"
assert jq -e '.status == "failed"' "$directory/run.json"
pass 'database and log-writer failures propagate'

EXTRA_ENV=("TEST_LOGGER_FAIL_EARLY=true" "TEST_BLOCK=1")
start="$SECONDS"
invoke hammerdb
assert test "$status" != 0
assert test "$((SECONDS - start))" -lt 10
directory="$(last_run)"
assert jq -e '.status == "failed" and .completed_iterations == 0' "$directory/run.json"
pass 'early logging failure cancels a blocked workload without a configured timeout'

EXTRA_ENV=("HDB_WAREHOUSES=0" "HDB_DISTRIBUTED_LOAD=true")
invoke hammerdb --prepare
assert test "$status" == 0
assert grep -q 'prepare|warehouses=0|first=1' "$TEST_ROOT/trace"
EXTRA_ENV=("HDB_WAREHOUSES=0")
invoke hammerdb --check
assert test "$status" != 0
EXTRA_ENV=("HDB_WAREHOUSES=0" "HDB_DISTRIBUTED_LOAD=true" "TEST_NO_RANGE_SUPPORT=1")
invoke hammerdb --prepare
assert test "$status" != 0
assert grep -q 'Distributed loading requires' "$TEST_ROOT/current.log"
pass 'zero-warehouse finalization stays inside the HammerDB adapter and requires compatible tooling'

password="literal p'a\$ss[brackets]; word"
EXTRA_ENV=("PGPASSWORD=$password" "RUN_ITERATIONS=1")
invoke hammerdb
assert test "$status" == 0
directory="$(last_run)"
if grep -RFq -- "$password" "$directory";
then
    printf 'Secret was written into a run artifact\n' >&2
    exit 1
fi

assert test "$(stat -c %a "$directory")" == 700
pass 'Tcl preserves special-character credentials without persisting them in artifacts'

{
    printf 'PGPASSWORD=%q\n' "$password"
    cat -- "$TEST_ROOT/connection.env"
} >"$TEST_ROOT/password.env"

printf '%s' "$password" >"$TEST_ROOT/expected-password"
EXTRA_ENV=("RUN_ITERATIONS=1")
invoke hammerdb --prepare --connection-env "$TEST_ROOT/password.env"
assert test "$status" == 0
assert test "$(trace_count '^prepare|')" == 1
prepare_directory="$(last_run)"
invoke hammerdb --connection-env "$TEST_ROOT/password.env"
assert test "$status" == 0
assert test "$(trace_count '^run|')" == 1
directory="$(last_run)"
if grep -RFq -- "$password" "$prepare_directory" "$directory";
then
    printf 'A private connection password was written into a run artifact\n' >&2
    exit 1
fi

rm -- "$TEST_ROOT/expected-password"
pass 'a private connection password reaches Tcl preparation and execution unchanged without leaking'

EXTRA_ENV=("PGHOST=127.0.0.1" "PGPORT=55432" "PG_TARGET_MODE=temporary" "PG_CONFIG=$TEST_ROOT/bin/pg_config"
    "RUN_PREPARE_MODE=once" "PG_REMOVE_DATA=true")
invoke hammerdb
assert test "$status" == 0
assert test "$(trace_count '^initdb|')" == 1
assert test "$(trace_count '^pg_ctl|start|')" == 1
assert test "$(trace_count '^pg_ctl|stop|')" == 1
assert test "$(trace_count '^prepare|')" == 1
assert test "$(trace_count '^run|')" == 2
directory="$(last_run)"
assert test ! -e "$directory/postgresql/data"
assert jq -e '.server_version == "17.42-test-server"' "$directory/target.json"
pass 'temporary cluster is owned once per run, prepared once, stopped, and optionally removed'

cp -- "$ROOT/initdb.env.sample" "$TEST_ROOT/server.env"
# shellcheck disable=SC2016
printf 'printf "server_config\\n" >>"$TEST_TRACE"\n' >>"$TEST_ROOT/server.env"
EXTRA_ENV+=("PG_SERVER_ENV_FILE=$TEST_ROOT/server.env")
invoke hammerdb
assert test "$status" == 0
assert test "$(trace_count '^server_config$')" == 1
assert grep -q 'pg_ctl|start|.*max_connections=100' "$TEST_ROOT/trace"
assert grep -q 'pg_ctl|start|.*checkpoint_timeout=30min' "$TEST_ROOT/trace"
pass 'a private copy of the optional server template is sourced once and applied'

EXTRA_ENV+=("TEST_HDB_FAIL=73")
invoke hammerdb
assert test "$status" == 73
assert test "$(trace_count '^pg_ctl|stop|')" == 1
directory="$(last_run)"
assert test -d "$directory/postgresql/data"
pass 'failed temporary runs stop PostgreSQL and retain data for diagnosis'

EXTRA_ENV=("PGHOST=127.0.0.1" "PG_TARGET_MODE=temporary" "PG_CONFIG=$TEST_ROOT/bin/pg_config"
    "RUN_PREPARE_MODE=once" "TEST_PG_START_FAIL=true")
invoke hammerdb
assert test "$status" != 0
assert test "$(trace_count '^pg_ctl|stop|')" == 1
pass 'partial local-server startup is cleaned up'

printf 'SELECT deliberately_invalid;\n' >"$TEST_ROOT/init.sql"
EXTRA_ENV=("PGHOST=127.0.0.1" "PG_TARGET_MODE=temporary" "PG_CONFIG=$TEST_ROOT/bin/pg_config"
    "RUN_PREPARE_MODE=once" "PG_INIT_SQL=$TEST_ROOT/init.sql" "TEST_SQL_FILE_FAIL=$TEST_ROOT/init.sql")
invoke hammerdb
assert test "$status" == 39
assert test "$(trace_count '^pg_ctl|stop|')" == 1
assert test "$(trace_count '^prepare|')" == 0
pass 'initialization SQL failure aborts before preparation and stops the owned cluster'

EXTRA_ENV=("PGHOST=127.0.0.1" "PG_TARGET_MODE=temporary" "PG_CONFIG=$TEST_ROOT/bin/pg_config"
    "RUN_PREPARE_MODE=once" "PG_REMOVE_DATA=true" "TEST_PG_STOP_FAIL=true")
invoke hammerdb
assert test "$status" != 0
directory="$(last_run)"
assert test -d "$directory/postgresql/data"
assert jq -e '.status == "failed"' "$directory/run.json"
pass 'shutdown failure prevents success and data removal'

EXTRA_ENV=("RUN_TIMEOUT_SECONDS=1" "TEST_BLOCK=1")
start=$SECONDS
invoke hammerdb
assert test "$status" != 0
assert test "$((SECONDS - start))" -lt 10
directory="$(last_run)"
assert jq -e '.status == "failed" and .completed_iterations == 0' "$directory/run.json"
pass 'timeout bounds a blocked adapter'

rm -f -- "$TEST_ROOT/blocking"
: >"$TEST_ROOT/trace"
env -i "${BASE_ENV[@]}" "TEST_BLOCK=1" "PGHOST=127.0.0.1" "PG_TARGET_MODE=temporary" \
    "PG_CONFIG=$TEST_ROOT/bin/pg_config" "RUN_PREPARE_MODE=once" \
    bash "$ROOT/wrapper.sh" hammerdb >"$TEST_ROOT/current.log" 2>&1 &
wrapper_pid=$!
for ((attempt = 0; attempt < 100; attempt++)); do
    [[ ! -f "$TEST_ROOT/blocking" ]] || break
    sleep 0.05
done
assert test -f "$TEST_ROOT/blocking"
kill -TERM "$wrapper_pid"
status=0
wait "$wrapper_pid" || status=$?
assert test "$status" == 143
directory="$(last_run)"
assert jq -e '.status == "interrupted"' "$directory/run.json"
assert test "$(trace_count '^pg_ctl|stop|')" == 1
IFS= read -r native_pid <"$TEST_ROOT/hammerdb.pid"
native_state="$(ps -o stat= -p "$native_pid" || :)"
[[ -z "$native_state" || "$native_state" == Z* ]] ||
{
    printf 'Native adapter process survived cancellation: %s\n' "$native_pid" >&2
    exit 1
}

pass 'cancellation records interrupted status and stops owned adapter/server processes'

EXTRA_ENV=("RUN_ITERATIONS=1")
invoke hammerdb
assert test "$status" == 0
first_directory="$(last_run)"
invoke hammerdb
assert test "$status" == 0
second_directory="$(last_run)"
assert test "$first_directory" != "$second_directory"
assert test -f "$first_directory/iteration-1/result.json"
pass 'repeated invocations preserve earlier artifacts'

source "$ROOT/lib/common.sh"
text=$'quotes " slash \\ newline\n tab\t control\001'
encoded="$(json_string "$text")"
# shellcheck disable=SC2016
assert jq -en --arg text "$text" --argjson actual "$encoded" '$text == $actual'
pass 'JSON metadata escaping'

printf '\n%s runner checks passed.\n' "$passed"
