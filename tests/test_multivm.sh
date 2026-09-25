#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
for command in tclsh jq; do
    command -v "$command" >/dev/null || { printf 'Missing test dependency: %s\n' "$command" >&2; exit 1; }

done
umask 077
[[ -x "$ROOT/tests/fixtures/fake-command.sh" && -x "$ROOT/tests/fixtures/fake-ssh.sh" ]] ||
{
    printf 'Test fixtures must be executable; refusing to fall back to native commands\n' >&2
    exit 1
}

mkdir -p -- "$ROOT/tests/.work"
TEST_ROOT="$ROOT/tests/.work/multivm-$$-$RANDOM"
mkdir -- "$TEST_ROOT"
trap '
    if [[ "$?" == 0 ]];
    then
        rm -r -- "$TEST_ROOT";
    else
        printf "Fixtures preserved: %s\n" "$TEST_ROOT" >&2;
    fi

' EXIT
mkdir -p "$TEST_ROOT/bin" "$TEST_ROOT/hammer db" "$TEST_ROOT/home" "$TEST_ROOT/packets"
for command in psql pg_config initdb pg_ctl tee; do
    ln -s "$ROOT/tests/fixtures/fake-command.sh" "$TEST_ROOT/bin/$command"
done
ln -s "$ROOT/tests/fixtures/fake-ssh.sh" "$TEST_ROOT/bin/ssh"
ln -s "$ROOT/tests/fixtures/fake-command.sh" "$TEST_ROOT/hammer db/hammerdbcli"
REMOTE_REPO="$TEST_ROOT/remote repo'quoted"
mkdir -p "$REMOTE_REPO/lib" "$REMOTE_REPO/hammerdb"
cp -- "$ROOT/wrapper.sh" "$ROOT/connection.env.sample" "$ROOT/run.env.sample" "$REMOTE_REPO/"
cp -- "$ROOT/lib/"*.sh "$REMOTE_REPO/lib/"
cp -- "$ROOT/hammerdb/hammerdb.sh" "$ROOT/hammerdb/hammerdb.env.sample" "$ROOT/hammerdb/tpcc.tcl" \
    "$ROOT/hammerdb/hammerdb_cleanup_citus.sql" "$ROOT/hammerdb/hammerdb_maintenance_citus.sql" \
    "$REMOTE_REPO/hammerdb/"
printf 'keep-shared-environment\n' >"$REMOTE_REPO/myenv.sh"
printf 'keep-private-connection\n' >"$REMOTE_REPO/connection.local.env"
printf '# Runners\r\nrunner-a\r\nrunner-b\r\nrunner-c\r\n' >"$TEST_ROOT/hosts"
cp -- "$ROOT/hammerdb/hammerdb.env.sample" "$TEST_ROOT/benchmark.env"
cp -- "$ROOT/run.env.sample" "$TEST_ROOT/run.env"
printf "export HDB_SUPERUSER_PASSWORD=''\n" >>"$TEST_ROOT/benchmark.env"
printf 'export RUN_ALLOW_DESTRUCTIVE=true\n' >>"$TEST_ROOT/run.env"
password=$'literal password \'"$[brackets];\nsecond line'
{
    printf 'PGPASSWORD=%q\n' "$password"
    cat -- "$ROOT/connection.env.sample"
    printf 'export PGDATABASE=loader_db\nexport PGUSER=loader_user\n'
} >"$TEST_ROOT/connection.env"

printf '%s' "$password" >"$TEST_ROOT/expected-password"
BASE_ENV=(
    "PATH=$TEST_ROOT/bin:/usr/bin:/bin" "HOME=$TEST_ROOT/home"
    "TEST_ROOT=$TEST_ROOT" "TEST_BIN=$TEST_ROOT/bin" "TEST_PROJECT_ROOT=$ROOT"
    "TEST_TCLSH=$(command -v tclsh)" "TEST_REAL_TEE=$(command -v tee)"
    "TEST_TRACE=$TEST_ROOT/trace" "TEST_SSH_ARGUMENTS=$TEST_ROOT/ssh-arguments"
    "TEST_EXPECT_EMPTY_ADMIN=true"
    "CONNECTION_ENV_FILE=$TEST_ROOT/connection.env"
    "BENCHMARK_ENV_FILE=$TEST_ROOT/benchmark.env" "RUN_ENV_FILE=$TEST_ROOT/run.env"
    "PGHOST=mock-db.invalid" "PGUSER=initial_user" "PGDATABASE=initial_db"
    "HAMMERDB_HOME=$TEST_ROOT/hammer db" "HDB_BUILD_VUS=4"
    "RUN_OUTPUT_ROOT=$TEST_ROOT/results" "LOG_DIR=$TEST_ROOT/logs"
)
EXTRA_ENV=("HDB_CITUS_COMPAT=true" "PG_DBASE=ignored_database" "PG_USER=ignored_user"
    "PG_COUNT_WARE=0" "PG_NUM_VU=0")
DEFAULT_CONFIG_ARGS=(--connection-env "$TEST_ROOT/connection.env"
    --benchmark-env "$TEST_ROOT/benchmark.env" --run-env "$TEST_ROOT/run.env")
CONFIG_ARGS=("${DEFAULT_CONFIG_ARGS[@]}")
passed=0

assert()
{
    if ! "$@";
    then
        printf 'Assertion failed: ' >&2
        printf '%q ' "$@" >&2
        printf '\nSee %s/current.log\n' "$TEST_ROOT" >&2
        exit 1
    fi
}

invoke()
{
    : >"$TEST_ROOT/trace"
    : >"$TEST_ROOT/ssh-arguments"
    set +e
    env -i "${BASE_ENV[@]}" "${EXTRA_ENV[@]}" \
        bash "$ROOT/setup/run_tpcc_load_multivm.sh" \
        "$TEST_ROOT/hosts" "$REMOTE_REPO" "$@" \
        "${CONFIG_ARGS[@]}" >"$TEST_ROOT/current.log" 2>&1
    status=$?
    set -e
}

pass()
{
    passed=$((passed + 1))
    printf 'PASS: %s\n' "$1"
}

invoke 10
assert test "$status" == 0
assert grep -q 'runner-a first=2 count=3 last=4' "$TEST_ROOT/current.log"
assert grep -q 'runner-b first=5 count=3 last=7' "$TEST_ROOT/current.log"
assert grep -q 'runner-c first=8 count=3 last=10' "$TEST_ROOT/current.log"
assert grep -q '^prepare|warehouses=1|first=1|vus=1|host=runner-a|phase=bootstrap$' "$TEST_ROOT/trace"
assert grep -q '^prepare|warehouses=3|first=2|vus=3|host=runner-a|phase=data$' "$TEST_ROOT/trace"
assert grep -q '^prepare|warehouses=3|first=5|vus=3|host=runner-b|phase=data$' "$TEST_ROOT/trace"
assert grep -q '^prepare|warehouses=3|first=8|vus=3|host=runner-c|phase=data$' "$TEST_ROOT/trace"
assert grep -q '^prepare|warehouses=0|first=1|vus=1|host=runner-a|phase=finalize$' "$TEST_ROOT/trace"
assert test "$(grep -c '^ssh|.*|--check|preflight$' "$TEST_ROOT/trace")" == 3
for host in runner-a runner-b runner-c
do
    assert grep -Fq '"citus": true,' "$TEST_ROOT/logs/preflight.$host.log"
done
# shellcheck disable=SC2016
assert awk -F'|' '
    $1 == "ssh" && $4 == "preflight" { checked++ }
    $1 == "ssh" && $4 == "cleanup" { if (checked != 3) exit 1; cleaned++ }
    END { if (cleaned != 1) exit 1 }
' "$TEST_ROOT/trace"
assert grep -q 'psql|database=loader_db|user=loader_user' "$TEST_ROOT/trace"
assert test ! -e "$REMOTE_REPO/connection.env"
assert test ! -e "$REMOTE_REPO/hammerdb/hammerdb.env"
assert test ! -e "$REMOTE_REPO/run.env"
pass 'complete serialized configuration drives remote preparation without any shipped runtime defaults'

assert test -z "$(find "$TEST_ROOT/logs" "$REMOTE_REPO/benchmark-results" -name '.request-*' -print -quit)"
assert test -z "$(find "$TEST_ROOT/packets" -mindepth 1 -print -quit)"
if grep -Fq 'literal password' "$TEST_ROOT/ssh-arguments";
then
    printf 'A credential appeared in SSH command arguments\n' >&2
    exit 1
fi

if grep -RFq 'literal password' "$TEST_ROOT/results" "$TEST_ROOT/logs";
then
    printf 'A credential appeared in persistent run artifacts\n' >&2
    exit 1
fi

assert grep -Fxq 'keep-shared-environment' "$REMOTE_REPO/myenv.sh"
assert grep -Fxq 'keep-private-connection' "$REMOTE_REPO/connection.local.env"
pass 'file-sourced credentials and explicit empty admin password survive private stdin configuration'

EXTRA_ENV=("TEST_FAIL_PREFLIGHT_HOST=runner-b")
invoke 10
assert test "$status" != 0
if grep -q '|cleanup$' "$TEST_ROOT/trace";
then
    printf 'Cleanup ran after preflight failed\n' >&2
    exit 1
fi

assert grep -q 'Remote completion is unknown' "$TEST_ROOT/current.log"
pass 'failed runner preflight prevents destructive cleanup'

EXTRA_ENV=("TEST_FAIL_DATA_HOST=runner-b")
invoke 10
assert test "$status" != 0
if grep -q '|finalize$' "$TEST_ROOT/trace";
then
    printf 'Finalization ran after a data loader failed\n' >&2
    exit 1
fi

assert grep -q 'refusing post-data DDL' "$TEST_ROOT/current.log"
assert grep -q 'exit_code=23' "$TEST_ROOT/logs/data.runner-b.log.status"
assert test -z "$(find "$TEST_ROOT/logs" "$REMOTE_REPO/benchmark-results" -name '.request-*' -print -quit)"
pass 'parallel failures retain true status and block finalization'

EXTRA_ENV=()
printf 'runner-a\n' >"$TEST_ROOT/hosts"
invoke 1
assert test "$status" == 0
assert test "$(grep -c '^prepare|' "$TEST_ROOT/trace")" == 2
if grep -q '|data$' "$TEST_ROOT/trace";
then
    printf 'A one-warehouse load started a redundant data phase\n' >&2
    exit 1
fi

pass 'one runner and one warehouse only bootstrap and finalize'

printf 'runner-a\nrunner-b\nrunner-c\n' >"$TEST_ROOT/hosts"
invoke 3
assert test "$status" == 0
assert grep -q '^prepare|warehouses=1|first=2|vus=1|host=runner-b|phase=data$' "$TEST_ROOT/trace"
assert grep -q '^prepare|warehouses=1|first=3|vus=1|host=runner-c|phase=data$' "$TEST_ROOT/trace"
pass 'warehouse count equal to runner count skips the empty first slice'

printf 'runner-a\nrunner-a\n' >"$TEST_ROOT/hosts"
invoke 10
assert test "$status" != 0
assert test ! -s "$TEST_ROOT/trace"
printf 'runner-a;not-a-command\n' >"$TEST_ROOT/hosts"
invoke 10
assert test "$status" != 0
assert test ! -s "$TEST_ROOT/trace"
pass 'duplicate and malformed hosts are rejected before SSH'

printf 'runner-a\n' >"$TEST_ROOT/hosts"
# shellcheck disable=SC2016
printf 'printf "sample executed\\n" >>"$TEST_TRACE"\n' >"$TEST_ROOT/trap.sample"
ln -s trap.sample "$TEST_ROOT/sample-alias.env"
for group in connection benchmark run
do
    for file in "$TEST_ROOT/trap.sample" "$TEST_ROOT/sample-alias.env"
    do
        EXTRA_ENV=()
        CONFIG_ARGS=("${DEFAULT_CONFIG_ARGS[@]}" "--$group-env" "$file")
        invoke 1
        assert test "$status" != 0
        assert grep -iq 'sample' "$TEST_ROOT/current.log"
        assert test ! -s "$TEST_ROOT/trace"
        EXTRA_ENV=("${group^^}_ENV_FILE=$file")
        CONFIG_ARGS=()
        invoke 1
        assert test "$status" != 0
        assert test ! -s "$TEST_ROOT/trace"
    done
done
CONFIG_ARGS=("${DEFAULT_CONFIG_ARGS[@]}")
pass 'distributed CLI/environment selectors reject templates and sample aliases before SSH'

EXTRA_ENV=("PG_SERVER_ENV_FILE=$ROOT/initdb.env.sample")
invoke 1
assert test "$status" != 0
assert grep -iq 'sample' "$TEST_ROOT/current.log"
assert test ! -s "$TEST_ROOT/trace"
pass 'distributed loading rejects a server tuning sample before contacting runners'

EXTRA_ENV=()
printf 'RUN_ALLOW_DESTRUCTIVE=false\n' >>"$TEST_ROOT/run.env"
invoke 10
assert test "$status" != 0
assert test ! -s "$TEST_ROOT/trace"
pass 'distributed cleanup requires explicit destructive permission'

printf '\n%s multi-VM checks passed.\n' "$passed"
