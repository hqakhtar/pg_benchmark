#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
source "$ROOT/lib/common.sh"

usage()
{
    cat <<'EOF'
Usage: run_tpcc_load_multivm.sh HOSTS_FILE REMOTE_REPO [TOTAL_WAREHOUSES]
    [--connection-env FILE] [--benchmark-env FILE] [--run-env FILE]

Load the three configuration groups used by wrapper.sh hammerdb, check all
runners, clean up once, bootstrap warehouse 1, load disjoint warehouse ranges,
then finalize post-data DDL. This does not run benchmark iterations.

Requires PG_TARGET_MODE=existing and RUN_ALLOW_DESTRUCTIVE=true.
TOTAL_WAREHOUSES defaults to HDB_WAREHOUSES; build VUs come from HDB_BUILD_VUS
and are capped to each slice. HAMMERDB_HOME, PG_PSQL, PGPASSFILE (if used),
and RUN_OUTPUT_ROOT are paths on the runners.

Optional pilot settings:
  LOG_DIR   Local orchestration log directory (default: a unique logs/load-*).
  SSH_OPTS  Additional whitespace-separated SSH arguments (not shell code).

Use the matching CONNECTION_ENV_FILE, BENCHMARK_ENV_FILE, RUN_ENV_FILE
variables instead of file flags if preferred. No shared remote config is edited.
Selected files must be complete private configurations, not .sample templates.
EOF
}

usage_error()
{
    printf 'ERROR: %s\n' "$*" >&2
    exit 2
}

connection_file="${CONNECTION_ENV_FILE:-}"
benchmark_file="${BENCHMARK_ENV_FILE:-}"
run_file="${RUN_ENV_FILE:-}"
arguments=()
while (($#)); do
    case "$1" in
        -h|--help) usage; exit 0 ;;
        --connection-env|--benchmark-env|--run-env)
            [[ $# -ge 2 && -n "$2" && "$2" != --* ]] || usage_error "$1 requires a filename"
            case "$1" in
                --connection-env) connection_file="$2" ;;
                --benchmark-env) benchmark_file="$2" ;;
                --run-env) run_file="$2" ;;
            esac
            shift 2
            ;;
        -*) usage_error "Unknown option: $1" ;;
        *) arguments+=("$1"); shift ;;
    esac
done
[[ ${#arguments[@]} -ge 2 && ${#arguments[@]} -le 3 ]] || { usage >&2; exit 2; }

load_configuration "$ROOT" hammerdb "$connection_file" "$benchmark_file" "$run_file"
HOSTS_FILE="${arguments[0]}"
REMOTE_REPO="${arguments[1]}"
TOTAL_WAREHOUSES="${arguments[2]:-$HDB_WAREHOUSES}"
validate_integer TOTAL_WAREHOUSES "$TOTAL_WAREHOUSES" 1
validate_integer HDB_BUILD_VUS "$HDB_BUILD_VUS" 1
[[ "$PG_TARGET_MODE" == existing ]] || { fail "Distributed loading requires PG_TARGET_MODE=existing"; exit 1; }

[[ "$RUN_ALLOW_DESTRUCTIVE" == true ]] || { fail "Distributed loading requires RUN_ALLOW_DESTRUCTIVE=true"; exit 1; }

[[ -n "$REMOTE_REPO" && -r "$HOSTS_FILE" ]] || { fail "Provide a readable hosts file and remote repository path"; exit 1; }

require_command ssh

read_runner_hosts "$HOSTS_FILE"
((TOTAL_WAREHOUSES >= ${#HOSTS[@]})) || { fail "Warehouse count must be at least the runner count"; exit 1; }

SSH_ARGUMENTS=(-o BatchMode=yes -o StrictHostKeyChecking=accept-new -o ConnectTimeout=15)
if [[ -n "${SSH_OPTS:-}" ]];
then
    read -r -a extra_ssh_arguments <<<"$SSH_OPTS"
    SSH_ARGUMENTS+=("${extra_ssh_arguments[@]}")
fi

umask 077
if [[ -n "${LOG_DIR:-}" ]];
then
    LOG_DIR="$(absolute_path "$LOG_DIR")"
    mkdir -p -- "$LOG_DIR"
else
    mkdir -p ./logs
    LOG_DIR="$(absolute_path ./logs)/load-$(date -u +%Y%m%dT%H%M%SZ)-$$-$RANDOM"
    mkdir -- "$LOG_DIR"
fi

BATCH_ID="$(date -u +%Y%m%dT%H%M%SZ)-$$"
WORK_ROOT="$RUN_OUTPUT_ROOT"

safe_host()
{
    printf '%s' "$1" | tr -c 'A-Za-z0-9._-' '_'
}

# Quote values as Bash literals, preserving spaces and metacharacters remotely.
serialize_environment()
{
    local key
    for key in "$@"; do
        if [[ -v "$key" ]];
        then
            local -n setting="$key"
            printf 'export %s=%q\n' "$key" "$setting" || return 1
            unset -n setting
        fi

    done
}

# Each phase gets complete private configs without changing shared runner files.
remote_request()
{
    local local_host="$1" phase="$2" action="$3"
    local connection_payload benchmark_payload run_payload
    local -x HDB_FIRST_WAREHOUSE="$4" HDB_WAREHOUSES="$5" HDB_BUILD_VUS="$6"
    local -x HDB_RUN_VUS="$HDB_BUILD_VUS"
    local -x HDB_DISTRIBUTED_LOAD=true HDB_RESET_SCHEMA=false
    local -x RUN_PREPARE_MODE=reuse RUN_ITERATIONS=1
    local -x RUN_OUTPUT_ROOT
    RUN_OUTPUT_ROOT="$WORK_ROOT/load-$BATCH_ID/$(safe_host "$local_host")/$phase"
    connection_payload="$(serialize_environment PGHOST PGPORT PGDATABASE PGUSER PGSSLMODE \
        PGCONNECT_TIMEOUT PGMAINTENANCE_DB PG_TARGET_MODE PG_PSQL PGPASSWORD PGPASSFILE \
        PG_CONFIG PG_SERVER_ENV_FILE PG_INIT_SQL PG_REMOVE_DATA)" || return 1
    benchmark_payload="$(serialize_environment HAMMERDB_HOME HDB_WORKLOAD HDB_WAREHOUSES \
        HDB_FIRST_WAREHOUSE HDB_BUILD_VUS HDB_RUN_VUS HDB_RAMPUP_MINUTES HDB_DURATION_MINUTES \
        HDB_SUPERUSER HDB_SUPERUSER_PASSWORD HDB_CITUS_COMPAT HDB_CITUS_LOADBALANCER_PORT \
        HDB_CITUS_DIRECT_WORKERS HDB_DISTRIBUTED_LOAD HDB_RESET_SCHEMA HDB_MAINTENANCE HDB_VACUUM)" || return 1
    run_payload="$(serialize_environment RUN_ITERATIONS RUN_OUTPUT_ROOT RUN_LABEL \
        RUN_PREPARE_MODE RUN_COOLDOWN_SECONDS RUN_TIMEOUT_SECONDS RUN_ALLOW_DESTRUCTIVE)" || return 1
    printf 'remote_repo=%q\nremote_action=%q\nrequest_phase=%q\n' "$REMOTE_REPO" "$action" "$phase" || return 1
    printf 'connection_payload=%q\nbenchmark_payload=%q\nrun_payload=%q\n' \
        "$connection_payload" "$benchmark_payload" "$run_payload" || return 1
    # Leave remote paths and process IDs for the remote shell to expand.
    cat <<'REMOTE'
set -euo pipefail
umask 077
cd -- "$remote_repo"
mkdir -p -- ./benchmark-results
request_dir="$PWD/benchmark-results/.request-$$-$RANDOM"
mkdir -- "$request_dir"
worker_pid=""
cleanup_request()
{
    rm -f -- "$request_dir/connection.env" "$request_dir/benchmark.env" "$request_dir/run.env"
    rmdir -- "$request_dir"
}

interrupt_request()
{
    trap - INT TERM HUP
    if [[ -n "$worker_pid" ]];
    then
        kill -TERM "$worker_pid" 2>/dev/null || :
        wait "$worker_pid" 2>/dev/null || :
    fi

    exit 143
}

trap cleanup_request EXIT
trap interrupt_request INT TERM HUP
printf '%s\n' "$connection_payload" >"$request_dir/connection.env"
printf '%s\n' "$benchmark_payload" >"$request_dir/benchmark.env"
printf '%s\n' "$run_payload" >"$request_dir/run.env"
unset connection_payload benchmark_payload run_payload
unset PGPASSWORD PGPASSFILE HDB_SUPERUSER_PASSWORD
printf '[%s] Repository: %s\n' "$request_phase" "$remote_repo"
./wrapper.sh hammerdb "$remote_action" \
    --connection-env "$request_dir/connection.env" \
    --benchmark-env "$request_dir/benchmark.env" \
    --run-env "$request_dir/run.env" &
worker_pid=$!
result=0
wait "$worker_pid" || result=$?
worker_pid=""
exit "$result"
REMOTE
}

remote_job() (
    host="$1" phase="$2" action="$3" first="$4" count="$5" vus="$6"
    logfile="$LOG_DIR/$phase.$(safe_host "$host").log"
    ssh_pid=""
    request_file=""
    trap '
        if [[ -n "$request_file" ]];
        then
            rm -f -- "$request_file";
            rmdir -- "${request_file%/*}";
        fi

    ' EXIT
    trap '
        if [[ -n "$ssh_pid" ]];
        then
            kill -TERM "$ssh_pid" 2>/dev/null || :;
            wait "$ssh_pid" 2>/dev/null || :;
        fi;

        exit 143
    ' INT TERM HUP
    started="$SECONDS"
    request_directory="$LOG_DIR/.request-$BASHPID-$RANDOM"
    mkdir -- "$request_directory"
    request_file="$request_directory/config.env"
    if ! remote_request "$host" "$phase" "$action" "$first" "$count" "$vus" >"$request_file";
    then
        printf 'ERROR: Could not serialize configuration for %s on %s\n' "$phase" "$host" >&2
        exit 1
    fi

    # Credentials travel through stdin, not SSH command-line arguments.
    ssh "${SSH_ARGUMENTS[@]}" -- "$host" bash -s \
        <"$request_file" \
        >"$logfile" 2>&1 &
    ssh_pid=$!
    result=0
    wait "$ssh_pid" || result=$?
    ssh_pid=""
    printf 'elapsed_seconds=%s\nexit_code=%s\n' "$((SECONDS - started))" "$result" >"$logfile.status"
    if ((result != 0));
    then
        printf 'ERROR: %s failed on %s (exit %s); see %s\n' "$phase" "$host" "$result" "$logfile" >&2
        if ((result == 255));
        then
            printf 'Remote completion is unknown after SSH failure; inspect %s before retrying.\n' "$host" >&2
        fi

    fi

    exit "$result"
)

active_jobs=()
stop_jobs()
{
    local pid
    for pid in "${active_jobs[@]}"; do
        if [[ -n "$pid" ]];
        then
            kill -TERM "$pid" 2>/dev/null || :;
        fi

    done
    for pid in "${active_jobs[@]}"; do
        if [[ -n "$pid" ]];
        then
            wait "$pid" 2>/dev/null || :;
        fi

    done
    active_jobs=()
}

trap stop_jobs EXIT
trap 'exit 130' INT
trap 'exit 143' TERM HUP

run_single()
{
    local result=0
    # Even serial phases run as jobs so signal traps can stop their SSH process.
    remote_job "$@" &
    active_jobs=("$!")
    wait "${active_jobs[0]}" || result=$?
    active_jobs=()
    return "$result"
}

# Reserve warehouse 1 for bootstrap; assign disjoint slices to the data jobs.
firsts=() counts=()
next=1
base=$((TOTAL_WAREHOUSES / ${#HOSTS[@]}))
remainder=$((TOTAL_WAREHOUSES % ${#HOSTS[@]}))
printf 'Warehouse plan (warehouse 1 is bootstrapped separately):\n'
for ((index = 0; index < ${#HOSTS[@]}; index++)); do
    count="$base"
    if ((index < remainder));
    then
        count=$((count + 1));
    fi

    first="$next"
    next=$((next + count))
    if ((index == 0));
    then
        first=2;
        count=$((count - 1));
    fi

    firsts+=("$first")
    counts+=("$count")
    if ((count > 0));
    then
        printf '  %s first=%s count=%s last=%s\n' "${HOSTS[$index]}" "$first" "$count" "$((first + count - 1))"
    fi

done
printf 'Log directory: %s\n' "$LOG_DIR"
total_start="$SECONDS"

printf '[preflight] Checking all runners before cleanup\n'
for host in "${HOSTS[@]}"; do
    run_single "$host" preflight --check 1 1 1
done
printf '[cleanup] Removing existing benchmark objects on %s\n' "${HOSTS[0]}"
run_single "${HOSTS[0]}" cleanup --cleanup 1 1 1
printf '[phase1] Bootstrapping warehouse 1\n'
phase_start="$SECONDS"
run_single "${HOSTS[0]}" bootstrap --prepare 1 1 1
phase1_seconds=$((SECONDS - phase_start))

printf '[phase2] Loading remaining warehouse ranges\n'
phase_start="$SECONDS"
for ((index = 0; index < ${#HOSTS[@]}; index++)); do
    count="${counts[$index]}"
    ((count > 0)) || continue
    vus="$HDB_BUILD_VUS"
    if ((vus > count));
    then
        vus="$count";
    fi

    remote_job "${HOSTS[$index]}" data --prepare "${firsts[$index]}" "$count" "$vus" &
    active_jobs+=("$!")
done
# Every data job must succeed before post-data DDL is allowed to run.
phase2_failed=false
for ((index = 0; index < ${#active_jobs[@]}; index++)); do
    if ! wait "${active_jobs[$index]}";
    then
        phase2_failed=true;
    fi

    active_jobs[$index]=""
done
active_jobs=()
[[ "$phase2_failed" == false ]] || { fail "At least one data loader failed; refusing post-data DDL"; exit 1; }

phase2_seconds=$((SECONDS - phase_start))

printf '[phase3] Finalizing post-data DDL\n'
phase_start="$SECONDS"
run_single "${HOSTS[0]}" finalize --prepare 1 0 1
phase3_seconds=$((SECONDS - phase_start))
printf 'Load completed successfully.\nTiming (seconds): bootstrap=%s parallel=%s finalize=%s total=%s\n' \
    "$phase1_seconds" "$phase2_seconds" "$phase3_seconds" "$((SECONDS - total_start))"
printf 'Logs: %s\n' "$LOG_DIR"
