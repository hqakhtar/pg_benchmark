#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
Usage:
  run_tpcc_load_multivm.sh <hosts_file> <remote_pg_benchmark_dir> <pg_config_path> <hammerdb_install_dir> <work_root> [total_warehouses]

Purpose:
  Load data using many runner VMs (no benchmark run). This script performs:
  1) cleanup once on first runner
  2) Phase 1 DDL on first runner (PG_FIRST_WARE=1 PG_COUNT_WARE=0)
  3) Phase 2 parallel data load on all runners
  4) Phase 3 post-data DDL on first runner (PG_FIRST_WARE=1 PG_COUNT_WARE=0)

Example:
  ./setup/run_tpcc_load_multivm.sh \
    ./hosts.txt \
    /opt/HammerDB/pg_benchmark \
    /usr/bin/pg_config \
    /opt/HammerDB \
    /tmp/hdb-load

  # Or provide the warehouse count explicitly:
  ./setup/run_tpcc_load_multivm.sh \
    ./hosts.txt \
    /opt/HammerDB/pg_benchmark \
    /usr/bin/pg_config \
    /opt/HammerDB \
    /tmp/hdb-load \
    1000000

Required env vars on pilot:
  PGHOST PGPORT PGPASSWORD PG_SUPERUSER PG_USER PG_DBASE PG_DEFAULTDBASE

Optional env vars:
  ENABLE_CITUS=true|false         default: true
  PHASE2_NUM_VU=<n>               default: 200
  SSH_OPTS='-o StrictHostKeyChecking=accept-new'
  LOG_DIR=<dir>                   default: ./logs/load-<timestamp>
  PG_COUNT_WARE=<n>               used when total_warehouses is omitted

Notes:
  - Warehouse slices are computed automatically from hosts_file.
  - On each runner, myenv.sh is generated from myenv.sh.sample, patched with
    runner-specific values, sourced, then wrapper.sh is executed.
EOF
}

if [[ $# -lt 5 || $# -gt 6 ]]; then
  usage
  exit 1
fi

HOSTS_FILE="$1"
REMOTE_DIR="$2"
PG_CONFIG_PATH="$3"
HAMMERDB_HOME="$4"
WORK_ROOT="$5"
TOTAL_WARE="${6:-${PG_COUNT_WARE:-}}"

: "${PGHOST:?PGHOST is required}"
: "${PGPORT:?PGPORT is required}"
: "${PGPASSWORD:?PGPASSWORD is required}"
: "${PG_SUPERUSER:?PG_SUPERUSER is required}"
: "${PG_USER:?PG_USER is required}"
: "${PG_DBASE:?PG_DBASE is required}"
: "${PG_DEFAULTDBASE:?PG_DEFAULTDBASE is required}"

ENABLE_CITUS="${ENABLE_CITUS:-true}"
PHASE2_NUM_VU="${PHASE2_NUM_VU:-200}"
SSH_OPTS="${SSH_OPTS:--o StrictHostKeyChecking=accept-new}"
LOG_DIR="${LOG_DIR:-./logs/load-$(date +%Y%m%d-%H%M%S)}"

mkdir -p "$LOG_DIR"

if [[ ! -f "$HOSTS_FILE" ]]; then
  echo "hosts file not found: $HOSTS_FILE" >&2
  exit 1
fi

if [[ -z "$TOTAL_WARE" ]]; then
  echo "total_warehouses is required either as the 6th argument or via PG_COUNT_WARE" >&2
  exit 1
fi

if ! [[ "$TOTAL_WARE" =~ ^[0-9]+$ ]] || [[ "$TOTAL_WARE" -le 0 ]]; then
  echo "total_warehouses must be a positive integer" >&2
  exit 1
fi

if ! [[ "$PHASE2_NUM_VU" =~ ^[0-9]+$ ]] || [[ "$PHASE2_NUM_VU" -le 0 ]]; then
  echo "PHASE2_NUM_VU must be a positive integer" >&2
  exit 1
fi

mapfile -t HOSTS < <(grep -vE '^\s*($|#)' "$HOSTS_FILE")
if [[ ${#HOSTS[@]} -eq 0 ]]; then
  echo "no hosts found in $HOSTS_FILE" >&2
  exit 1
fi

RUNNER_COUNT=${#HOSTS[@]}
FIRST_RUNNER="${HOSTS[0]}"
BASE_SLICE=$(( TOTAL_WARE / RUNNER_COUNT ))
REMAINDER=$(( TOTAL_WARE % RUNNER_COUNT ))

if [[ $BASE_SLICE -eq 0 ]]; then
  echo "total_warehouses ($TOTAL_WARE) must be >= number of runners ($RUNNER_COUNT)" >&2
  exit 1
fi

RANGE_HOSTS=()
RANGE_FIRSTS=()
RANGE_COUNTS=()
RANGE_ENDS=()

build_range_plan() {
  local first=1
  local covered=0
  local expected_first=1

  for i in "${!HOSTS[@]}"; do
    local host="${HOSTS[$i]}"
    local extra=0
    local slice
    local last

    if [[ $i -lt $REMAINDER ]]; then
      extra=1
    fi
    slice=$(( BASE_SLICE + extra ))
    last=$(( first + slice - 1 ))

    RANGE_HOSTS+=("$host")
    RANGE_FIRSTS+=("$first")
    RANGE_COUNTS+=("$slice")
    RANGE_ENDS+=("$last")

    if [[ $first -ne $expected_first ]]; then
      echo "range plan error: expected next first warehouse $expected_first, got $first for host $host" >&2
      exit 1
    fi

    covered=$(( covered + slice ))
    expected_first=$(( last + 1 ))
    first=$(( last + 1 ))
  done

  if [[ $covered -ne $TOTAL_WARE ]]; then
    echo "range plan error: covered $covered warehouses, expected $TOTAL_WARE" >&2
    exit 1
  fi

  if [[ ${RANGE_FIRSTS[0]} -ne 1 ]]; then
    echo "range plan error: first slice must start at warehouse 1" >&2
    exit 1
  fi

  local last_idx=$(( ${#RANGE_ENDS[@]} - 1 ))
  if [[ ${RANGE_ENDS[$last_idx]} -ne $TOTAL_WARE ]]; then
    echo "range plan error: final slice ends at ${RANGE_ENDS[$last_idx]}, expected $TOTAL_WARE" >&2
    exit 1
  fi
}

print_range_plan() {
  echo "Phase 2 range plan:"
  for i in "${!RANGE_HOSTS[@]}"; do
    echo "  ${RANGE_HOSTS[$i]} first=${RANGE_FIRSTS[$i]} count=${RANGE_COUNTS[$i]} last=${RANGE_ENDS[$i]}"
  done
}

build_range_plan

safe_host() {
  echo "$1" | tr -c 'a-zA-Z0-9._-' '_'
}

run_remote_prepare() {
  local host="$1"
  local first_ware="$2"
  local count_ware="$3"
  local num_vu="$4"
  local log_file="$5"
  local remote_work_dir="$WORK_ROOT/$(safe_host "$host")"

  ssh $SSH_OPTS "$host" "
    set -euo pipefail
    cd '$REMOTE_DIR'
    mkdir -p '$remote_work_dir'
    if [[ ! -f myenv.sh.sample ]]; then
      echo 'missing myenv.sh.sample in $REMOTE_DIR' >&2
      exit 1
    fi
    cp myenv.sh.sample myenv.sh
    cat >> myenv.sh <<'MYENV_APPEND'
export PGHOST='$PGHOST'
export PGPORT='$PGPORT'
export PGPASSWORD='$PGPASSWORD'
export PG_SUPERUSER='$PG_SUPERUSER'
export PG_USER='$PG_USER'
export PG_DBASE='$PG_DBASE'
export PG_DEFAULTDBASE='$PG_DEFAULTDBASE'
export ENABLE_CITUS='$ENABLE_CITUS'
export PG_FIRST_WARE='$first_ware'
export PG_COUNT_WARE='$count_ware'
export PG_NUM_VU='$num_vu'
export PG_VU='$num_vu'
MYENV_APPEND
    source ./myenv.sh
    ./wrapper.sh -C '$PG_CONFIG_PATH' -H '$HAMMERDB_HOME' -t '$remote_work_dir' -c -P
  " >"$log_file" 2>&1
}

run_cleanup_on_first_runner() {
  local host="$1"
  local log_file="$2"

  ssh $SSH_OPTS "$host" "
    set -euo pipefail
    cd '$REMOTE_DIR'
    export PGPASSWORD='$PGPASSWORD'
    psql -v ON_ERROR_STOP=1 \
      -h '$PGHOST' -p '$PGPORT' -U '$PG_USER' -d '$PG_DBASE' \
      -f hammerdb/hammerdb_cleanup_citus.sql
  " >"$log_file" 2>&1
}

echo "Runners: ${HOSTS[*]}"
echo "Total warehouses: $TOTAL_WARE"
echo "Base slice: $BASE_SLICE remainder: $REMAINDER"
echo "Log dir: $LOG_DIR"
print_range_plan

echo "[cleanup] once on first runner: $FIRST_RUNNER"
run_cleanup_on_first_runner "$FIRST_RUNNER" "$LOG_DIR/cleanup.$(safe_host "$FIRST_RUNNER").log"

echo "[phase1] ddl on first runner: $FIRST_RUNNER"
run_remote_prepare "$FIRST_RUNNER" 1 0 1 "$LOG_DIR/phase1.$(safe_host "$FIRST_RUNNER").log"

echo "[phase2] parallel data load"
pids=()
host_for_pid=()
for i in "${!RANGE_HOSTS[@]}"; do
  host="${RANGE_HOSTS[$i]}"
  first="${RANGE_FIRSTS[$i]}"
  slice="${RANGE_COUNTS[$i]}"

  vu="$PHASE2_NUM_VU"
  if [[ $vu -gt $slice ]]; then
    vu="$slice"
  fi

  log_file="$LOG_DIR/phase2.$(safe_host "$host").log"
  echo "  $host first=$first count=$slice num_vu=$vu"
  run_remote_prepare "$host" "$first" "$slice" "$vu" "$log_file" &
  pids+=("$!")
  host_for_pid+=("$host")
done

phase2_failed=0
for i in "${!pids[@]}"; do
  if ! wait "${pids[$i]}"; then
    echo "[phase2] FAILED on ${host_for_pid[$i]} (see $LOG_DIR/phase2.$(safe_host "${host_for_pid[$i]}").log)" >&2
    phase2_failed=1
  fi
done

if [[ $phase2_failed -ne 0 ]]; then
  echo "[phase2] one or more runners failed; aborting before phase3" >&2
  exit 1
fi

echo "[phase3] post-data ddl on first runner: $FIRST_RUNNER"
run_remote_prepare "$FIRST_RUNNER" 1 0 1 "$LOG_DIR/phase3.$(safe_host "$FIRST_RUNNER").log"

echo "Load completed successfully."
echo "Logs: $LOG_DIR"
