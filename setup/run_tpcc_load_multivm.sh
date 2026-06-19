#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
Usage:
  run_tpcc_load_multivm.sh <hosts_file> <remote_pg_benchmark_dir> <pg_config_path> <hammerdb_install_dir> <work_root> [total_warehouses]

Purpose:
  Load data using many runner VMs (no benchmark run). This script performs:
  1) cleanup once on first runner
  2) Phase 1 bootstrap on first runner (warehouse 1)
  3) Phase 2 parallel data load on all runners for remaining warehouses
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
  PHASE2_NUM_VU=<n>               default: PG_NUM_VU (or 200 if PG_NUM_VU unset)
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
PHASE2_NUM_VU="${PHASE2_NUM_VU:-${PG_NUM_VU:-200}}"
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

print_effective_coverage() {
  local phase2_start phase2_end
  local last_idx=$(( ${#RANGE_ENDS[@]} - 1 ))

  phase2_start="${RANGE_FIRSTS[0]}"
  phase2_end="${RANGE_ENDS[$last_idx]}"

  echo "Coverage summary:"
  echo "  Phase 1 (bootstrap): warehouses 1..1 on $FIRST_RUNNER"
  echo "  Phase 2 (parallel): warehouses ${phase2_start}..${phase2_end} across ${#RANGE_HOSTS[@]} runners"
  echo "  Full expected coverage: warehouses 1..$TOTAL_WARE"
}

build_range_plan

# Phase 1 bootstraps warehouse 1 on the first runner. Phase 2 should therefore
# start from warehouse 2 and load the remaining range in parallel.
if [[ ${#RANGE_COUNTS[@]} -gt 0 ]]; then
  RANGE_FIRSTS[0]=$(( RANGE_FIRSTS[0] + 1 ))
  RANGE_COUNTS[0]=$(( RANGE_COUNTS[0] - 1 ))
fi

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

now_epoch() {
  date +%s
}

fmt_duration() {
  local total="$1"
  local h m s
  h=$(( total / 3600 ))
  m=$(( (total % 3600) / 60 ))
  s=$(( total % 60 ))
  printf "%02dh:%02dm:%02ds" "$h" "$m" "$s"
}

total_start="$(now_epoch)"
phase_cleanup_secs=0
phase1_secs=0
phase2_secs=0
phase3_secs=0

echo "Runners: ${HOSTS[*]}"
echo "Total warehouses: $TOTAL_WARE"
echo "Base slice: $BASE_SLICE remainder: $REMAINDER"
echo "Log dir: $LOG_DIR"
print_range_plan
print_effective_coverage

echo "[cleanup] once on first runner: $FIRST_RUNNER"
phase_start="$(now_epoch)"
run_cleanup_on_first_runner "$FIRST_RUNNER" "$LOG_DIR/cleanup.$(safe_host "$FIRST_RUNNER").log"
phase_cleanup_secs=$(( $(now_epoch) - phase_start ))
echo "[cleanup] completed in $(fmt_duration "$phase_cleanup_secs")"

echo "[phase1] ddl on first runner: $FIRST_RUNNER"
phase_start="$(now_epoch)"
run_remote_prepare "$FIRST_RUNNER" 1 1 1 "$LOG_DIR/phase1.$(safe_host "$FIRST_RUNNER").log"
phase1_secs=$(( $(now_epoch) - phase_start ))
echo "[phase1] completed in $(fmt_duration "$phase1_secs")"

echo "[phase2] parallel data load"
phase_start="$(now_epoch)"
pids=()
host_for_pid=()
phase2_host_start_secs=()
phase2_host_elapsed_secs=()
phase2_host_meta_files=()
for i in "${!RANGE_HOSTS[@]}"; do
  host="${RANGE_HOSTS[$i]}"
  first="${RANGE_FIRSTS[$i]}"
  slice="${RANGE_COUNTS[$i]}"
  last="${RANGE_ENDS[$i]}"

  if [[ "$slice" -le 0 ]]; then
    continue
  fi

  vu="$PHASE2_NUM_VU"
  if [[ $vu -gt $slice ]]; then
    vu="$slice"
  fi

  log_file="$LOG_DIR/phase2.$(safe_host "$host").log"
  meta_file="$LOG_DIR/phase2.$(safe_host "$host").timing"
  rm -f "$meta_file"
  echo "  $host first=$first last=$last count=$slice num_vu=$vu"
  host_start="$(now_epoch)"
  (
    host_job_start="$(now_epoch)"
    run_remote_prepare "$host" "$first" "$slice" "$vu" "$log_file"
    rc="$?"
    host_job_end="$(now_epoch)"
    echo "$((host_job_end - host_job_start)) $rc" > "$meta_file"
    exit "$rc"
  ) &
  pids+=("$!")
  host_for_pid+=("$host")
  phase2_host_start_secs+=("$host_start")
  phase2_host_elapsed_secs+=("0")
  phase2_host_meta_files+=("$meta_file")
done

phase2_failed=0
for i in "${!pids[@]}"; do
  host_elapsed=""

  if ! wait "${pids[$i]}"; then
    if [[ -f "${phase2_host_meta_files[$i]}" ]]; then
      read -r host_elapsed _ < "${phase2_host_meta_files[$i]}" || true
    fi
    if ! [[ "${host_elapsed:-}" =~ ^[0-9]+$ ]]; then
      host_elapsed=$(( $(now_epoch) - phase2_host_start_secs[$i] ))
    fi
    phase2_host_elapsed_secs[$i]="$host_elapsed"
    echo "[phase2] FAILED on ${host_for_pid[$i]} (see $LOG_DIR/phase2.$(safe_host "${host_for_pid[$i]}").log)" >&2
    echo "[phase2] ${host_for_pid[$i]} elapsed: $(fmt_duration "$host_elapsed")" >&2
    phase2_failed=1
  else
    if [[ -f "${phase2_host_meta_files[$i]}" ]]; then
      read -r host_elapsed _ < "${phase2_host_meta_files[$i]}" || true
    fi
    if ! [[ "${host_elapsed:-}" =~ ^[0-9]+$ ]]; then
      host_elapsed=$(( $(now_epoch) - phase2_host_start_secs[$i] ))
    fi
    phase2_host_elapsed_secs[$i]="$host_elapsed"
    echo "[phase2] ${host_for_pid[$i]} completed in $(fmt_duration "$host_elapsed")"
  fi
done

if [[ $phase2_failed -ne 0 ]]; then
  phase2_secs=$(( $(now_epoch) - phase_start ))
  echo "[phase2] elapsed before failure: $(fmt_duration "$phase2_secs")" >&2
  echo "[phase2] one or more runners failed; aborting before phase3" >&2
  exit 1
fi
phase2_secs=$(( $(now_epoch) - phase_start ))
echo "[phase2] completed in $(fmt_duration "$phase2_secs")"

if [[ ${#host_for_pid[@]} -gt 0 ]]; then
  echo "[phase2] host timing summary"
  for i in "${!host_for_pid[@]}"; do
    echo "  ${host_for_pid[$i]} : $(fmt_duration "${phase2_host_elapsed_secs[$i]}")"
  done
fi

echo "[phase3] post-data ddl on first runner: $FIRST_RUNNER"
phase_start="$(now_epoch)"
run_remote_prepare "$FIRST_RUNNER" 1 0 1 "$LOG_DIR/phase3.$(safe_host "$FIRST_RUNNER").log"
phase3_secs=$(( $(now_epoch) - phase_start ))
echo "[phase3] completed in $(fmt_duration "$phase3_secs")"

total_secs=$(( $(now_epoch) - total_start ))

echo
echo "Timing summary"
echo "  cleanup : $(fmt_duration "$phase_cleanup_secs")"
echo "  phase1  : $(fmt_duration "$phase1_secs")"
echo "  phase2  : $(fmt_duration "$phase2_secs")"
echo "  phase3  : $(fmt_duration "$phase3_secs")"
echo "  total   : $(fmt_duration "$total_secs")"

echo "Load completed successfully."
echo "Logs: $LOG_DIR"
