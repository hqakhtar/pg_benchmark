#!/usr/bin/env bash
set -Eeuo pipefail

# Resolve assets relative to this script, not the caller's directory.
FRAMEWORK_ROOT="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
readonly FRAMEWORK_ROOT
source "$FRAMEWORK_ROOT/lib/common.sh"

usage()
{
    cat <<'EOF'
Usage: wrapper.sh BENCHMARK [OPTIONS]
       wrapper.sh --verify [--connection-env FILE] [--benchmark-env FILE] [--run-env FILE]
       wrapper.sh --monitor [DIR ...] [--run-env FILE] [-- MONITOR_OPTIONS]
       wrapper.sh --setup [--host HOST] [-- ANSIBLE_OPTIONS]
       wrapper.sh --update-hammerdb [--host HOST] [-- ANSIBLE_OPTIONS]
       wrapper.sh --update-config [--host HOST] [-- ANSIBLE_OPTIONS]
       wrapper.sh --setup-control-machine
       wrapper.sh --azure-create [-- AZURE_OPTIONS]
       wrapper.sh --azure-delete [-- AZURE_OPTIONS]

Load connection, benchmark, and run environments, then invoke the selected
benchmark adapter. Only modules shipped in this checkout are supported.
Setup instead provisions runner machines through Ansible, without loading
benchmark environments or running a benchmark.
Update actions run only the selected Ansible tasks on existing runners.

Options:
  --setup-control-machine Install local provisioning tools (Ubuntu/Debian)
  --setup                 Provision all VMs in this checkout's hosts.txt
  --update-hammerdb       Replace HammerDB from the local archive on runners
  --update-config         Upload private connection, benchmark, and run env files
  --azure-create          Create Azure VMs and add aliases to hosts.txt
  --azure-delete          Delete Azure VMs and remove their aliases from hosts.txt
  --monitor               Watch load/run directories for worker failures
  --host HOST             Select one VM for setup/updates (need not be listed)
  --check                 Validate runner and target settings without connecting
  --prepare               Prepare data without running iterations
  --load                  Load HammerDB data in parallel on hosts.txt runners
  --verify                Check HammerDB warehouse ID coverage without changing data
  --cleanup               Remove benchmark data (requires destructive opt-in)
  --connection-env FILE    Select a complete private connection configuration
  --benchmark-env FILE     Select a complete private benchmark configuration
  --run-env FILE           Select a complete private run configuration
  -h, --help              Show this help

Default action: run. Configuration selectors may also be supplied through
CONNECTION_ENV_FILE, BENCHMARK_ENV_FILE, and RUN_ENV_FILE.
Without selectors, private connection.env, BENCHMARK/BENCHMARK.env, and run.env
are required in this checkout. Copy the matching *.env.sample templates first.
Files ending in .sample (including symlink targets) are never accepted.
For hammerdb --load, set RUN_REMOTE_REPO in the selected run environment;
the total warehouse count comes from HDB_WAREHOUSES. Initial cleanup requires
RUN_ALLOW_DESTRUCTIVE=true. This action does not run benchmark iterations.
Verification defaults to hammerdb when no benchmark is supplied. It checks
HDB_FIRST_WAREHOUSE through HDB_FIRST_WAREHOUSE + HDB_WAREHOUSES - 1 on an
existing target, not the contents of the other TPC-C tables.
For setup/updates, copy hosts.txt.sample to hosts.txt and edit it, or select --host.
Run --setup-control-machine once to install missing control-machine tools.
It does not configure runner VMs or install benchmark/database software.
Ansible options go after --, for example: --update-hammerdb -- --check
HammerDB updates replace the selected version, including same-version rebuilds.
Config updates overwrite private env files; edit local copies before uploading.
The Ansible command is printed before execution. Keep secrets in private
variable files, not inline arguments; use -e @FILE after --.
Use dedicated runners: provisioning upgrades packages and stops PostgreSQL.
Azure actions use setup/azure/azure-vms.env by default. Override with
--env-file PATH after --; other Azure script options also go after --.
Existing hosts are preserved. Dry runs never change hosts.txt.
Monitoring is read-only and needs Python 3. Without directories it reads the
selected run environment and picks the newest matching local load/run directory.
Explicit directories bypass configuration; historical failures also trigger it.
Monitor options go after --, for example: --monitor ./logs/load-ID -- --interval 1
It exits 1 on a worker failure or 2 on a monitor error; Ctrl-C stops watching.
See README.md for examples.

Supported benchmarks:
  hammerdb               HammerDB TPROC-C (TPC-C) for PostgreSQL

Examples:
  ./wrapper.sh hammerdb --check
  ./wrapper.sh hammerdb --prepare
  ./wrapper.sh hammerdb --load --run-env ./run-load.env
  ./wrapper.sh --verify
  ./wrapper.sh hammerdb
  ./wrapper.sh --setup --host runner.example.com -- --check
EOF
}

usage_error()
{
    printf 'ERROR: %s\n' "$*" >&2
    exit 2
}

BENCHMARK_TYPE=""
RUN_ACTION=run

# CLI file selectors override these environment-provided defaults.
connection_file="${CONNECTION_ENV_FILE:-}"
benchmark_file="${BENCHMARK_ENV_FILE:-}"
run_file="${RUN_ENV_FILE:-}"
action_set=false
config_selected=false
setup_host=""
ansible_options=()
azure_options=()
monitor_directories=()
monitor_options=()
monitor_target_config_selected=false

while (($#)); do
    case "$1" in
        -h|--help) usage; exit 0 ;;
        --setup-control-machine|--setup|--update-hammerdb|--update-config|--azure-create|--azure-delete|--monitor|--check|--prepare|--load|--verify|--cleanup)
            [[ "$action_set" == false ]] || usage_error "Choose only one action"
            RUN_ACTION="${1#--}"
            action_set=true
            shift
            ;;
        --host)
            [[ $# -ge 2 && -n "$2" && "$2" != -* ]] || usage_error "--host requires a VM name"
            [[ -z "$setup_host" ]] || usage_error "Choose only one VM with --host"
            setup_host="$2"
            shift 2
            ;;
        --connection-env|--benchmark-env|--run-env)
            [[ $# -ge 2 && -n "$2" && "$2" != --* ]] ||
            {
                usage_error "$1 requires a filename"
            }

            case "$1" in
                --connection-env) connection_file="$2"; monitor_target_config_selected=true ;;
                --benchmark-env) benchmark_file="$2"; monitor_target_config_selected=true ;;
                --run-env) run_file="$2" ;;
            esac
            config_selected=true
            shift 2
            ;;
        --)
            shift
            case "$RUN_ACTION" in
                setup|update-hammerdb|update-config) ansible_options=("$@") ;;
                azure-create|azure-delete) azure_options=("$@") ;;
                monitor) monitor_options=("$@") ;;
                *) usage_error "Options after -- require --setup, --update-hammerdb, --update-config, --azure-create, --azure-delete or --monitor" ;;
            esac
            break
            ;;
        -*) usage_error "Unknown option '$1' (see --help)" ;;
        *)
            if [[ "$RUN_ACTION" == monitor ]]
            then
                monitor_directories+=("$1")
            else
                [[ -z "$BENCHMARK_TYPE" ]] || usage_error "Unexpected argument: $1"
                BENCHMARK_TYPE="$1"
            fi
            shift
            ;;
    esac
done

if [[ "$RUN_ACTION" == monitor ]]
then
    [[ -z "$BENCHMARK_TYPE" && -z "$setup_host" && "$monitor_target_config_selected" == false ]] ||
        usage_error "--monitor takes optional directories and --run-env, not a benchmark, host, or target configuration"
    if ((${#monitor_directories[@]} == 0))
    then
        load_environment "${run_file:-$FRAMEWORK_ROOT/run.env}"
        [[ -n "${RUN_OUTPUT_ROOT:-}" && -n "${RUN_LABEL:-}" ]] ||
            { fail "Monitoring requires RUN_OUTPUT_ROOT and RUN_LABEL in the selected run environment"; exit 1; }
        if [[ -n "${LOG_DIR:-}" ]]
        then
            monitor_directories=("$LOG_DIR")
        else
            monitor_options=(
                --run-output-root "$RUN_OUTPUT_ROOT"
                --run-label "$RUN_LABEL"
                --load-log-root "${RUN_LOAD_LOG_ROOT:-./logs}"
                "${monitor_options[@]}"
            )
        fi
    elif [[ "$config_selected" == true ]]
    then
        usage_error "--run-env is only used when --monitor selects the directory automatically"
    fi
    require_command python3 || exit 1
    monitor_script="$FRAMEWORK_ROOT/monitor_workers.py"
    [[ -f "$monitor_script" && -r "$monitor_script" ]] ||
        { fail "Worker monitor must be a readable regular file: $monitor_script"; exit 1; }
    exec python3 "$monitor_script" "${monitor_directories[@]}" "${monitor_options[@]}"
fi

if [[ "$RUN_ACTION" == azure-create || "$RUN_ACTION" == azure-delete ]]
then
    [[ -z "$BENCHMARK_TYPE" && -z "$setup_host" && "$config_selected" == false ]] ||
        usage_error "Azure actions take no benchmark, host, or benchmark environment selectors"
    azure_script="$FRAMEWORK_ROOT/setup/azure/${RUN_ACTION#azure-}-azure-vms.sh"
    [[ -f "$azure_script" && -r "$azure_script" ]] ||
        { fail "Azure VM script must be a readable regular file: $azure_script"; exit 1; }
    azure_env_selected=false
    for option in "${azure_options[@]}"
    do
        case "$option" in
            --env-file|--env-file=*) azure_env_selected=true ;;
            --hosts-file|--hosts-file=*) usage_error "Azure wrapper actions always use this checkout's hosts.txt" ;;
        esac
    done
    if [[ "$azure_env_selected" == false ]]
    then
        azure_options=(--env-file "$FRAMEWORK_ROOT/setup/azure/azure-vms.env" "${azure_options[@]}")
    fi
    exec bash "$azure_script" --hosts-file "$FRAMEWORK_ROOT/hosts.txt" "${azure_options[@]}"
fi

if [[ "$RUN_ACTION" == setup-control-machine ]]
then
    [[ -z "$BENCHMARK_TYPE" && -z "$setup_host" && "$config_selected" == false ]] ||
    {
        usage_error "--setup-control-machine takes no benchmark, host, or environment selectors"
    }

    control_setup="$FRAMEWORK_ROOT/setup/setup_control_machine.sh"
    [[ -f "$control_setup" && -r "$control_setup" ]] ||
    {
        fail "Control-machine setup script must be a readable regular file: $control_setup"
        exit 1
    }

    exec bash "$control_setup"
fi

if [[ "$RUN_ACTION" == setup || "$RUN_ACTION" == update-hammerdb || "$RUN_ACTION" == update-config ]]
then
    [[ -z "$BENCHMARK_TYPE" ]] || usage_error "--$RUN_ACTION does not take a benchmark name; use --host for a single VM"
    [[ "$config_selected" == false ]] || usage_error "--$RUN_ACTION uses Ansible variables, not benchmark environment selectors"
    require_command ansible-playbook ||
    {
        printf 'Run %q --setup-control-machine to install local provisioning tools first.\n' "$FRAMEWORK_ROOT/wrapper.sh" >&2
        exit 1
    }

    playbook="$FRAMEWORK_ROOT/setup/ansible/runner_setup.yml"
    [[ -f "$playbook" && -r "$playbook" ]] || { fail "Ansible playbook must be a readable regular file: $playbook"; exit 1; }

    if [[ -n "$setup_host" ]]
    then
        validate_runner_host "$setup_host"
        inventory="$setup_host,"
    else
        inventory="$FRAMEWORK_ROOT/hosts.txt"
        read_runner_hosts "$inventory"
    fi

    action_options=()
    case "$RUN_ACTION" in
        update-hammerdb)
            action_options=(--tags hammerdb -e install_hammerdb=true -e reinstall_hammerdb=true)
            ;;
        update-config)
            action_options=(--tags benchmark_config -e update_benchmark_config=true)
            ;;
    esac
    ansible_command=(ansible-playbook -i "$inventory" "$playbook" "${action_options[@]}" "${ansible_options[@]}")
    # Print reusable shell quoting, but execute the argument array directly.
    printf 'Ansible command (run from %q):\n' "$PWD"
    printf '%q' "${ansible_command[0]}"
    printf ' %q' "${ansible_command[@]:1}"
    printf '\n'
    exec "${ansible_command[@]}"
fi

[[ -z "$setup_host" ]] || usage_error "--host is only valid with --setup, --update-hammerdb or --update-config"
if [[ "$RUN_ACTION" == verify && -z "$BENCHMARK_TYPE" ]]
then
    BENCHMARK_TYPE=hammerdb
fi

[[ -n "$BENCHMARK_TYPE" ]] ||
{
    printf "ERROR: BENCHMARK is required. Supported benchmarks: hammerdb\n" >&2
    printf "Try './wrapper.sh hammerdb --check' or './wrapper.sh --help'.\n" >&2
    exit 2
}

readonly BENCHMARK_TYPE RUN_ACTION
export BENCHMARK_TYPE RUN_ACTION

if [[ "$RUN_ACTION" == load ]]
then
    [[ "$BENCHMARK_TYPE" == hammerdb ]] || usage_error "--load is only supported for hammerdb"
    loader="$FRAMEWORK_ROOT/setup/run_tpcc_load_multivm.sh"
    [[ -f "$loader" && -r "$loader" ]] ||
        { fail "Parallel loader must be a readable regular file: $loader"; exit 1; }
    load_options=()
    [[ -z "$connection_file" ]] || load_options+=(--connection-env "$connection_file")
    [[ -z "$benchmark_file" ]] || load_options+=(--benchmark-env "$benchmark_file")
    [[ -z "$run_file" ]] || load_options+=(--run-env "$run_file")
    load_pid=""
    stop_load()
    {
        if [[ -n "$load_pid" ]]
        then
            kill -TERM "$load_pid" 2>/dev/null || :
            wait "$load_pid" 2>/dev/null || :
        fi
    }
    trap stop_load EXIT
    trap 'exit 130' INT
    trap 'exit 143' TERM HUP
    TIMEFORMAT='Timing [overall]: real=%3Rs user=%3Us sys=%3Ss'
    time {
        bash "$loader" "$FRAMEWORK_ROOT/hosts.txt" "${load_options[@]}" &
        load_pid=$!
        load_status=0
        wait "$load_pid" || load_status=$?
        load_pid=""
    }
    exit "$load_status"
fi

trap 'printf "ERROR: Configuration or runner failed at line %s (exit %s)\n" "$LINENO" "$?" >&2' ERR
# Resolve configuration once; adapters receive the exported settings.

load_configuration "$FRAMEWORK_ROOT" "$BENCHMARK_TYPE" "$connection_file" "$benchmark_file" "$run_file"

BENCHMARK_MODULE="$FRAMEWORK_ROOT/$BENCHMARK_TYPE/$BENCHMARK_TYPE.sh"
readonly BENCHMARK_MODULE

source "$FRAMEWORK_ROOT/lib/postgresql.sh"
source "$FRAMEWORK_ROOT/lib/runner.sh"
source "$BENCHMARK_MODULE"

# Keep preflight offline; target changes start only in runner_main.
runner_validate
postgresql_validate

if [[ "$RUN_ACTION" == check ]];
then
    printf 'Runner and target check passed.\nBenchmark: %s\nTarget: %s@%s:%s/%s\n' \
        "$BENCHMARK_TYPE" "$PGUSER" "$PGHOST" "$PGPORT" "$PGDATABASE"
    printf 'Run: %s iteration(s), prepare=%s, output=%s\n' \
        "$RUN_ITERATIONS" "$RUN_PREPARE_MODE" "$RUN_OUTPUT_ROOT"

    benchmark_describe
    exit 0
fi

runner_main
