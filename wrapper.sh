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
       wrapper.sh --setup [--host HOST] [-- ANSIBLE_OPTIONS]
       wrapper.sh --setup-control-machine

Load connection, benchmark, and run environments, then invoke the selected
benchmark adapter. Only modules shipped in this checkout are supported.
Setup instead provisions runner machines through Ansible, without loading
benchmark environments or running a benchmark.

Options:
  --setup-control-machine Install local provisioning tools (Ubuntu/Debian)
  --setup                 Provision all VMs in this checkout's hosts.txt
  --host HOST             Provision just this VM (setup only; need not be listed)
  --check                 Validate configuration without connecting or running
  --prepare               Prepare data without running iterations
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
For setup, copy hosts.txt.sample to hosts.txt and edit it, or select --host.
Run --setup-control-machine once to install missing control-machine tools.
It does not configure runner VMs or install benchmark/database software.
Ansible options go after --, for example: --setup -- --check
The Ansible command is printed before execution. Keep secrets in private
variable files, not inline arguments; use -e @FILE after --.
Use dedicated runners: provisioning upgrades packages and stops PostgreSQL.
See README.md for examples.
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

while (($#)); do
    case "$1" in
        -h|--help) usage; exit 0 ;;
        --setup-control-machine|--setup|--check|--prepare|--cleanup)
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
                --connection-env) connection_file="$2" ;;
                --benchmark-env) benchmark_file="$2" ;;
                --run-env) run_file="$2" ;;
            esac
            config_selected=true
            shift 2
            ;;
        --)
            [[ "$RUN_ACTION" == setup ]] || usage_error "Ansible options after -- require --setup"
            shift
            ansible_options=("$@")
            break
            ;;
        -*) usage_error "Unknown option '$1' (see --help)" ;;
        *)
            [[ -z "$BENCHMARK_TYPE" ]] || usage_error "Unexpected argument: $1"
            BENCHMARK_TYPE="$1"
            shift
            ;;
    esac
done

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

if [[ "$RUN_ACTION" == setup ]]
then
    [[ -z "$BENCHMARK_TYPE" ]] || usage_error "--setup does not take a benchmark name; use --host for a single VM"
    [[ "$config_selected" == false ]] || usage_error "--setup uses Ansible variables, not benchmark environment selectors"
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

    ansible_command=(ansible-playbook -i "$inventory" "$playbook" "${ansible_options[@]}")
    # Print reusable shell quoting, but execute the argument array directly.
    printf 'Ansible command (run from %q):\n' "$PWD"
    printf '%q' "${ansible_command[0]}"
    printf ' %q' "${ansible_command[@]:1}"
    printf '\n'
    exec "${ansible_command[@]}"
fi

[[ -z "$setup_host" ]] || usage_error "--host is only valid with --setup"
[[ -n "$BENCHMARK_TYPE" ]] || { usage >&2; exit 2; }

readonly BENCHMARK_TYPE RUN_ACTION
export BENCHMARK_TYPE RUN_ACTION

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
benchmark_validate

if [[ "$RUN_ACTION" == check ]];
then
    printf 'Configuration check passed.\nBenchmark: %s\nTarget: %s@%s:%s/%s\n' \
        "$BENCHMARK_TYPE" "$PGUSER" "$PGHOST" "$PGPORT" "$PGDATABASE"
    printf 'Run: %s iteration(s), prepare=%s, output=%s\n' \
        "$RUN_ITERATIONS" "$RUN_PREPARE_MODE" "$RUN_OUTPUT_ROOT"

    benchmark_describe
    exit 0
fi

runner_main
