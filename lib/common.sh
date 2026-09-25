#!/usr/bin/env bash

fail()
{
    printf 'ERROR: %s\n' "$*" >&2
    return 1
}

validate_integer()
{
    local name="$1" value="$2" minimum="$3" maximum="${4:-2147483647}"
    if [[ ! "$value" =~ ^[0-9]{1,10}$ ]] ||
        ((10#$value < minimum || 10#$value > maximum));
    then
        fail "$name must be an integer between $minimum and $maximum (got '$value')"
        return 1
    fi

    # Normalize leading zeros so later arithmetic remains decimal.
    printf -v "$name" '%d' "$((10#$value))"
    export "${name?}"
}

validate_boolean()
{
    case "$2" in
        true|false) ;;
        *) fail "$1 must be true or false (got '$2')"; return 1 ;;
    esac
}

require_command()
{
    command -v -- "$1" >/dev/null 2>&1 ||
    {
        fail "Required executable not found: $1"
        return 1
    }
}

validate_runner_host()
{
    [[ "$1" =~ ^[A-Za-z0-9_][A-Za-z0-9_.:@%-]*$ ]] ||
    {
        fail "Invalid runner host: $1"
        return 1
    }
}

# Provisioning and distributed loading share the same plain-host validation.
read_runner_hosts()
{
    local file="$1" host
    local -A seen_hosts=()
    [[ -f "$file" && -r "$file" ]] ||
    {
        fail "Runner hosts file must be a readable regular file: $file. Create and edit a private copy of hosts.txt.sample."
        return 1
    }

    HOSTS=()
    while IFS= read -r host || [[ -n "$host" ]]; do
        host="${host%%#*}"
        host="${host#"${host%%[![:space:]]*}"}"
        host="${host%"${host##*[![:space:]]}"}"
        [[ -n "$host" ]] || continue
        validate_runner_host "$host" || return 1
        [[ -z "${seen_hosts[$host]+present}" ]] || { fail "Duplicate runner host: $host"; return 1; }

        seen_hosts["$host"]=true
        HOSTS+=("$host")
    done <"$file"
    [[ ${#HOSTS[@]} -gt 0 ]] || { fail "No runners in $file"; return 1; }
}

absolute_path()
{
    realpath -m -- "$1"
}

environment_path()
{
    local file="$1" require_readable="${2:-true}" resolved
    if [[ "${file,,}" == *.sample ]]
    then
        fail "Sample files cannot be used as environment configuration: $file"
        return 1
    fi

    resolved="$(absolute_path "$file")" || return 1
    if [[ "${resolved,,}" == *.sample ]]
    then
        fail "Environment configuration resolves to a sample file: $file"
        return 1
    fi

    if [[ "$require_readable" == true && ( ! -f "$file" || ! -r "$file" ) ]]
    then
        fail "Environment file must be a readable regular file: $file. Copy the matching *.env.sample template to a private file first."
        return 1
    fi

    printf '%s\n' "$resolved"
}

load_environment()
{
    local file restore_allexport=false status=0
    file="$(environment_path "$1")" || return 1
    if [[ "$-" != *a* ]]
    then
        restore_allexport=true
    fi

    # Trusted env files may use plain assignments; export them for child hooks.
    set -a
    # shellcheck source=/dev/null
    source "$file"
    status=$?
    if [[ "$restore_allexport" == true ]]
    then
        set +a
    fi

    if ((status != 0))
    then
        fail "Environment file failed (exit $status): $file"
        return "$status"
    fi
}

# The loader is also used by remote preparation; adapters never source config.
load_configuration()
{
    local root="$1" type="$2"
    local connection_file="${3:-}" benchmark_file="${4:-}" run_file="${5:-}"
    require_command realpath || return 1
    [[ "$type" =~ ^[a-z][a-z0-9_]*$ ]] ||
    {
        fail "Invalid benchmark type: $type"
        return 1
    }

    [[ -f "$root/$type/$type.sh" ]] ||
    {
        fail "Unsupported benchmark type: $type"
        return 1
    }

    CONNECTION_CONFIG="$(environment_path "${connection_file:-$root/connection.env}")" || return 1
    BENCHMARK_CONFIG="$(environment_path "${benchmark_file:-$root/$type/$type.env}")" || return 1
    RUN_CONFIG="$(environment_path "${run_file:-$root/run.env}")" || return 1
    # Each selected file is complete; templates are never runtime defaults.
    load_environment "$CONNECTION_CONFIG"
    load_environment "$BENCHMARK_CONFIG"
    load_environment "$RUN_CONFIG"
    if [[ -n "${PG_SERVER_ENV_FILE:-}" ]]
    then
        # Remote-only paths need not exist on the pilot, but cannot be samples.
        environment_path "$PG_SERVER_ENV_FILE" false >/dev/null || return 1
    fi

    export CONNECTION_CONFIG BENCHMARK_CONFIG RUN_CONFIG
}

# Encode metadata locally without adding a runtime dependency on jq.
json_string()
{
    local value="$1" character ordinal i
    printf '"'
    for ((i = 0; i < ${#value}; i++)); do
        character="${value:i:1}"
        case "$character" in
            '"') printf '\\"' ;;
            \\) printf '%s' "\\\\" ;;
            $'\n') printf '\\n' ;;
            $'\r') printf '\\r' ;;
            $'\t') printf '\\t' ;;
            [[:cntrl:]])
                printf -v ordinal '%d' "'$character"
                printf '\\u%04x' "$ordinal"
                ;;
            *) printf '%s' "$character" ;;
        esac
    done
    printf '"'
}

json_field()
{
    printf '  "%s": ' "$1"
    json_string "$2"
    printf '%s\n' "${3-,}"
}

utc_now()
{
    date -u +%Y-%m-%dT%H:%M:%SZ
}
