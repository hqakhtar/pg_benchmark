#!/usr/bin/env bash
# Shared configuration helpers for the Azure VM scripts. Source this package
# file from an executed script; environment files themselves are parsed as data.

azvm_config_error() {
    printf 'ERROR: %s\n' "$*" >&2
    return 1
}

azvm_load_env_file() {
    local azvm_arg azvm_file='' azvm_seen=false azvm_json azvm_dir azvm_variable azvm_value
    local -a azvm_remaining=()

    # Help never opens or evaluates a settings file and needs no dependencies.
    for azvm_arg in "$@"; do
        if [[ $azvm_arg == --help || $azvm_arg == -h ]]; then
            AZVM_SCRIPT_ARGS=(--help)
            AZVM_ENV_FILE=''
            AZVM_ENV_DIR=''
            return 0
        fi
    done
    while (( $# )); do
        case $1 in
            --env-file)
                [[ $azvm_seen == false ]] || { azvm_config_error '--env-file may be supplied only once.'; return 1; }
                [[ $# -ge 2 && -n $2 && $2 != --* ]] || { azvm_config_error 'Missing path after --env-file.'; return 1; }
                azvm_file=$2; azvm_seen=true; shift 2 ;;
            --env-file=*)
                [[ $azvm_seen == false ]] || { azvm_config_error '--env-file may be supplied only once.'; return 1; }
                azvm_file=${1#--env-file=}
                [[ -n $azvm_file ]] || { azvm_config_error 'Missing path in --env-file=PATH.'; return 1; }
                azvm_seen=true; shift ;;
            *) azvm_remaining+=("$1"); shift ;;
        esac
    done
    [[ $azvm_seen == true ]] || { azvm_config_error 'Required argument: --env-file PATH. Run with --help for usage.'; return 1; }
    for azvm_arg in python3 jq; do
        command -v "$azvm_arg" >/dev/null 2>&1 || { azvm_config_error "Required command not found: $azvm_arg."; return 1; }
    done

    # Parse everything successfully before changing any configuration variable.
    # No source/eval, command substitution, or arbitrary environment expansion
    # is performed on the supplied file.
    azvm_json=$(python3 - "$azvm_file" "${HOME-}" <<'PY'
import json
import os
import re
import sys

allowed = {
    "AZVM_SUBSCRIPTION", "AZVM_RESOURCE_GROUP", "AZVM_LOCATION",
    "AZVM_COUNT", "AZVM_NAME_PREFIX", "AZVM_SSH_KEY", "AZVM_ADMIN_USER",
    "AZVM_START", "AZVM_DIGITS", "AZVM_IMAGE", "AZVM_SIZE",
    "AZVM_OS_DISK_SIZE", "AZVM_STORAGE_SKU", "AZVM_SECURITY_TYPE",
    "AZVM_ACCELERATED_NETWORKING", "AZVM_SSH_SOURCE", "AZVM_PUBLIC_IP",
    "AZVM_SUBNET_ID", "AZVM_VNET_CIDR", "AZVM_SUBNET_CIDR",
    "AZVM_UPDATE_SSH_CONFIG", "AZVM_SSH_CONFIG_FILE", "AZVM_SSH_PRIVATE_KEY",
    "AZVM_SSH_PORT", "AZVM_SSH_ADDRESS",
}
filename = os.path.abspath(sys.argv[1])
user_home = sys.argv[2]
assignment = re.compile(r"(?:export[ \t]+)?([A-Za-z_][A-Za-z0-9_]*)[ \t]*=[ \t]*(.*)\Z")


def fail(line, message):
    print(f"ERROR: {filename}:{line}: {message}", file=sys.stderr)
    raise SystemExit(1)


def home_expansion(text, index, line):
    if text.startswith("${HOME}", index):
        end = index + len("${HOME}")
    else:
        match = re.match(r"\$([A-Za-z_][A-Za-z0-9_]*)", text[index:])
        if not match or match[1] != "HOME":
            fail(line, "only $HOME or ${HOME} expansion is supported; shell commands and other expansions are not allowed")
        end = index + len(match[0])
    if not user_home:
        fail(line, "$HOME is empty or undefined")
    return user_home, end


def parse_value(text, line):
    result = []
    index = 0
    quote = None
    while index < len(text):
        char = text[index]
        if quote == "'":
            if char == "'":
                quote = None
            else:
                result.append(char)
            index += 1
            continue
        if char == quote and quote == '"':
            quote = None
            index += 1
            continue
        if quote is None:
            if char in "'\"":
                quote = char
                index += 1
                continue
            if char in " \t":
                rest = text[index:].lstrip(" \t")
                if rest and not rest.startswith("#"):
                    fail(line, "quote values containing spaces; only a comment may follow a value")
                break
            if char == "#" and index == 0:
                break
            if char in ";&|<>()":
                fail(line, "shell operators and commands are not allowed in environment files")
        if char == "`":
            fail(line, "command substitution is not allowed in environment files")
        if char == "$":
            expanded, index = home_expansion(text, index, line)
            result.append(expanded)
            continue
        if char == "\\":
            if index + 1 >= len(text):
                fail(line, "unfinished escape; multiline values are not supported")
            following = text[index + 1]
            if quote is None or following in '$`"\\':
                result.append(following)
                index += 2
            else:
                result.append(char)
                index += 1
            continue
        result.append(char)
        index += 1
    if quote is not None:
        fail(line, "unterminated quote; values must fit on one line")
    value = "".join(result)
    if any(ord(char) < 32 or ord(char) == 127 for char in value):
        fail(line, "control characters are not allowed in configuration values")
    return value


if not os.path.isfile(filename):
    fail(1, "environment file does not exist or is not a regular file")
try:
    with open(filename, "r", encoding="utf-8-sig", newline=None) as stream:
        contents = stream.read()
except (OSError, UnicodeError) as error:
    fail(1, f"cannot read environment file: {error}")

settings = {}
for number, raw in enumerate(contents.splitlines(), 1):
    line = raw.lstrip(" \t")
    if not line or line.startswith("#"):
        continue
    match = assignment.fullmatch(line)
    if not match:
        fail(number, "expected [export] AZVM_NAME=value, a blank line, or a # comment")
    name, value = match.groups()
    if name not in allowed:
        fail(number, f"unknown or unsupported setting {name}")
    if name in settings:
        fail(number, f"duplicate setting {name}; keep one assignment per setting")
    settings[name] = parse_value(value, number)

print(json.dumps({"file": filename, "directory": os.path.dirname(filename), "values": settings}, separators=(",", ":")))
PY
    ) || return 1
    azvm_file=$(jq -erj '.file' <<< "$azvm_json") || { azvm_config_error 'Cannot parse the environment-file path.'; return 1; }
    azvm_dir=$(jq -erj '.directory' <<< "$azvm_json") || { azvm_config_error 'Cannot parse the environment-file directory.'; return 1; }

    # Stale exports from a previous source command must not fill missing values.
    for azvm_variable in "${!AZVM_@}"; do
        unset -v "$azvm_variable" || { azvm_config_error "Cannot clear inherited setting $azvm_variable."; return 1; }
    done
    while IFS= read -r -d '' azvm_variable && IFS= read -r -d '' azvm_value; do
        printf -v "$azvm_variable" '%s' "$azvm_value" || { azvm_config_error "Cannot load setting $azvm_variable."; return 1; }
        export -n "$azvm_variable" || { azvm_config_error "Cannot keep setting $azvm_variable local to the script."; return 1; }
    done < <(jq -j '.values | to_entries[] | .key, "\u0000", .value, "\u0000"' <<< "$azvm_json")
    AZVM_ENV_FILE=$azvm_file
    AZVM_ENV_DIR=$azvm_dir
    # The guarded expansion also works with nounset in Bash 4.0 through 4.3.
    AZVM_SCRIPT_ARGS=(${azvm_remaining[@]+"${azvm_remaining[@]}"})
}

azvm_resolve_path() {
    [[ $# == 1 && -n $1 ]] || { azvm_config_error 'A nonempty file path is required.'; return 1; }
    [[ -n ${AZVM_ENV_DIR-} ]] || { azvm_config_error 'Load --env-file before resolving configuration paths.'; return 1; }
    python3 - "$AZVM_ENV_DIR" "$1" <<'PY'
import os
import sys
value = sys.argv[2]
if any(ord(char) < 32 or ord(char) == 127 for char in value):
    print("ERROR: control characters are not allowed in file paths", file=sys.stderr)
    raise SystemExit(1)
print(os.path.abspath(os.path.join(sys.argv[1], value)), end="")
PY
}

azvm_hosts_action() {
    [[ -n ${HOSTS_FILE:-} ]] || return 0
    python3 "$SCRIPT_DIR/azure-vm-hosts.py" "$1" "$HOSTS_FILE" "${@:2}" ||
        { azvm_config_error "Runner hosts operation failed ($1): $HOSTS_FILE"; return 1; }
}
