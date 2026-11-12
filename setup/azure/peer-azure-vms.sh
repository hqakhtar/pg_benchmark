#!/usr/bin/env bash
if [[ "${BASH_SOURCE[0]}" != "$0" ]]
then
    printf 'Run this script as a command; do not source it. Use --env-file PATH.\n' >&2
    return 1
fi

set -Eeuo pipefail
SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/azure-vm-common.sh"
azvm_load_env_file "$@" || exit 1

if [[ "${AZVM_SCRIPT_ARGS[0]-}" == --help ]]
then
    exec python3 "$SCRIPT_DIR/azure-vm-peering.py" --help
fi

exec python3 "$SCRIPT_DIR/azure-vm-peering.py" \
    --subscription "${AZVM_SUBSCRIPTION-}" \
    --resource-group "${AZVM_RESOURCE_GROUP-}" \
    --name-prefix "${AZVM_NAME_PREFIX-}" \
    --count "${AZVM_COUNT-}" \
    --start "${AZVM_START-1}" \
    --digits "${AZVM_DIGITS-2}" \
    "${AZVM_SCRIPT_ARGS[@]}"
