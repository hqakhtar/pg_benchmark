#!/usr/bin/env bash
set -Eeuo pipefail

# Run one hook in an isolated process, inheriting the resolved configuration.
ROOT="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
source "$ROOT/lib/common.sh"
source "$ROOT/lib/postgresql.sh"

trap 'printf "ERROR: Adapter failed at line %s (exit %s)\n" "$LINENO" "$?" >&2' ERR

module="$1"
hook="$2"
directory="$3"
case "$hook" in
    benchmark_prepare|benchmark_run|benchmark_cleanup) ;;
    *) fail "Unknown adapter operation: $hook"; exit 2 ;;
esac
# shellcheck source=/dev/null
source "$module"
"$hook" "$directory"
