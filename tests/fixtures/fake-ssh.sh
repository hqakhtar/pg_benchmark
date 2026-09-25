#!/usr/bin/env bash
set -euo pipefail

{
    printf 'ssh-argv:'
    printf ' <%s>' "$@"
    printf '\n'
} >>"$TEST_SSH_ARGUMENTS"

while (($#)); do
    case "$1" in
        -o) shift 2 ;;
        --) shift; break ;;
        *) break ;;
    esac
done
host="$1"
shift
[[ "$#" == 2 && "$1" == bash && "$2" == -s ]] || exit 64
packet_directory="$TEST_ROOT/packets/$BASHPID-$RANDOM"
mkdir -- "$packet_directory"
packet="$packet_directory/request"
trap 'rm -f -- "$packet"; rmdir -- "$packet_directory"' EXIT
cat >"$packet"
action="" phase=""
while IFS= read -r line; do
    case "$line" in
        remote_action=*) action="${line#remote_action=}" ;;
        request_phase=*) phase="${line#request_phase=}" ;;
    esac
done <"$packet"
printf 'ssh|%s|%s|%s\n' "$host" "$action" "$phase" >>"$TEST_TRACE"
if [[ "${TEST_FAIL_PREFLIGHT_HOST:-}" == "$host" && "$phase" == preflight ]];
then
    exit 255
fi

if [[ "${TEST_FAIL_DATA_HOST:-}" == "$host" && "$phase" == data ]];
then
    exit 23
fi

env -i \
    "PATH=$TEST_BIN:/usr/bin:/bin" "HOME=$TEST_ROOT/home" \
    "TEST_ROOT=$TEST_ROOT" "TEST_BIN=$TEST_BIN" "TEST_PROJECT_ROOT=$TEST_PROJECT_ROOT" \
    "TEST_TCLSH=$TEST_TCLSH" "TEST_REAL_TEE=$TEST_REAL_TEE" "TEST_TRACE=$TEST_TRACE" \
    "TEST_CURRENT_HOST=$host" "TEST_CURRENT_PHASE=$phase" \
    "TEST_EXPECT_EMPTY_ADMIN=${TEST_EXPECT_EMPTY_ADMIN:-false}" \
    bash -s <"$packet"
