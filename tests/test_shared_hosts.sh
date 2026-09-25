#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
for command in ansible-inventory ansible-playbook jq; do
    command -v "$command" >/dev/null ||
    {
        printf 'Missing test dependency: %s\n' "$command" >&2
        exit 1
    }

done
umask 077
mkdir -p -- "$ROOT/tests/.work"
TEST_ROOT="$ROOT/tests/.work/shared-hosts-$$-$RANDOM"
mkdir -- "$TEST_ROOT"
trap '
    if [[ "$?" == 0 ]];
    then
        rm -r -- "$TEST_ROOT";
    else
        printf "Fixtures preserved: %s\n" "$TEST_ROOT" >&2;
    fi

' EXIT
export ANSIBLE_LOCAL_TEMP="$TEST_ROOT/ansible-local"
export ANSIBLE_NOCOLOR=1
printf '# Shared runner hosts\nrunner-a\n\nrunner-b' >"$TEST_ROOT/hosts.txt"

assert_hosts()
{
    local output="$1" expected="$2" actual
    # Ansible does not preserve inventory order when listing playbook targets.
    actual="$(awk '
        /^[[:space:]]*hosts \([0-9]+\):$/ { in_hosts = 1; next }
        in_hosts && /^      [^[:space:]]+$/ { print $1 }
    ' "$output" | LC_ALL=C sort)"
    expected="$(printf '%s\n' "$expected" | LC_ALL=C sort)"
    if [[ "$actual" != "$expected" ]];
    then
        printf 'Unexpected playbook host selection:\n' >&2
        cat -- "$output" >&2
        exit 1
    fi
}

ansible-inventory -i "$TEST_ROOT/hosts.txt" --list >"$TEST_ROOT/inventory.json"
jq -e '.ungrouped.hosts == ["runner-a", "runner-b"]' "$TEST_ROOT/inventory.json"
printf 'PASS: Ansible accepts the shared plain hosts list, including comments and blank lines\n'

ansible-playbook -i "$TEST_ROOT/hosts.txt" "$ROOT/setup/ansible/runner_setup.yml" \
    --list-hosts >"$TEST_ROOT/all-hosts.log"
assert_hosts "$TEST_ROOT/all-hosts.log" $'runner-a\nrunner-b'
printf 'PASS: provisioning selects every host without requiring an inventory group\n'

ansible-playbook -i "$TEST_ROOT/hosts.txt" "$ROOT/setup/ansible/runner_setup.yml" \
    --list-hosts --limit runner-b >"$TEST_ROOT/limited-hosts.log"
assert_hosts "$TEST_ROOT/limited-hosts.log" runner-b
printf 'PASS: provisioning can still limit execution to one runner\n'

ansible-playbook -i "$TEST_ROOT/hosts.txt" "$ROOT/setup/ansible/runner_setup.yml" \
    --syntax-check
printf 'PASS: provisioning syntax is valid with the shared hosts list\n'

CHECKOUT="$TEST_ROOT/checkout with spaces"
mkdir -p -- "$CHECKOUT/lib" "$CHECKOUT/setup/ansible"
cp -- "$ROOT/wrapper.sh" "$CHECKOUT/wrapper.sh"
cp -- "$ROOT/lib/common.sh" "$CHECKOUT/lib/common.sh"
cp -- "$ROOT/setup/ansible/runner_setup.yml" "$ROOT/setup/ansible/vars.yml" "$CHECKOUT/setup/ansible/"
cp -- "$TEST_ROOT/hosts.txt" "$CHECKOUT/hosts.txt"
(
    cd -- "$TEST_ROOT"
    bash "$CHECKOUT/wrapper.sh" --setup -- --list-hosts
) >"$TEST_ROOT/wrapper-all-hosts.log"
assert_hosts "$TEST_ROOT/wrapper-all-hosts.log" $'runner-a\nrunner-b'
printf 'PASS: wrapper setup passes the checkout hosts list to real Ansible without runtime configs\n'

rm -- "$CHECKOUT/hosts.txt"
(
    cd -- "$TEST_ROOT"
    bash "$CHECKOUT/wrapper.sh" --setup --host new-runner -- --list-hosts
) >"$TEST_ROOT/wrapper-single-host.log"
assert_hosts "$TEST_ROOT/wrapper-single-host.log" new-runner
printf 'PASS: wrapper single-VM setup selects an unlisted VM without a hosts file\n'
printf '\n6 shared-host checks passed; host listing never connects to runners.\n'
