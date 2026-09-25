#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
BASH_COMMAND_PATH="$(command -v bash)"
umask 077
mkdir -p -- "$ROOT/tests/.work"
TEST_ROOT="$ROOT/tests/.work/setup-$$-$RANDOM"
mkdir -- "$TEST_ROOT"
trap '
    if [[ "$?" == 0 ]]
    then
        rm -r -- "$TEST_ROOT"
    else
        printf "Fixtures preserved: %s\n" "$TEST_ROOT" >&2
    fi

' EXIT
CHECKOUT="$TEST_ROOT/checkout 'quoted"
CALLER="$TEST_ROOT/caller directory"
mkdir -p -- "$CHECKOUT/lib" "$CHECKOUT/setup/ansible" "$CALLER" \
    "$TEST_ROOT/bin" "$TEST_ROOT/without-ansible"
cp -- "$ROOT/wrapper.sh" "$CHECKOUT/wrapper.sh"
cp -- "$ROOT/lib/common.sh" "$CHECKOUT/lib/common.sh"
printf '%s\n' '---' >"$CHECKOUT/setup/ansible/runner_setup.yml"
cat >"$CHECKOUT/setup/setup_control_machine.sh" <<'CONTROL_SETUP'
#!/usr/bin/env bash
set -euo pipefail
[[ $# == 0 ]] || exit 64
printf '%s\n' "$PWD" >"$TEST_CONTROL_TRACE"
exit "${TEST_CONTROL_EXIT:-0}"
CONTROL_SETUP
printf '# Shared runners\r\nrunner-a\r\n\r\nrunner-b' >"$CHECKOUT/hosts.txt"
printf 'wrong-checkout-runner\n' >"$CALLER/hosts.txt"
for command in bash cat dirname
do
    ln -s -- "$(command -v "$command")" "$TEST_ROOT/bin/$command"
    ln -s -- "$(command -v "$command")" "$TEST_ROOT/without-ansible/$command"
done
# The child PATH cannot reach a real Ansible executable or native database tools.
cat >"$TEST_ROOT/bin/ansible-playbook" <<'ANSIBLE'
#!/usr/bin/env bash
set -euo pipefail
printf '%s\0' "$PWD" "$@" >"$TEST_ANSIBLE_TRACE"
printf 'Mock Ansible reached.\n'
exit "${TEST_ANSIBLE_EXIT:-0}"
ANSIBLE
chmod +x "$TEST_ROOT/bin/ansible-playbook"
# shellcheck disable=SC2016
printf 'printf "config sourced\\n" >"$TEST_CONFIG_TRACE"\nexit 91\n' >"$TEST_ROOT/poison.env"
BASE_ENV=(
    "PATH=$TEST_ROOT/bin" "HOME=$TEST_ROOT"
    "TEST_ANSIBLE_TRACE=$TEST_ROOT/ansible.argv" "TEST_CONFIG_TRACE=$TEST_ROOT/config.trace"
    "TEST_CONTROL_TRACE=$TEST_ROOT/control.trace"
    "CONNECTION_ENV_FILE=$TEST_ROOT/poison.env"
    "BENCHMARK_ENV_FILE=$TEST_ROOT/poison.env" "RUN_ENV_FILE=$TEST_ROOT/poison.env"
    "PGPASSWORD=synthetic-inherited-credential"
)
EXTRA_ENV=()
passed=0

assert()
{
    if ! "$@"
    then
        printf 'Assertion failed: ' >&2
        printf '%q ' "$@" >&2
        printf '\nSee %s/current.log\n' "$TEST_ROOT" >&2
        exit 1
    fi
}

invoke()
{
    rm -f -- "$TEST_ROOT/ansible.argv" "$TEST_ROOT/config.trace" "$TEST_ROOT/control.trace"
    status=0
    (
        cd -- "$CALLER"
        env -i "${BASE_ENV[@]}" "${EXTRA_ENV[@]}" \
            "$BASH_COMMAND_PATH" "$CHECKOUT/wrapper.sh" "$@"
    ) >"$TEST_ROOT/current.log" 2>&1 || status=$?
    assert test ! -e "$TEST_ROOT/config.trace"
    assert test ! -e "$CHECKOUT/benchmark-results"
    if grep -Fq 'synthetic-inherited-credential' "$TEST_ROOT/current.log"
    then
        printf 'An inherited credential appeared in setup output\n' >&2
        exit 1
    fi
}

assert_invocation()
{
    local i argument rendered expected_header
    local -a actual=() expected=("$CALLER" "$@")
    assert test ! -e "$TEST_ROOT/control.trace"
    assert test -f "$TEST_ROOT/ansible.argv"
    while IFS= read -r -d '' argument
    do
        actual+=("$argument")
    done <"$TEST_ROOT/ansible.argv"
    assert test "${#actual[@]}" == "${#expected[@]}"
    for ((i = 0; i < ${#expected[@]}; i++)); do
        assert test "${actual[i]}" == "${expected[i]}"
    done
    printf -v expected_header 'Ansible command (run from %q):' "$CALLER"
    printf -v rendered ' %q' "$@"
    assert grep -Fxq -- "$expected_header" "$TEST_ROOT/current.log"
    assert grep -Fxq -- "ansible-playbook$rendered" "$TEST_ROOT/current.log"
    assert test "$(grep -n '^ansible-playbook ' "$TEST_ROOT/current.log" | cut -d: -f1)" -lt \
        "$(grep -n '^Mock Ansible reached' "$TEST_ROOT/current.log" | cut -d: -f1)"
}

pass()
{
    passed=$((passed + 1))
    printf 'PASS: %s\n' "$1"
}

invoke --help
assert test "$status" == 0
assert grep -Fq -- '--setup [--host HOST]' "$TEST_ROOT/current.log"
assert grep -Fq -- '--setup-control-machine' "$TEST_ROOT/current.log"
assert test ! -e "$TEST_ROOT/ansible.argv"
assert test ! -e "$TEST_ROOT/control.trace"
pass 'help describes standalone setup without invoking Ansible or loading configs'

EXTRA_ENV=("PATH=$TEST_ROOT/without-ansible")
invoke --setup-control-machine
assert test "$status" == 0
assert grep -Fxq "$CALLER" "$TEST_ROOT/control.trace"
assert test ! -e "$TEST_ROOT/ansible.argv"
assert test ! -e "$CHECKOUT/connection.env"
EXTRA_ENV=("PATH=$TEST_ROOT/without-ansible" "TEST_CONTROL_EXIT=48")
invoke --setup-control-machine
assert test "$status" == 48
assert test ! -e "$TEST_ROOT/ansible.argv"
EXTRA_ENV=()
pass 'control-machine setup is independent of Ansible and benchmark configs and preserves failures'

for argument in hammerdb --setup --check --prepare --cleanup
do
    invoke --setup-control-machine "$argument"
    assert test "$status" == 2
    assert test ! -e "$TEST_ROOT/control.trace"
    assert test ! -e "$TEST_ROOT/ansible.argv"
done
invoke --setup-control-machine --host runner-a
assert test "$status" == 2
invoke --setup-control-machine -- --check
assert test "$status" == 2
for group in connection benchmark run
do
    invoke --setup-control-machine "--$group-env" "$TEST_ROOT/poison.env"
    assert test "$status" == 2
    assert test ! -e "$TEST_ROOT/control.trace"
done
pass 'control-machine installation cannot be combined with runner or benchmark actions'

invoke --setup
assert test "$status" == 0
assert_invocation -i "$CHECKOUT/hosts.txt" "$CHECKOUT/setup/ansible/runner_setup.yml"
assert test ! -e "$CHECKOUT/connection.env"
assert test ! -e "$CHECKOUT/hammerdb/hammerdb.env"
assert test ! -e "$CHECKOUT/run.env"
pass 'setup targets the checkout hosts file from any directory without benchmark environments'

rm -- "$CHECKOUT/hosts.txt"
invoke --setup --host new-runner.example
assert test "$status" == 0
assert_invocation -i new-runner.example, "$CHECKOUT/setup/ansible/runner_setup.yml"
pass 'a single new VM uses inline inventory and needs no hosts file'

invoke --setup
assert test "$status" != 0
assert grep -Fq 'hosts.txt.sample' "$TEST_ROOT/current.log"
assert test ! -e "$TEST_ROOT/ansible.argv"
printf '# No selected VMs\n\n' >"$CHECKOUT/hosts.txt"
invoke --setup
assert test "$status" != 0
assert grep -Fq 'No runners' "$TEST_ROOT/current.log"
assert test ! -e "$TEST_ROOT/ansible.argv"
pass 'missing and empty host lists fail instead of succeeding with no selected VMs'

for contents in $'runner-a\nrunner-a\n' $'[runners]\nrunner-a\n' \
    $'runner-a ansible_user=someone\n' $'runner-a;not-a-command\n'
do
    printf '%s' "$contents" >"$CHECKOUT/hosts.txt"
    invoke --setup
    assert test "$status" != 0
    assert test ! -e "$TEST_ROOT/ansible.argv"
done
pass 'duplicate and non-plain host lists are rejected before Ansible'

for host in '' '-not-a-host' 'runner-a,runner-b' 'runner-a runner-b' '[runners]' 'runner-a;not-a-command'
do
    invoke --setup --host "$host"
    assert test "$status" != 0
    assert test ! -e "$TEST_ROOT/ansible.argv"
done
invoke --setup --host
assert test "$status" == 2
invoke --setup --host runner-a --host runner-b
assert test "$status" == 2
assert test ! -e "$TEST_ROOT/ansible.argv"
pass 'single-VM selection rejects missing, multiple, and malformed names'

invoke --setup --check
assert test "$status" == 2
invoke --prepare --setup
assert test "$status" == 2
invoke hammerdb --setup
assert test "$status" == 2
invoke hammerdb --host runner-a
assert test "$status" == 2
invoke --setup -- -e reinstall_hammerdb=true
assert test "$status" != 0
assert test ! -e "$TEST_ROOT/ansible.argv"
for group in connection benchmark run
do
    invoke --setup "--$group-env" "$TEST_ROOT/poison.env"
    assert test "$status" == 2
    assert test ! -e "$TEST_ROOT/ansible.argv"
done
invoke hammerdb -- --check
assert test "$status" == 2
assert test ! -e "$TEST_ROOT/ansible.argv"
pass 'setup rejects benchmark actions/selectors and options cannot bypass target validation'

options=(--check --tags hammerdb -e reinstall_hammerdb=true
    -e "@private variables'quoted.yml" -e 'label=spaces [$] "quotes"; literal')
invoke --setup --host new-runner -- "${options[@]}"
assert test "$status" == 0
assert_invocation -i new-runner, "$CHECKOUT/setup/ansible/runner_setup.yml" "${options[@]}"
pass 'Ansible options retain their exact argument boundaries and are printed with reusable shell quoting'

EXTRA_ENV=("TEST_ANSIBLE_EXIT=47")
invoke --setup --host new-runner
assert test "$status" == 47
assert_invocation -i new-runner, "$CHECKOUT/setup/ansible/runner_setup.yml"
EXTRA_ENV=()
pass 'the printed command precedes execution and native Ansible failures retain their exit status'

EXTRA_ENV=("PATH=$TEST_ROOT/without-ansible")
invoke --setup --host new-runner
assert test "$status" != 0
assert grep -Fq 'Required executable not found: ansible-playbook' "$TEST_ROOT/current.log"
assert grep -Fq -- '--setup-control-machine' "$TEST_ROOT/current.log"
assert test ! -e "$TEST_ROOT/ansible.argv"
assert test ! -e "$TEST_ROOT/control.trace"
EXTRA_ENV=()
rm -- "$CHECKOUT/setup/ansible/runner_setup.yml"
invoke --setup --host new-runner
assert test "$status" != 0
assert grep -Fq 'Ansible playbook must be a readable regular file' "$TEST_ROOT/current.log"
assert test ! -e "$TEST_ROOT/ansible.argv"
pass 'missing provisioning dependencies and assets produce actionable errors'

rm -- "$CHECKOUT/setup/setup_control_machine.sh"
invoke --setup-control-machine
assert test "$status" != 0
assert grep -Fq 'Control-machine setup script must be a readable regular file' "$TEST_ROOT/current.log"
assert test ! -e "$TEST_ROOT/control.trace"
assert test ! -e "$TEST_ROOT/ansible.argv"
pass 'a missing local installer cannot silently report success'

printf '\n%s setup checks passed; Ansible was mocked and no VMs were contacted.\n' "$passed"
