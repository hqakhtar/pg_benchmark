#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
BASH_COMMAND_PATH="$(command -v bash)"
umask 077
mkdir -p -- "$ROOT/tests/.work"
TEST_ROOT="$ROOT/tests/.work/control-machine-$$-$RANDOM"
mkdir -- "$TEST_ROOT"
trap '
    if [[ "$?" == 0 ]]
    then
        rm -r -- "$TEST_ROOT"
    else
        printf "Fixtures preserved: %s\n" "$TEST_ROOT" >&2
    fi

' EXIT
mkdir -p -- "$TEST_ROOT/bin" "$TEST_ROOT/state" "$TEST_ROOT/home"
for command in bash dirname ln
do
    ln -s -- "$(command -v "$command")" "$TEST_ROOT/bin/$command"
done

cat >"$TEST_ROOT/command-fixture" <<'FIXTURE'
#!/usr/bin/env bash
set -euo pipefail
command="${0##*/}"
{
    printf '%s' "$command"
    printf '|%s' "$@"
    printf '\n'
} >>"$TEST_ROOT/trace"

case "$command" in
    id)
        [[ "$*" == -u ]] || exit 64
        printf '%s\n' "${TEST_UID:-1000}"
        ;;
    sudo)
        if [[ -n "${TEST_SUDO_EXIT:-}" ]]
        then
            exit "$TEST_SUDO_EXIT"
        fi

        exec "$@"
        ;;
    dpkg-query)
        [[ "$*" == '-W -f=${Status} ca-certificates' ]] || exit 64
        if [[ -n "${TEST_DPKG_EXIT:-}" ]]
        then
            printf 'Synthetic package-query failure\n' >&2
            exit "$TEST_DPKG_EXIT"
        fi

        [[ -f "$TEST_ROOT/state/certificates" ]] || exit 1
        printf 'install ok installed'
        ;;
    apt-get)
        case "$1" in
            update) exit "${TEST_APT_UPDATE_EXIT:-0}" ;;
            install)
                [[ "$2" == -y && "$3" == --no-install-recommends ]] || exit 64
                if [[ -n "${TEST_APT_INSTALL_EXIT:-}" ]]
                then
                    exit "$TEST_APT_INSTALL_EXIT"
                fi

                [[ "${TEST_APT_NO_EFFECT:-false}" == false ]] || exit 0
                shift 3
                for package in "$@"
                do
                    commands=()
                    case "$package" in
                        ansible) commands=(ansible ansible-playbook ansible-galaxy ansible-inventory ansible-doc) ;;
                        openssh-client) commands=(ssh scp) ;;
                        rsync|git|tar|gzip) commands=("$package") ;;
                        ca-certificates) : >"$TEST_ROOT/state/certificates" ;;
                        *) printf 'Unexpected package: %s\n' "$package" >&2; exit 64 ;;
                    esac
                    for executable in "${commands[@]}"
                    do
                        ln -sf -- "$TEST_ROOT/command-fixture" "$TEST_ROOT/bin/$executable"
                    done
                done
                ;;
            *) exit 64 ;;
        esac
        ;;
    ansible-doc)
        [[ "$*" == '--type module --list_files ansible.posix' ]] || exit 64
        if [[ -n "${TEST_DOC_EXIT:-}" ]]
        then
            printf 'Synthetic Ansible discovery failure\n' >&2
            exit "$TEST_DOC_EXIT"
        fi

        if [[ -f "$TEST_ROOT/state/collection" ]]
        then
            printf 'ansible.posix.synchronize /synthetic/collection/synchronize.py\n'
        fi

        ;;
    ansible-galaxy)
        [[ "$*" == 'collection install ansible.posix:1.5.4' ]] || exit 64
        if [[ -n "${TEST_GALAXY_EXIT:-}" ]]
        then
            exit "$TEST_GALAXY_EXIT"
        fi

        if [[ "${TEST_GALAXY_NO_EFFECT:-false}" == false ]]
        then
            : >"$TEST_ROOT/state/collection"
        fi

        ;;
    ansible-playbook)
        [[ $# == 4 && "$1" == -i && "$2" == localhost, &&
           "$3" == "$TEST_PROJECT_ROOT/setup/ansible/runner_setup.yml" &&
           "$4" == --syntax-check ]] || exit 64
        exit "${TEST_SYNTAX_EXIT:-0}"
        ;;
    *)
        printf 'Unexpected command execution: %s\n' "$command" >&2
        exit 64
        ;;
esac
FIXTURE
chmod +x "$TEST_ROOT/command-fixture"

BASE_ENV=("PATH=$TEST_ROOT/bin" "HOME=$TEST_ROOT/home" "TEST_ROOT=$TEST_ROOT" "TEST_PROJECT_ROOT=$ROOT")
EXTRA_ENV=()
commands=(apt-get dpkg-query id sudo ansible ansible-playbook ansible-galaxy ansible-inventory ansible-doc
    rsync ssh scp git tar gzip)
passed=0

reset_case()
{
    for command in "${commands[@]}"
    do
        ln -sf -- "$TEST_ROOT/command-fixture" "$TEST_ROOT/bin/$command"
    done
    : >"$TEST_ROOT/state/certificates"
    : >"$TEST_ROOT/state/collection"
    EXTRA_ENV=()
}

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
    : >"$TEST_ROOT/trace"
    status=0
    env -i "${BASE_ENV[@]}" "${EXTRA_ENV[@]}" \
        "$BASH_COMMAND_PATH" "$ROOT/setup/setup_control_machine.sh" "$@" \
        >"$TEST_ROOT/current.log" 2>&1 || status=$?
    if [[ "$status" != 0 ]] && grep -Fq 'Control-machine setup complete.' "$TEST_ROOT/current.log"
    then
        printf 'A failed setup reported success\n' >&2
        exit 1
    fi
}

assert_no_install()
{
    if grep -Eq '^(apt-get|sudo|ansible-galaxy)\|' "$TEST_ROOT/trace"
    then
        printf 'Unexpected install or privilege escalation; see %s/trace\n' "$TEST_ROOT" >&2
        exit 1
    fi
}

assert_no_syntax_check()
{
    if grep -q '^ansible-playbook|' "$TEST_ROOT/trace"
    then
        printf 'Ansible ran after dependency setup failed\n' >&2
        exit 1
    fi
}

pass()
{
    passed=$((passed + 1))
    printf 'PASS: %s\n' "$1"
}

reset_case
rm -- "$TEST_ROOT/bin/sudo"
invoke
assert test "$status" == 0
assert_no_install
assert grep -Fxq "ansible-playbook|-i|localhost,|$ROOT/setup/ansible/runner_setup.yml|--syntax-check" "$TEST_ROOT/trace"
assert grep -Fq 'Control-machine setup complete.' "$TEST_ROOT/current.log"
pass 'ready machines need neither sudo nor downloads and only run an offline syntax check'

reset_case
for command in ansible ansible-playbook ansible-galaxy ansible-inventory ansible-doc rsync ssh scp git tar gzip
do
    rm -- "$TEST_ROOT/bin/$command"
done
rm -- "$TEST_ROOT/state/certificates" "$TEST_ROOT/state/collection"
invoke
assert test "$status" == 0
assert grep -Fxq 'sudo|apt-get|update' "$TEST_ROOT/trace"
assert grep -Fxq 'sudo|apt-get|install|-y|--no-install-recommends|ansible|rsync|git|tar|gzip|openssh-client|ca-certificates' "$TEST_ROOT/trace"
assert grep -Fxq 'ansible-galaxy|collection|install|ansible.posix:1.5.4' "$TEST_ROOT/trace"
assert test "$(grep -c '^sudo|' "$TEST_ROOT/trace")" == 2
for command in ansible ansible-playbook ansible-galaxy ansible-inventory ansible-doc rsync ssh scp git tar gzip
do
    assert test -x "$TEST_ROOT/bin/$command"
done
invoke
assert test "$status" == 0
assert_no_install
pass 'fresh setup installs only control tools, keeps Galaxy unprivileged, and is idempotent'

reset_case
rm -- "$TEST_ROOT/bin/rsync" "$TEST_ROOT/bin/sudo"
EXTRA_ENV=("TEST_UID=0")
invoke
assert test "$status" == 0
assert grep -Fxq 'apt-get|install|-y|--no-install-recommends|rsync' "$TEST_ROOT/trace"
if grep -q '^sudo|' "$TEST_ROOT/trace"
then
    printf 'A root login unexpectedly required sudo\n' >&2
    exit 1
fi

pass 'a root login installs missing packages directly without changing existing tools'

reset_case
rm -- "$TEST_ROOT/state/collection" "$TEST_ROOT/bin/sudo"
invoke
assert test "$status" == 0
assert grep -Fxq 'ansible-galaxy|collection|install|ansible.posix:1.5.4' "$TEST_ROOT/trace"
if grep -Eq '^(apt-get|sudo)\|' "$TEST_ROOT/trace"
then
    printf 'Collection-only installation unexpectedly used apt or sudo\n' >&2
    exit 1
fi

pass 'a missing collection alone is installed for the invoking user without system changes'

reset_case
rm -- "$TEST_ROOT/bin/rsync" "$TEST_ROOT/bin/sudo"
invoke
assert test "$status" != 0
assert grep -Fq 'requires sudo or a root login' "$TEST_ROOT/current.log"
assert_no_install
reset_case
rm -- "$TEST_ROOT/bin/apt-get"
invoke
assert test "$status" != 0
assert grep -Fq 'requires Ubuntu/Debian' "$TEST_ROOT/current.log"
assert_no_install
reset_case
EXTRA_ENV=("SUDO_USER=normal-user" "TEST_UID=0")
invoke
assert test "$status" != 0
assert grep -Fq 'without sudo' "$TEST_ROOT/current.log"
assert_no_install
pass 'unsupported package managers, missing privileges, and whole-command sudo fail safely'

reset_case
rm -- "$TEST_ROOT/bin/rsync"
EXTRA_ENV=("TEST_SUDO_EXIT=71")
invoke
assert test "$status" == 71
assert_no_syntax_check
EXTRA_ENV=("TEST_APT_UPDATE_EXIT=72")
invoke
assert test "$status" == 72
if grep -q '^apt-get|install|' "$TEST_ROOT/trace"
then
    printf 'Package installation ran after apt update failed\n' >&2
    exit 1
fi

assert_no_syntax_check
EXTRA_ENV=("TEST_APT_INSTALL_EXIT=73")
invoke
assert test "$status" == 73
assert_no_syntax_check
pass 'sudo, apt update, and package-install errors retain their exit status and stop setup'

reset_case
rm -- "$TEST_ROOT/bin/rsync"
EXTRA_ENV=("TEST_APT_NO_EFFECT=true")
invoke
assert test "$status" != 0
assert grep -Fq 'Required executable not found: rsync' "$TEST_ROOT/current.log"
assert_no_syntax_check
reset_case
rm -- "$TEST_ROOT/state/certificates"
EXTRA_ENV=("TEST_APT_NO_EFFECT=true")
invoke
assert test "$status" != 0
assert grep -Fq 'CA certificates are not installed' "$TEST_ROOT/current.log"
assert_no_syntax_check
pass 'successful package-manager exits must actually provide the requested dependencies'

reset_case
rm -- "$TEST_ROOT/state/collection"
EXTRA_ENV=("TEST_GALAXY_EXIT=74")
invoke
assert test "$status" == 74
assert_no_syntax_check
EXTRA_ENV=("TEST_GALAXY_NO_EFFECT=true")
invoke
assert test "$status" != 0
assert grep -Fq 'ansible.posix.synchronize is still unavailable' "$TEST_ROOT/current.log"
assert_no_syntax_check
pass 'collection download and search-path failures cannot report a usable setup'

reset_case
EXTRA_ENV=("TEST_DPKG_EXIT=75")
invoke
assert test "$status" == 75
assert grep -Fq 'Cannot inspect the CA certificates package' "$TEST_ROOT/current.log"
assert_no_install
assert_no_syntax_check
reset_case
EXTRA_ENV=("TEST_DOC_EXIT=76")
invoke
assert test "$status" == 76
assert_no_install
assert_no_syntax_check
reset_case
EXTRA_ENV=("TEST_SYNTAX_EXIT=77")
invoke
assert test "$status" == 77
assert_no_install
pass 'broken package queries, Ansible discovery, and syntax validation fail explicitly'

reset_case
invoke --host runner-a
assert test "$status" == 2
assert test ! -s "$TEST_ROOT/trace"
pass 'the installer accepts no runner targets or other provisioning arguments'

printf '\n%s control-machine checks passed; package managers, sudo, and Ansible were mocked.\n' "$passed"
