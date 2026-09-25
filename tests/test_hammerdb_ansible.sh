#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
for command in ansible-playbook jq tar gzip sha256sum
do
    command -v "$command" >/dev/null ||
    {
        printf 'Missing test dependency: %s\n' "$command" >&2
        exit 1
    }

done
umask 077
mkdir -p -- "$ROOT/tests/.work"
TEST_ROOT="$ROOT/tests/.work/hammerdb-ansible-$$-$RANDOM"
mkdir -- "$TEST_ROOT"
trap '
    if [[ "$?" == 0 ]];
    then
        rm -r -- "$TEST_ROOT";
    else
        printf "Fixtures preserved: %s\n" "$TEST_ROOT" >&2;
    fi

' EXIT
export ANSIBLE_LOCAL_TEMP="$TEST_ROOT/ansible-local" ANSIBLE_REMOTE_TEMP="$TEST_ROOT/ansible-remote"
export ANSIBLE_NOCOLOR=1 TMPDIR="$TEST_ROOT"
export HAMMERDB_TEST_EXECUTION_MARKER="$TEST_ROOT/launcher-executed"
INSTALL_NAME=HammerDB-5.0
PLAYBOOK="$ROOT/setup/ansible/runner_setup.yml"
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

refute()
{
    if "$@"
    then
        printf 'Unexpected success: ' >&2
        printf '%q ' "$@" >&2
        printf '\nSee %s/current.log\n' "$TEST_ROOT" >&2
        exit 1
    fi
}

pass()
{
    passed=$((passed + 1))
    printf 'PASS: %s\n' "$1"
}

make_archive()
{
    local label="$1" directory="${2:-$INSTALL_NAME}" mode="${3:-0755}"
    local source="$TEST_ROOT/sources/$label"
    mkdir -p -- "$source/$directory" "$TEST_ROOT/archives"
    cat >"$source/$directory/hammerdbcli" <<'SH'
#!/bin/sh
: >"${HAMMERDB_TEST_EXECUTION_MARKER:?}"
exit 99
SH
    chmod "$mode" "$source/$directory/hammerdbcli"
    printf '%s\n' "$label" >"$source/$directory/payload"
    ln -s payload "$source/$directory/payload-link"
    ln "$source/$directory/payload" "$source/$directory/payload-hardlink"
    tar -czf "$TEST_ROOT/archives/$label.tar.gz" -C "$source" "$directory"
}

start_case()
{
    CASE_ROOT="$TEST_ROOT/$1"
    inventory="localhost,"
    mkdir -p -- "$CASE_ROOT/archives"
    for host in localhost runner-a runner-b
    do
        cp -- "$TEST_ROOT/archives/good.tar.gz" "$CASE_ROOT/archives/$host.tar.gz"
    done
    jq -n --arg root "$CASE_ROOT" --arg user "$(id -un)" '{
        ansible_connection: "local",
        ansible_become: false,
        ansible_user: $user,
        runner_user: $user,
        runner_home: ($root + "/runners/{{ inventory_hostname }}"),
        local_hammerdb_tarball: ($root + "/archives/{{ inventory_hostname }}.tar.gz")
    }' >"$CASE_ROOT/vars.json"
}

invoke()
{
    status=0
    # Never run untagged provisioning, a remote connection, or an actual launcher.
    ansible-playbook -i "$inventory" -c local "$PLAYBOOK" --tags hammerdb \
        -e @"$CASE_ROOT/vars.json" "$@" >"$TEST_ROOT/current.log" 2>&1 || status=$?
    assert test ! -e "$HAMMERDB_TEST_EXECUTION_MARKER"
}

seed_install()
{
    local home="$CASE_ROOT/runners/$1"
    mkdir -p -- "$home"
    tar -xzf "$TEST_ROOT/archives/good.tar.gz" -C "$home"
    printf 'keep existing contents\n' >"$home/$INSTALL_NAME/old-only"
    chmod 0711 "$home/$INSTALL_NAME"
    chmod 0751 "$home/$INSTALL_NAME/hammerdbcli"
}

snapshot()
{
    local directory="$1" output="$2"
    (
        cd -- "$directory"
        find . -printf '%P\t%y\t%m\t%U\t%G\t%i\t%s\t%T@\t%C@\t%l\n' | LC_ALL=C sort
        find . -type f -print0 | LC_ALL=C sort -z | xargs -0 -r sha256sum
    ) >"$output"
}

assert_preserved()
{
    snapshot "$1" "$TEST_ROOT/after.state"
    assert cmp "$2" "$TEST_ROOT/after.state"
}

assert_no_changes()
{
    assert grep -Eq 'changed=0[[:space:]]' "$TEST_ROOT/current.log"
    refute grep -Eq 'changed=[1-9]' "$TEST_ROOT/current.log"
}

assert_preflight_failure()
{
    assert test "$status" != 0
    assert grep -Fq "$1" "$TEST_ROOT/current.log"
    assert_no_changes
    refute grep -Fq 'TASK [Ensure HammerDB parent directory exists]' "$TEST_ROOT/current.log"
}

assert_installed()
{
    local directory="${3:-$INSTALL_NAME}" label="${2:-good}"
    local target="$CASE_ROOT/runners/$1/$directory"
    assert test -d "$target"
    assert test -x "$target/hammerdbcli"
    assert cmp "$TEST_ROOT/sources/$label/$directory/hammerdbcli" "$target/hammerdbcli"
    assert grep -Fxq "$label" "$target/payload"
    assert test "$(readlink "$target/payload-link")" == payload
    assert test "$(stat -c %i "$target/payload-hardlink")" == "$(stat -c %i "$target/payload")"
    assert test "$(stat -c %u "$target")" == "$(id -u)"
    assert test "$(stat -c %g "$target")" == "$(id -g)"
}

make_archive good
make_archive replacement
make_archive version-6 HammerDB-6.0
make_archive wrong-version HammerDB-4.9
make_archive nonexecutable "$INSTALL_NAME" 0644
make_archive empty
: >"$TEST_ROOT/sources/empty/$INSTALL_NAME/hammerdbcli"
tar -czf "$TEST_ROOT/archives/empty.tar.gz" -C "$TEST_ROOT/sources/empty" "$INSTALL_NAME"
make_archive no-launcher
rm -- "$TEST_ROOT/sources/no-launcher/$INSTALL_NAME/hammerdbcli"
tar -czf "$TEST_ROOT/archives/no-launcher.tar.gz" -C "$TEST_ROOT/sources/no-launcher" "$INSTALL_NAME"
make_archive unsafe-link
ln -s ../../outside "$TEST_ROOT/sources/unsafe-link/$INSTALL_NAME/escape"
tar -czf "$TEST_ROOT/archives/unsafe-link.tar.gz" -C "$TEST_ROOT/sources/unsafe-link" "$INSTALL_NAME"
make_archive indirect-link
ln -s . "$TEST_ROOT/sources/indirect-link/$INSTALL_NAME/redirect"
ln -s redirect/../outside "$TEST_ROOT/sources/indirect-link/$INSTALL_NAME/escape"
tar -czf "$TEST_ROOT/archives/indirect-link.tar.gz" -C "$TEST_ROOT/sources/indirect-link" "$INSTALL_NAME"
tar -cf "$TEST_ROOT/archives/nested-file.tar" -C "$TEST_ROOT/sources/good" "$INSTALL_NAME"
tar -rf "$TEST_ROOT/archives/nested-file.tar" -C "$TEST_ROOT/sources/good" \
    --transform="s#/payload\$#/hammerdbcli/child#" "$INSTALL_NAME/payload"
gzip -c "$TEST_ROOT/archives/nested-file.tar" >"$TEST_ROOT/archives/nested-file.tar.gz"
printf 'not gzip or tar\n' >"$TEST_ROOT/archives/malformed.tar.gz"
printf 'not tar\n' | gzip >"$TEST_ROOT/archives/not-tar.tar.gz"
head -c -4 "$TEST_ROOT/archives/good.tar.gz" >"$TEST_ROOT/archives/truncated.tar.gz"
tar -czf "$TEST_ROOT/archives/traversal.tar.gz" -C "$TEST_ROOT/sources/good" \
    --transform="s#^$INSTALL_NAME#$INSTALL_NAME/../outside#" "$INSTALL_NAME"

start_case version-mismatch
cp -- "$TEST_ROOT/archives/version-6.tar.gz" "$CASE_ROOT/archives/localhost.tar.gz"
invoke
assert_preflight_failure 'Invalid HammerDB archive'
assert grep -Fq 'HammerDB-6.0' "$TEST_ROOT/current.log"
assert test ! -e "$CASE_ROOT/runners"
invoke --check
assert_preflight_failure 'Invalid HammerDB archive'
assert test ! -e "$CASE_ROOT/runners"
pass 'a 6.0-only archive with the default 5.0 selection fails without creating either installation'

seed_install localhost
target="$CASE_ROOT/runners/localhost/$INSTALL_NAME"
snapshot "$target" "$TEST_ROOT/version-mismatch.state"
invoke
assert test "$status" == 0
assert_no_changes
assert_preserved "$target" "$TEST_ROOT/version-mismatch.state"
assert test ! -e "$CASE_ROOT/runners/localhost/HammerDB-6.0"
invoke -e reinstall_hammerdb=true
assert_preflight_failure 'Invalid HammerDB archive'
assert_preserved "$target" "$TEST_ROOT/version-mismatch.state"
assert test ! -e "$CASE_ROOT/runners/localhost/HammerDB-6.0"
invoke -e hammerdb_version=6.0
assert test "$status" == 0
assert_installed localhost version-6 HammerDB-6.0
assert_preserved "$target" "$TEST_ROOT/version-mismatch.state"
pass 'changing only the archive never upgrades 5.0; selecting 6.0 installs alongside the unchanged 5.0'

start_case version-6-fresh
cp -- "$TEST_ROOT/archives/version-6.tar.gz" "$CASE_ROOT/archives/localhost.tar.gz"
invoke -e hammerdb_version=6.0
assert test "$status" == 0
assert_installed localhost version-6 HammerDB-6.0
assert test ! -e "$CASE_ROOT/runners/localhost/HammerDB-5.0"
snapshot "$CASE_ROOT/runners/localhost/HammerDB-6.0" "$TEST_ROOT/version-6.state"
rm -- "$CASE_ROOT/archives/localhost.tar.gz"
invoke -e hammerdb_version=6.0
assert test "$status" == 0
assert_no_changes
assert_preserved "$CASE_ROOT/runners/localhost/HammerDB-6.0" "$TEST_ROOT/version-6.state"
assert test ! -e "$CASE_ROOT/runners/localhost/HammerDB-5.0"
pass 'an explicit 6.0 selection installs and reuses only HammerDB-6.0, never an empty 5.0 directory'

start_case fresh
for tag in hammerdb pg_benchmark pg_benchmark_update benchmark_config
do
    ansible-playbook -i "$inventory" -c local "$PLAYBOOK" --list-tasks --tags "$tag" \
        -e @"$CASE_ROOT/vars.json" >"$TEST_ROOT/current.log" 2>&1
    if [[ "$tag" == hammerdb ]]
    then
        assert grep -Fq 'Validate HammerDB archive integrity and layout' "$TEST_ROOT/current.log"
        assert grep -Fq 'Copy and extract HammerDB tarball' "$TEST_ROOT/current.log"
        refute grep -Eq 'Update apt|Install prerequisites|PGDG|Install PostgreSQL|screenrc|Upload pg_benchmark|Copy private' \
            "$TEST_ROOT/current.log"
    else
        refute grep -Fq 'TAGS: [hammerdb]' "$TEST_ROOT/current.log"
        refute grep -Fq 'HammerDB archive' "$TEST_ROOT/current.log"
    fi

done
pass 'HammerDB tags are independent; repository/configuration tags exclude archive preflight'

invoke
assert test "$status" == 0
assert_installed localhost
assert grep -Eq '^localhost[[:space:]]*: .*changed=[1-9].*failed=0' "$TEST_ROOT/current.log"
pass 'default setup installs a fresh requested version without executing its launcher'

target="$CASE_ROOT/runners/localhost/$INSTALL_NAME"
printf 'must survive normal setup\n' >"$target/retained"
chmod 0711 "$target"
chmod 0751 "$target/hammerdbcli"
chmod 0600 "$target/payload"
snapshot "$target" "$TEST_ROOT/existing.state"
invoke
assert test "$status" == 0
assert_no_changes
assert_preserved "$target" "$TEST_ROOT/existing.state"
rm -- "$CASE_ROOT/archives/localhost.tar.gz"
invoke
assert test "$status" == 0
assert_no_changes
assert grep -Fq 'no local HammerDB archive is required' "$TEST_ROOT/current.log"
assert_preserved "$target" "$TEST_ROOT/existing.state"
pass 'repeated setup preserves contents, modes, ownership and inodes even without a local archive'

start_case relative-archive
relative_archive="../../tests/.work/${TEST_ROOT##*/}/relative-archive/archives/localhost.tar.gz"
invoke -e "local_hammerdb_tarball=$relative_archive"
assert test "$status" == 0
assert_installed localhost
pass 'relative archives resolve consistently against the playbook for validation and extraction'

start_case skipped
seed_install localhost
target="$CASE_ROOT/runners/localhost/$INSTALL_NAME"
rm -- "$target/hammerdbcli" "$CASE_ROOT/archives/localhost.tar.gz"
snapshot "$target" "$TEST_ROOT/skipped.state"
invoke -e install_hammerdb=false -e reinstall_hammerdb=true
assert test "$status" == 0
assert_no_changes
assert_preserved "$target" "$TEST_ROOT/skipped.state"
pass 'install_hammerdb=false skips even incomplete installations and explicit reinstall requests'

start_case reinstall
seed_install localhost
target="$CASE_ROOT/runners/localhost/$INSTALL_NAME"
mkdir -- "$CASE_ROOT/runners/localhost/HammerDB-4.9"
printf 'older version\n' >"$CASE_ROOT/runners/localhost/HammerDB-4.9/preserved"
snapshot "$CASE_ROOT/runners" "$TEST_ROOT/reinstall.state"
for archive in missing directory symlink unreadable malformed not-tar truncated wrong-version \
    no-launcher nonexecutable empty unsafe-link indirect-link nested-file traversal
do
    local_archive="$CASE_ROOT/archives/$archive.tar.gz"
    message='Invalid HammerDB archive'
    case "$archive" in
        missing)
            message='A readable regular HammerDB archive is required'
            ;;
        directory)
            mkdir -- "$local_archive"
            message='A readable regular HammerDB archive is required'
            ;;
        symlink)
            ln -s "$TEST_ROOT/archives/good.tar.gz" "$local_archive"
            message='A readable regular HammerDB archive is required'
            ;;
        unreadable)
            cp -- "$TEST_ROOT/archives/good.tar.gz" "$local_archive"
            chmod 000 "$local_archive"
            if [[ -r "$local_archive" ]]
            then
                printf 'SKIP: current user can read mode-000 archives\n'
                continue
            fi

            message='A readable regular HammerDB archive is required'
            ;;
        *)
            cp -- "$TEST_ROOT/archives/$archive.tar.gz" "$local_archive"
            ;;
    esac
    invoke -e reinstall_hammerdb=true -e "local_hammerdb_tarball=$local_archive"
    assert_preflight_failure "$message"
    assert_preserved "$CASE_ROOT/runners" "$TEST_ROOT/reinstall.state"
done
pass 'unusable, corrupt, wrong-layout, nonexecutable and unsafe archives preserve existing installations'

for directory in / . .. ../outside HammerDB-4.9
do
    invoke -e reinstall_hammerdb=true -e "hammerdb_dir_name=$directory"
    assert_preflight_failure 'Unsafe HammerDB installation target'
    assert_preserved "$CASE_ROOT/runners" "$TEST_ROOT/reinstall.state"
done
invoke -e reinstall_hammerdb=true -e hammerdb_dest=/
assert_preflight_failure 'Unsafe HammerDB installation target'
assert_preserved "$CASE_ROOT/runners" "$TEST_ROOT/reinstall.state"
pass 'unsafe or mismatched deletion targets fail before filesystem changes'

invoke -e reinstall_hammerdb=true -e "local_hammerdb_tarball=$TEST_ROOT/archives/replacement.tar.gz"
assert test "$status" == 0
assert_installed localhost replacement
assert test ! -e "$target/old-only"
assert grep -Fxq 'older version' "$CASE_ROOT/runners/localhost/HammerDB-4.9/preserved"
pass 'explicit reinstall replaces only the selected version after successful preflight'

start_case incomplete
inventory="missing-cli,empty-cli,nonexec-cli,linked-cli,linked-root,not-directory,"
for host in missing-cli empty-cli nonexec-cli linked-cli linked-root not-directory
do
    seed_install "$host"
    cp -- "$TEST_ROOT/archives/good.tar.gz" "$CASE_ROOT/archives/$host.tar.gz"
done
rm -- "$CASE_ROOT/runners/missing-cli/$INSTALL_NAME/hammerdbcli"
: >"$CASE_ROOT/runners/empty-cli/$INSTALL_NAME/hammerdbcli"
chmod 0644 "$CASE_ROOT/runners/nonexec-cli/$INSTALL_NAME/hammerdbcli"
rm -- "$CASE_ROOT/runners/linked-cli/$INSTALL_NAME/hammerdbcli"
ln -s payload "$CASE_ROOT/runners/linked-cli/$INSTALL_NAME/hammerdbcli"
mv -- "$CASE_ROOT/runners/linked-root/$INSTALL_NAME" "$CASE_ROOT/link-target"
ln -s "$CASE_ROOT/link-target" "$CASE_ROOT/runners/linked-root/$INSTALL_NAME"
rm -r -- "$CASE_ROOT/runners/not-directory/$INSTALL_NAME"
printf 'not a directory\n' >"$CASE_ROOT/runners/not-directory/$INSTALL_NAME"
snapshot "$CASE_ROOT/runners" "$TEST_ROOT/incomplete.state"
snapshot "$CASE_ROOT/link-target" "$TEST_ROOT/link-target.state"
invoke
assert_preflight_failure 'set -e reinstall_hammerdb=true'
for host in missing-cli empty-cli nonexec-cli linked-cli linked-root not-directory
do
    assert grep -Fq "fatal: [$host]" "$TEST_ROOT/current.log"
done
assert_preserved "$CASE_ROOT/runners" "$TEST_ROOT/incomplete.state"
invoke -e reinstall_hammerdb=true
assert test "$status" == 0
for host in missing-cli empty-cli nonexec-cli linked-cli linked-root not-directory
do
    assert_installed "$host"
    assert test ! -e "$CASE_ROOT/runners/$host/$INSTALL_NAME/old-only"
done
assert_preserved "$CASE_ROOT/link-target" "$TEST_ROOT/link-target.state"
pass 'incomplete directories, nonregular launchers and invalid targets require explicit reinstall and recover safely'

start_case check
invoke --check
assert test "$status" == 0
assert_no_changes
assert grep -Fq 'check mode will not remove or install' "$TEST_ROOT/current.log"
assert test ! -e "$CASE_ROOT/runners"
rm -- "$CASE_ROOT/archives/localhost.tar.gz"
invoke --check
assert_preflight_failure 'A readable regular HammerDB archive is required'
assert test ! -e "$CASE_ROOT/runners"
seed_install localhost
snapshot "$CASE_ROOT/runners" "$TEST_ROOT/check.state"
invoke --check
assert test "$status" == 0
assert_no_changes
assert_preserved "$CASE_ROOT/runners" "$TEST_ROOT/check.state"
invoke --check -e reinstall_hammerdb=true -e "local_hammerdb_tarball=$TEST_ROOT/archives/replacement.tar.gz"
assert test "$status" == 0
assert_no_changes
assert_preserved "$CASE_ROOT/runners" "$TEST_ROOT/check.state"
pass 'check mode validates prerequisites but never installs or removes files'

for mode in fresh reinstall
do
    start_case "multi-$mode"
    inventory="runner-a,runner-b,"
    flags=()
    if [[ "$mode" == reinstall ]]
    then
        seed_install runner-a
        seed_install runner-b
        snapshot "$CASE_ROOT/runners" "$TEST_ROOT/multi.state"
        flags=(-e reinstall_hammerdb=true)
    fi

    cp -- "$TEST_ROOT/archives/malformed.tar.gz" "$CASE_ROOT/archives/runner-b.tar.gz"
    invoke "${flags[@]}"
    assert_preflight_failure 'Invalid HammerDB archive'
    assert grep -Fq 'fatal: [runner-b -> localhost]' "$TEST_ROOT/current.log"
    if [[ "$mode" == reinstall ]]
    then
        assert_preserved "$CASE_ROOT/runners" "$TEST_ROOT/multi.state"
    else
        assert test ! -e "$CASE_ROOT/runners"
    fi

done
pass 'one failed host prevents fresh installs or destructive reinstalls on every selected host'

for installed in runner-a runner-b
do
    start_case "mixed-$installed"
    inventory="runner-a,runner-b,"
    missing=runner-a
    if [[ "$installed" == runner-a ]]
    then
        missing=runner-b
    fi

    seed_install "$installed"
    snapshot "$CASE_ROOT/runners/$installed" "$TEST_ROOT/mixed.state"
    rm -- "$CASE_ROOT/archives/runner-a.tar.gz" "$CASE_ROOT/archives/runner-b.tar.gz"
    invoke
    assert_preflight_failure 'A readable regular HammerDB archive is required'
    assert test ! -e "$CASE_ROOT/runners/$missing"
    assert_preserved "$CASE_ROOT/runners/$installed" "$TEST_ROOT/mixed.state"
    cp -- "$TEST_ROOT/archives/good.tar.gz" "$CASE_ROOT/archives/$missing.tar.gz"
    invoke
    assert test "$status" == 0
    assert_installed "$missing"
    assert_preserved "$CASE_ROOT/runners/$installed" "$TEST_ROOT/mixed.state"
    assert grep -Eq "^${installed}[[:space:]]*: .*changed=0 .*failed=0" "$TEST_ROOT/current.log"
done
pass 'mixed installed/missing hosts are checked individually in either inventory order'

printf '\n%s local HammerDB Ansible checks passed; no remote provisioning or benchmark execution.\n' "$passed"
