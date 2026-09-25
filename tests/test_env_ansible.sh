#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
for command in ansible-playbook rsync jq
do
    command -v "$command" >/dev/null ||
    {
        printf 'Missing test dependency: %s\n' "$command" >&2
        exit 1
    }

done
umask 077
mkdir -p -- "$ROOT/tests/.work"
TEST_ROOT="$ROOT/tests/.work/env-ansible-$$-$RANDOM"
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
mkdir -p -- "$TEST_ROOT/source/hammerdb" "$TEST_ROOT/source/nested" \
    "$TEST_ROOT/runner/hammerdb" "$TEST_ROOT/runner/nested" "$TEST_ROOT/private/hammerdb"
jq -n --arg root "$TEST_ROOT" --arg user "$(id -un)" '{
    ansible_connection: "local",
    ansible_become: false,
    ansible_user: $user,
    runner_user: $user,
    runner_home: ($root + "/home"),
    pg_benchmark_local_path: ($root + "/source"),
    pg_benchmark_dest: ($root + "/runner"),
    benchmark_config_local_dir: ($root + "/private"),
    update_pg_benchmark: true,
    update_benchmark_config: false,
    install_hammerdb: false
}' >"$TEST_ROOT/vars.json"

for path in connection.env run.env hammerdb/hammerdb.env initdb.env
do
    cp -- "$ROOT/$path.sample" "$TEST_ROOT/source/$path.sample"
    cp -- "$ROOT/$path.sample" "$TEST_ROOT/private/$path"
done
for path in connection.env run.env hammerdb/hammerdb.env .env .env.production \
    nested/secret.env.local nested/UPPER.ENV
do
    printf 'pilot private settings\n' >"$TEST_ROOT/source/$path"
    printf 'runner private settings\n' >"$TEST_ROOT/runner/$path"
done
printf 'must not be uploaded\n' >"$TEST_ROOT/source/new.env"
printf 'obsolete public file\n' >"$TEST_ROOT/runner/stale.txt"
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
    status=0
    # Only file-sync/configuration tags, on localhost, within this test's tree.
    ansible-playbook -i localhost, -c local "$ROOT/setup/ansible/runner_setup.yml" \
        -e @"$TEST_ROOT/vars.json" "$@" >"$TEST_ROOT/current.log" 2>&1 || status=$?
}

pass()
{
    passed=$((passed + 1))
    printf 'PASS: %s\n' "$1"
}

invoke --tags pg_benchmark_update
assert test "$status" == 0
for path in connection.env run.env hammerdb/hammerdb.env initdb.env
do
    assert cmp "$TEST_ROOT/source/$path.sample" "$TEST_ROOT/runner/$path.sample"
done
for path in connection.env run.env hammerdb/hammerdb.env .env .env.production \
    nested/secret.env.local nested/UPPER.ENV
do
    assert grep -Fxq 'runner private settings' "$TEST_ROOT/runner/$path"
done
assert test ! -e "$TEST_ROOT/runner/new.env"
assert test ! -e "$TEST_ROOT/runner/stale.txt"
pass 'repository sync ships samples but neither uploads nor deletes any env-name variants'

invoke --tags benchmark_config
assert test "$status" == 0
assert grep -Fxq 'runner private settings' "$TEST_ROOT/runner/connection.env"
pass 'private configuration upload remains opt-in'

printf 'CONFIG_TEST_SECRET=synthetic-private-config-marker\n' >>"$TEST_ROOT/private/connection.env"
invoke --tags benchmark_config -e update_benchmark_config=true
assert test "$status" == 0
for path in connection.env run.env hammerdb/hammerdb.env
do
    assert cmp "$TEST_ROOT/private/$path" "$TEST_ROOT/runner/$path"
    assert test "$(stat -c %a "$TEST_ROOT/runner/$path")" == 600
done
if grep -q 'synthetic-private-config-marker' "$TEST_ROOT/current.log"
then
    printf 'Private content appeared in Ansible output\n' >&2
    exit 1
fi

pass 'opt-in uploads use default runtime paths, mode 0600, and suppress private contents'

cp -- "$TEST_ROOT/runner/connection.env" "$TEST_ROOT/original.env"
printf 'CONFIG_TEST_SECRET=must-not-be-uploaded\n' >>"$TEST_ROOT/private/connection.env"
cp -- "$ROOT/initdb.env.sample" "$TEST_ROOT/private/initdb.env.sample"
cp -- "$ROOT/initdb.env.sample" "$TEST_ROOT/private/template.SAMPLE"
ln -s initdb.env.sample "$TEST_ROOT/private/alias.env"
for path in initdb.env.sample template.SAMPLE alias.env ../outside.env missing.env
do
    jq -n --arg path "$path" '{benchmark_config_files: ["connection.env", $path]}' \
        >"$TEST_ROOT/selection.json"
    invoke --tags benchmark_config -e update_benchmark_config=true -e @"$TEST_ROOT/selection.json"
    assert test "$status" != 0
    assert cmp "$TEST_ROOT/original.env" "$TEST_ROOT/runner/connection.env"
done
pass 'samples, symlinks, traversal, and missing files fail before any private upload'

printf '\n%s local Ansible environment checks passed; no runner connections or provisioning.\n' "$passed"
