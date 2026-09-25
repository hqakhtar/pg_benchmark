#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
umask 077
mkdir -p -- "$ROOT/tests/.work"
TEST_ROOT="$ROOT/tests/.work/env-policy-$$-$RANDOM"
mkdir -- "$TEST_ROOT"
trap '
    if [[ "$?" == 0 ]];
    then
        rm -r -- "$TEST_ROOT";
    else
        printf "Fixtures preserved: %s\n" "$TEST_ROOT" >&2;
    fi

' EXIT

unset GIT_DIR GIT_WORK_TREE GIT_INDEX_FILE GIT_COMMON_DIR GIT_OBJECT_DIRECTORY \
    GIT_ALTERNATE_OBJECT_DIRECTORIES GIT_CONFIG_PARAMETERS
export GIT_CONFIG_NOSYSTEM=1 GIT_CONFIG_GLOBAL=/dev/null GIT_CONFIG_COUNT=0
export GIT_AUTHOR_NAME='Environment policy test' GIT_COMMITTER_NAME='Environment policy test'
export GIT_AUTHOR_EMAIL='env-policy@example.invalid' GIT_COMMITTER_EMAIL='env-policy@example.invalid'

REPO="$TEST_ROOT/repo"
mkdir -p -- "$REPO/setup" "$REPO/.githooks" "$REPO/nested"
cp -- "$ROOT/setup/check_env_files.sh" "$ROOT/setup/install_git_hooks.sh" "$REPO/setup/"
cp -- "$ROOT/.githooks/pre-commit" "$ROOT/.githooks/pre-push" "$REPO/.githooks/"
cp -- "$ROOT/.gitignore" "$REPO/"
cd -- "$REPO"
git init --quiet
bash setup/install_git_hooks.sh >"$TEST_ROOT/install.log"
git add -- .gitignore setup .githooks
git commit --quiet -m 'Safe starting tree'
BASE="$(git rev-parse HEAD)"
ZERO=0000000000000000000000000000000000000000
passed=0

assert()
{
    if ! "$@"
    then
        printf 'Assertion failed: ' >&2
        printf '%q ' "$@" >&2
        printf '\nSee %s\n' "$TEST_ROOT" >&2
        exit 1
    fi
}

reject()
{
    if "$@" >"$TEST_ROOT/rejected.log" 2>&1
    then
        printf 'Expected rejection: ' >&2
        printf '%q ' "$@" >&2
        printf '\n' >&2
        exit 1
    fi
}

pass()
{
    passed=$((passed + 1))
    printf 'PASS: %s\n' "$1"
}

push_update()
{
    printf 'refs/heads/topic %s refs/heads/topic %s\n' "$1" "$2" >"$TEST_ROOT/update"
}

assert test "$(git config --local --get core.hooksPath)" == .githooks
bash setup/install_git_hooks.sh >"$TEST_ROOT/install.log"
pass 'hook installation is repository-local and idempotent'

for path in .env connection.env .env.local nested/service.env.production \
    nested/UPPER.ENV nested/UPPER.ENV.LOCAL 'nested/space name.env' $'nested/line\nbreak.env'
do
    assert git check-ignore -q -- "$path"
    printf 'not a real credential\n' >"$path"
    git add -f -- "$path"
    reject bash setup/check_env_files.sh --staged
    assert grep -q 'Private environment path' "$TEST_ROOT/rejected.log"
    (
        cd nested
        reject bash ../setup/check_env_files.sh --staged
    )
    reject git commit --quiet -m 'Must be blocked'
    assert test "$(git rev-parse HEAD)" == "$BASE"
    git rm --quiet --cached -- "$path"
done
pass 'ignores and pre-commit reject force-added env files at any depth, including unusual filenames'

for path in .env.sample connection.env.sample nested/service.env.sample nested/UPPER.ENV.SAMPLE
do
    if git check-ignore -q -- "$path"
    then
        printf 'Template unexpectedly ignored: %s\n' "$path" >&2
        exit 1
    fi

    printf '# Template only\n' >"$path"
    git add -- "$path"
done
git commit --quiet -m 'Ship templates only'
SAFE="$(git rev-parse HEAD)"
bash setup/check_env_files.sh --tree
bash setup/check_env_files.sh --range "$ZERO" HEAD
push_update "$SAFE" "$ZERO"
bash .githooks/pre-push origin <"$TEST_ROOT/update"
pass 'templates are tracked and safe new-branch history passes'

printf 'not a real credential\n' >tracked.env
git add -f -- tracked.env
git -c core.hooksPath=/dev/null commit --quiet -m 'Simulate bypassed local guard'
BAD="$(git rev-parse HEAD)"
reject bash setup/check_env_files.sh --tree HEAD
(
    cd nested
    reject bash ../setup/check_env_files.sh --tree HEAD
)
reject bash setup/check_env_files.sh --staged
rm -- tracked.env
reject bash setup/check_env_files.sh --staged
printf 'Unrelated edit\n' >allowed.txt
git add -- allowed.txt
reject git commit --quiet -m 'Still contains a tracked env file'
git rm --quiet --cached -- tracked.env
bash setup/check_env_files.sh --staged
git commit --quiet -m 'Remove private path without removing the test history'
CLEAN="$(git rev-parse HEAD)"
bash setup/check_env_files.sh --tree HEAD
pass 'the entire staged index is checked, even tracked paths deleted only from the worktree'

reject bash setup/check_env_files.sh --range "$SAFE" "$CLEAN"
assert grep -q "$BAD" "$TEST_ROOT/rejected.log"
push_update "$CLEAN" "$SAFE"
reject bash .githooks/pre-push origin <"$TEST_ROOT/update"
pass 'pre-push and CI ranges detect env files added and deleted in intermediate commits'

bash setup/check_env_files.sh --range "$BAD" "$CLEAN"
push_update "$CLEAN" "$BAD"
bash .githooks/pre-push origin <"$TEST_ROOT/update"
git update-ref refs/remotes/origin/main "$BAD"
push_update "$CLEAN" "$ZERO"
bash .githooks/pre-push origin <"$TEST_ROOT/update"
git update-ref refs/remotes/origin/main "$SAFE"
reject bash .githooks/pre-push origin <"$TEST_ROOT/update"
git update-ref -d refs/remotes/origin/main
reject bash .githooks/pre-push origin <"$TEST_ROOT/update"
pass 'known remote history can be remediated, while new-branch history is checked conservatively'

git tag -a safe-tag -m 'Safe annotated tag' "$SAFE"
git tag -a unsafe-tag -m 'Unsafe annotated tag' "$BAD"
printf 'refs/tags/safe-tag %s refs/tags/safe-tag %s\n' \
    "$(git rev-parse safe-tag)" "$ZERO" >"$TEST_ROOT/update"
bash .githooks/pre-push origin <"$TEST_ROOT/update"
printf 'refs/tags/unsafe-tag %s refs/tags/unsafe-tag %s\n' \
    "$(git rev-parse unsafe-tag)" "$ZERO" >"$TEST_ROOT/update"
reject bash .githooks/pre-push origin <"$TEST_ROOT/update"
push_update "$SAFE" "$ZERO"
printf 'refs/tags/unsafe-tag %s refs/tags/unsafe-tag %s\n' \
    "$(git rev-parse unsafe-tag)" "$ZERO" >>"$TEST_ROOT/update"
reject bash .githooks/pre-push origin <"$TEST_ROOT/update"
pass 'annotated tags and every ref in multi-ref pushes are checked'

push_update "$ZERO" "$BAD"
bash .githooks/pre-push origin <"$TEST_ROOT/update"
push_update "$CLEAN" 1111111111111111111111111111111111111111
reject bash .githooks/pre-push origin <"$TEST_ROOT/update"
assert grep -q 'Fetch the required history' "$TEST_ROOT/rejected.log"
blob="$(printf 'arbitrary tag target\n' | git hash-object -w --stdin)"
push_update "$blob" "$ZERO"
reject bash .githooks/pre-push origin <"$TEST_ROOT/update"
reject bash setup/check_env_files.sh --range invalid-ref HEAD
pass 'deletions pass, but missing history and non-commit refs fail closed'

printf '%s\n' "$CLEAN" >.git/shallow
reject bash setup/check_env_files.sh --range "$ZERO" HEAD
assert grep -q 'full clone' "$TEST_ROOT/rejected.log"
push_update "$CLEAN" "$ZERO"
reject bash .githooks/pre-push origin <"$TEST_ROOT/update"
push_update "$ZERO" "$BAD"
bash .githooks/pre-push origin <"$TEST_ROOT/update"
rm -- .git/shallow
pass 'shallow clones cannot silently omit push or CI history'

git replace "$BAD" "$SAFE"
reject bash setup/check_env_files.sh --range "$SAFE" "$CLEAN"
git replace -d "$BAD" >/dev/null
pass 'replacement objects cannot hide a prohibited commit tree'

git config --local --unset core.hooksPath
printf '#!/usr/bin/env bash\nexit 0\n' >.git/hooks/pre-commit
chmod +x .git/hooks/pre-commit
reject bash setup/install_git_hooks.sh
assert grep -q 'Existing executable hook' "$TEST_ROOT/rejected.log"
assert test -x .git/hooks/pre-commit
rm -- .git/hooks/pre-commit
git config --local core.hooksPath existing-hooks
reject bash setup/install_git_hooks.sh
assert test "$(git config --local --get core.hooksPath)" == existing-hooks
pass 'installer refuses to overwrite or hide existing hook setups'

printf '\n%s environment-policy checks passed; no pushes performed.\n' "$passed"
