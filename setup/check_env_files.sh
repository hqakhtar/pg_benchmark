#!/usr/bin/env bash
set -euo pipefail
export GIT_NO_REPLACE_OBJECTS=1

usage()
{
    cat <<'EOF'
Usage: check_env_files.sh --staged
       check_env_files.sh --tree [REVISION]
       check_env_files.sh --range BASE HEAD
       check_env_files.sh --pre-push REMOTE [URL]

Reject .env / *.env / *.env.* files, except *.env.sample (case-insensitive).
--staged checks the whole index, not just changed files.
--range checks HEAD and every commit reachable from HEAD but not BASE.
Use an all-zero BASE to check the entire reachable history.
--pre-push reads Git's ref updates from stdin; it does not contact the remote.
EOF
}

fail()
{
    printf 'ERROR: %s\n' "$*" >&2
    return 1
}

check_paths()
{
    local context="$1" path name rejected=false
    while IFS= read -r -d '' path
    do
        name="${path##*/}"
        case "${name,,}" in
            *.env.sample) ;;
            *.env|*.env.*)
                printf 'ERROR: Private environment path in %s: %q\n' "$context" "$path" >&2
                rejected=true
                ;;
        esac
    done
    if [[ "$rejected" == true ]]
    then
        fail "Only *.env.sample templates may be committed. Remove private paths from the index and from every new commit; deleting them only at the tip is not enough."
        return 1
    fi
}

resolve_commit()
{
    git rev-parse --verify --end-of-options "$1^{commit}" 2>/dev/null ||
    {
        fail "Cannot inspect commit $1. Fetch the required history; non-commit refs are not supported."
        return 1
    }
}

require_history()
{
    if [[ "$(git rev-parse --is-shallow-repository)" != false ]]
    then
        fail "History checks require a full clone. Fetch/unshallow before retrying."
        return 1
    fi
}

declare -A checked_commits=()
check_tree()
{
    local commit="$1"
    [[ -z "${checked_commits[$commit]+checked}" ]] || return 0
    git ls-tree -r --full-tree --name-only -z "$commit" -- | check_paths "commit $commit" || return 1
    checked_commits["$commit"]=true
}

check_commits()
{
    local commit
    while IFS= read -r commit
    do
        [[ -n "$commit" ]] || continue
        check_tree "$commit" || return 1
    done
}

check_range()
{
    local base="$1" head commits
    require_history || return 1
    head="$(resolve_commit "$2")" || return 1
    check_tree "$head" || return 1
    if [[ "$base" =~ ^0+$ ]]
    then
        commits="$(git rev-list "$head")" || return 1
    else
        base="$(resolve_commit "$base")" || return 1
        commits="$(git rev-list "$head" --not "$base")" || return 1
    fi

    check_commits <<<"$commits"
}

check_push()
{
    local remote="$1" local_ref local_oid remote_ref remote_oid extra head base commits
    while read -r local_ref local_oid remote_ref remote_oid extra
    do
        [[ -n "$local_ref" && -n "$local_oid" && -n "$remote_ref" && -n "$remote_oid" && -z "$extra" ]] ||
        {
            fail "Malformed pre-push ref update"
            return 1
        }

        [[ ! "$local_oid" =~ ^0+$ ]] || continue
        require_history || return 1
        head="$(resolve_commit "$local_oid")" || return 1
        check_tree "$head" || return 1
        # Existing remote history is not retroactively rewritten by this policy.
        if [[ "$remote_oid" =~ ^0+$ ]]
        then
            commits="$(git rev-list "$head" --not "--remotes=$remote")" || return 1
        else
            base="$(resolve_commit "$remote_oid")" || return 1
            commits="$(git rev-list "$head" --not "$base" "--remotes=$remote")" || return 1
        fi

        check_commits <<<"$commits" || return 1
    done
}

case "${1:-}" in
    --staged)
        [[ $# == 1 ]] || { usage >&2; exit 2; }

        git ls-files --cached --full-name -z -- :/ | check_paths 'the staged index'
        ;;
    --tree)
        [[ $# -le 2 ]] || { usage >&2; exit 2; }

        commit="$(resolve_commit "${2:-HEAD}")"
        check_tree "$commit"
        ;;
    --range)
        [[ $# == 3 ]] || { usage >&2; exit 2; }

        check_range "$2" "$3"
        ;;
    --pre-push)
        [[ $# -ge 2 && $# -le 3 && -n "$2" ]] || { usage >&2; exit 2; }

        check_push "$2"
        ;;
    -h|--help) usage ;;
    *) usage >&2; exit 2 ;;
esac
