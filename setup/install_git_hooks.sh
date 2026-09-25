#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
cd -- "$ROOT"
git rev-parse --git-dir >/dev/null

current="$(git config --get core.hooksPath || :)"
case "$current" in
    .githooks|"$ROOT/.githooks") ;;
    "")
        hooks="$(git rev-parse --git-path hooks)"
        for hook in "$hooks/"*
        do
            [[ -x "$hook" && -f "$hook" && "$hook" != *.sample ]] || continue
            printf 'ERROR: Existing executable hook %q would be hidden. Integrate the environment checks into your hooks instead.\n' "$hook" >&2
            exit 1
        done
        ;;
    *)
        printf 'ERROR: core.hooksPath is already %q. Integrate the environment checks there instead of replacing it.\n' "$current" >&2
        exit 1
        ;;
esac

for hook in pre-commit pre-push
do
    [[ -x "$ROOT/.githooks/$hook" ]] ||
    {
        printf 'ERROR: Repository hook is missing or not executable: %s\n' "$hook" >&2
        exit 1
    }

done

git config --local core.hooksPath .githooks
printf 'Installed repository-local pre-commit and pre-push guards. Other clones must run this installer too.\n'
