#!/usr/bin/env bash
set -Eeuo pipefail

ROOT="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
source "$ROOT/lib/common.sh"

[[ $# == 0 ]] ||
{
    printf 'ERROR: Use wrapper.sh --setup-control-machine without additional options\n' >&2
    exit 2
}

trap 'printf "ERROR: Control-machine setup failed at line %s (exit %s); no runner setup was started.\n" "$LINENO" "$?" >&2' ERR

if [[ -n "${SUDO_USER:-}" && "$SUDO_USER" != root ]]
then
    fail "Run --setup-control-machine without sudo. Only package installation is elevated; Ansible collections belong to your normal user."
    exit 1
fi

if ! command -v apt-get >/dev/null || ! command -v dpkg-query >/dev/null
then
    fail "Automatic control-machine setup requires Ubuntu/Debian with apt-get and dpkg-query. On other systems, install Ansible, ansible.posix, rsync, OpenSSH client, Git, tar, gzip, and CA certificates manually."
    exit 1
fi

playbook="$ROOT/setup/ansible/runner_setup.yml"
[[ -f "$playbook" && -r "$playbook" ]] ||
{
    fail "Ansible playbook must be a readable regular file: $playbook"
    exit 1
}

run_setup_command()
{
    printf 'Running:'
    printf ' %q' "$@"
    printf '\n'
    "$@"
}

# Respect tools already supplied by a virtual environment or another installer.
packages=()
ansible_commands=(ansible ansible-playbook ansible-galaxy ansible-inventory ansible-doc)
for command in "${ansible_commands[@]}"
do
    if ! command -v "$command" >/dev/null
    then
        packages+=(ansible)
        break
    fi

done
for command in rsync git tar gzip
do
    if ! command -v "$command" >/dev/null
    then
        packages+=("$command")
    fi

done
if ! command -v ssh >/dev/null || ! command -v scp >/dev/null
then
    packages+=(openssh-client)
fi

certificate_status=""
if certificate_status="$(dpkg-query -W -f='${Status}' ca-certificates)"
then
    :
else
    status=$?
    [[ "$status" == 1 ]] ||
    {
        printf 'ERROR: Cannot inspect the CA certificates package (dpkg-query exit %s)\n' "$status" >&2
        exit "$status"
    }

fi

if [[ "$certificate_status" != 'install ok installed' ]]
then
    packages+=(ca-certificates)
fi

if ((${#packages[@]}))
then
    privilege=()
    if [[ "$(id -u)" != 0 ]]
    then
        command -v sudo >/dev/null ||
        {
            fail "Installing missing packages requires sudo or a root login: ${packages[*]}"
            exit 1
        }

        privilege=(sudo)
    fi

    run_setup_command "${privilege[@]}" apt-get update
    run_setup_command "${privilege[@]}" apt-get install -y --no-install-recommends "${packages[@]}"
    hash -r
else
    printf 'Control-machine packages are already available; skipping apt.\n'
fi

for command in "${ansible_commands[@]}" rsync ssh scp git tar gzip
do
    require_command "$command"
done
[[ "$(dpkg-query -W -f='${Status}' ca-certificates)" == 'install ok installed' ]] ||
{
    fail "CA certificates are not installed; package setup did not complete"
    exit 1
}

collection_pattern="(^|"$'\n'")ansible[.]posix[.]synchronize[[:space:]]"
collection_modules="$(ANSIBLE_NOCOLOR=1 ansible-doc --type module --list_files ansible.posix)"
if [[ ! "$collection_modules" =~ $collection_pattern ]]
then
    # This fallback also supports the Ansible 2.10 packaged by Ubuntu 22.04.
    run_setup_command ansible-galaxy collection install ansible.posix:1.5.4
    collection_modules="$(ANSIBLE_NOCOLOR=1 ansible-doc --type module --list_files ansible.posix)"
    [[ "$collection_modules" =~ $collection_pattern ]] ||
    {
        fail "ansible.posix.synchronize is still unavailable. Check Ansible's collection search paths and rerun setup."
        exit 1
    }

fi

# Syntax validation is local-only: it neither connects to hosts nor runs tasks.
run_setup_command ansible-playbook -i localhost, "$playbook" --syntax-check
printf 'Control-machine setup complete. No runner VMs, database services, or benchmark environments were changed.\n'
printf 'Next: configure SSH and your runner targets, then run %q --setup [--host HOST].\n' "$ROOT/wrapper.sh"
