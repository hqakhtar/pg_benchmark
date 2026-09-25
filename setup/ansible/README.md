# Runner provisioning with Ansible

This optional playbook prepares dedicated Ubuntu runners. Provisioning is
separate from benchmark execution. It uses the
[shared plain hosts file](../../README.md#shared-runner-hosts), not a separate
inventory, and follows the repository's
[sample-only environment policy](../../README.md#three-configuration-groups).

## Prerequisites

- SSH access to each runner with a user that can use `sudo`.
- Local provisioning tools on the **control machine** (the computer running
  this checkout). On Ubuntu/Debian, install missing prerequisites from the
  checkout root:

  ```bash
  ./wrapper.sh --setup-control-machine
  ```

  This supplies Ansible, `ansible.posix`, `rsync`, the OpenSSH client, Git,
  `tar`, `gzip`, and CA certificates. Existing tools are reused; only missing
  system packages use APT/sudo, and collections are installed for the invoking
  user. Run without `sudo`; the installer elevates package commands itself.
  A direct root login is also supported.
  Other platforms must install these tools manually.

- A locally built HammerDB archive for runners needing an installation or an
  explicit reinstall. The default is
  `setup/ansible/HammerDB-5.0-Prod-Linux.tar.gz`; see
  [HammerDB installation](#hammerdb-installation) for overrides and opt-out.

The control-machine action is explicit, repeatable, and requires no hosts or
benchmark environment files. It neither provisions runners nor installs
PostgreSQL/HammerDB locally, upgrades the whole system, or changes services.
Missing tools require network access and package-install privileges; a
collection-only install does not require sudo. Available collections are not
replaced. If missing, `ansible.posix:1.5.4` is installed for compatibility with
distribution Ansible versions, including Ubuntu 22.04's Ansible 2.10.
Tool and collection checks plus an offline playbook syntax check must pass
before success is reported. See the
[control-machine setup details](../../README.md#control-machine-setup).

## First run

Use the same private `hosts.txt` as the multi-machine SSH helpers. From the
control machine's Git checkout root, install the local Git guards and create
the hosts file only if it does not already exist, then edit the shared list:

```bash
./wrapper.sh --setup-control-machine
bash setup/install_git_hooks.sh
test -e hosts.txt || cp hosts.txt.sample hosts.txt
${EDITOR:-vi} hosts.txt
cd setup/ansible

ansible -i ../../hosts.txt all -m ping -e @vars.yml
ansible-playbook -i ../../hosts.txt runner_setup.yml --check
ansible-playbook -i ../../hosts.txt runner_setup.yml
```

Ansible accepts this plain host-per-line file directly as an INI inventory.
No separate inventory file or generated copy is needed. The playbook targets
every host in the selected file; keep only dedicated runner machines in it.
Use `--limit HOST` to select one runner.

The defaults in [vars.yml](vars.yml) install PostgreSQL 17, the runner
prerequisites, this repository, and HammerDB when the requested version is
absent. The playbook stops/disables the system
PostgreSQL service; use it on dedicated runners, not database servers.
Private configuration upload remains explicitly opt-in. **No runtime `.env`
files are required just to provision runners.**

### SSH connection configuration

The recommended approach is to define runner connections in the control
account's `~/.ssh/config` and list the **`Host` aliases** in `hosts.txt`.
For example, a `Host citus-runner-m1` entry can supply the real `HostName`, key,
port, and jump host, while the hosts file contains just `citus-runner-m1`.
See the [SSH configuration example](../../README.md#recommended-ssh-configuration).

The hosts file selects which VMs Ansible operates on; unrelated entries in
your SSH configuration are not included. The same aliases work with the
direct SSH helpers and `./wrapper.sh --setup --host ALIAS`.

Use one common runner username. SSH defaults in [vars.yml](vars.yml) set
`ansible_user=azureuser` and first-connection host-key acceptance. Keep the
`User` setting for your runner aliases consistent with that username:
Ansible's explicit `ansible_user` overrides SSH's `User`, while the direct SSH
helpers honor SSH's setting. Override Ansible defaults with
`-e @vars.local.yml` when needed, including on the `ansible ... -m ping`
command above. Keep keys and connection details in SSH configuration rather
than duplicating them in inventory or benchmark environment files.

### Wrapper entry point

From the checkout root, the wrapper offers the same provisioning separately
from benchmark execution:

```bash
./wrapper.sh --setup-control-machine
./wrapper.sh --setup
./wrapper.sh --setup --host NEW_VM
./wrapper.sh --setup -- --check
./wrapper.sh --setup --host NEW_VM -- -e @setup/ansible/vars.local.yml
```

`--setup-control-machine` only prepares the local provisioning tools and takes
no extra arguments. It is not run implicitly by `--setup`.
`--setup` selects the root `hosts.txt`. `--host NEW_VM` uses an inline inventory;
the new runner does not have to be listed in that file. Additional Ansible CLI
arguments follow `--`. The wrapper preserves the caller's working directory, so
relative paths in those arguments are relative to where it was invoked.

Before execution, the wrapper prints the exact generated Ansible invocation,
shell-escaped for reference and manual reuse from the same working directory.
The printed command is visible in terminal output and captured logs: pass
sensitive variables with `-e @private-vars-file` (for example the ignored
`-e @setup/ansible/vars.local.yml` above), not inline secret values.

## Private configuration

The checkout contains `*.env.sample` templates only. Copy them to private `.env`
files; do not pass templates to the wrapper or select them for configuration
upload. Normal repository synchronization does not create runtime `.env` files.

From `setup/ansible`, create private copies at the wrapper's default paths
without overwriting existing files:

```bash
umask 077
cp -n ../../connection.env.sample ../../connection.env
cp -n ../../hammerdb/hammerdb.env.sample ../../hammerdb/hammerdb.env
cp -n ../../run.env.sample ../../run.env
chmod 600 ../../connection.env ../../hammerdb/hammerdb.env ../../run.env
${EDITOR:-vi} ../../connection.env ../../hammerdb/hammerdb.env ../../run.env
```

Set runner-specific paths in these files. `HAMMERDB_HOME` should point to the
HammerDB directory on each runner. With the playbook's installation defaults,
that is `/home/azureuser/HammerDB-5.0`, not `/opt/HammerDB` from the environment
template. Adjust it for your `runner_home` and HammerDB version. Prefer existing
password files or externally provided credentials to storing passwords in these
files.
In the private connection file, replace the `PGPASSWORD` guard with your quoted
password or `export PGPASSWORD=''` for password-file/trust authentication.
Alternatively, supply `PGPASSWORD` through the runner's environment. Leaving it
unset causes a Bash error when the configuration is sourced.

### Synchronization and explicit upload

Normal repository synchronization includes `*.env.sample` templates but excludes
all `.env`, `*.env`, and `*.env.*` runtime files (case-insensitive), the shared
hosts file, private variable files, old combined environments, default output
directories, and backup/patch files.
It neither uploads nor deletes the private configuration when upload is
disabled, even when mirroring deletions. Keep private files within those env
naming patterns or outside the checkout; arbitrary filenames are not
automatically recognized as secrets. Runtime files must be complete private
copies; neither the wrapper nor distributed loader ever sources the templates.

Private upload is disabled by default (`update_benchmark_config: false`).
To explicitly upload all three files with mode `0600`, put this in the ignored
`vars.local.yml`:

```yaml
update_benchmark_config: true
```

After the repository has been synchronized, upload just the private
configuration:

```bash
ansible-playbook -i ../../hosts.txt runner_setup.yml \
  --tags benchmark_config -e @vars.local.yml
```

For a full provisioning run that also uploads private configuration, omit
`--tags benchmark_config`.

All selected config files are checked before any configuration copy tasks.
`.sample` names (case-insensitive), file symlinks, missing files, absolute paths,
and `..` traversal are rejected.
File contents are suppressed from Ansible logs/diffs.
`benchmark_config_local_dir` and `benchmark_config_files` can change the source
directory and relative file list; destination parent directories must exist.
The default file list is `connection.env`, `hammerdb/hammerdb.env`, and `run.env`.

Optional [local-server tuning](../../README.md#optional-temporary-local-postgresql)
is not uploaded by default. If needed, also copy `initdb.env.sample` to a private
`initdb.env`, add it alongside all three default entries in
`benchmark_config_files`, and set `PG_SERVER_ENV_FILE` to its path on the runner.
Replacing `benchmark_config_files` replaces the whole upload list.

### Running on a provisioned runner

On a runner, the wrapper loads these default private paths without selectors:

```bash
cd ~/pg_benchmark
./wrapper.sh hammerdb --check
```

For alternative file names, use `--connection-env`, `--benchmark-env`, and
`--run-env` or their corresponding environment selectors. Each selected file
must be complete; the wrapper does not layer it over another file or a template.
If private upload is disabled on a fresh runner, create and edit the private
copies there before running. Even `--check` requires all three private files.

## HammerDB installation

Installing HammerDB is enabled by default (`install_hammerdb: true`) and is
separate from uploading private environment configuration. The requested
version defaults to `5.0`; its installation directory is
`{{ hammerdb_dest }}/HammerDB-{{ hammerdb_version }}`, normally
`/home/azureuser/HammerDB-5.0`. Other installed versions are left alone.

- **Absent:** install the requested version from the controller's archive.
- **Complete:** leave the existing installation unchanged, including ownership,
  modes and extra files. No local archive is required or inspected.
- **Incomplete:** fail without overwriting it. Explicitly opt into reinstall
  after checking the target and providing a valid archive.

Completeness means a real directory (not a symlink) containing a readable,
nonempty, executable regular `hammerdbcli` file, checked as `runner_user`.
The version is identified by the directory name; preflight never executes the
launcher, a benchmark, or PostgreSQL, and does not certify bundled libraries.

Use the default archive location above, or set an alternative in the ignored
`vars.local.yml`:

```yaml
local_hammerdb_tarball: "/path/to/HammerDB-5.0-Prod-Linux.tar.gz"
```

Relative archive paths are resolved against `setup/ansible`, consistently for
both preflight and extraction; an absolute path avoids ambiguity.

### Choosing a different HammerDB version

`hammerdb_version` selects the requested version; **changing only
`local_hammerdb_tarball` does not change it**. To install an archive containing
`HammerDB-6.0/`, set both values in the private Ansible overrides:

```yaml
hammerdb_version: "6.0"
local_hammerdb_tarball: "/path/to/HammerDB-6.0-Prod-Linux.tar.gz"
```

With the archive in the default controller location, setting just the version
also changes the default archive name to `HammerDB-6.0-Prod-Linux.tar.gz`.
Keep the YAML version quoted: it must be a string.

From the checkout root, apply the private overrides to one runner:

```bash
./wrapper.sh --setup --host runner-a -- \
  --tags hammerdb -e @setup/ansible/vars.local.yml
```

The archive is extracted to `hammerdb_dest` and must supply exactly the selected
versioned tree. A 6.0-only archive with the default 5.0 selection fails preflight
when installation is needed; neither an empty `HammerDB-5.0` nor a
`HammerDB-6.0` installation is created. Ansible's `unarchive.creates` setting
is a skip condition, not a command to create a directory.

If a complete 5.0 installation already exists and 5.0 remains selected, normal
setup keeps it without inspecting the archive. Explicitly selecting 6.0
installs or reuses 6.0 alongside it; it does not remove 5.0. Update
`HAMMERDB_HOME` in the private benchmark environment to the selected runner
path, normally `/home/azureuser/HammerDB-6.0` for this example.

### Preflight and replacement protection

Read-only HammerDB preflight runs for **every selected runner before any system
update, repository synchronization, or installation removal**. The play uses a
linear, all-host failure barrier: a failed prerequisite on any selected runner
stops provisioning on all selected runners. Mixed installed/fresh inventories
are checked per host, not just against the first host.

Only runners needing installation require a controller-side archive. It must
be a readable regular file, not a symlink. Preflight checks the gzip stream and
tar integrity, the expected versioned installation tree, and a readable,
nonempty, executable regular launcher. Extra top-level paths, traversal,
duplicate members, special files, links outside the installation tree, paths
through non-directory members, and hard links without regular archive targets
are rejected. Missing, corrupt or wrong-layout archives leave existing installations
intact. Use trusted archives; these checks are not signature verification.

The destination must be an absolute, non-root path without trailing slashes or
`.` / `..` components. The installation basename must be
`HammerDB-<version>`, with a simple version beginning with a digit.
`reinstall_hammerdb` defaults to `false`; only an explicit `true`, together with
installation enabled, allows removal of that specific version's target.

From `setup/ansible`:

```bash
# Provision without installing or changing HammerDB.
ansible-playbook -i ../../hosts.txt runner_setup.yml -e install_hammerdb=false

# Install only if absent, leaving a complete installation unchanged.
ansible-playbook -i ../../hosts.txt runner_setup.yml --tags hammerdb

# Explicitly replace the requested version, only after successful preflight.
ansible-playbook -i ../../hosts.txt runner_setup.yml \
  --tags hammerdb -e reinstall_hammerdb=true
```

`install_hammerdb=false` wins even if reinstall is requested. `--check` still
validates needed archives but never removes or installs HammerDB files.
`--tags hammerdb` is independent of system provisioning; repository-only tags
(`pg_benchmark` or `pg_benchmark_update`) and `--tags benchmark_config` perform
no HammerDB work or archive checks, despite installation being enabled by
default. Repository synchronization does not upload runtime files.

## Git protection

Private `.env` files stay on the control machine and runners; only sample
templates belong in Git. The hook installer is required in every Git clone,
not on a runner that merely receives an rsync copy.
Local hooks reject prohibited staged files and outgoing commit history; the
`env-policy` CI check adds a merge guard when required by repository rules.
Hooks are bypassable and CI runs after a push, so guaranteed push rejection
requires hosting-side rules.

See the [Git protection setup and limits](../../README.md#keeping-private-environment-files-out-of-git)
and the [root README](../../README.md) for benchmark preparation and execution.

The [local provisioning checks](../../README.md#provisioning-and-tests) validate
host selection and private-file handling without connecting to any runner.
Run `bash tests/test_hammerdb_ansible.sh` from the checkout root for HammerDB
regressions. It uses synthetic archives, the current user, only HammerDB-tagged
tasks on local inventory aliases, and private fixtures under `tests/.work`;
it never performs system provisioning or executes a benchmark.
