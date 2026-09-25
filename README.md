# pg_benchmark

A small Bash runner for repeatable PostgreSQL benchmarks. The runner owns
configuration loading, iteration policy, process supervision, and artifacts.
Each benchmark adapter owns its workload configuration, preparation, execution,
and result parsing.

**HammerDB TPCC/TPROC-C is the only implemented benchmark.** There are no pgbench
or sysbench adapters yet.

**Configuration is sample-only in Git.** Copy the shipped `*.env.sample`
templates to private `.env` files before running. Templates are not valid
runtime configuration, and private copies must not be committed.
Control-machine setup and runner provisioning are separate and do not load
these files.

## Workflow at a glance

For remote runner VMs, follow this sequence. The **control machine** runs
Ansible; a **runner** executes the benchmark against the PostgreSQL target.
Skip provisioning steps that are already complete.

| Step | Where | What to do |
| --- | --- | --- |
| 1. [Set up the local system](#control-machine-setup) | Control machine | Run `./wrapper.sh --setup-control-machine` to install missing provisioning tools. |
| 2. [Configure SSH](#recommended-ssh-configuration) and [select runner hosts](#shared-runner-hosts) | Control machine | Define SSH aliases, keys, and the common username; list the selected aliases in `hosts.txt`. |
| 3. [Configure environment files](#three-configuration-groups) | Control machine | Create private copies of the connection, HammerDB, and run templates. Use paths appropriate for the runners. |
| 4. [Set up the runner VMs](#runner-vm-setup) | Control machine | Provide the matching HammerDB archive, then use `--setup` for all listed VMs or `--setup --host HOST` for one. |
| 5. [Place private configuration on runners](setup/ansible/README.md#synchronization-and-explicit-upload) | Control machine / runner | Explicitly upload the private environment files, or create and edit them on each runner. Normal repository synchronization excludes them. |
| 6. [Validate configuration and prepare data](#quick-start-an-existing-postgresql-target) | Runner | Run `./wrapper.sh hammerdb --check`; use `--prepare` when a new dataset is needed. |
| 7. [Run the benchmark](#public-interface) | Runner | Run `./wrapper.sh hammerdb` using the configured [iteration and preparation policy](#run-policy). |
| 8. [View logs and results](#output-and-failures) | Runner | Open the reported run directory under `RUN_OUTPUT_ROOT` for logs, execution status, and NOPM/TPM results. |

Runner setup does not start a benchmark. Run benchmark commands from the
checkout on the selected runner; the wrapper does not automatically run
benchmark iterations across every VM in the hosts list.

## Quick start: an existing PostgreSQL target

Use a dedicated benchmark database. Schema reset/cleanup can drop TPCC/TPCH
tables and matching objects in `public`, and maintenance resets statistics.
Do not point these operations at an application database.

Run these commands from the repository root after installing the
[runtime prerequisites](#provisioning-and-tests) and HammerDB. Install the
repository-local Git guards once per checkout, then create three private
configuration files. `cp -n` preserves existing private files:

```bash
bash setup/install_git_hooks.sh
umask 077
cp -n connection.env.sample connection.env
cp -n hammerdb/hammerdb.env.sample hammerdb/hammerdb.env
cp -n run.env.sample run.env
chmod 600 connection.env hammerdb/hammerdb.env run.env

${EDITOR:-vi} connection.env hammerdb/hammerdb.env run.env
```

Edit the private copies, not the templates. Do not rename the templates or
symlink the private paths to them; keep the samples reusable and free of secrets.

Set the target's `PGHOST`, `PGPORT`, `PGDATABASE`, and `PGUSER` in the connection
file. Set `HAMMERDB_HOME` to the directory containing `hammerdbcli` in the
HammerDB file. Prefer `PGPASSFILE` or `~/.pgpass` to storing passwords in a
configuration file. If schema creation needs a different account, configure
`HDB_SUPERUSER` and provide its authentication too.
Set `PGPASSWORD` explicitly, even if empty for password-file/trust authentication;
leaving it unset triggers the template's Bash guard.

```bash
./wrapper.sh hammerdb --check
./wrapper.sh hammerdb --prepare
./wrapper.sh hammerdb
```

Preparation is only needed for a new dataset. Subsequent runs reuse it by
default. Preparation does not implicitly delete existing tables.

`--check` is an **offline preflight**: it validates configuration, executables,
adapter assets, and resource limits. It does not connect to PostgreSQL, start
HammerDB, create run output, or verify server privileges/native tool features.
Every real operation repeats these checks before it starts.

## Three configuration groups

| Group | Template | Default private file | Selector (environment / CLI) |
| --- | --- | --- | --- |
| Connection | [connection.env.sample](connection.env.sample) | `connection.env` | `CONNECTION_ENV_FILE` / `--connection-env` |
| Benchmark | [hammerdb/hammerdb.env.sample](hammerdb/hammerdb.env.sample) | `hammerdb/hammerdb.env` | `BENCHMARK_ENV_FILE` / `--benchmark-env` |
| Run | [run.env.sample](run.env.sample) | `run.env` | `RUN_ENV_FILE` / `--run-env` |

The repository ships **templates only**, never runtime environment files.
Without selectors, all three private files above must exist in the checkout.
These default paths are relative to the checkout, not the caller's directory.
Missing files fail with a setup hint, even if settings are already in the shell.
Files ending in `.sample` (case-insensitive), and symlinks resolving to them,
are rejected rather than sourced, including for `--check`.

The fourth template, [initdb.env.sample](initdb.env.sample), is optional and used
only for [temporary local PostgreSQL tuning](#optional-temporary-local-postgresql).
It does not replace any of the three required configuration groups.

The wrapper loads each selected file exactly once, in connection → benchmark →
run order. A selected file **replaces the entire group**; it is not layered over
another private file or a shipped template. Start each file from its matching
sample so all required settings are present. The adapter never reloads config.
A CLI selector takes precedence over its corresponding selector environment
variable; only the selected paths are inspected.

Files follow normal Bash assignment semantics. Use explicit assignments
such as `export PGHOST="database.example"` when a file must override a shell
value; `${PGHOST:-localhost}` deliberately preserves an existing value.
Plain assignments are exported for child processes too. Alternate private
paths such as `connection.local.env` remain supported, but must be selected
explicitly; they are not discovered automatically.

Files are trusted Bash input; keep them to assignments/exports and never load
an untrusted file. Do not put provisioning or benchmark commands in them.
Relative selectors and settings resolve against the directory where the
wrapper was invoked, not the directory containing the configuration file.

### Selecting private files elsewhere

After creating complete private copies at alternative paths, select them
explicitly:

```bash
./wrapper.sh hammerdb --check \
  --connection-env "$HOME/bench-config/connection.env" \
  --benchmark-env "$HOME/bench-config/hammerdb.env" \
  --run-env "$HOME/bench-config/run.env"
```

The equivalent environment selectors are `CONNECTION_ENV_FILE`,
`BENCHMARK_ENV_FILE`, and `RUN_ENV_FILE`. Unspecified selectors still use the
default private paths; they never fall back to a sample. An alternative file
containing only a few overrides is not a substitute for a complete configuration.

### Connection settings

Use standard libpq names: `PGHOST`, `PGPORT`, `PGDATABASE`, `PGUSER`,
`PGSSLMODE`, `PGCONNECT_TIMEOUT`, and optional `PGPASSWORD`/`PGPASSFILE`.
`PGDATABASE` must be a database name, not a connection URI or keyword string.
`PGMAINTENANCE_DB` is the existing database used to bootstrap a new dataset
(normally `postgres`).

The connection template exports `PGPASSWORD` with an unset-only Bash guard.
In the private copy, replace that assignment with your quoted password, or with
`export PGPASSWORD=''` for password-file/trust authentication. Alternatively,
export `PGPASSWORD` before invoking the wrapper. Leaving it unset produces the
error "Your password goes here"; an explicitly empty value is accepted.
The exported value reaches HammerDB's Tcl `pg_pass` setting in memory rather
than through `diset`, which logs and saves values.

HammerDB translates this same target into its Tcl settings. SQL maintenance
and cleanup pass the host, port, database, and intended role explicitly.
`HDB_SUPERUSER` may differ from the workload's `PGUSER`; it does not select a
different target database.

Existing servers need a `psql` client (`PG_PSQL`, default `psql`), **not**
`pg_config`, `initdb`, or a local PostgreSQL server installation.

### HammerDB settings

The main settings are `HAMMERDB_HOME`, `HDB_WAREHOUSES`, `HDB_BUILD_VUS`,
`HDB_RUN_VUS`, `HDB_RAMPUP_MINUTES`, and `HDB_DURATION_MINUTES`.
`HDB_BUILD_VUS` cannot exceed the warehouse count.

- `HDB_WORKLOAD=tpcc` is the only supported workload.
- `HDB_RESET_SCHEMA=false` means preparation does not drop existing data.
- `HDB_RESET_SCHEMA=true` resets before preparation and also requires
  `RUN_ALLOW_DESTRUCTIVE=true`.
- `HDB_MAINTENANCE=true` performs SQL maintenance before each benchmark
  iteration. There are no hidden 60-second sleeps.
- `HDB_VACUUM=true` enables HammerDB's timed-driver vacuum behavior.
- `HDB_SUPERUSER_PASSWORD` defaults to `PGPASSWORD` when supplied. Otherwise
  libpq password-file authentication can be used for each role.

Stock PostgreSQL HammerDB dictionaries work for ordinary non-Citus preparation
and execution. `HDB_CITUS_COMPAT=true` uses the connection options expected by this
repository's Citus-compatible HammerDB build. Distributed preparation also
requires a build supporting `pg_first_ware` and the zero-warehouse post-data
DDL protocol. These capabilities are checked by Tcl before it starts the
workload; offline preflight cannot establish that an installation supports
them.

### Run policy

| `RUN_PREPARE_MODE` | Behavior |
| --- | --- |
| `reuse` (default) | Run against prepared data; no schema preparation |
| `once` | Prepare once, then execute all iterations |
| `each` | Prepare before every iteration |

For HammerDB, `each` requires `HDB_RESET_SCHEMA=true` and
`RUN_ALLOW_DESTRUCTIVE=true`; otherwise preflight rejects it.
`--prepare` performs one preparation operation regardless of iteration count.

In the run template, `RUN_ITERATIONS` defaults to 3. `RUN_OUTPUT_ROOT` defaults to
`./benchmark-results`; `RUN_LABEL` supplies the human-readable part of each
unique run ID. `RUN_COOLDOWN_SECONDS` defaults to 0.
`RUN_TIMEOUT_SECONDS=0` disables the timeout; a positive value bounds each
adapter operation, including preparation and cleanup.

Schema cleanup is separate from stopping processes:

```bash
RUN_ALLOW_DESTRUCTIVE=true ./wrapper.sh hammerdb --cleanup
```

The selected run file with an explicit `RUN_ALLOW_DESTRUCTIVE=false` still wins
over that shell assignment. Cleanup must target a dedicated benchmark
database. The runner never drops data merely because a run finished.

## Public interface

```text
wrapper.sh BENCHMARK [--check | --prepare | --cleanup]
    [--connection-env FILE]
    [--benchmark-env FILE]
    [--run-env FILE]
wrapper.sh --setup-control-machine
wrapper.sh --setup [--host HOST] [-- ANSIBLE_OPTIONS]
```

No action flag means run. `--help` lists the interface. Only one action can be
selected. Unknown tools, unsupported options, missing files, and invalid combinations
fail explicitly.
Only documented environment settings are used; other variables are not
interpreted as aliases.

## Keeping private environment files out of Git

`.gitignore` excludes `.env`, `*.env`, and `*.env.*` at every depth, allowing
only `*.env.sample` templates (case-insensitive). Ignoring files does not protect
already-tracked or force-added files, so the repository also provides:

- **Local hooks:** run the [installer](setup/install_git_hooks.sh) with
  `bash setup/install_git_hooks.sh` in each clone. It sets only this repository's
  `core.hooksPath`, refusing to hide an existing hook setup.
  [Pre-commit](.githooks/pre-commit) checks the **entire staged index**.
  [Pre-push](.githooks/pre-push) checks every pushed tip, including annotated
  tags, and newly reachable commit trees. Adding an env file and deleting it
  in a later commit is still rejected.
- **CI:** [Environment file policy](.github/workflows/env-policy.yml) checks
  pushes and pull requests, including introduced commit history, with a full
  checkout. The GitHub check/job is named `env-policy`.

Cloning the repository or having ignore rules does not activate these hooks.
Each contributor must install them in their own clone. Commit templates, not
the configured private copies.

Run the [policy checker](setup/check_env_files.sh) manually with:

```bash
bash setup/check_env_files.sh --staged
bash setup/check_env_files.sh --tree HEAD
bash setup/check_env_files.sh --range BASE_COMMIT HEAD
```

Stage removals of formerly tracked env files along with the new templates.
To keep an existing private file locally while untracking it, use
`git rm --cached -- path/to/private.env`; do not merely rely on `.gitignore`.
Remove prohibited paths from **every new commit**, not just the working tree.
Fetch the destination's refs before pushing; missing history and shallow clones
are rejected rather than silently skipping checks. New branches use known
destination remote-tracking refs as their baseline, or inspect all reachable
history when none are known. CI uses the event base, or the default branch for
a newly created branch/tag. Existing remote history is not rewritten.

**Enforcement limits:** Git hooks are opt-in, editable, and bypassable (for
example with `--no-verify`). Actions run **after** Git has accepted a push.
Require `env-policy` in GitHub branch protection/rulesets, restrict bypasses,
and protect changes to the guard/workflow to gate merges into protected branches.
For actual push-time rejection, configure hosting-side file-path push rules or
a server-side pre-receive hook where supported. Repository files alone cannot
enforce this on every client or remote; no remote rules are configured here.

This is a filename policy, not a secret scanner: credentials in a template or
an arbitrary filename are not detected. Keep private configs named `.env`,
`*.env`, or `*.env.*` (never ending in `.sample`), or outside the checkout.
Previously published secrets still require credential rotation and, if needed,
coordinated history cleanup.

## Optional temporary local PostgreSQL

Local-server management is implemented separately from the benchmark adapter.
For optional tuning, create and edit a private copy from the repository root:

```bash
umask 077
cp -n initdb.env.sample initdb.env
chmod 600 initdb.env
${EDITOR:-vi} initdb.env
```

Set these values in the connection environment:

```bash
export PG_TARGET_MODE=temporary
export PGHOST=127.0.0.1
export PGPORT=55432
export PG_CONFIG=/usr/bin/pg_config
export PG_SERVER_ENV_FILE=./initdb.env
export PG_REMOVE_DATA=false
```

Set `RUN_PREPARE_MODE=once` in the run environment. Use an available port and
run as a non-root operating-system user.

The runner creates **one cluster per invocation**, initializes `PGUSER` as its
bootstrap superuser, creates the target database, prepares it, and reuses that
cluster for all iterations. The cluster binds to loopback and uses trust
authentication; this mode is for an isolated benchmark machine, not a shared
or production database server.

[initdb.env.sample](initdb.env.sample) is the optional **server tuning template**,
not a connection environment. Its private copy supplies `PG_SERVER_OPTIONS`
only for temporary clusters. `PG_SERVER_ENV_FILE` never accepts a `.sample`
file or an alias resolving to one, even when targeting an existing server.
For example, a tuning file can set
`PG_SERVER_OPTIONS="-c shared_preload_libraries=pg_stat_statements"`.
Adjust connection capacity when increasing virtual-user counts.
`PG_INIT_SQL` optionally runs SQL against `PGMAINTENANCE_DB` after startup,
with SQL errors treated as failures.

Only the cluster created by this invocation is stopped. Successful runs remove
its data directory only when `PG_REMOVE_DATA=true`. Failed/interrupted runs
retain data and logs. Prepare-only rejects automatic data removal.
Existing servers are never started or stopped.

## Shared runner hosts

Keep one private `hosts.txt` in the repository root, with one runner hostname
or SSH alias per line. It is the shared host list for:

- [Wrapper setup](#runner-vm-setup).
- [Ansible provisioning](setup/ansible/README.md).
- [Multi-runner setup](setup/setup_multi_runners.sh).
- [Multi-VM data preparation](setup/run_tpcc_load_multivm.sh).

Create it from the sample only if it does not already exist:

```bash
test -e hosts.txt || cp hosts.txt.sample hosts.txt
${EDITOR:-vi} hosts.txt
```

Ansible reads the same plain file directly as its inventory; do not maintain a
second list. Keep inventory group headers and connection variables out of this
file. Ansible's SSH defaults are in [setup/ansible/vars.yml](setup/ansible/vars.yml).
The private hosts file is ignored by Git and excluded from runner uploads.

The list names runner machines, not PostgreSQL servers. Single-runner
benchmarking does not read it; the database target still comes from `PGHOST`.
For a new runner not yet in the list, use `./wrapper.sh --setup --host HOST`.
This selects only that VM without modifying the shared file.

### Recommended SSH configuration

Keep SSH connection details in `~/.ssh/config` on the control machine, under
the account that runs the framework. Put the aliases from its **`Host` entries**
in `hosts.txt`; they do not need to be DNS-resolvable hostnames. OpenSSH maps
each alias to the address in `HostName`.

For example, in `~/.ssh/config`:

```sshconfig
Host citus-runner-m1
    HostName 192.0.2.10

Host citus-runner-m2
    HostName 192.0.2.11

Host citus-runner-*
    User azureuser
    IdentityFile ~/.ssh/benchmark_ed25519
    IdentitiesOnly yes
```

Replace the example addresses and key path with your own. The corresponding
private `hosts.txt` contains only the aliases:

```text
citus-runner-m1
citus-runner-m2
```

Ansible and the direct SSH helpers use these same aliases. The hosts file
selects the runner VMs; it does not need to duplicate their IP addresses, keys,
ports, or jump-host settings. Unrelated entries in `~/.ssh/config` are not
automatically selected. `./wrapper.sh --setup --host citus-runner-m1` can also
select an alias directly, without a hosts file.

Use a consistent username across runner VMs. Keep SSH's `User` aligned with
`ansible_user` in the private Ansible overrides (default: `azureuser` in
[vars.yml](setup/ansible/vars.yml)). Ansible's explicit username takes precedence
over SSH's `User`; the direct SSH helpers use the SSH configuration.

SSH reads all matching blocks, but the first value for most settings wins.
Put specific host entries before wildcard defaults, and remember that explicit
command-line options take precedence. Some settings, including `IdentityFile`,
can accumulate across matching blocks.

Inspect the effective connection details without opening an SSH session:

```bash
ssh -G citus-runner-m1 | grep -E '^(hostname|user|port|identityfile|proxyjump) '
```

For passphrase-protected keys, load the key into `ssh-agent` before unattended
runs. The multi-VM loader uses `BatchMode=yes` and cannot prompt for passwords
or key passphrases. Keep SSH configuration and private keys outside the
repository; they do not belong in benchmark environment files.

## Multi-VM data preparation

[setup/run_tpcc_load_multivm.sh](setup/run_tpcc_load_multivm.sh) is an advanced
HammerDB **data loader**, not a distributed benchmark execution mode. It uses
the same three configuration groups, wrapper interface, and
[shared runner hosts](#shared-runner-hosts).

Configure `HAMMERDB_HOME` and `RUN_OUTPUT_ROOT` for the runner machines, use
`PG_TARGET_MODE=existing`, and explicitly enable `RUN_ALLOW_DESTRUCTIVE=true`
for the initial cleanup:

```bash
./setup/run_tpcc_load_multivm.sh \
  ./hosts.txt \
  /opt/pg_benchmark \
  1000000 \
  --connection-env ./connection.env \
  --benchmark-env ./hammerdb/hammerdb.env \
  --run-env ./run.env
```

The optional warehouse count otherwise comes from `HDB_WAREHOUSES`. The loader
checks runners before cleanup, bootstraps warehouse 1, partitions the remaining
warehouses, then finalizes post-data DDL. A failed parallel job prevents
finalization. Configuration is invocation-specific: the loader sends complete
private configurations over SSH stdin and does not rewrite shared runner files
or depend on runtime defaults in the remote checkout. `.sample` selections are
rejected before SSH. Request files are private and removed on completion;
interrupted SSH connections still require checking the runner before retrying.

## Output and failures

Each operation creates a new private directory under `RUN_OUTPUT_ROOT`:

```text
benchmark-<timestamp>-<unique-suffix>/
    run.json
    benchmark.json
    target.json
    prepare/                    # when preparation is requested
    iteration-1/
        output.log
        hammerdb.log
        workload.tcl
        status.json
        result.json
    postgresql/                 # temporary local targets only
```

- `run.json`: selected config files, execution policy, status, completed
  iterations, timestamps, and exit code.
- `benchmark.json`: resolved, non-secret workload settings.
- `target.json`: the actual connected PostgreSQL version and target, not the
  version of a client-side `pg_config`.
- `result.json`: HammerDB version and native NOPM/TPM values with explicit
  units. NOPM is not renamed to a generic TPS metric.

The copied [Tcl workload](hammerdb/tpcc.tcl) reads credentials from the process
environment; the framework does not embed them into scripts or configuration
metadata. Native tool logs should nevertheless be treated as sensitive.

Nonzero tool exits, unsuccessful virtual users, missing completion markers,
missing/ambiguous metrics, SQL errors, and log-writer failures fail the run.
The runner stops subsequent iterations and retains the logs. Interruptions
terminate the adapter's owned process group and record an interrupted run.

## Adapter contract

[wrapper.sh](wrapper.sh) selects and loads a module for benchmark actions.
Runner setup invokes Ansible directly; control-machine setup installs local
provisioning prerequisites. Neither loads an adapter or runtime env files.
Commands execute as argument arrays, never by evaluating printed shell text.
Shared mechanics live in [lib/](lib/).
[hammerdb/hammerdb.sh](hammerdb/hammerdb.sh) is a sourceable module, not a
second CLI.

| Hook | Contract |
| --- | --- |
| `benchmark_validate` | Validate benchmark settings/assets without starting a workload |
| `benchmark_describe` | Emit resolved non-secret settings as JSON |
| `benchmark_prepare DIRECTORY` | Prepare the dataset, including any tool-specific distributed phase |
| `benchmark_run DIRECTORY` | Run one iteration and write a validated `result.json` |
| `benchmark_cleanup DIRECTORY` | Explicitly remove benchmark objects after checking permission |

Loading a module defines its functions; it must not execute a benchmark or
reload environment files. Execution hooks run in supervised child processes
and return nonzero on failure. Use argument arrays/direct commands, not shell
text for the wrapper to evaluate. Workload settings, Tcl, warehouse ranges,
and result parsing remain inside the HammerDB implementation.

## Bash formatting

Put `then` on its own line after each `if` or `elif` condition, including Bash
inside traps and heredocs. Multiline function and brace-group opening braces
also go on their own line; keep single-line `{ ... }` groups compact. Separate
a closing brace block from following code with a blank line, unless the next
line is another closing brace. Add a blank line after `fi`, except immediately
before the end of a function. Do not add blank lines at end of file.

## Provisioning and tests

### Control-machine setup

The **control machine** is the computer running this checkout and Ansible,
not one of the remote runner VMs. On Ubuntu/Debian with APT, prepare it once:

```bash
./wrapper.sh --setup-control-machine
```

This explicit, standalone action installs missing control tools:

- Ansible CLI tools (using the distribution's `ansible` package when needed).
- `rsync`, OpenSSH client (`ssh`/`scp`), Git, `tar`, `gzip`, and CA certificates.
- The `ansible.posix` collection, if its `synchronize` module is not available.

Existing tools and collections are reused. Missing system packages use
`apt-get update` and `apt-get install`; only these commands use `sudo` when
needed. Run the wrapper as the account that will invoke Ansible, **not with
sudo**, so collection installation uses that account's configured collection
path (normally `~/.ansible/collections`). Direct root logins are also supported.
Network access is needed only when packages or the collection are missing.
Systems without APT must install these prerequisites manually.

The missing-collection fallback is pinned to `ansible.posix:1.5.4` for
compatibility with distribution Ansible versions, including Ubuntu 22.04's
Ansible 2.10. Already available collections are not replaced or downgraded.
Setup verifies tool availability, collection discovery, and the runner
playbook's syntax with a local-only inventory before reporting success.
Package, privilege, download, or verification failures stop the command.

It prints installation commands, performs no full-system upgrade, installs no
PostgreSQL or HammerDB software, and does not change database services, SSH
keys, benchmark configuration, or runner VMs. It needs neither a hosts file nor
runtime `.env` files. Repeating the command skips already available tools and
does not repeat downloads. It does not accept benchmark, host, environment, or
Ansible options.

Local preparation is **never implicit**: `--setup` still provisions only the
selected runners and does not install tools on the control machine.

### Runner VM setup

Provisioning is separate from benchmark execution. Prepare the
[control-machine tools](#control-machine-setup), then ensure SSH and sudo access
to the dedicated runners. The [Ansible setup guide](setup/ansible/README.md)
describes these prerequisites and the settings in
[setup/ansible/vars.yml](setup/ansible/vars.yml).

From the repository root:

```bash
# Provision every VM in the checkout's private hosts.txt.
./wrapper.sh --setup

# Provision just one VM, whether or not it is in hosts.txt.
./wrapper.sh --setup --host new-runner
```

The all-runner form uses [the shared hosts list](#shared-runner-hosts) relative
to the checkout, even when called from another directory. Missing, empty,
duplicate, or malformed host lists fail before Ansible runs. The single-VM
form supplies an inline Ansible inventory and needs no hosts file.
Neither form loads the three benchmark environments, requires a PostgreSQL
password, or starts a benchmark. Benchmark names, environment selectors, and
benchmark actions cannot be combined with `--setup`.

Pass Ansible options after `--`; relative option paths are resolved from the
caller's working directory:

```bash
# Ansible check mode, not the wrapper's benchmark --check.
./wrapper.sh --setup -- --check

# Override SSH, installation, or upload settings using a private variable file.
./wrapper.sh --setup -- -e @setup/ansible/vars.local.yml

# Explicitly replace the requested HammerDB installation on one VM only.
./wrapper.sh --setup --host new-runner -- --tags hammerdb -e reinstall_hammerdb=true
```

Before execution, the wrapper prints the complete, shell-escaped
`ansible-playbook` command and its working directory. Copy that command to run
Ansible manually from the displayed directory. Arguments are passed directly,
not reinterpreted as shell code, and Ansible's exit status is preserved.
**Printed arguments are visible:** use private variable files (`-e @FILE`),
Ansible Vault, or password prompts rather than inline credentials.

HammerDB installation is enabled by default. A missing requested version is
installed; an existing complete installation is left untouched. Replacing an
existing installation requires `reinstall_hammerdb=true`, and an incomplete
installation fails with instructions rather than being silently overwritten.
An installation is recognized by a real versioned directory containing a
readable, nonempty, executable regular `hammerdbcli` file. This is a structural
check, not a benchmark or bundled-library health test.
Required archives are checked before provisioning changes. By default, provide
`HammerDB-5.0-Prod-Linux.tar.gz` in `setup/ansible/`, or override
`local_hammerdb_tarball` in the private Ansible variables.
For a different version, set `hammerdb_version` as well (for example `"6.0"`
for an archive containing `HammerDB-6.0/`). The version is not inferred from
the archive path; mismatched contents fail preflight when installation is
needed, without creating a directory for either version. A complete existing
installation of the selected version is retained without inspecting the archive.
Also set the private `HAMMERDB_HOME` to the matching installation path.
See [version selection](setup/ansible/README.md#choosing-a-different-hammerdb-version).
Use `-e install_hammerdb=false` to skip installation. Private environment-file
upload remains opt-in with `update_benchmark_config=true`.

**Use dedicated runner VMs, not database servers.** Full provisioning upgrades
system packages, installs PostgreSQL tools, and stops/disables the system
PostgreSQL service. Skipping HammerDB installation does not skip those tasks;
use Ansible tags when only a specific operation is intended.

The [standalone Ubuntu setup](setup/setup_runner_ubuntu.sh) remains available for
dedicated runners. [setup/build_hammerdb.sh](setup/build_hammerdb.sh) builds a
HammerDB source checkout.

### Validation

Runtime prerequisites are Bash 4.3+, GNU coreutils, util-linux (`setsid`), a
PostgreSQL client, and a compatible HammerDB installation. Temporary local
targets additionally need PostgreSQL server tools.

The regression suites use stubbed native commands and never contact databases
or runners. They require Bash, Tcl (`tclsh`), and `jq` in addition to the normal
shell utilities. The environment-policy suite additionally uses isolated local
Git repositories; it never pushes:

```bash
bash tests/test_runner.sh
bash tests/test_multivm.sh
bash tests/test_setup.sh
bash tests/test_control_machine_setup.sh
bash tests/test_env_policy.sh
```

The setup suite uses a fake Ansible executable to verify target selection,
configuration isolation, argument forwarding, printed commands, and failures.
The control-machine suite also mocks package managers and sudo; it checks
missing-dependency installation, idempotence, privilege handling, and failure
propagation without installing anything on the test machine.

The optional provisioning checks also require Ansible, `ansible.posix`, and
`rsync`. They verify shared-host inventory parsing, playbook host selection,
syntax, sample rejection, and private-file upload/sync behavior. File operations
and HammerDB installation checks use local fixtures under `tests/.work/`;
no remote runners are contacted or provisioned:

```bash
bash tests/test_shared_hosts.sh
bash tests/test_env_ansible.sh
bash tests/test_hammerdb_ansible.sh
```
