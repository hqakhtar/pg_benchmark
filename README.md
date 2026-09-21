# pg_benchmark

Automated PostgreSQL (and [Citus](https://www.citusdata.com/)) benchmarking
with [HammerDB](https://www.hammerdb.com/). The toolkit can stand up a
PostgreSQL cluster, build a TPCC schema, run repeatable benchmark iterations,
and collect summarized results. It supports both single-host runs and
distributed data loads driven from many runner VMs.

## Features

- One-command benchmarking via [wrapper.sh](wrapper.sh) against a new or an
  existing PostgreSQL cluster.
- Optional `initdb`, schema build, and data-directory cleanup, all controlled
  by command-line flags.
- Multiple benchmark iterations with a one-line summary per run.
- Citus compatibility mode for benchmarking distributed clusters.
- Multi-VM parallel TPCC data loading from a list of runner hosts.
- Helper scripts and an Ansible playbook to provision runner VMs and build
  HammerDB from source.
- A read-only workload classifier (OLTP / OLAP / HTAP / TIME_SERIES) built on
  `pg_stat_statements`.

## Quick start

This path creates an isolated PostgreSQL data directory, runs one TPCC
iteration, and removes the data directory afterward. Make sure the `PGPORT` in
`myenv.sh` is not already in use.

```bash
cp myenv.sh.sample myenv.sh
${EDITOR:-vi} myenv.sh

PG_CONFIG_PATH="$(command -v pg_config)"
HAMMERDB_HOME="/opt/HammerDB-5.0"  # directory containing hammerdbcli

./wrapper.sh --check \
  -C "$PG_CONFIG_PATH" \
  -H "$HAMMERDB_HOME" \
  -t ./benchmark-results \
  -E ./myenv.sh

./wrapper.sh -I -S -Z -i 1 \
  -C "$PG_CONFIG_PATH" \
  -H "$HAMMERDB_HOME" \
  -t ./benchmark-results \
  -E ./myenv.sh
```

`--check` validates paths, configuration, resource limits, and effective TPCC
settings without initializing PostgreSQL or running HammerDB. The sample uses
small localhost defaults; increase its warehouse, build-user, run-user, ramp,
and duration values for a real benchmark.

## Repository layout

| Path | Description |
|------|-------------|
| [wrapper.sh](wrapper.sh) | Main entry point. Sources the config files and drives the benchmark. |
| [pg.env](pg.env) | PostgreSQL server (`initdb`/runtime) configuration. |
| [myenv.sh.sample](myenv.sh.sample) | Sample environment file for connection details and TPCC sizing. |
| [hosts.txt.sample](hosts.txt.sample) | Sample list of runner hosts for multi-VM loads. |
| [hammerdb/](hammerdb/) | HammerDB benchmark driver, env file, and cleanup/maintenance SQL. |
| [helper_sqls/](helper_sqls/) | Utility SQL (e.g. Citus shard distribution helpers). |
| [setup/](setup/) | Provisioning scripts, multi-VM orchestration, and Ansible playbook. |
| [workload_analysis/fspg_ec_advisor/](workload_analysis/fspg_ec_advisor/) | FSPG EC Advisor PostgreSQL extension and operator documentation. |

## Prerequisites

- Bash 4 or newer.
- A PostgreSQL installation; use `pg_config` from the exact version under
  test.
- Any extension you want to benchmark already installed (e.g.
  `pg_stat_statements`, `pg_stat_monitor`).
- A HammerDB installation whose root contains an executable `hammerdbcli`.

To build HammerDB from source, run
[setup/build_hammerdb.sh](setup/build_hammerdb.sh) from the HammerDB source
directory. Runner provisioning is covered below.

## Configuration

`wrapper.sh` combines three configuration layers:

1. The optional file passed with `-E`, normally `myenv.sh`, supplies local
   connection and TPCC values.
2. [hammerdb/hammerdb.env](hammerdb/hammerdb.env) fills in any unset benchmark
   values with portable defaults.
3. [pg.env](pg.env), or the file passed with `-e`, supplies PostgreSQL server
   settings used when `-I` starts a temporary cluster.

Copy [myenv.sh.sample](myenv.sh.sample) to the ignored `myenv.sh` file and edit
that local copy. Pass it with `-E ./myenv.sh`; it is not loaded implicitly.
Prefer `~/.pgpass` over storing `PGPASSWORD` in the file.

The most commonly changed values are `PGHOST`, `PGPORT`, `PGUSER`, `PG_DBASE`,
`PG_COUNT_WARE` (warehouses), `PG_NUM_VU` (schema-build virtual users), `PG_VU`
(benchmark virtual users), `PG_DURATION`, and `PG_RAMPUP`. `PG_NUM_VU` cannot
exceed `PG_COUNT_WARE`.

## Running on a single host

`wrapper.sh` requires three mandatory arguments:

- `-C` path to `pg_config`
- `-H` path to the HammerDB installation directory
- `-t` a working folder where the script creates the data directory and logs;
  the folder is created if needed

By default the script targets an **existing** PostgreSQL cluster. Use only a
dedicated benchmark database: cleanup can drop TPCC/TPCH tables and other
matching objects from the `public` schema. To use a temporary local cluster
instead, add `-I` (run `initdb`), `-S` (build schema), and `-Z` (remove the data
directory), as shown in Quick start.

Example - benchmark an already prepared, dedicated cluster:

```bash
./wrapper.sh \
  -C /home/vagrant/postgres.14/inst/bin/pg_config \
  -H /home/vagrant/HammerDB-4.4 \
  -t /tmp/xyz \
  -E ./myenv.sh
```

Example - create a fresh cluster, build the schema, and benchmark:

```bash
./wrapper.sh -I -S -Z \
  -C /home/vagrant/postgres.14/inst/bin/pg_config \
  -H /home/vagrant/HammerDB-4.4 \
  -t /tmp/xyz \
  -E ./myenv.sh
```

### Options

| Option | Variable | Description |
|--------|----------|-------------|
| `-h`, `--help` | | Show usage. |
| `--check` | | Validate configuration and exit without benchmarking. |
| `-C` | `PG_CONFIG` | Path to `pg_config`. **Required.** |
| `-H` | | HammerDB installation directory. **Required.** |
| `-t` | | Working folder for the data directory and logs. **Required.** |
| `-b` | `BENCHMARK_TYPE` | Benchmark type (default: `hammerdb`). |
| `-c` | | Enable Citus compatibility mode. |
| `-e` | `PG_CONF_FILE` | PostgreSQL configuration file (default: [pg.env](pg.env)). |
| `-E` | `ENV_FILE` | Connection and TPCC environment file. |
| `-I` | | Run `initdb`. |
| `-i` | `ITERATIONS` | Number of iterations (default: 3). |
| `-l` | `PRELOAD_LIBRARY` | Shared preload library. |
| `-n` | `BENCHMARK_NAME` | Benchmark name (default: `tpcc`). |
| `-O` | | Build the schema once only, then run the maintenance script. |
| `-P` | | Prepare only: build the schema and exit without benchmarking. |
| `-r` | `PG_INIT_SQL` | SQL script to run after `initdb`. |
| `-S` | | Build a new schema on every iteration. |
| `-Z` | | Remove the data directory. |

## Citus compatibility

Pass `-c` to enable Citus mode. In this mode the appropriate cleanup and
maintenance SQL files (e.g. [hammerdb/hammerdb_cleanup_citus.sql](hammerdb/hammerdb_cleanup_citus.sql),
[hammerdb/hammerdb_maintenance_citus.sql](hammerdb/hammerdb_maintenance_citus.sql))
are used. [helper_sqls/shard_funcs.sql](helper_sqls/shard_funcs.sql) provides
utilities to inspect shard distribution.

## Multi-VM (distributed) data loading

For large TPCC loads, you can drive many runner VMs in parallel with
[setup/run_tpcc_load_multivm.sh](setup/run_tpcc_load_multivm.sh). It computes
warehouse slices from a hosts file and performs:

1. Cleanup once on the first runner.
2. Phase 1 bootstrap on the first runner (warehouse 1).
3. Phase 2 parallel data load across all runners for the remaining warehouses.
4. Phase 3 post-data DDL on the first runner.

```bash
cp hosts.txt.sample hosts.txt
${EDITOR:-vi} hosts.txt

./setup/run_tpcc_load_multivm.sh \
  ./hosts.txt \
  /opt/HammerDB/pg_benchmark \
  /usr/bin/pg_config \
  /opt/HammerDB \
  /tmp/hdb-load \
  1000000        # total warehouses (optional)
```

Connection and sizing details are provided through environment variables (or a
sourced `myenv.sh`): `PGHOST`, `PGPORT`, `PGPASSWORD`, `PG_SUPERUSER`,
`PG_USER`, `PG_DBASE`, `PG_DEFAULTDBASE`. See the script's usage for the full
list of optional variables (`ENABLE_CITUS`, `PHASE2_NUM_VU`, `SSH_OPTS`,
`LOG_DIR`, …).

### Provisioning runners

The recommended path is the idempotent Ansible playbook. Its
[setup guide](setup/ansible/README.md) covers prerequisites, private inventory,
dry runs, local overrides, and optional HammerDB installation:

```bash
cd setup/ansible
cp inventory.ini inventory.local.ini
${EDITOR:-vi} inventory.local.ini
ansible-playbook -i inventory.local.ini runner_setup.yml --check
ansible-playbook -i inventory.local.ini runner_setup.yml
```

[setup/setup_runner_ubuntu.sh](setup/setup_runner_ubuntu.sh) is a standalone
alternative for a dedicated Ubuntu runner. Set `POSTGRES_VERSION`,
`REPOSITORY_URL`, or `REPOSITORY_DESTINATION` in its environment to override
its defaults. [setup/setup_multi_runners.sh](setup/setup_multi_runners.sh) can
send that script to every host in an ignored `hosts.txt` file.

## Output

Under the working directory, the script creates:

- A `PG-<VERSION>` folder containing the data directory plus `server`/`initdb`
  log files.
- One log file per benchmark iteration.
- A summary log file with a one-line result per iteration, for example:

```
Vuser 1:TEST RESULT : System achieved 42332 NOPM from 97427 PostgreSQL TPM
Vuser 1:TEST RESULT : System achieved 38385 NOPM from 88486 PostgreSQL TPM
Vuser 1:TEST RESULT : System achieved 37331 NOPM from 86015 PostgreSQL TPM
```

## Workload analysis

After a benchmark, use the
[FSPG EC Advisor extension](workload_analysis/fspg_ec_advisor/) to capture and
classify `pg_stat_statements` workload evidence. Its
[README](workload_analysis/fspg_ec_advisor/README.md) covers installation, API,
automation, and tests.