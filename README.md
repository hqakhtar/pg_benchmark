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
| [workload_analysis/](workload_analysis/) | Workload classifier SQL and its [README](workload_analysis/README.md). |

## Prerequisites

- A PostgreSQL installation (use `pg_config` from that installation).
- Any extension you want to benchmark already installed (e.g.
  `pg_stat_statements`, `pg_stat_monitor`).
- A HammerDB installation.

To build HammerDB from source and install it on Ubuntu runners, see
[setup/build_hammerdb.sh](setup/build_hammerdb.sh),
[setup/setup_hammerdb.sh](setup/setup_hammerdb.sh), and
[setup/setup_runner_ubuntu.sh](setup/setup_runner_ubuntu.sh).

## Configuration

`wrapper.sh` sources two configuration files:

- [pg.env](pg.env) — PostgreSQL server settings passed to `initdb`/startup via
  `PG_INITDB_OPTS` (e.g. `shared_buffers`, `max_wal_size`).
- [hammerdb/hammerdb.env](hammerdb/hammerdb.env) — connection details and TPCC
  parameters such as `PG_COUNT_WARE` (warehouses), `PG_NUM_VU` (build virtual
  users), `PG_VU` (run virtual users), `PG_DURATION`, and `PG_RAMPUP`.

For multi-VM runs and to keep environment-specific values out of version
control, copy [myenv.sh.sample](myenv.sh.sample) to `myenv.sh` and adjust the
connection and sizing variables (`PGHOST`, `PGPORT`, `PGUSER`, `PG_COUNT_WARE`,
etc.).

## Running on a single host

`wrapper.sh` requires three mandatory arguments:

- `-C` path to `pg_config`
- `-H` path to the HammerDB installation directory
- `-t` a working folder where the script creates the data directory and logs

By default the script runs against an **existing** PostgreSQL cluster. To set
up a fresh cluster, add `-I` (run `initdb`), `-S` (build schema), and `-Z`
(remove the data directory).

Example — benchmark an existing cluster:

```bash
./wrapper.sh \
  -C /home/vagrant/postgres.14/inst/bin/pg_config \
  -H /home/vagrant/HammerDB-4.4 \
  -t /tmp/xyz
```

Example — create a fresh cluster, build the schema, and benchmark:

```bash
./wrapper.sh -I -S -Z \
  -C /home/vagrant/postgres.14/inst/bin/pg_config \
  -H /home/vagrant/HammerDB-4.4 \
  -t /tmp/xyz
```

### Options

| Option | Variable | Description |
|--------|----------|-------------|
| `-h` | | Show usage. |
| `-C` | `PG_CONFIG` | Path to `pg_config`. **Required.** |
| `-H` | | HammerDB installation directory. **Required.** |
| `-t` | | Working folder for the data directory and logs. **Required.** |
| `-b` | `BENCHMARK_TYPE` | Benchmark type (default: `hammerdb`). |
| `-c` | | Enable Citus compatibility mode. |
| `-e` | `PG_CONF_FILE` | PostgreSQL configuration file (default: [pg.env](pg.env)). |
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

- [setup/setup_runner_ubuntu.sh](setup/setup_runner_ubuntu.sh) installs
  PostgreSQL, tooling, and clones this repository on an Ubuntu host.
- [setup/setup_multi_runners.sh](setup/setup_multi_runners.sh) runs a given
  script across all hosts listed in a hosts file.
- [setup/ansible/runner_setup.yml](setup/ansible/runner_setup.yml) provides a
  declarative equivalent of the runner setup:

  ```bash
  cd setup/ansible
  ansible-playbook -i inventory.ini runner_setup.yml
  ```

List your runner hostnames in a `hosts.txt` file (see
[hosts.txt.sample](hosts.txt.sample)).

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

After a benchmark, you can classify the workload captured in
`pg_stat_statements` (OLTP / OLAP / HTAP / TIME_SERIES) with the read-only
profiler in [workload_analysis/](workload_analysis/):

```bash
psql -X -v ON_ERROR_STOP=1 -d <database> \
  -f workload_analysis/workload_score_pg_stat_statements.sql
```

See [workload_analysis/README.md](workload_analysis/README.md) for details.