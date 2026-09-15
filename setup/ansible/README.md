# Runner provisioning with Ansible

This playbook prepares Ubuntu runner VMs with PostgreSQL and the tools needed
by `pg_benchmark`. Run it from the control machine that contains this checkout.

## Prerequisites

- SSH access to each runner with a user that can use `sudo`.
- Ansible and `rsync` on the control machine.
- The `ansible.posix` collection used by the `synchronize` task:

  ```bash
  ansible-galaxy collection install ansible.posix
  ```

## First run

Create a private inventory and add the runner addresses:

```bash
cd setup/ansible
cp inventory.ini inventory.local.ini
${EDITOR:-vi} inventory.local.ini
```

Confirm connectivity, preview the changes, and apply them:

```bash
ansible -i inventory.local.ini runners -m ping
ansible-playbook -i inventory.local.ini runner_setup.yml --check
ansible-playbook -i inventory.local.ini runner_setup.yml
```

The checked-in defaults in `vars.yml` install PostgreSQL 17 and upload this
repository. They do not upload `myenv.sh` or install HammerDB until those
features are explicitly enabled.

## Local overrides

Keep machine-specific values in the ignored `vars.local.yml` file:

```yaml
pg_version: "17"
update_myenv: true
install_hammerdb: true
local_hammerdb_tarball: "/path/to/HammerDB-5.0-Prod-Linux.tar.gz"
```

Create `myenv.sh` before enabling its upload, then pass the overrides:

```bash
cp ../../myenv.sh.sample ../../myenv.sh
${EDITOR:-vi} ../../myenv.sh
ansible-playbook -i inventory.local.ini runner_setup.yml -e @vars.local.yml
```

The playbook checks optional local files before changing the corresponding
remote installation. Use `--tags myenv`, `--tags hammerdb`, or
`--tags pg_benchmark_update` to run only that part of the setup.