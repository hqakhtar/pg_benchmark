# citus-benchmarking

Creates a disposable Azure environment containing one Ubuntu VM and a two-node
Citus development/test cluster running in Docker:

- PostgreSQL 17
- Citus 14.2
- `demouser` bootstrap user
- one coordinator container
- one worker container

## Files

- `create-citus-vm.sh`: validation, dry run, deployment, and VM configuration
- `main.bicep`: subscription-scope resource-group deployment
- `infrastructure.bicep`: VM and networking resources

## Defaults

- Subscription: `88abe223-c630-4f2c-8782-00bb5be874f6`
- Region: `East Asia` (`eastasia`)
- Resource-group prefix: `citus-benchmarking`
- VM: `citus-benchmark`
- VM size: `Standard_D4s_v5`
- SSH public key: `~/.ssh/id_ed25519.pub`

## Prerequisites

Run from Bash, WSL, or Azure Cloud Shell with:

- Azure CLI
- an active `az login`
- permissions to run subscription-scope deployments and create the resources
- `ssh`, `scp`, and `curl`
- an existing OpenSSH key pair

## Validate and preview

```bash
chmod +x create-citus-vm.sh
bash -n create-citus-vm.sh
./create-citus-vm.sh --help
./create-citus-vm.sh --dry-run
```

`--dry-run` performs local checks, validates the subscription, region, SKU, and
SSH public key, then runs Azure Resource Manager what-if. It does not prompt for
the PostgreSQL password or create target resources.

## Deploy

```bash
./create-citus-vm.sh
```

Example with overrides:

```bash
./create-citus-vm.sh \
  --location "East Asia" \
  --vm-size Standard_E4s_v5 \
  --source-cidr 203.0.113.10/32
```

## Arguments

```text
--dry-run
--subscription ID
--location LOCATION
--resource-group NAME
--resource-group-prefix PREFIX
--vm-name NAME
--vm-size SKU
--admin-user USER
--ssh-public-key PATH
--ssh-private-key PATH
--source-cidr CIDR
--skip-sku-check
--keep-setup-script
--help, -h
```

No positional arguments are accepted.

## Cleanup

The script prints the exact cleanup command at the end. The form is:

```bash
az group delete \
  --subscription "88abe223-c630-4f2c-8782-00bb5be874f6" \
  --name "<resource-group-name>" \
  --yes
```

## Security notes

- Linux password authentication is disabled.
- Ports 22 and 5432 are restricted to `--source-cidr`.
- If `--source-cidr` is omitted, the script attempts to detect the caller's
  public IPv4 address and uses a `/32` rule.
- The PostgreSQL password is not sent to Azure Resource Manager.
- The password persists on the VM in `/opt/citus-benchmarking/postgres-password`
  as a root-owned mode-600 Docker Compose secret source so containers can
  restart. Delete the resource group to remove the environment.
- This topology is intended for development and benchmarking, not production.
