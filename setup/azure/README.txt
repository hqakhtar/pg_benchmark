AZURE VM BATCH SCRIPTS
=====================

Package v9

Files (keep all eight together)
  create-azure-vms.sh       Create individually named Linux VMs.
  delete-azure-vms.sh       Delete selected VMs and their dedicated resources.
  azure-vm-common.sh        Required data-file loader and path helpers.
  azure-vm-ssh-config.py    Required helper when local SSH updates are enabled.
  azure-vm-delete-attached.py  Required helper for --include-unmanaged deletion.
  azure-vm-hosts.py         Required helper when --hosts-file is supplied.
  azure-vms.env             Deployment and local SSH settings.
  README.txt               Usage, cleanup behavior and compatibility notes.

The scripts use Azure CLI directly, not Terraform or a scale set. They create
VMs sequentially and wait for each operation to finish.

WRAPPER AND RUNNER INVENTORY

From the pg_benchmark checkout, use:

  ./wrapper.sh --azure-create -- --dry-run
  ./wrapper.sh --azure-create
  ./wrapper.sh --azure-delete -- --dry-run
  ./wrapper.sh --azure-delete -- --interactive

The wrapper uses setup/azure/azure-vms.env unless --env-file PATH is supplied
after --. It always passes the checkout's hosts.txt as --hosts-file; that path
cannot be overridden through the wrapper. Other script options go after --.
Benchmark environment files and Ansible are not required for these actions.

Direct invocation of either Azure script supports optional --hosts-file PATH.
This path is relative to the current working directory, not the env file.
Without that flag, standalone scripts do not modify any runner inventory.
Creation adds each successfully created VM alias without duplicates. Deletion
removes selected aliases only after confirmed VM absence/deletion and user
approval, including --include-unmanaged. Unrelated hosts, comments and blank
lines are preserved. Skipped/failed VM deletions retain their aliases; completed
VM changes remain recorded if a later operation fails. Dry runs and cancelled
deletions leave inventory unchanged. An absent inventory is created privately
on the first successful addition; deletion does not create one.

The hosts file must be a plain list of hosts, not grouped Ansible INI inventory.
Symlinks and duplicate/invalid existing hosts are rejected before Azure writes.
Atomic replacements preserve existing file permissions. A persistent FILE.lock
beside the inventory serializes concurrent updates; keep it in place.
Enable managed SSH configuration to make the added VM aliases resolvable, then
run ./wrapper.sh --setup separately to provision the selected runners.

0. EXPLICIT ENVIRONMENT FILE

Both scripts require --env-file PATH. Edit azure-vms.env with your subscription,
region, resource group, VM range, image/size, public key and local SSH settings.
The file is read afresh on every run; do not source it into your current shell.

  ./create-azure-vms.sh --env-file ./azure-vms.env --dry-run
  ./create-azure-vms.sh --env-file ./azure-vms.env

For deletion, run ONE appropriate command:

  ./delete-azure-vms.sh --env-file ./azure-vms.env --dry-run
  ./delete-azure-vms.sh --env-file ./azure-vms.env --interactive
  ./delete-azure-vms.sh --env-file ./azure-vms.env --yes

Setting precedence is:
  explicit CLI flag > selected file > built-in default

Previously exported AZVM_* variables are ignored, including variables left by
an older source command. Other normal environment settings, such as HOME and
Azure CLI authentication, remain available. The settings file never changes
the parent shell and is parsed as data without source/eval or command execution.
The optional export keyword is accepted for compatibility with older files;
loaded settings are internal to the script and are not exported to child tools.

Supported file syntax:
  - One AZVM_NAME=value assignment per line, optionally preceded by export.
  - Single/double quotes, blank lines, and # comments.
  - $HOME or ${HOME} expansion in unquoted/double-quoted values.
  - Single-quoted values are literal; "$HOME/.ssh/key" expands the home path.
  - UTF-8 (with optional BOM) and Unix or Windows CRLF line endings.

Only documented AZVM_* settings are accepted. Unknown or duplicate settings,
commands, active command substitution, other variable expansions and multiline
values produce a file:line error before Azure requests. Headings must start
with #. In particular, an un-commented "Azure VM deployment settings ..." line
from a copied file must be corrected to a comment.

--env-file=PATH is also accepted. Its path is relative to the current working
directory; key/config paths inside the file (or their CLI overrides) are relative
to the env file's directory unless absolute. Quoted ~ is not expanded: use HOME.
--help works without --env-file. Execute the main scripts; sourcing them is
explicitly rejected before they can change shell options or umask.

For a one-command override:

  ./create-azure-vms.sh --env-file ./azure-vms.env --count 2 --start 5 --dry-run
  ./delete-azure-vms.sh --env-file ./azure-vms.env --count 2 --start 5 --interactive

The file enables public IPs and sets AZVM_SSH_SOURCE="0.0.0.0/0", allowing SSH
connection attempts on TCP 22 from every IPv4 address. Login still requires the
matching private key. This opens the SSH rule, not every TCP/UDP port.

Without --interactive or --yes, deletion asks once for the complete plan.
Shared-network cleanup runs only when the owned network is unused; interactive
mode asks separately for it. Use --keep-network to preserve it. Confirmation
and dry-run switches remain explicit CLI actions; the file cannot enable them.
The explicit --include-unmanaged action supports manually provisioned basic VMs;
see section 5A. It always preserves VNets/subnets and is not enabled by the file.

Environment-to-flag mapping:
  AZVM_SUBSCRIPTION            --subscription
  AZVM_RESOURCE_GROUP          --resource-group
  AZVM_LOCATION                --location
  AZVM_NAME_PREFIX             --name-prefix
  AZVM_COUNT                   --count
  AZVM_START                   --start
  AZVM_DIGITS                  --digits (only 2 is accepted)
  AZVM_ADMIN_USER              --admin-user
  AZVM_SSH_KEY                 --ssh-key
  AZVM_SSH_SOURCE              --ssh-source
  AZVM_PUBLIC_IP               --public-ip true|false (or --no-public-ip)
  AZVM_IMAGE                   --image
  AZVM_SIZE                    --size
  AZVM_OS_DISK_SIZE            --os-disk-size
  AZVM_STORAGE_SKU             --storage-sku
  AZVM_SECURITY_TYPE           --security-type
  AZVM_ACCELERATED_NETWORKING   --accelerated-networking
  AZVM_SUBNET_ID                --subnet-id
  AZVM_VNET_CIDR                --vnet-cidr
  AZVM_SUBNET_CIDR              --subnet-cidr
  AZVM_UPDATE_SSH_CONFIG        --update-ssh-config true|false (both scripts)
  AZVM_SSH_CONFIG_FILE          --ssh-config-file (both scripts)
  AZVM_SSH_PRIVATE_KEY          --ssh-private-key (creation)
  AZVM_SSH_PORT                 --ssh-port (creation)
  AZVM_SSH_ADDRESS              --ssh-address auto|public|private (creation)

Deletion uses subscription, resource group, name prefix, count, start, digits,
subnet ID and local SSH update/config settings. It does not require a private
key or SSH port to remove entries. A nonempty subnet ID preserves the external
network. A blank subscription stops before Azure requests. An empty subnet ID
means use the managed VNet. Numeric suffixes stay at two digits, 01 through 20.

0A. LOCAL SSH CONFIG ENTRIES

To enable this feature in an existing configured env file, add these settings
once, using your actual local private key path:

  export AZVM_UPDATE_SSH_CONFIG="true"
  export AZVM_SSH_CONFIG_FILE="$HOME/.ssh/config"
  export AZVM_SSH_PRIVATE_KEY="$HOME/.ssh/id_rsa"
  export AZVM_SSH_PORT="22"
  export AZVM_SSH_ADDRESS="auto"

AZVM_SSH_KEY remains the PUBLIC key installed on Azure. AZVM_SSH_PRIVATE_KEY
is the local IdentityFile used by your SSH client; it must be a readable regular
file at creation time. Its contents are never read or uploaded by this feature.
The scripts do not verify the public/private key pair; supply a matching pair.
A passphrase-protected key works normally when SSH later prompts or uses an agent.

The bundled template enables SSH updates and uses id_ed25519/id_ed25519.pub;
existing files that omit AZVM_UPDATE_SSH_CONFIG default to false. Set it to
false (or use --update-ssh-config false) to leave SSH configuration untouched.
Keep it true, with the same config path, for deletion to remove managed entries.

After each successfully created and tagged VM, the script reads its assigned IP
and adds/updates a Host block such as:

  Host hamid-vm-r06
      HostName 203.0.113.10
      Port 22
      User azuresu
      IdentityFile "/home/you/.ssh/id_rsa"
      IdentitiesOnly yes

The IP above is illustrative. HostName uses the actual assigned address; User
uses the effective admin user, including any --admin-user override. You can
then run ssh hamid-vm-r06. auto selects the public IP when public IPs are enabled,
otherwise the private IP. private always uses the private address, which needs
an existing network route. public requires public IP creation to be enabled.
AZVM_SSH_PORT sets the CLIENT entry only. The supplied VM SSH service and added
NSG rule use port 22; a different client port requires separately configured
server/network forwarding. The feature does not configure a jump host or sshd.

Entries are stored in a marked managed section at the start of the selected
config, before general defaults. Metadata binds each alias to its subscription,
resource group and VM name. Unmanaged content is preserved byte-for-byte; a
Host * reset preserves its original leading global settings. Creation refuses
an explicit manual alias collision in the file or supported Include files.
It reads Includes but never executes Match exec directives. Host-dependent
Include tokens that cannot be inspected safely produce a preflight error.

Do not hand-edit the managed section or its metadata. The helper detects edited,
malformed or duplicate markers and stops for inspection. Mutation uses a lock
and an atomic replacement, with mode 0600 for the config and 0700 for newly
created directories. A harmless config.azvm.lock file remains for locking.
Symlink config/lock files are refused. Helpers run locally in Linux/WSL/macOS;
the default HOME/.ssh/config is the config for that environment's SSH client.

Before any Azure writes, creation validates the entire set of requested aliases
and the key/config paths. It adds each entry after that VM succeeds, so a later
failure retains entries for already completed VMs. A failed local write leaves
the created VM in place and reports the failure. Dry runs do not edit the SSH
config or create its directory, replacement files or locks. Public-key validation
uses a temporary public-key file that is removed on exit.

Deletion includes matching managed entries in the plan. Declining a VM or a
failed VM deletion preserves its entry. Once VM absence is confirmed, the entry
is removed even if a later dependent-resource cleanup fails. Stale owned entries
can also be removed when the VM or its resource group is already absent, using
the same --dry-run/--interactive/--yes behavior. Entries from another scope and
unmanaged entries are preserved. Known-hosts files are not changed.

This integrates entries for VMs created with the new option enabled; creation
still rejects already occupied VM names. It does not adopt manually created VMs
or automatically populate entries for VMs made by earlier package versions.

0B. CLEAR VARIABLES EXPORTED BY AN EARLIER SOURCE COMMAND

There is no general undo for source. To remove the AZVM_* variables in the
current Bash shell after sourcing an older settings file, run:

  for azvm_var in "${!AZVM_@}"; do
      unset -v "$azvm_var"
  done
  unset -v azvm_var

Check with compgen -v AZVM_ || true; no output means none remain. Unsetting
removes these variables; it cannot recover
values that were overwritten before sourcing, or undo arbitrary commands in a
file. The provided old env file only assigned AZVM_* values. Do not execute the
unset loop inside a separate bash process if you want to clean the current shell.
The new --env-file workflow needs no unsourcing after it finishes.

1. PREREQUISITES

Run in Bash 4 or newer on Linux/WSL, or in Azure Cloud Shell's Bash environment.
On macOS, install a current Bash rather than using the older system Bash.
Required commands:
  - Azure CLI (az), current version recommended
  - jq
  - Python 3 (python3), using only its standard library
  - ssh-keygen and standard utilities including mktemp (creation)

Install Azure CLI: https://learn.microsoft.com/cli/azure/install-azure-cli
For Ubuntu/WSL, jq and ssh-keygen can be installed with:

  sudo apt-get update
  sudo apt-get install jq python3 openssh-client

Authenticate and find the subscription ID:

  az login
  az account list --query '[].{Name:name,ID:id}' --output table

An existing authenticated service principal or managed identity also works.
The scripts pass --subscription explicitly and do not change the CLI's default
subscription with az account set.

Region validation uses az rest against the resolved subscription's locations
endpoint (API version 2022-12-01). The relative URL uses the active Azure cloud's
Resource Manager endpoint. This avoids az account list-locations, which does
not accept --subscription. The subscription in azure-vms.env or the command
line remains authoritative even when a different subscription is the CLI
default. If Azure returns more than one locations page, validation follows
nextLink until the requested region is found or all pages have been checked.

Your identity needs permissions to read/create/delete VMs, disks, NICs, public
IPs, NSGs and the relevant network, plus permission to tag resources. Contributor
on an existing target resource group is a common starting point, subject to
your organization's policies. Creating a resource group also needs permission
at subscription scope. An existing subnet in another resource group requires
the appropriate read/join permissions there. Resource locks or Azure Policy
can still prevent an operation even when RBAC grants the resource permission.
Creation preflight also needs subscription-level read access to regional VM
SKU metadata and VM usage/quotas. A failed quota read stops creation.

Ensure the selected region has a compatible VM image and size and that your
subscription has enough regional and VM-family vCPU quota. These scripts do
not increase quota, register providers, bypass policy or accept Marketplace
terms. A new subscription may need Microsoft.Compute and Microsoft.Network
registered by an authorized administrator.

If you need a new SSH key, generate one in a NEW file (do not overwrite an
existing key you still need):

  ssh-keygen -t ed25519 -f "$HOME/.ssh/azure_lab_ed25519"

Supply the .pub file to creation. Use the matching private key with ssh when
connecting. The two public-key entries in the original requirements are
implemented as one SSH public key installed for the admin account on every VM.
RSA keys of at least 2048 bits and Ed25519 keys are accepted.

2. QUICK START WITH EXPLICIT COMMAND-LINE ARGUMENTS

Make the scripts executable:

  chmod +x create-azure-vms.sh delete-azure-vms.sh

Set these values to your subscription and your actual public IPv4 address:

  SUB='<subscription-id>'
  RG='hamid-lab-rg'
  REGION='eastus'
  SSH_SOURCE='<your-public-ip>/32'

The <...> values are placeholders. A /32 permits that one source address.
203.0.113.10/32 in --help is documentation-only, not a usable Internet address.
If your connection uses a VPN, use the address Azure will see for that route.

Preview four VMs:

  ./create-azure-vms.sh --env-file ./azure-vms.env \
    --subscription "$SUB" \
    --resource-group "$RG" \
    --location "$REGION" \
    --name-prefix hamid-vm-r \
    --count 4 \
    --ssh-key "$HOME/.ssh/azure_lab_ed25519.pub" \
    --ssh-private-key "$HOME/.ssh/azure_lab_ed25519" \
    --ssh-source "$SSH_SOURCE" \
    --dry-run

Run the same command WITHOUT --dry-run to create the VMs. The creation command
proceeds directly after its checks; it has no extra confirmation prompt.

The VM names will be:
  hamid-vm-r01
  hamid-vm-r02
  hamid-vm-r03
  hamid-vm-r04

Creation prints private and public IP addresses when complete. Connect using:

  ssh -i "$HOME/.ssh/azure_lab_ed25519" azuresu@<vm-public-ip>

To provide the key text rather than a filename, replace --ssh-key's value with
a quoted complete one-line public key. This also exercises the literal input:

  --ssh-key "$(cat "$HOME/.ssh/azure_lab_ed25519.pub")"

3. CREATION OPTIONS

Required settings (in --env-file or through CLI flags)
  --subscription VALUE     Subscription name or ID; ID is unambiguous.
  --resource-group NAME    Existing group, or a new group the script creates.
  --location REGION       Region code, e.g. eastus or westeurope.
  --name-prefix PREFIX    Lowercase name prefix before the numeric suffix.
  --count N               1 to 20 VMs, with final index no higher than 20.
  --ssh-key FILE_OR_KEY   One OpenSSH public key, as a path or literal text.
  --ssh-source CIDR       Required when public IPs are enabled (the default).

Defaults and overrides
  --admin-user NAME       azuresu
  --start N               1; allowed starting numbers are 1 to 20
  --digits N              Fixed at 2; retained as a compatibility option
  --image IMAGE           Canonical:ubuntu-24_04-lts:server:latest
  --size SIZE             Standard_D2s_v5
  --os-disk-size GIB      64
  --storage-sku SKU       StandardSSD_LRS
  --security-type TYPE    TrustedLaunch; Standard is also supported
  --accelerated-networking BOOL
                          true; set false only for other compatible sizes/images
  --vnet-cidr CIDR        10.50.0.0/16
  --subnet-cidr CIDR      10.50.0.0/24
  --subnet-id ID          Optional full ID of an existing subnet
  --public-ip BOOL        true or false; overrides AZVM_PUBLIC_IP
  --no-public-ip          Omit public IPs; requires a private access route
  --dry-run              Read/validate/print only; no Azure writes
  --help                 Show built-in help

Numeric suffixes are always exactly two digits, from 01 to 20. The starting
number and count must each be 1..20, and start + count - 1 must not exceed 20.
For example, --start 19 --count 2 selects 19 and 20; --start 20 --count 2 is
rejected before any Azure request. --start 0, --count 21 and --digits 3 are also
rejected. Leading-zero inputs such as --start 08 are parsed as decimal, producing
the suffix 08. The prefix must begin with a lowercase letter and contain only
lowercase letters, digits and hyphens, up to 48 characters.

For a different admin, VM size and disk, append, for example:

  --admin-user hamid --size Standard_D4s_v5 \
  --os-disk-size 128 --storage-sku Premium_LRS

For an Ubuntu 22.04 image accepted by Azure CLI, use --image Ubuntu2204.
The chosen image, architecture, VM size, security type and storage SKU must be
compatible. TrustedLaunch explicitly enables Secure Boot and vTPM. Set
--security-type Standard when your chosen supported image/size requires it.
Accelerated networking is explicitly enabled on every NIC by default because
Microsoft lists it as required for Dsv5. For a different size or image without
support, use --accelerated-networking false; do not disable it for Dsv5.
Marketplace images with purchase plans need extra provisioning work beyond
this script. For repeatable experiments, use a specific image version instead
of latest. The script does not install PostgreSQL or other workload software.

To add two more VMs after 01 through 04, repeat the creation options with:

  --start 5 --count 2

Keep the same prefix and network options so the managed VNet can be reused.
Rerunning an already occupied range is rejected rather than treated as an
update. Requested VM, disk, NIC, NSG and PIP names are all checked before any
Azure mutation, including PIP names when --no-public-ip is used.

4. NETWORKING

By default the script creates or reuses an owned shared VNet named
PREFIX-vnet, with a subnet named default. A pre-existing VNet with that name
must have the script's ownership tags, requested region and matching address
ranges; otherwise creation stops. Choose address ranges that fit your planned
network and do not overlap networks you need to connect later.

Each VM gets a dedicated NSG, NIC and Standard static IPv4 public IP. The NSG
adds TCP port 22 from --ssh-source. Azure's default NSG rules remain in place,
including VNet-internal traffic rules. The source restriction is for the added
SSH rule; it does not isolate the VMs from one another. Existing subnet NSGs,
routes, peering and guest firewalls can further affect connectivity.

To join an existing subnet, add this flag to your normal creation command:

  --subnet-id "/subscriptions/$SUB/resourceGroups/network-rg/providers/Microsoft.Network/virtualNetworks/lab-vnet/subnets/vms"

The subnet must already exist in the same subscription and region. Its VNet
can be in another resource group. The script does not edit or retag it. With
--subnet-id, the managed --vnet-cidr and --subnet-cidr options are unused.

For private-only VMs add --no-public-ip and optionally omit --ssh-source.
Provide VPN, Bastion, a jump host or another existing private access path.
The script does not create any of those services.

Private-only VMs also need configured egress for package downloads and other
Internet access, such as NAT Gateway or an appropriate routed firewall.
Microsoft documents private-subnet defaults for new VNets using APIs released
after March 31, 2026. Do not depend on implicit outbound access. A public IP
attached to the NIC is an explicit outbound method for the default public mode.

5. DELETION

Use the same subscription, resource group, prefix and starting index that
identify the VMs you want to remove. Suffix width is always two digits and the
selected range must stay within 01..20. Region/key/admin are not needed.
Preserve the original AZVM_SUBNET_ID setting if an existing subnet was used.

Preview without changing anything:

  ./delete-azure-vms.sh --env-file ./azure-vms.env --subscription "$SUB" --resource-group "$RG" \
    --name-prefix hamid-vm-r --count 4 --dry-run

Ask separately about each VM and its dedicated resources:

  ./delete-azure-vms.sh --env-file ./azure-vms.env --subscription "$SUB" --resource-group "$RG" \
    --name-prefix hamid-vm-r --count 4 --interactive

Delete without prompting, for unattended scripts:

  ./delete-azure-vms.sh --env-file ./azure-vms.env --subscription "$SUB" --resource-group "$RG" \
    --name-prefix hamid-vm-r --count 4 --yes

With neither --interactive nor --yes, the script displays the plan and asks
once for the entire batch, including the eligible shared network cleanup.
In --interactive mode, it asks per VM and then separately about the unused
owned network. An empty answer or closed standard input means no.
--interactive and --yes cannot be combined. --dry-run never prompts or writes.

To select only 03 and 04:

  ./delete-azure-vms.sh --env-file ./azure-vms.env --subscription "$SUB" --resource-group "$RG" \
    --name-prefix hamid-vm-r --start 3 --count 2 --interactive

Deleted for each approved VM
  VM resource
  VM-osdisk (including its contents)
  VM-nic
  VM-pip, if present
  VM-nsg

Deleted after approved VM cleanup, when eligible
  The script-owned PREFIX-vnet and its default subnet, once unused

Retained
  Resource group
  Existing/external VNet and subnet supplied through --subnet-id
  A shared VNet still used by other VMs or services
  Networks with added subnets, peering, NAT, routes, NSGs, service links or
    other configuration outside the package's original network layout
  Other VM ranges and unrelated resources

To intentionally retain the owned network for reuse:

  ./delete-azure-vms.sh --env-file ./azure-vms.env --yes --keep-network

The shared network is identified by its exact PREFIX-vnet name, resource ID and
managedBy/vmBatch ownership tags. Its live default subnet is checked for active
connections, including references to resources in other resource groups. A
preview permits connections from the selected VM NICs because those will be
removed by the VM cleanup; the final check requires no remaining connections.
Skipping a VM interactively or selecting only part of a batch therefore leaves
the still-used network intact. If a shared network cannot be removed, the script
prints the reason. It does not remove unrelated services to unblock deletion.

The script can also remove an eligible empty owned network when the VMs have
already been deleted, such as after a partial deployment or cleanup failure.
Keep --subnet-id / AZVM_SUBNET_ID nonempty for an externally supplied subnet;
this disables automatic network cleanup. Deletion of the owned VNet removes
its default subnet as part of the same Azure operation.

OS disk deletion is permanent. There is no keep-disk option in this package.
Default deletion manages the original VM layout created by these scripts.
When additional data disks/NICs have been attached or deletion settings have
changed, deletion refuses the VM so the altered layout can be reviewed.

The script checks exact names, these ownership tags, and live relationships:
  managedBy=azure-vm-batch-v1
  vmBatch=<name-prefix>
  vmName=<full-VM-name>

Default deletion refuses untagged/unowned resources, disks moved to other VMs, changed NIC
attachments, public IPs attached elsewhere, or NSGs shared with other NICs or
subnets. Tags are an accident-prevention convention, not an authorization
boundary. Do not run competing create/delete/reconfiguration jobs for the
same resource set while one of these scripts is running.

5A. MANUALLY PROVISIONED VMS: EXPLICIT ATTACHMENT DELETION

The error "VM has missing or different ownership tags" is expected when a VM
was created outside this package. Manual VMs can also have automatically named
disks/NICs and different Delete/Detach settings. Adding the package's tags alone
does not make those resources match the default cleanup layout.

Use --include-unmanaged to authorize deletion of the exact selected VM names
and their verified dedicated attachments, without requiring or changing tags.
Always preview the subscription, VM range and full resource IDs first. For
example, these two alternative commands select ONLY hamid-vm-r01:

  ./delete-azure-vms.sh --env-file ./azure-vms.env \
    --subscription "$SUB" --resource-group "$RG" \
    --name-prefix hamid-vm-r --start 1 --count 1 \
    --include-unmanaged --dry-run

  ./delete-azure-vms.sh --env-file ./azure-vms.env \
    --subscription "$SUB" --resource-group "$RG" \
    --name-prefix hamid-vm-r --start 1 --count 1 \
    --include-unmanaged --interactive

The entire selection is read and previewed before confirmation or deletion.
--interactive asks per VM; without it deletion asks once for the batch.
--yes is available for unattended use. Empty/closed input declines deletion.
--dry-run changes neither Azure resources nor SSH config nor recovery files.

This mode supports one managed OS disk, exactly one NIC with one IP
configuration, an optional public IP and an optional NIC-associated NSG. The
VM must have NO data disks. The VM/OS disk/NIC/public IP must be in the selected
subscription and resource group. Multiple NICs/IP configurations, VM scale set
membership, private endpoint/load-balancer/application-gateway NIC attachments,
unmanaged/ephemeral OS disks and cross-scope cleanup targets cause refusal
before cloud changes. This deliberately covers the simple manual VM layout;
it is not a general resource-group cleanup command.

Deletion uses live attachment IDs, including automatically generated names.
The plan includes the VM, its managed OS disk (contents permanently deleted),
its NIC and its public IP, if any. The helper accepts Delete, Detach or omitted
delete options and explicitly removes reviewed dependencies that remain after
Azure deletes the VM. No tags or deletion options are rewritten.

A NIC-associated NSG is included only when it is in the selected resource
group, dedicated to the selected NIC and has no subnet or flow-log references.
External/shared NSGs are retained; an NSG that becomes shared after the plan is
also retained. All VNets/subnets, the resource group, user-assigned identities,
storage accounts, snapshots and unrelated resources are preserved. The VM's
own extensions and system-assigned identity follow Azure's VM lifecycle.
--keep-network is unnecessary here because this mode always retains networks.

The current attachment delete options matter: Azure can delete resources
automatically with the VM. Therefore, the helper refuses attached data disks
and cross-scope disk/NIC/public-IP resources before deleting anything, even
if it could otherwise leave them out of its explicit cleanup list.

Recovery after partial failure

After confirmation and before the first cloud deletion, the helper saves the
reviewed exact IDs and immutable resource-instance identifiers to a private
JSON journal. If Azure omits a required instance identifier (VM vmId, disk
uniqueId, or NIC/public-IP/eligible-NSG resourceGuid), the helper refuses before
deletion rather than record an attachment it could not verify on retry.
Its default directory is:

  $XDG_STATE_HOME/azure-vm-scripts/delete-attached
  or, when XDG_STATE_HOME is unset/empty:
  $HOME/.local/state/azure-vm-scripts/delete-attached

Use --delete-state-dir PATH to select another location. Relative paths are
resolved against the selected env file's directory. The journal contains no
credentials or key contents. Keep it until dependency cleanup completes.

On a failed deletion, inspect the error and rerun the SAME scoped command,
including --include-unmanaged, the same state directory and the same SSH-config
setting. You may preview
the retry with --dry-run first. The helper uses its recorded attachment IDs
even if the VM has already gone. It rechecks resource-instance identifiers and
live relationships, and refuses replacements/reassigned resources. It never
searches for untagged orphan resources by name prefix. Without a journal, an
already absent manual VM has no discoverable attachment list and no unrelated
resource is inferred to belong to it. A successful cleanup removes its journal.

When AZVM_UPDATE_SSH_CONFIG=true, the matching package-managed SSH entry is
removed only after the VM is confirmed absent. Declined/failed VM deletions
retain that entry. Manual SSH entries and entries for other scopes are kept.

6. FAILURES AND RERUNS

Creation stops at the first failure. Previously completed VMs and partial
resources remain, and the script exits nonzero. There is no automatic rollback.
Use deletion --dry-run on the same numeric range to see eligible cleanup.

An OS disk is created by Azure as part of VM provisioning, then tagged only
after the script verifies an owned VM points to the exact expected disk ID.
If provisioning fails before that relationship exists, an untagged disk can
remain. The script reports the issue; the deletion script will refuse that
untagged disk. Inspect it manually rather than blindly adding ownership tags.

Deletion also stops on a failed Azure operation. Earlier completed deletions
stay completed. Rerunning safely skips missing resources and can finish owned
orphan cleanup, provided ownership and attachment checks still pass. Azure
inventory/attachment propagation can lag after an operation; if the script
reports a pending/missing relationship, wait for Azure to settle and preview
again. Do not assume an access/network error means a resource is absent.

Both dry runs make authenticated READ calls. They validate arguments, naming,
existing resources and relevant relationships. Creation also resolves the
exact requested VM size, reads its family and full quota vCPU count, and checks
both that family and total regional vCPU usage for the entire new batch:

  additional vCPUs = AZVM_COUNT * quota vCPUs per VM
  required total   = current usage + additional vCPUs

The requested size and resolved quota family are printed with usage, current
limit and required total. Each quota must cover the complete batch before any
resource is created, including a resource group or network. The starting index
only controls names; --start 4 --count 1 checks one new VM. For constrained CPU
sizes, the full vCPUs capability counts toward quota, not vCPUsAvailable.

Quota families are read from SKU metadata, not inferred from spelling.
Standard_E96ads_v6 (AMD) and Standard_E96ds_v6 (Intel) use different families.
If matching an existing VM, copy its exact hardwareProfile.vmSize and check the
resolved family. CLI --size overrides AZVM_SIZE from the explicitly selected file.

The quota read is a snapshot; another deployment can consume quota afterward.
This preflight does not reserve capacity or validate every resource quota,
policy, image compatibility rule or SKU subscription restriction. --all is used
for SKU metadata, including restricted SKUs; an adequate vCPU quota is not a
guarantee that a particular VM will be deployable. Azure still validates the
actual creation request. Check subscription availability and quotas with:

  az vm list-skus --subscription "$SUB" --location "$REGION" \
    --resource-type virtualMachines --size Standard_D2s_v5 --output table
  az vm list-usage --subscription "$SUB" --location "$REGION" --output table

7. VERIFICATION AND REFERENCES

Package v9 adds explicit --include-unmanaged deletion using actual attachment
IDs, a reviewed cleanup plan and recovery journals for partial failures. The
default ownership-checked mode remains available. The attachment helper is
tested offline against manual VM layouts, Delete/Detach cascades, shared NSGs,
read errors, scope changes and partial-failure recovery. See section 5A for its
supported topology and retained resources. No live Azure deletion was performed.

Package v8 introduced required --env-file and optional local SSH config management.
The parser reads data rather than executing shell code, ignores inherited AZVM_*
values, and resolves relative file paths against the selected file's directory.
SSH entries are individually scoped and written after each successful VM; local
cleanup is tied to confirmed VM absence and the deletion plan's confirmations.
Keep all package files together so the relevant helpers remain available.

Quota value parsing fix (package v7): az vm list-usage can return currentValue
and limit as strings, even though the underlying Compute API uses integers.
The Azure CLI 2.91.0 VMListUsage output formatter explicitly calls str() on
both fields. Earlier package checks incorrectly rejected this normal output.
Creation now accepts JSON numbers or strings containing only decimal digits,
converts strings to numbers, and retains the existing non-negative, integral
value checks and implementation bound of 2147483647. Missing, duplicated and
invalid quota records produce distinct errors; invalid errors include the two
raw values. Unknown values are never assumed to be zero or sufficient quota.
Both family and regional quota checks remain enabled for the complete batch.

The v7 regression reproduces the old failure using correctly named records
with sufficient string-valued quotas, then verifies the same input succeeds
after normalization. The normal simulated CLI response now includes this
actual CLI formatting. Numeric, mixed-type and insufficient quotas, malformed
values, and distinct missing/duplicate/invalid diagnostics are also covered.

Request diagnostics (introduced in package v6): creation prints an
"Effective config" line with its package version before Azure reads and
collision checks, including the script path,
subscription selector, resource group, VM size, region, start and count after
CLI overrides. Before each actual az vm create call, "VM create request" shows
the canonical subscription ID and the resource group, name, location and size
read directly from the finalized argument array. Both lines go to stderr;
capture stdout and stderr together if saving a run log. Bash %q formatting
makes whitespace and control characters visible. SSH key contents are omitted.

These diagnostics expose the effective request when an environment file and
an Azure error appear to disagree. They do not change the deployment settings
or resolve an unexplained service-side family mismatch. A dry run prints the
effective configuration and quota information, but no "VM create request"
because it never reaches an actual VM create call. If a VM has since been
created manually, select only the remaining unused names; existing VMs are
rejected by collision checks and must not be retagged just to bypass ownership
checks. For example, after 04 exists, --start 5 --count 2 selects only 05 and 06.

Focused logging checks compare the request logs with a simulated CLI's actual
argv and verify explicit CLI size overrides and early collision diagnostics.
V8 verifies overrides against the selected settings file.

Compatibility fix (package v5): creation and dry-run now check the entire
requested batch against both VM-family and total regional vCPU quotas before
any writes. The exact SKU and Azure-provided family are shown explicitly to
expose size/family mismatches. Invalid or unavailable quota metadata stops the
preflight instead of silently bypassing it.

Deletion now reads the subscription ID as JSON and emits the parsed ID without
a newline. Native Windows az can emit CRLF when invoked from Bash/WSL; a raw
TSV capture could retain CR and corrupt the next --subscription argument.
Single-value jq captures used for IDs, counts and absence checks also omit
their terminating newline. This covers the critical cleanup path when a VM
does not exist but its tagged NIC, public IP and NSG remain after failure.

Compatibility fix (package v4): both scripts now request JSON for az group
exists and parse exactly one Boolean value. They normalize true and false
without a trailing newline. This avoids matching raw TSV output, which is
sensitive to surrounding whitespace or retained carriage returns. Invalid or
empty responses and failed commands still stop before resource changes; a
failed read is never interpreted as a missing resource group.

Compatibility fix (package v3): the original region check passed --subscription
to az account list-locations, which rejects that argument. It now uses the
subscription-scoped az rest request described above. That error occurs during
read-only validation, before any Azure resources are created. Keep your existing
configured azure-vms.env when replacing the scripts with this package.

The scripts were syntax-checked and exercised with a stateful simulated Azure
CLI covering creation/deletion, confirmation modes, scope, ownership, partial
failure and changed-resource cases. The updated checks also cover shared-network
cleanup, retained external/active networks, and the fixed suffix range 01..20.
The locations regression checks reject the original unsupported argument and
exercise subscription scoping, pagination, failed reads and malformed responses.
The group-existence checks cover true/false, CRLF and surrounding whitespace,
empty or malformed output, incorrect JSON types, multiple JSON values and
commands that fail despite printing a Boolean. The exact raw output behind
the user's reported group-existence error was not available; tests demonstrate
the old text comparison's sensitivity and verify the typed JSON replacement.
The quota regressions cover distinct AMD/Intel family keys, both quota tiers,
whole-batch arithmetic, exact-size matching, constrained-vCPU accounting and
failed or malformed reads. A deletion regression covers a subscription JSON
response with CRLF. No request to increase quota or delete resources was made.
No live Azure deployment was performed.
Actual Azure service responses, subscription capacity and policies still need
to be validated in your subscription.

Additional references checked November 2, 2026:
  OpenSSH client configuration, Host/IdentityFile/Include and first-value rules
  https://man.openbsd.org/ssh_config
  Bash variables, exports and command execution environments
  https://www.gnu.org/software/bash/manual/html_node/Environment.html
  https://www.gnu.org/software/bash/manual/html_node/Bourne-Shell-Builtins.html

  Azure CLI 2.91.0 quota output conversion (VMListUsage._output)
  https://github.com/Azure/azure-cli/blob/azure-cli-2.91.0/src/azure-cli/azure/cli/command_modules/vm/operations/vm.py#L255-L265

  Compute SKU metadata and regional usage commands
  https://learn.microsoft.com/en-us/cli/azure/vm?view=azure-cli-latest#az-vm-list-skus
  https://learn.microsoft.com/en-us/cli/azure/vm?view=azure-cli-latest#az-vm-list-usage

  Eadsv6 (AMD) and Edsv6 (Intel) size series
  https://learn.microsoft.com/en-us/azure/virtual-machines/sizes/memory-optimized/eadsv6-series
  https://learn.microsoft.com/en-us/azure/virtual-machines/sizes/memory-optimized/edsv6-series

  Resource-group existence command and JSON output support
  https://learn.microsoft.com/en-us/cli/azure/group?view=azure-cli-latest#az-group-exists

  jq: Boolean parsing, --slurp, --join-output and exit status
  https://jqlang.org/manual/

Microsoft references checked October 31, 2026:
  Account commands and their supported subscription arguments
  https://learn.microsoft.com/en-us/cli/azure/account?view=azure-cli-latest

  az rest, subscription selection and relative Resource Manager URLs
  https://learn.microsoft.com/en-us/cli/azure/reference-index?view=azure-cli-latest#az-rest

  Subscription locations REST API
  https://learn.microsoft.com/en-us/rest/api/resources/subscriptions/list-locations?view=rest-resources-2022-12-01

  Azure VM CLI and creation options
  https://learn.microsoft.com/en-us/cli/azure/vm?view=azure-cli-latest

  VM, disk, NIC and public IP deletion behavior
  https://learn.microsoft.com/en-us/azure/virtual-machines/delete

  SSH public key support
  https://learn.microsoft.com/en-us/azure/virtual-machines/linux/create-ssh-keys-detailed

  NIC CLI and existing subnet IDs
  https://learn.microsoft.com/en-us/cli/azure/network/nic?view=azure-cli-latest

  NSG rule options
  https://learn.microsoft.com/en-us/cli/azure/network/nsg/rule?view=azure-cli-latest

  Explicit outbound access and private subnet defaults
  https://learn.microsoft.com/en-us/azure/virtual-network/ip-services/default-outbound-access

  Resource tagging behavior
  https://learn.microsoft.com/en-us/azure/azure-resource-manager/management/tag-resources

  Regional and VM-family vCPU quotas
  https://learn.microsoft.com/en-us/azure/virtual-machines/quotas

  Trusted Launch support
  https://learn.microsoft.com/en-us/azure/virtual-machines/trusted-launch

  Dsv5 specifications and accelerated networking requirement
  https://learn.microsoft.com/en-us/azure/virtual-machines/sizes/general-purpose/dsv5-series

  VNet deletion prerequisites
  https://learn.microsoft.com/en-us/azure/virtual-network/manage-virtual-network

  Subnet dependency diagnostics
  https://learn.microsoft.com/en-us/troubleshoot/azure/virtual-network/virtual-network-troubleshoot-cannot-delete-modify-subnet
