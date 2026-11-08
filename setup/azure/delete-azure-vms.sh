#!/usr/bin/env bash
# Delete exact selected VM names and verified dedicated resources. Default
# cleanup requires package ownership; --include-unmanaged resolves attachments.
# Remove only eligible owned shared networks; always retain resource groups.
if [[ ${BASH_SOURCE[0]} != "$0" ]]; then
  printf 'Run this script as a command; do not source it. Use --env-file PATH.\n' >&2
  return 1
fi
set -Eeuo pipefail
umask 077

SCRIPT_DIR=$(CDPATH= cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd -P)
[[ -r $SCRIPT_DIR/azure-vm-common.sh ]] || { printf 'ERROR: Missing azure-vm-common.sh; keep all package files together.\n' >&2; exit 1; }
source "$SCRIPT_DIR/azure-vm-common.sh"
azvm_load_env_file "$@" || exit 1
set -- ${AZVM_SCRIPT_ARGS[@]+"${AZVM_SCRIPT_ARGS[@]}"}

usage() {
  cat <<'EOF'
Usage:
  ./delete-azure-vms.sh --env-file PATH --subscription SUB --resource-group RG \
    --name-prefix hamid-vm-r --count 3 [options]

Required file:
  --env-file PATH         Read settings from this file; never source it.

Required settings (supply flags or matching AZVM_* entries in the file):
  --subscription VALUE    Subscription ID or name; never changes az account set.
  --resource-group VALUE  Resource group containing the VMs.
  --name-prefix VALUE     Prefix used for creation (e.g. hamid-vm-r).
  --count N               Number of consecutive VM names to select (1..20).

Options:
  --start N               First index (1..20; default: 1); final index <=20.
  --digits N              Must be 2; retained for compatibility.
  --subnet-id ID          Existing subnet used at creation; preserves its network.
  --control-vm NAME       Controller used at creation; always preserves its network.
  --control-resource-group NAME
                         Controller's group; does not require controller lookup.
  --update-ssh-config BOOL Remove matching managed SSH entries (default: false).
  --ssh-config-mode MODE  split (default) or single (legacy); same as creation.
  --ssh-config-file PATH  Main Include file in split mode (never edited on deletion);
                         legacy output in single mode. Default: $HOME/.ssh/config.
  --ssh-public-config-file PATH
                         Default: sshconfig-pg-benchmark-public.conf in the
                         command's current working directory.
  --ssh-private-config-file PATH
                         Default: sshconfig-pg-benchmark-private.conf in the
                         command's current working directory.
  --hosts-file PATH       Remove confirmed absent/deleted VM aliases from this
                         plain hosts file, preserving unrelated hosts.
                         Relative paths use the current directory.
  --include-unmanaged     Explicitly inspect/delete the selected basic VMs even
                         without package tags, using their actual attachments.
                         Supports one managed OS disk, one NIC and no data disks.
                         Always retains VNets, subnets, identities and shared NSGs.
  --delete-state-dir PATH Recovery journal directory for --include-unmanaged.
                         Default: $XDG_STATE_HOME/azure-vm-scripts/delete-attached
                         or $HOME/.local/state/azure-vm-scripts/delete-attached.
  --keep-network          Preserve the package-owned shared network for reuse.
  --interactive          Ask separately for each VM and its dedicated resources.
  --yes                  Delete without prompting; intended for automation.
  --dry-run              Preview without changing Azure or local SSH config.
  -h, --help             Show this help.

Environment-file example (edit the file first):
  ./delete-azure-vms.sh --env-file ./azure-vms.env --dry-run
  ./delete-azure-vms.sh --env-file ./azure-vms.env --interactive

Reads AZVM_SUBSCRIPTION, AZVM_RESOURCE_GROUP, AZVM_NAME_PREFIX, AZVM_COUNT,
AZVM_START, AZVM_DIGITS, AZVM_SUBNET_ID, AZVM_CONTROL_VM, AZVM_CONTROL_RESOURCE_GROUP,
AZVM_UPDATE_SSH_CONFIG, AZVM_SSH_CONFIG_MODE, AZVM_SSH_CONFIG_FILE,
AZVM_SSH_PUBLIC_CONFIG_FILE and AZVM_SSH_PRIVATE_CONFIG_FILE from the selected file.
CLI arguments override the file;
inherited AZVM_* variables are ignored. Relative paths use the file's directory.
Default split SSH files use the command's working directory instead. Run from
the creation directory or provide the same explicit public/private config paths.
Confirmation, keep-network and dry-run switches are explicit CLI actions.
VM numeric suffixes are always exactly two digits, from 01 through 20.

Default: display the entire plan, then ask once before deleting the batch.
--interactive and --yes are mutually exclusive. An empty answer means no.
Prompts read standard input; with closed stdin nothing is deleted.

Cleanup includes the VM, OS disk, NIC, public IP and dedicated NSG. Tagged
leftovers from a partial deployment are also eligible. The owned PREFIX-vnet
and its default subnet are deleted once unused, with no additional subnets or
detected dependencies. Other users, extra configuration, external subnets, and
--keep-network preserve the shared network. The resource group is retained.
Interactive mode asks separately before deleting the unused shared network.
In default mode, additional data disks, NICs, changed deletion settings, unowned
resources or unexpected VM attachments cause refusal.
With --include-unmanaged, discovered IDs are previewed instead of assuming the
package naming convention. No ownership tags are added or changed. A recovery
journal is saved after confirmation and before deleting each VM, so the same
command can resume exact dependency cleanup after a partial failure. Dry runs
create no journal. Missing VMs without a journal do not authorize orphan scans.
Managed SSH entries are removed only after a confirmed deletion/absence of the
matching VM. Skipped or failed VM deletions retain their entries. Matching stale
entries can also be removed when the VM/resource group is already absent, with
the same dry-run and confirmation behavior. Manual SSH entries are preserved.

Examples:
  ./delete-azure-vms.sh --env-file ./azure-vms.env --subscription "$SUB" --resource-group hamid-rg \
    --name-prefix hamid-vm-r --count 4 --dry-run
  ./delete-azure-vms.sh --env-file ./azure-vms.env --subscription "$SUB" --resource-group hamid-rg \
    --name-prefix hamid-vm-r --count 4 --interactive
  ./delete-azure-vms.sh --env-file ./azure-vms.env --subscription "$SUB" --resource-group hamid-rg \
    --name-prefix hamid-vm-r --start 3 --count 2 --yes
EOF
}

die() { printf 'ERROR: %s\n' "$*" >&2; exit 1; }
need_value() {
  [[ $# -ge 2 && -n $2 && $2 != --* ]] || die "Missing value for $1."
}
number() {
  [[ $2 =~ ^[0-9]{1,10}$ ]] || die "$1 must be a decimal integer (1-10 digits)."
  local n=$((10#$2))
  (( n >= $3 && n <= $4 )) || die "$1 must be between $3 and $4."
  printf '%s' "$n"
}

SUBSCRIPTION=${AZVM_SUBSCRIPTION-}
RESOURCE_GROUP=${AZVM_RESOURCE_GROUP-}
PREFIX=${AZVM_NAME_PREFIX-}
COUNT=${AZVM_COUNT-}
START=${AZVM_START-1}
DIGITS=${AZVM_DIGITS-2}
SUBNET_ID=${AZVM_SUBNET_ID-}
CONTROL_VM=${AZVM_CONTROL_VM-}
CONTROL_RESOURCE_GROUP=${AZVM_CONTROL_RESOURCE_GROUP-}
UPDATE_SSH_CONFIG=${AZVM_UPDATE_SSH_CONFIG-false}
UPDATE_SSH_CONFIG_SET=${AZVM_UPDATE_SSH_CONFIG+set}
SSH_CONFIG_FILE=${AZVM_SSH_CONFIG_FILE-$HOME/.ssh/config}
SSH_CONFIG_MODE=${AZVM_SSH_CONFIG_MODE-split}
WORKING_DIRECTORY=${AZVM_WORK_DIR-$PWD}
SSH_PUBLIC_CONFIG_FILE=${AZVM_SSH_PUBLIC_CONFIG_FILE-$WORKING_DIRECTORY/sshconfig-pg-benchmark-public.conf}
SSH_PRIVATE_CONFIG_FILE=${AZVM_SSH_PRIVATE_CONFIG_FILE-$WORKING_DIRECTORY/sshconfig-pg-benchmark-private.conf}
INTERACTIVE=false; YES=false; DRY_RUN=false; KEEP_NETWORK=false
HOSTS_FILE=''
INCLUDE_UNMANAGED=false; DELETE_STATE_DIR_SET=false
DELETE_STATE_DIR=${XDG_STATE_HOME:-$HOME/.local/state}/azure-vm-scripts/delete-attached
while (( $# )); do
  case $1 in
    --subscription) need_value "$@"; SUBSCRIPTION=$2; shift 2 ;;
    --resource-group) need_value "$@"; RESOURCE_GROUP=$2; shift 2 ;;
    --name-prefix) need_value "$@"; PREFIX=$2; shift 2 ;;
    --count) need_value "$@"; COUNT=$2; shift 2 ;;
    --start) need_value "$@"; START=$2; shift 2 ;;
    --digits) need_value "$@"; DIGITS=$2; shift 2 ;;
    --subnet-id) need_value "$@"; SUBNET_ID=$2; shift 2 ;;
    --control-vm) need_value "$@"; CONTROL_VM=$2; shift 2 ;;
    --control-resource-group) need_value "$@"; CONTROL_RESOURCE_GROUP=$2; shift 2 ;;
    --update-ssh-config) need_value "$@"; UPDATE_SSH_CONFIG=$2; UPDATE_SSH_CONFIG_SET=set; shift 2 ;;
    --ssh-config-file) need_value "$@"; SSH_CONFIG_FILE=$2; shift 2 ;;
    --ssh-config-mode) need_value "$@"; SSH_CONFIG_MODE=$2; shift 2 ;;
    --ssh-public-config-file) need_value "$@"; SSH_PUBLIC_CONFIG_FILE=$2; shift 2 ;;
    --ssh-private-config-file) need_value "$@"; SSH_PRIVATE_CONFIG_FILE=$2; shift 2 ;;
    --hosts-file) need_value "$@"; HOSTS_FILE=$2; shift 2 ;;
    --include-unmanaged) INCLUDE_UNMANAGED=true; shift ;;
    --delete-state-dir) need_value "$@"; DELETE_STATE_DIR=$2; DELETE_STATE_DIR_SET=true; shift 2 ;;
    --keep-network) KEEP_NETWORK=true; shift ;;
    --interactive) INTERACTIVE=true; shift ;;
    --yes) YES=true; shift ;;
    --dry-run) DRY_RUN=true; shift ;;
    -h|--help) usage; exit 0 ;;
    *) die "Unknown argument: $1 (see --help)." ;;
  esac
done

azvm_hosts_action check || die 'Runner hosts preflight failed; no Azure resources were changed.'

[[ ${BASH_VERSINFO[0]} -ge 4 ]] || die 'Bash 4 or newer is required.'
for dependency in az jq; do
  command -v "$dependency" >/dev/null 2>&1 || die "Required command not found: $dependency"
done
[[ -n $SUBSCRIPTION && -n $RESOURCE_GROUP && -n $PREFIX && -n $COUNT ]] ||
  die '--subscription, --resource-group, --name-prefix and --count (or their matching entries in --env-file) are required.'
[[ $PREFIX =~ ^[a-z][a-z0-9-]*$ && ${#PREFIX} -le 48 ]] ||
  die '--name-prefix must start with a lowercase letter and contain only lowercase letters, digits or hyphens (max 48 characters).'
COUNT=$(number --count "$COUNT" 1 20)
START=$(number --start "$START" 1 20)
DIGITS=$(number --digits "$DIGITS" 2 2)
END=$((START + COUNT - 1))
(( END <= 20 )) || die 'The final VM index (start + count - 1) must not exceed 20; suffixes are limited to 01..20.'
[[ $YES == false || $INTERACTIVE == false ]] || die '--yes and --interactive cannot be combined.'
[[ $DELETE_STATE_DIR_SET == false || $INCLUDE_UNMANAGED == true ]] || die '--delete-state-dir is only used with --include-unmanaged.'
[[ -n $CONTROL_VM || -z $CONTROL_RESOURCE_GROUP ]] || die '--control-resource-group requires --control-vm.'
if [[ -n $CONTROL_VM ]]; then
  [[ $CONTROL_VM =~ ^[a-zA-Z0-9][a-zA-Z0-9_.-]{0,63}$ ]] || die 'Invalid --control-vm name.'
  CONTROL_RESOURCE_GROUP=${CONTROL_RESOURCE_GROUP:-$RESOURCE_GROUP}
  [[ $CONTROL_RESOURCE_GROUP =~ ^[a-zA-Z0-9._\(\)-]+$ && ${#CONTROL_RESOURCE_GROUP} -le 90 && $CONTROL_RESOURCE_GROUP != *. ]] ||
    die 'Invalid --control-resource-group name.'
fi
azvm_ssh_config_settings || die 'Invalid local SSH configuration settings.'
if [[ $UPDATE_SSH_CONFIG == true ]]; then
  [[ -r $SCRIPT_DIR/azure-vm-ssh-config.py ]] || die 'Missing azure-vm-ssh-config.py; keep all package files together.'
  [[ -n $SSH_CONFIG_FILE ]] || die 'AZVM_SSH_CONFIG_FILE / --ssh-config-file must not be empty.'
fi

SUBSCRIPTION_JSON=$(az account show --subscription "$SUBSCRIPTION" --query id \
  --output json --only-show-errors) || die 'Cannot resolve subscription. Run az login and check access.'
SUBSCRIPTION_ID=$(jq -erjs 'select(length == 1) | .[0] |
  select(type == "string" and length > 0)' <<< "$SUBSCRIPTION_JSON") ||
  die 'Azure returned no valid subscription ID.'
azr() { az "$@" --subscription "$SUBSCRIPTION_ID" --only-show-errors; }
if [[ $INCLUDE_UNMANAGED == true ]]; then
  [[ -r $SCRIPT_DIR/azure-vm-delete-attached.py ]] || die 'Missing azure-vm-delete-attached.py; keep all package files together.'
  DELETE_STATE_DIR=$(azvm_resolve_path "$DELETE_STATE_DIR") || die 'Cannot resolve the deletion journal directory.'
  ATTACHED_ARGS=(--subscription "$SUBSCRIPTION_ID" --resource-group "$RESOURCE_GROUP" --state-dir "$DELETE_STATE_DIR")
  for ((i=START; i<=END; i++)); do
    printf -v name '%s%02d' "$PREFIX" "$i"
    ATTACHED_ARGS+=(--vm-name "$name")
  done
  if [[ $UPDATE_SSH_CONFIG == true ]]; then
    if [[ $SSH_CONFIG_MODE == split ]]; then
      ATTACHED_ARGS+=(--ssh-config "$SSH_PUBLIC_CONFIG_FILE" --ssh-config "$SSH_PRIVATE_CONFIG_FILE")
    else
      ATTACHED_ARGS+=(--ssh-config "$SSH_CONFIG_FILE")
    fi
  fi
  if [[ -n $HOSTS_FILE ]]; then ATTACHED_ARGS+=(--hosts-file "$HOSTS_FILE"); fi
  if [[ $DRY_RUN == true ]]; then ATTACHED_ARGS+=(--dry-run); fi
  if [[ $INTERACTIVE == true ]]; then ATTACHED_ARGS+=(--interactive); fi
  if [[ $YES == true ]]; then ATTACHED_ARGS+=(--yes); fi
  printf 'Settings file: %s\n' "$AZVM_ENV_FILE"
  if python3 "$SCRIPT_DIR/azure-vm-delete-attached.py" "${ATTACHED_ARGS[@]}"; then exit 0; else exit $?; fi
fi
SSH_CONFIG_NAMES_JSON='[]'
if [[ $UPDATE_SSH_CONFIG == true ]]; then
  azvm_ssh_scope_args
  SSH_CHECK_ARGS=("${SSH_SCOPE_ARGS[@]}")
  for ((i=START; i<=END; i++)); do
    printf -v name '%s%02d' "$PREFIX" "$i"
    SSH_CHECK_ARGS+=(--vm-name "$name")
  done
  SSH_STATE=$(python3 "$SCRIPT_DIR/azure-vm-ssh-config.py" "$SSH_CHECK_REMOVE_ACTION" "${SSH_CHECK_ARGS[@]}") ||
    die 'Local SSH config preflight failed; no resources were changed.'
  SSH_CONFIG_NAMES_JSON=$(jq -ecj '.managed_vm_names | select(type == "array" and all(.[]; type == "string"))' <<< "$SSH_STATE") ||
    die 'Invalid response from the local SSH config helper.'
fi

# Parse one JSON boolean without a trailing newline; avoid raw TSV comparisons.
read_rg_exists() {
  RG_EXISTS=$(azr group exists --name "$RESOURCE_GROUP" --output json) ||
    die 'Cannot check the resource group; no changes made.'
  RG_EXISTS=$(jq -erjs 'select(length == 1 and (.[0] | type == "boolean")) | .[0] | tostring' <<< "$RG_EXISTS") ||
    die 'Unexpected response from az group exists; expected a JSON boolean.'
}
read_rg_exists
case $RG_EXISTS in
  false)
    if [[ $SSH_CONFIG_NAMES_JSON == '[]' && -z $HOSTS_FILE ]]; then
      printf 'Resource group %s does not exist in %s. Nothing to delete.\n' "$RESOURCE_GROUP" "$SUBSCRIPTION_ID"
      exit 0
    fi
    printf 'Resource group %s is absent; checking its selected stale SSH/hosts entries.\n' "$RESOURCE_GROUP"
    ;;
  true) ;;
  *) die 'Unexpected response from az group exists.' ;;
esac

MARKER=azure-vm-batch-v1
BASE_ID="/subscriptions/$SUBSCRIPTION_ID/resourceGroups/$RESOURCE_GROUP/providers"
VNET_NAME="$PREFIX-vnet"
VNET_ID="$BASE_ID/Microsoft.Network/virtualNetworks/$VNET_NAME"
load_inventory() {
  # Recheck a previously absent group before removing local stale entries.
  if [[ $RG_EXISTS == false ]]; then read_rg_exists; fi
  if [[ $RG_EXISTS == false ]]; then INVENTORY='[]'; return 0; fi
  INVENTORY=$(azr resource list --resource-group "$RESOURCE_GROUP" --output json) ||
    die 'Cannot list resources; refusing to interpret a failed read as an empty resource group.'
  jq -e 'type == "array"' >/dev/null <<<"$INVENTORY" || die 'Invalid resource-list response.'
}
find_resource() {
  jq -cj --arg kind "$1" --arg name "$2" \
    '[.[] | select((.type | ascii_downcase) == ($kind | ascii_downcase) and
      (.name | ascii_downcase) == ($name | ascii_downcase))] | first // null' <<<"$INVENTORY"
}
owned() {
  jq -e --arg marker "$MARKER" --arg batch "$PREFIX" --arg name "$VM_NAME" \
    '.tags.managedBy == $marker and .tags.vmBatch == $batch and .tags.vmName == $name' \
    >/dev/null <<<"$1" || die "Refusing $VM_NAME: $2 has missing or different ownership tags. For a manually provisioned VM, use --include-unmanaged to inspect its actual attachments."
}
read_resource() {
  # Azure read failures abort. Only a successful resource-list result establishes absence.
  azr "$@" --resource-group "$RESOURCE_GROUP" --output json
}

validate_one() {
  VM_NAME=$1
  VM_ID="$BASE_ID/Microsoft.Compute/virtualMachines/$VM_NAME"
  DISK_ID="$BASE_ID/Microsoft.Compute/disks/$VM_NAME-osdisk"
  NIC_ID="$BASE_ID/Microsoft.Network/networkInterfaces/$VM_NAME-nic"
  PIP_ID="$BASE_ID/Microsoft.Network/publicIPAddresses/$VM_NAME-pip"
  NSG_ID="$BASE_ID/Microsoft.Network/networkSecurityGroups/$VM_NAME-nsg"
  VM=$(find_resource Microsoft.Compute/virtualMachines "$VM_NAME")
  DISK=$(find_resource Microsoft.Compute/disks "$VM_NAME-osdisk")
  NIC=$(find_resource Microsoft.Network/networkInterfaces "$VM_NAME-nic")
  PIP=$(find_resource Microsoft.Network/publicIPAddresses "$VM_NAME-pip")
  NSG=$(find_resource Microsoft.Network/networkSecurityGroups "$VM_NAME-nsg")
  COMPONENTS=()

  if [[ $VM != null ]]; then
    VM=$(read_resource vm show --name "$VM_NAME") || die "Cannot read VM $VM_NAME."
    owned "$VM" VM
    jq -e --arg disk "$DISK_ID" --arg nic "$NIC_ID" '
      ((.storageProfile.dataDisks // []) | length) == 0 and
      ((.storageProfile.osDisk.managedDisk.id // "" | ascii_downcase) == ($disk | ascii_downcase)) and
      ((.storageProfile.osDisk.deleteOption // "" | ascii_downcase) == "delete") and
      ((.networkProfile.networkInterfaces // []) | length) == 1 and
      ((.networkProfile.networkInterfaces[0].id // "" | ascii_downcase) == ($nic | ascii_downcase)) and
      ((.networkProfile.networkInterfaces[0].deleteOption // "" | ascii_downcase) == "delete")
      ' >/dev/null <<<"$VM" ||
      die "Refusing $VM_NAME: disk/NIC attachments or deletion settings have changed. Inspect this VM manually."
    # The OS disk and NIC must be visible so their ownership can be verified.
    [[ $DISK != null && $NIC != null ]] ||
      die "Refusing $VM_NAME: attached OS disk/NIC is missing from inventory. Retry after provisioning completes."
    COMPONENTS+=(VM)
  fi

  if [[ $DISK != null ]]; then
    DISK=$(read_resource disk show --name "$VM_NAME-osdisk") || die "Cannot read OS disk for $VM_NAME."
    owned "$DISK" OS-disk
    jq -e --arg vm "$VM_ID" --arg present "$([[ $VM == null ]] && printf no || printf yes)" '
      ((.managedBy // "") == "" or
        ($present == "yes" and ((.managedBy | ascii_downcase) == ($vm | ascii_downcase)))) and
      ((.managedByExtended // []) | all(.[]; ($present == "yes") and ((. | ascii_downcase) == ($vm | ascii_downcase))))
      ' >/dev/null <<<"$DISK" || die "Refusing $VM_NAME: OS disk is attached to an unexpected VM."
    COMPONENTS+=(OS-disk)
  fi

  if [[ $NIC != null ]]; then
    NIC=$(read_resource network nic show --name "$VM_NAME-nic") || die "Cannot read NIC for $VM_NAME."
    owned "$NIC" NIC
    jq -e --arg vm "$VM_ID" --arg pip "$PIP_ID" --arg nsg "$NSG_ID" \
      --arg present "$([[ $VM == null ]] && printf no || printf yes)" '
      ((.virtualMachine.id // "") == "" or
        ($present == "yes" and ((.virtualMachine.id | ascii_downcase) == ($vm | ascii_downcase)))) and
      ((.privateEndpoint // null) == null) and
      ((.networkSecurityGroup.id // "") == "" or
        ((.networkSecurityGroup.id | ascii_downcase) == ($nsg | ascii_downcase))) and
      ((.ipConfigurations // []) | length) == 1 and
      ((.ipConfigurations // []) | all(.[];
        ((.publicIPAddress.id // "") == "" or ((.publicIPAddress.id | ascii_downcase) == ($pip | ascii_downcase))) and
        ((.loadBalancerBackendAddressPools // []) | length) == 0 and
        ((.loadBalancerInboundNatRules // []) | length) == 0 and
        ((.applicationGatewayBackendAddressPools // []) | length) == 0))
      ' >/dev/null <<<"$NIC" || die "Refusing $VM_NAME: NIC has unexpected attachments or IP configurations."
    if jq -e '.ipConfigurations[] | .publicIPAddress.id != null' >/dev/null <<<"$NIC"; then
      [[ $PIP != null ]] || die "Refusing $VM_NAME: attached public IP is missing from inventory."
    fi
    COMPONENTS+=(NIC)
  fi

  if [[ $PIP != null ]]; then
    PIP=$(read_resource network public-ip show --name "$VM_NAME-pip") || die "Cannot read public IP for $VM_NAME."
    owned "$PIP" public-IP
    jq -e --arg nic "$NIC_ID" --arg present "$([[ $NIC == null ]] && printf no || printf yes)" '
      ((.ipConfiguration.id // "") == "" or
        ($present == "yes" and ((.ipConfiguration.id | ascii_downcase) | startswith(($nic | ascii_downcase) + "/ipconfigurations/")))) and
      ((.natGateway // null) == null) and ((.linkedPublicIPAddress // null) == null)
      ' >/dev/null <<<"$PIP" || die "Refusing $VM_NAME: public IP is attached elsewhere."
    COMPONENTS+=(public-IP)
  fi

  if [[ $NSG != null ]]; then
    NSG=$(read_resource network nsg show --name "$VM_NAME-nsg") || die "Cannot read NSG for $VM_NAME."
    owned "$NSG" NSG
    jq -e --arg nic "$NIC_ID" --arg present "$([[ $NIC == null ]] && printf no || printf yes)" '
      ((.subnets // []) | length) == 0 and
      ((.networkInterfaces // []) | all(.[];
        ($present == "yes") and ((.id | ascii_downcase) == ($nic | ascii_downcase))))
      ' >/dev/null <<<"$NSG" || die "Refusing $VM_NAME: dedicated NSG is now shared with another NIC or subnet."
    COMPONENTS+=(NSG)
  fi
}

inspect_network() {
  # plan: selected NIC connections may disappear through the approved VM plan.
  # empty: every connection must be gone before deleting this shared resource.
  local mode=$1 candidate vnet subnet allowed_nics='[]'
  NETWORK_STATUS=retained
  NETWORK_REASON=''
  if [[ $KEEP_NETWORK == true ]]; then
    NETWORK_REASON='--keep-network was specified'
    return 0
  fi
  if [[ -n $CONTROL_VM ]]; then
    NETWORK_REASON='the controller network is external and must be preserved'
    return 0
  fi
  if [[ -n $SUBNET_ID ]]; then
    NETWORK_REASON='an existing subnet was supplied through --subnet-id / AZVM_SUBNET_ID'
    return 0
  fi
  candidate=$(find_resource Microsoft.Network/virtualNetworks "$VNET_NAME")
  if [[ $candidate == null ]]; then
    NETWORK_STATUS=absent
    NETWORK_REASON='not present'
    return 0
  fi
  if ! jq -e --arg marker "$MARKER" --arg batch "$PREFIX" \
      '.tags.managedBy == $marker and .tags.vmBatch == $batch' >/dev/null <<<"$candidate"; then
    NETWORK_REASON='does not have the package ownership tags'
    return 0
  fi
  vnet=$(read_resource network vnet show --name "$VNET_NAME") ||
    die "Cannot inspect shared network $VNET_NAME; no network deletion attempted."
  jq -e 'type == "object"' >/dev/null <<<"$vnet" || die 'Invalid VNet response.'
  if ! jq -e --arg marker "$MARKER" --arg batch "$PREFIX" --arg id "$VNET_ID" '
      .tags.managedBy == $marker and .tags.vmBatch == $batch and
      ((.id // "" | ascii_downcase) == ($id | ascii_downcase))
      ' >/dev/null <<<"$vnet"; then
    NETWORK_REASON='live ownership or resource ID differs from the expected network'
    return 0
  fi
  if ! jq -e '
      (.subnets | type) == "array" and (.subnets | length) <= 1 and
      (.subnets | all(.[]; .name == "default")) and
      ((.virtualNetworkPeerings // []) | length) == 0 and
      ((.defaultPublicNatGateway // null) == null) and
      ((.ddosProtectionPlan // null) == null) and
      ((.flowLogs // []) | length) == 0 and
      ((.ipAllocations // []) | length) == 0
      ' >/dev/null <<<"$vnet"; then
    NETWORK_REASON='extra/missing subnet topology, peerings, NAT, DDoS, flow logs or IP allocations'
    return 0
  fi
  if [[ $(jq -j '.subnets | length' <<<"$vnet") == 1 ]]; then
    # A subnet may be used by NICs in other resource groups or non-VM services.
    # Inspect its live references instead of relying only on this RG inventory.
    subnet=$(read_resource network vnet subnet show --vnet-name "$VNET_NAME" --name default) ||
      die "Cannot inspect the default subnet of $VNET_NAME."
    jq -e 'type == "object"' >/dev/null <<<"$subnet" || die 'Invalid subnet response.'
    if ! jq -e --arg id "$VNET_ID/subnets/default" '
        ((.id // "" | ascii_downcase) == ($id | ascii_downcase)) and
        ((.privateEndpoints // []) | length) == 0 and
        ((.ipConfigurationProfiles // []) | length) == 0 and
        ((.applicationGatewayIpConfigurations // []) | length) == 0 and
        ((.applicationGatewayIPConfigurations // []) | length) == 0 and
        ((.ipAllocations // []) | length) == 0 and
        ((.serviceAssociationLinks // []) | length) == 0 and
        ((.resourceNavigationLinks // []) | length) == 0 and
        ((.delegations // []) | length) == 0 and
        ((.natGateway // null) == null) and
        ((.networkSecurityGroup // null) == null) and
        ((.routeTable // null) == null) and
        ((.serviceEndpointPolicies // []) | length) == 0 and
        ((.serviceEndpoints // []) | length) == 0
        ' >/dev/null <<<"$subnet"; then
      NETWORK_REASON='the subnet has external service dependencies or added configuration'
      return 0
    fi
    if [[ $mode == plan ]]; then allowed_nics=$SELECTED_NIC_IDS_JSON; fi
    if ! jq -e --argjson nics "$allowed_nics" '
        ((.ipConfigurations // []) | all(.[];
          (.id // "") as $ip_id |
          any($nics[]; . as $nic_id |
            ($ip_id | ascii_downcase | startswith(($nic_id | ascii_downcase) + "/ipconfigurations/")))))
        ' >/dev/null <<<"$subnet"; then
      NETWORK_REASON='the subnet still has connections outside the selected cleanup'
      return 0
    fi
  fi
  NETWORK_STATUS=eligible
  NETWORK_REASON='owned network with no remaining dependencies after the selected cleanup'
}

confirm() {
  local reply=''
  printf '%s [y/N] ' "$1" >&2
  IFS= read -r reply || return 1
  [[ $reply == y || $reply == Y || $reply == yes || $reply == YES ]]
}

check_live_ssh_entry() {
  local name=$1 expected=$2 state present
  [[ $UPDATE_SSH_CONFIG == true ]] || return 0
  state=$(python3 "$SCRIPT_DIR/azure-vm-ssh-config.py" "$SSH_CHECK_REMOVE_ACTION" "${SSH_SCOPE_ARGS[@]}" --vm-name "$name") ||
    die "Cannot recheck the SSH config entry for $name; no deletion of this VM was attempted."
  present=$(jq -erj --arg name "$name" '(.managed_vm_names | index($name) != null) | tostring' <<< "$state") ||
    die 'Invalid response from the local SSH config helper.'
  [[ $present == "$expected" ]] || die "The managed SSH entry for $name changed after preview. Rerun to review the plan."
}

remove_ssh_entry() {
  local name=$1 result
  result=$(python3 "$SCRIPT_DIR/azure-vm-ssh-config.py" "$SSH_REMOVE_ACTION" "${SSH_SCOPE_ARGS[@]}" --vm-name "$name") ||
    die "VM $name is absent/deleted, but its local SSH entry could not be removed. Inspect the config and rerun deletion."
  if jq -e '.changed == true' >/dev/null <<< "$result"; then
    SSH_REMOVED=$((SSH_REMOVED + 1))
    printf 'Removed managed SSH entry: %s\n' "$name"
  fi
}

load_inventory
NAMES=(); SIGNATURES=(); SSH_SELECTED=(); SELECTED_NIC_IDS=(); ANY=false; NETWORK_PLANNED=false
printf 'Subscription: %s\nResource group: %s\n' "$SUBSCRIPTION_ID" "$RESOURCE_GROUP"
printf 'Settings file: %s\n' "$AZVM_ENV_FILE"
if [[ $UPDATE_SSH_CONFIG == true ]]; then
  if [[ $SSH_CONFIG_MODE == split ]]; then
    printf 'Managed SSH configs: public=%s; private=%s (main Includes retained)\n' "$SSH_PUBLIC_CONFIG_FILE" "$SSH_PRIVATE_CONFIG_FILE"
  else
    printf 'Managed SSH config: %s\n' "$SSH_CONFIG_FILE"
  fi
fi
printf 'Deletion plan (OS disk contents will be permanently deleted):\n'
for ((i=START; i<=END; i++)); do
  printf -v name '%s%02d' "$PREFIX" "$i"
  validate_one "$name"
  NAMES+=("$name")
  SIGNATURES+=("${COMPONENTS[*]}")
  HAS_SSH_ENTRY=$(jq -rj --arg name "$name" 'index($name) != null' <<< "$SSH_CONFIG_NAMES_JSON")
  SSH_SELECTED+=("$HAS_SSH_ENTRY")
  if [[ $NIC != null ]]; then SELECTED_NIC_IDS+=("$NIC_ID"); fi
  if (( ${#COMPONENTS[@]} )); then
    printf '  %-32s %s\n' "$name" "${COMPONENTS[*]}"
    ANY=true
  else
    printf '  %-32s %s\n' "$name" '(not present)'
  fi
  if [[ $HAS_SSH_ENTRY == true ]]; then
    printf '  %-32s %s\n' "$name" 'remove its managed local SSH entry after confirming VM absence'
    ANY=true
  fi
  if [[ -n $HOSTS_FILE ]]; then
    printf '  %-32s remove its runner hosts entry after confirming VM absence\n' "$name"
    ANY=true
  fi
done
SELECTED_NIC_IDS_JSON=$(jq -cn --args '$ARGS.positional' ${SELECTED_NIC_IDS[@]+"${SELECTED_NIC_IDS[@]}"})
inspect_network plan
if [[ $NETWORK_STATUS == eligible ]]; then
  NETWORK_PLANNED=true
  ANY=true
  printf '  Shared network %-17s delete with its default subnet once unused\n' "$VNET_NAME"
else
  printf 'Shared network retained/absent: %s (%s).\n' "$VNET_NAME" "$NETWORK_REASON"
fi
printf 'Retained: resource group and external or still-used shared resources.\n'
[[ $ANY == true ]] || { printf 'No eligible resources or managed SSH entries to delete.\n'; exit 0; }
if [[ $DRY_RUN == true ]]; then
  printf 'Dry run complete. No Azure resources or local SSH files were changed.\n'
  exit 0
fi
if [[ $YES == false && $INTERACTIVE == false ]]; then
  if ! confirm 'Delete the listed VM resources and managed SSH entries, and the eligible shared network once unused?'; then
    printf 'Cancelled. No Azure resources or local SSH files were changed.\n'; exit 0
  fi
fi

DELETED=0; SKIPPED=0; SSH_REMOVED=0
for idx in "${!NAMES[@]}"; do
  name=${NAMES[$idx]}
  [[ -n ${SIGNATURES[$idx]} || ${SSH_SELECTED[$idx]} == true || -n $HOSTS_FILE ]] || continue
  if [[ $INTERACTIVE == true ]]; then
    if ! confirm "Delete $name and its listed resources/managed SSH entry?"; then
      printf 'Skipped %s.\n' "$name"
      SKIPPED=$((SKIPPED + 1))
      continue
    fi
  fi
  # Check live relationships again after confirmation and before the first write.
  load_inventory
  validate_one "$name"
  [[ ${COMPONENTS[*]} == "${SIGNATURES[$idx]}" ]] ||
    die "The resource set for $name changed after preview. Rerun to review the current plan."
  check_live_ssh_entry "$name" "${SSH_SELECTED[$idx]}"
  if [[ -z ${SIGNATURES[$idx]} ]]; then
    # Inventory confirms absence; clean only explicitly selected local entries.
    azvm_hosts_action remove "$name" || die "Cannot remove absent VM $name from runner hosts."
    if [[ ${SSH_SELECTED[$idx]} == true ]]; then remove_ssh_entry "$name"; fi
    continue
  fi
  printf 'Deleting %s ...\n' "$name"
  if [[ $VM != null ]]; then
    azr vm delete --resource-group "$RESOURCE_GROUP" --name "$name" --yes --output none ||
      die "VM deletion failed for $name. Earlier deletions remain completed; inspect and rerun."
  fi

  # Delete waits synchronously. Refresh after the VM's automatic disk/NIC cleanup.
  load_inventory
  [[ $(find_resource Microsoft.Compute/virtualMachines "$name") == null ]] ||
    die "VM $name still appears in inventory; retry after Azure finishes deletion."
  azvm_hosts_action remove "$name" || die "VM $name was deleted, but its runner hosts entry could not be removed."
  if [[ ${SSH_SELECTED[$idx]} == true ]]; then remove_ssh_entry "$name"; fi
  validate_one "$name"
  [[ $VM == null ]] || die "VM $name still appears in inventory; retry after Azure finishes deletion."
  if [[ $NIC != null ]]; then
    azr network nic delete --resource-group "$RESOURCE_GROUP" --name "$name-nic" --output none ||
      die "NIC cleanup failed for $name; inspect and rerun."
  fi

  # Re-read after NIC deletion; Azure prevents removal while resources are in use.
  load_inventory
  validate_one "$name"
  [[ $VM == null && $NIC == null ]] || die "VM/NIC for $name reappeared or deletion is pending; inspect and rerun."
  if [[ $PIP != null ]]; then
    azr network public-ip delete --resource-group "$RESOURCE_GROUP" --name "$name-pip" --output none ||
      die "Public IP cleanup failed for $name; inspect and rerun."
  fi
  if [[ $NSG != null ]]; then
    azr network nsg delete --resource-group "$RESOURCE_GROUP" --name "$name-nsg" --output none ||
      die "NSG cleanup failed for $name; inspect and rerun."
  fi
  if [[ $DISK != null ]]; then
    azr disk delete --resource-group "$RESOURCE_GROUP" --name "$name-osdisk" --yes --output none ||
      die "OS disk cleanup failed for $name; inspect and rerun."
  fi
  DELETED=$((DELETED + 1))
  printf 'Deleted %s and its selected dedicated resources.\n' "$name"
done

NETWORK_DELETED=false
if [[ $NETWORK_PLANNED == true ]]; then
  load_inventory
  inspect_network empty
  if [[ $NETWORK_STATUS == eligible ]]; then
    NETWORK_APPROVED=true
    if [[ $INTERACTIVE == true ]]; then
      if ! confirm "Delete unused shared network $VNET_NAME and its default subnet?"; then
        NETWORK_APPROVED=false
        printf 'Retained shared network %s by request.\n' "$VNET_NAME"
      fi
    fi
    if [[ $NETWORK_APPROVED == true ]]; then
      # Recheck after a potentially long interactive pause. Azure also enforces
      # its own dependency checks when it processes the network deletion.
      load_inventory
      inspect_network empty
      if [[ $NETWORK_STATUS == eligible ]]; then
        azr network vnet delete --resource-group "$RESOURCE_GROUP" --name "$VNET_NAME" --output none ||
          die "Shared network cleanup failed for $VNET_NAME. VM cleanup remains completed; inspect and rerun."
        NETWORK_DELETED=true
        printf 'Deleted shared network %s and its default subnet, if present.\n' "$VNET_NAME"
      else
        printf 'Shared network retained/absent: %s (%s).\n' "$VNET_NAME" "$NETWORK_REASON"
      fi
    fi
  else
    printf 'Shared network retained/absent: %s (%s).\n' "$VNET_NAME" "$NETWORK_REASON"
  fi
fi
printf 'Completed: %s VM resource sets deleted; %s skipped; %s SSH entries removed; shared network deleted: %s. Resource group retained.\n' \
  "$DELETED" "$SKIPPED" "$SSH_REMOVED" "$NETWORK_DELETED"
