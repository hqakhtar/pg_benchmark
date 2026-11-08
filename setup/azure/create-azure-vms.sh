#!/usr/bin/env bash
# Create individually named Linux VMs with Azure CLI. Requires Bash 4+, az, jq,
# ssh-keygen. Creation is sequential; a failure leaves identifiable resources.
# No private SSH key is uploaded. Do not run concurrent deployments with the
# same subscription/resource group/name prefix: Azure PUT is not create-only.
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

PROGRAM=${0##*/}
MANAGER=azure-vm-batch-v1
# Only the explicitly selected data file provides defaults; CLI flags override it.
SUBSCRIPTION=${AZVM_SUBSCRIPTION-}
RESOURCE_GROUP=${AZVM_RESOURCE_GROUP-}
LOCATION=${AZVM_LOCATION-}
COUNT=${AZVM_COUNT-}
PREFIX=${AZVM_NAME_PREFIX-}
SSH_KEY=${AZVM_SSH_KEY-}
ADMIN_USER=${AZVM_ADMIN_USER-azuresu}
START=${AZVM_START-1}
DIGITS=${AZVM_DIGITS-2}
IMAGE=${AZVM_IMAGE-Canonical:ubuntu-24_04-lts:server:latest}
SIZE=${AZVM_SIZE-Standard_D2s_v5}
OS_DISK_SIZE=${AZVM_OS_DISK_SIZE-64}
STORAGE_SKU=${AZVM_STORAGE_SKU-StandardSSD_LRS}
SECURITY_TYPE=${AZVM_SECURITY_TYPE-TrustedLaunch}
ACCELERATED_NETWORKING=${AZVM_ACCELERATED_NETWORKING-true}
SSH_SOURCE=${AZVM_SSH_SOURCE-}
PUBLIC_IP=${AZVM_PUBLIC_IP-true}
SUBNET_ID=${AZVM_SUBNET_ID-}
CONTROL_VM=${AZVM_CONTROL_VM-}
CONTROL_RESOURCE_GROUP=${AZVM_CONTROL_RESOURCE_GROUP-}
VNET_CIDR=${AZVM_VNET_CIDR-10.50.0.0/16}
SUBNET_CIDR=${AZVM_SUBNET_CIDR-10.50.0.0/24}
UPDATE_SSH_CONFIG=${AZVM_UPDATE_SSH_CONFIG-false}
UPDATE_SSH_CONFIG_SET=${AZVM_UPDATE_SSH_CONFIG+set}
SSH_CONFIG_FILE=${AZVM_SSH_CONFIG_FILE-$HOME/.ssh/config}
SSH_CONFIG_MODE=${AZVM_SSH_CONFIG_MODE-split}
WORKING_DIRECTORY=${AZVM_WORK_DIR-$PWD}
SSH_PUBLIC_CONFIG_FILE=${AZVM_SSH_PUBLIC_CONFIG_FILE-$WORKING_DIRECTORY/sshconfig-pg-benchmark-public.conf}
SSH_PRIVATE_CONFIG_FILE=${AZVM_SSH_PRIVATE_CONFIG_FILE-$WORKING_DIRECTORY/sshconfig-pg-benchmark-private.conf}
SSH_PRIVATE_KEY=${AZVM_SSH_PRIVATE_KEY-}
SSH_PORT=${AZVM_SSH_PORT-22}
SSH_ADDRESS=${AZVM_SSH_ADDRESS-auto}
DRY_RUN=false
HOSTS_FILE=''
KEY_FILE=''
MUTATION_STARTED=false
SUBSCRIPTION_ID=''
VM_NAMES=()
declare -A VM_PRIVATE_ADDRESSES VM_PUBLIC_ADDRESSES

usage() {
    cat <<'USAGE'
Usage: create-azure-vms.sh --env-file PATH [options]

Required file:
  --env-file PATH            Read settings from this file; never source it.

Required settings (supply flags or matching AZVM_* entries in the file):
  --subscription NAME_OR_ID   Azure subscription (ID recommended).
  --resource-group NAME       Existing resource group, or one to create.
  --location REGION           Azure region name, e.g. eastus or westeurope.
  --count N                   Number of VMs (1..20; final index must be <=20).
  --name-prefix PREFIX        Prefix before the numeric suffix, e.g. hamid-vm-r.
  --ssh-key FILE_OR_KEY       One OpenSSH RSA/Ed25519 public key, file or literal.
  --ssh-source IPV4_CIDR      Allowed public SSH source; required for public VMs.

Optional:
  --admin-user NAME           Default: azuresu.
  --start N                  Starting number (1..20; default: 1).
  --digits N                 Compatibility option; only 2 is accepted.
  --image IMAGE              Default: Canonical:ubuntu-24_04-lts:server:latest.
  --size SIZE                Default: Standard_D2s_v5.
  --os-disk-size GIB          OS disk size, 1..4095 GiB (default: 64).
  --storage-sku SKU          Default: StandardSSD_LRS.
  --security-type TYPE       TrustedLaunch (default) or Standard.
  --accelerated-networking BOOL
                             true (default) or false. Dsv5 requires true;
                             use false for other sizes/images without support.
  --public-ip BOOL           true or false; overrides AZVM_PUBLIC_IP.
  --no-public-ip             Create private-only VMs. Requires an existing
                             access route; Internet egress needs explicit NAT,
                             firewall or another configured outbound method.
  --subnet-id RESOURCE_ID     Use an existing subnet in this subscription/region.
                             Its VNet is preserved and is never retagged.
  --control-vm NAME          Reuse this controller's primary NIC subnet.
                             Requires public IPs and split SSH configuration.
  --control-resource-group NAME
                             Controller's group; default: runner resource group.
  --vnet-cidr IPV4_CIDR       Managed VNet range (default: 10.50.0.0/16).
  --subnet-cidr IPV4_CIDR     Managed subnet range (default: 10.50.0.0/24).
  --update-ssh-config BOOL   Add local SSH entries (default: true with a controller,
                             otherwise false). Controller mode requires true.
  --ssh-config-mode MODE     split (default) or single (legacy output).
  --ssh-config-file PATH     Main Include destination in split mode; legacy
                             output in single mode. Default: $HOME/.ssh/config.
  --ssh-public-config-file PATH
                             Default: sshconfig-pg-benchmark-public.conf in the
                             command's current working directory.
  --ssh-private-config-file PATH
                             Default: sshconfig-pg-benchmark-private.conf in the
                             command's current working directory.
  --ssh-private-key PATH     Local IdentityFile; required when SSH updates are on.
  --ssh-port PORT            SSH client port (default: 22); does not alter sshd/NSG.
  --ssh-address MODE         Single mode: auto (default), public or private.
                             Split mode requires auto and writes both addresses.
  --hosts-file PATH          Add each created VM alias to this plain hosts file.
                             Preserve existing hosts; relative to current directory.
  --dry-run                  Validate/preview, including batch vCPU quota;
                             change neither Azure nor local SSH config.
  --help                     Show this help.

Environment-file example (edit the file first):
  ./create-azure-vms.sh --env-file ./azure-vms.env --dry-run
  ./create-azure-vms.sh --env-file ./azure-vms.env

CLI arguments override file settings; inherited AZVM_* variables are ignored.
The file supports assignments, optional export, quotes, comments and $HOME;
shell commands are rejected. Explicit relative file paths use the env file's
directory. Default split SSH files use the command's working directory instead.
No SSH output subdirectory is created. Run deletion from the same directory
or supply the same explicit public/private config paths.
For all IPv4 SSH sources use
AZVM_SSH_SOURCE="0.0.0.0/0" with AZVM_PUBLIC_IP=true.

Example:
  ./create-azure-vms.sh --env-file ./azure-vms.env --subscription '<subscription-id>' \
    --resource-group hamid-lab --location eastus --count 3 \
    --name-prefix hamid-vm-r --ssh-key "$HOME/.ssh/id_ed25519.pub" \
    --ssh-source 203.0.113.10/32

Names always have an exactly two-digit numeric suffix, from 01 through 20:
hamid-vm-r01, hamid-vm-r02, ..., hamid-vm-r20. The requested range must end at
20 or earlier. Names may not already exist, even if tagged by an earlier run.
To add VMs later, use --start with the first unused number within that range.

Each VM has its own -nic, -nsg, -osdisk and (unless private-only) -pip resources.
Without a controller or subnet ID, the shared VNet is PREFIX-vnet, subnet default. Reuse requires matching
ownership tags, region and address ranges. --vnet-cidr/--subnet-cidr only apply
when neither --control-vm nor --subnet-id is supplied. A supplied subnet must
belong to the controller's VNet when both selectors are used.
Split SSH mode writes VM_NAME (public) and VM_NAME-private (private) in separate
files; the main config receives only Include directives. It requires public IPs.
NSGs retain Azure's default VNet rules; the SSH
source restriction controls the additional public inbound SSH rule.

The matching deletion script can find resources by these tags:
  managedBy=azure-vm-batch-v1, vmBatch=PREFIX, vmName=VM_NAME
The shared VNet has managedBy/vmBatch tags only. The resource group is untagged.
VM deletion is configured to delete its OS disk and NIC. Public IP/NSG cleanup
is handled by the deletion script. It also removes the owned shared VNet and
default subnet once unused, unless --keep-network is passed to deletion.
No data disks are created.

Both creation and --dry-run resolve the exact VM size's quota family and check
current family and regional vCPU quotas for the entire requested batch. These
reads require subscription-level VM SKU/usage read permissions. They do not
reserve quota or capacity or establish that all Azure validations will pass.
USAGE
}

die() { printf 'ERROR: %s\n' "$*" >&2; exit 1; }
note() { printf '%s\n' "$*" >&2; }

cleanup() {
    local rc=$?
    trap - EXIT
    if [[ -n $KEY_FILE ]]; then rm -f -- "$KEY_FILE"; fi
    if (( rc != 0 )) && [[ $MUTATION_STARTED == true ]]; then
        printf '\nCreation stopped; completed VMs and partial resources were left in place.\n' >&2
        printf 'Cleanup scope: subscription=%s resource-group=%s vmBatch=%s\n' \
            "$SUBSCRIPTION_ID" "$RESOURCE_GROUP" "$PREFIX" >&2
        printf 'Use delete-azure-vms.sh with this subscription, resource group and name prefix.\n' >&2
    fi
    exit "$rc"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

need_value() {
    [[ $# -ge 2 && -n $2 && $2 != --* ]] || die "Missing value for $1."
}

# Validate before arithmetic to avoid octal interpretation or expression input.
uint_option() {
    local target=$1 raw=$2 minimum=$3 maximum=$4 value
    [[ $raw =~ ^[0-9]+$ && ${#raw} -le 10 ]] || die "$target must be an integer."
    value=$((10#$raw))
    (( value >= minimum && value <= maximum )) || \
        die "$target must be between $minimum and $maximum."
    printf -v "$target" '%d' "$value"
}

# Sets CIDR_NETWORK, CIDR_LAST and CIDR_SIZE. Require canonical IPv4 networks
# so comparisons do not accept ranges Azure would normalize differently.
check_cidr() {
    local cidr=$1 label=$2 ip bits a b c d octet value mask
    [[ $cidr =~ ^([0-9]{1,3}\.){3}[0-9]{1,3}/[0-9]{1,2}$ ]] || \
        die "$label must be an IPv4 CIDR, e.g. 203.0.113.10/32."
    ip=${cidr%/*}; bits=${cidr#*/}
    IFS=. read -r a b c d <<< "$ip"
    for octet in "$a" "$b" "$c" "$d"; do
        [[ $octet == 0 || $octet != 0* ]] || die "$label contains a leading-zero octet."
        (( 10#$octet <= 255 )) || die "$label contains an invalid IPv4 octet."
    done
    [[ $bits == 0 || $bits != 0* ]] || die "$label contains a leading-zero prefix length."
    bits=$((10#$bits))
    (( bits <= 32 )) || die "$label prefix length must be 0..32."
    value=$(( (10#$a << 24) | (10#$b << 16) | (10#$c << 8) | 10#$d ))
    if (( bits == 0 )); then mask=0; else mask=$(( (0xffffffff << (32-bits)) & 0xffffffff )); fi
    CIDR_NETWORK=$(( value & mask ))
    CIDR_SIZE=$(( 1 << (32-bits) ))
    CIDR_LAST=$(( CIDR_NETWORK + CIDR_SIZE - 1 ))
    (( value == CIDR_NETWORK )) || die "$label must use the network address for its prefix length."
}

az_cli() {
    az "$@" --subscription "$SUBSCRIPTION_ID" --only-show-errors
}

# Check both quota tiers for the entire new batch before creating resources.
# SKU family names come from Azure; similar size names can use different quotas.
check_vcpu_quota() {
    local skus sku family per_vm usage quota matches match_count values current limit required additional
    local insufficient=false
    skus=$(az_cli vm list-skus --location "$LOCATION" --resource-type virtualMachines \
        --size "$SIZE" --all --output json) || die 'Cannot read VM SKU metadata; quota preflight failed.'
    sku=$(jq -ecj --arg size "$SIZE" --arg location "$LOCATION" '
        select(type == "array") |
        [.[] | select(
            ((.name // "") | ascii_downcase) == ($size | ascii_downcase) and
            ((.resourceType // "") | ascii_downcase) == "virtualmachines" and
            any(.locations[]?; ascii_downcase == $location))] |
        select(length == 1) | .[0]' <<< "$skus") ||
        die "Cannot resolve exactly one VM SKU for $SIZE in $LOCATION."
    family=$(jq -erj '.family | select(type == "string" and length > 0)' <<< "$sku") ||
        die "VM SKU $SIZE has no quota-family metadata."
    per_vm=$(jq -erj '
        [.capabilities[]? | select(.name == "vCPUs") | .value | tonumber] |
        select(length == 1) | .[0] |
        select(. > 0 and . <= 2147483647 and . == floor)' <<< "$sku") ||
        die "VM SKU $SIZE has no valid quota vCPU count."
    # vCPUsAvailable can be lower for constrained sizes; quota uses full vCPUs.
    additional=$((COUNT * per_vm))
    printf '\nQuota preflight: size=%s; family=%s; region=%s\n' "$SIZE" "$family" "$LOCATION"
    printf 'New VMs: %s; quota vCPUs per VM: %s; additional vCPUs: %s\n' "$COUNT" "$per_vm" "$additional"
    usage=$(az_cli vm list-usage --location "$LOCATION" --output json) ||
        die 'Cannot read regional VM quota usage; quota preflight failed.'
    jq -e 'type == "array"' <<< "$usage" >/dev/null || die 'Invalid VM quota usage response.'
    for quota in "$family" cores; do
        matches=$(jq -ecj --arg quota "$quota" '
            [.[] | select(
                ((.name.value // "") | ascii_downcase) == ($quota | ascii_downcase))]' <<< "$usage") ||
            die "Cannot parse quota records for $quota in $LOCATION."
        match_count=$(jq -rj 'length' <<< "$matches") || die "Cannot count quota records for $quota."
        case $match_count in
            0) die "No matching quota record for $quota in $LOCATION. Expected name.value=$quota; quota could not be verified." ;;
            1) ;;
            *) die "Ambiguous quota records for $quota in $LOCATION: found $match_count matches; quota could not be verified." ;;
        esac
        # Azure CLI can stringify the integer fields returned by Compute.
        # Normalize only numbers or decimal integer strings; never default to 0.
        values=$(jq -erj '
            try (
                .[0] | [.currentValue, .limit] |
                map(if type == "number" then .
                    elif type == "string" then
                        if test("^[0-9]+\\z") then tonumber
                        else error("expected an unsigned integer string") end
                    else error("expected a number or integer string") end) |
                select(all(.[];
                    type == "number" and . >= 0 and . <= 2147483647 and . == floor)) |
                @tsv
            ) catch empty' <<< "$matches") ||
            die "Invalid numeric quota values for $quota in $LOCATION: $(jq -cj '.[0] | {currentValue, limit}' <<< "$matches"). Expected non-negative integers up to 2147483647, as JSON numbers or integer strings."
        IFS=$'\t' read -r current limit <<< "$values" || die "Cannot parse quota $quota."
        required=$((current + additional))
        printf '  %s: usage=%s; limit=%s; required for this batch=%s\n' "$quota" "$current" "$limit" "$required"
        if (( required > limit )); then insufficient=true; fi
    done
    [[ $insufficient == false ]] ||
        die "Insufficient vCPU quota for $COUNT new $SIZE VM(s) in $LOCATION. Check --size/AZVM_SIZE and the limits shown above; no resources were changed."
}

while (( $# )); do
    case $1 in
        --help|-h) usage; exit 0 ;;
        --dry-run) DRY_RUN=true; shift ;;
        --no-public-ip) PUBLIC_IP=false; shift ;;
        --subscription|--resource-group|--location|--count|--name-prefix|--ssh-key|\
        --admin-user|--start|--digits|--image|--size|--os-disk-size|--storage-sku|\
        --security-type|--accelerated-networking|--public-ip|--ssh-source|--subnet-id|--vnet-cidr|--subnet-cidr|\
        --control-vm|--control-resource-group|--update-ssh-config|--ssh-config-file|--ssh-config-mode|\
        --ssh-public-config-file|--ssh-private-config-file|--ssh-private-key|--ssh-port|--ssh-address|--hosts-file)
            need_value "$@"
            case $1 in
                --subscription) SUBSCRIPTION=$2 ;;
                --resource-group) RESOURCE_GROUP=$2 ;;
                --location) LOCATION=$2 ;;
                --count) COUNT=$2 ;;
                --name-prefix) PREFIX=$2 ;;
                --ssh-key) SSH_KEY=$2 ;;
                --admin-user) ADMIN_USER=$2 ;;
                --start) START=$2 ;;
                --digits) DIGITS=$2 ;;
                --image) IMAGE=$2 ;;
                --size) SIZE=$2 ;;
                --os-disk-size) OS_DISK_SIZE=$2 ;;
                --storage-sku) STORAGE_SKU=$2 ;;
                --security-type) SECURITY_TYPE=$2 ;;
                --accelerated-networking) ACCELERATED_NETWORKING=$2 ;;
                --public-ip) PUBLIC_IP=$2 ;;
                --ssh-source) SSH_SOURCE=$2 ;;
                --subnet-id) SUBNET_ID=$2 ;;
                --control-vm) CONTROL_VM=$2 ;;
                --control-resource-group) CONTROL_RESOURCE_GROUP=$2 ;;
                --vnet-cidr) VNET_CIDR=$2 ;;
                --subnet-cidr) SUBNET_CIDR=$2 ;;
                --update-ssh-config) UPDATE_SSH_CONFIG=$2; UPDATE_SSH_CONFIG_SET=set ;;
                --ssh-config-file) SSH_CONFIG_FILE=$2 ;;
                --ssh-config-mode) SSH_CONFIG_MODE=$2 ;;
                --ssh-public-config-file) SSH_PUBLIC_CONFIG_FILE=$2 ;;
                --ssh-private-config-file) SSH_PRIVATE_CONFIG_FILE=$2 ;;
                --ssh-private-key) SSH_PRIVATE_KEY=$2 ;;
                --ssh-port) SSH_PORT=$2 ;;
                --ssh-address) SSH_ADDRESS=$2 ;;
                --hosts-file) HOSTS_FILE=$2 ;;
            esac
            shift 2 ;;
        *) die "Unknown option: $1. Run $PROGRAM --help." ;;
    esac
done

azvm_hosts_action check || die 'Runner hosts preflight failed; no Azure resources were changed.'

for required in SUBSCRIPTION RESOURCE_GROUP LOCATION COUNT PREFIX SSH_KEY; do
    [[ -n ${!required} ]] || die "Missing required setting: $required. Set it in --env-file or supply its CLI flag (see --help)."
done
(( BASH_VERSINFO[0] >= 4 )) || die 'Bash 4 or newer is required.'
for command_name in az jq ssh-keygen mktemp; do
    command -v "$command_name" >/dev/null 2>&1 || die "Required command not found: $command_name."
done
[[ $PREFIX =~ ^[a-z][a-z0-9-]*$ && ${#PREFIX} -le 48 ]] || \
    die '--name-prefix must start with a lowercase letter and contain only lowercase letters, digits or hyphens (maximum 48 characters).'
[[ $RESOURCE_GROUP =~ ^[a-zA-Z0-9._\(\)-]+$ && ${#RESOURCE_GROUP} -le 90 && $RESOURCE_GROUP != *. ]] || \
    die 'Use an ASCII Azure resource group name of at most 90 characters, without a trailing period.'
[[ $LOCATION =~ ^[a-z0-9]+$ ]] || die '--location must be a region name such as eastus.'
[[ $ADMIN_USER =~ ^[a-z_][a-z0-9_-]*$ && ${#ADMIN_USER} -le 32 && $ADMIN_USER != root ]] || \
    die '--admin-user must be a non-root Linux username of at most 32 characters.'
[[ $SIZE =~ ^[a-zA-Z0-9_-]+$ ]] || die '--size must be an Azure VM size name.'
case $SECURITY_TYPE in TrustedLaunch|Standard) ;; *) die '--security-type must be TrustedLaunch or Standard.' ;; esac
case $ACCELERATED_NETWORKING in true|false) ;; *) die '--accelerated-networking must be true or false.' ;; esac
case $PUBLIC_IP in true|false) ;; *) die '--public-ip / AZVM_PUBLIC_IP must be true or false.' ;; esac
azvm_ssh_config_settings || die 'Invalid local SSH configuration settings.'
if [[ -n $CONTROL_VM ]]; then
    [[ $CONTROL_VM =~ ^[a-zA-Z0-9][a-zA-Z0-9_.-]{0,63}$ ]] || die 'Invalid --control-vm name.'
    CONTROL_RESOURCE_GROUP=${CONTROL_RESOURCE_GROUP:-$RESOURCE_GROUP}
    [[ $CONTROL_RESOURCE_GROUP =~ ^[a-zA-Z0-9._\(\)-]+$ && ${#CONTROL_RESOURCE_GROUP} -le 90 && $CONTROL_RESOURCE_GROUP != *. ]] ||
        die 'Invalid --control-resource-group name.'
    [[ $PUBLIC_IP == true && $UPDATE_SSH_CONFIG == true && $SSH_CONFIG_MODE == split ]] ||
        die 'Controller runners require public IPs and enabled split SSH configuration.'
    [[ -r $SCRIPT_DIR/azure_vm_network.py ]] || die 'Missing azure_vm_network.py; keep all package files together.'
elif [[ -n $CONTROL_RESOURCE_GROUP ]]; then
    die '--control-resource-group requires --control-vm.'
fi
[[ -n $IMAGE ]] || die '--image / AZVM_IMAGE must not be empty.'
case $STORAGE_SKU in
    Standard_LRS|StandardSSD_LRS|Premium_LRS|StandardSSD_ZRS|Premium_ZRS) ;;
    *) die 'Unsupported OS disk storage SKU. Use Standard_LRS, StandardSSD_LRS, Premium_LRS, StandardSSD_ZRS or Premium_ZRS.' ;;
esac
uint_option COUNT "$COUNT" 1 20
uint_option START "$START" 1 20
uint_option DIGITS "$DIGITS" 2 2
uint_option OS_DISK_SIZE "$OS_DISK_SIZE" 1 4095
(( START + COUNT - 1 <= 20 )) || die 'The final VM index (start + count - 1) must not exceed 20.'
if [[ $PUBLIC_IP == true && -z $SSH_SOURCE ]]; then
    die '--ssh-source IPv4_CIDR is required for public VMs. Use --no-public-ip for private-only VMs.'
fi
if [[ -n $SSH_SOURCE ]]; then check_cidr "$SSH_SOURCE" '--ssh-source'; fi
if [[ $SSH_KEY != 'ssh-rsa '* && $SSH_KEY != 'ssh-ed25519 '* ]]; then
    SSH_KEY=$(azvm_resolve_path "$SSH_KEY") || die 'Cannot resolve the SSH public-key path.'
fi
if [[ $UPDATE_SSH_CONFIG == true ]]; then
    [[ -r $SCRIPT_DIR/azure-vm-ssh-config.py ]] || die 'Missing azure-vm-ssh-config.py; keep all package files together.'
    [[ -n $SSH_CONFIG_FILE && -n $SSH_PRIVATE_KEY ]] || die 'SSH updates require AZVM_SSH_CONFIG_FILE and AZVM_SSH_PRIVATE_KEY (or their flags).'
    SSH_PRIVATE_KEY=$(azvm_resolve_path "$SSH_PRIVATE_KEY") || die 'Cannot resolve the SSH private-key path.'
    uint_option SSH_PORT "$SSH_PORT" 1 65535
    if [[ $SSH_CONFIG_MODE == split ]]; then
        [[ $PUBLIC_IP == true ]] || die 'Split SSH configuration requires public IPs; use single mode for private-only VMs.'
        [[ $SSH_ADDRESS == auto ]] || die '--ssh-address must be auto in split mode; use single mode to select one address.'
        SSH_ADDRESS_MODE=split
    else
        case $SSH_ADDRESS in
            auto) if [[ $PUBLIC_IP == true ]]; then SSH_ADDRESS_MODE=public; else SSH_ADDRESS_MODE=private; fi ;;
            public) [[ $PUBLIC_IP == true ]] || die '--ssh-address public requires public IP creation.'; SSH_ADDRESS_MODE=public ;;
            private) SSH_ADDRESS_MODE=private ;;
            *) die '--ssh-address / AZVM_SSH_ADDRESS must be auto, public or private.' ;;
        esac
    fi
fi

VNET_NAME=${PREFIX}-vnet
if [[ -z $SUBNET_ID && -z $CONTROL_VM ]]; then
    check_cidr "$VNET_CIDR" '--vnet-cidr'
    VNET_FIRST=$CIDR_NETWORK; VNET_LAST=$CIDR_LAST
    check_cidr "$SUBNET_CIDR" '--subnet-cidr'
    (( CIDR_NETWORK >= VNET_FIRST && CIDR_LAST <= VNET_LAST )) || die 'The subnet must be inside the VNet address range.'
    (( CIDR_SIZE >= 8 && COUNT <= CIDR_SIZE-5 )) || \
        die 'The subnet is too small for this VM count; Azure reserves five IPv4 addresses per subnet.'
fi
for ((number=START; number<START+COUNT; number++)); do
    printf -v vm_name '%s%02d' "$PREFIX" "$number"
    (( ${#vm_name} <= 64 )) || die "Generated VM name exceeds 64 characters: $vm_name."
    VM_NAMES+=("$vm_name")
done

# Show resolved settings before Azure reads or resource-collision checks.
# %q preserves otherwise invisible characters in the diagnostic output.
printf 'Effective config (v10): script=%q subscription-selector=%q resource-group=%q size=%q location=%q start=%s count=%s\n' \
    "${BASH_SOURCE[0]}" "$SUBSCRIPTION" "$RESOURCE_GROUP" "$SIZE" "$LOCATION" "$START" "$COUNT" >&2
printf 'Settings file: %s\n' "$AZVM_ENV_FILE" >&2

if [[ -f $SSH_KEY ]]; then
    [[ -r $SSH_KEY ]] || die 'The SSH public key file is not readable.'
    PUBLIC_KEY=$(<"$SSH_KEY")
else
    PUBLIC_KEY=$SSH_KEY
fi
PUBLIC_KEY=${PUBLIC_KEY//$'\r'/}
[[ $PUBLIC_KEY != *$'\n'* ]] || die 'Provide exactly one OpenSSH public key line.'
[[ $PUBLIC_KEY == 'ssh-rsa '* || $PUBLIC_KEY == 'ssh-ed25519 '* ]] || \
    die '--ssh-key must be an existing public-key file or a literal ssh-rsa/ssh-ed25519 public key; never a private key.'
KEY_FILE=$(mktemp "${TMPDIR:-/tmp}/azure-vm-public-key.XXXXXX") || die 'Could not create a temporary key file.'
printf '%s\n' "$PUBLIC_KEY" > "$KEY_FILE"
KEY_SUMMARY=$(ssh-keygen -lf "$KEY_FILE") || die 'ssh-keygen rejected the public key.'
read -r KEY_BITS _ <<< "$KEY_SUMMARY"
if [[ $PUBLIC_KEY == 'ssh-rsa '* ]]; then
    (( KEY_BITS >= 2048 )) || die 'Azure requires an RSA public key of at least 2048 bits.'
fi
unset PUBLIC_KEY

ACCOUNT=$(az account show --subscription "$SUBSCRIPTION" --only-show-errors --output json) || \
    die 'Cannot read the subscription. Run az login and check the subscription name/ID.'
SUBSCRIPTION_ID=$(jq -erj '.id | select(type == "string" and length > 0)' <<< "$ACCOUNT") || die 'Subscription response has no ID.'
SUBSCRIPTION_NAME=$(jq -rj '.name // .id' <<< "$ACCOUNT") || die 'Invalid subscription response.'
if [[ $UPDATE_SSH_CONFIG == true ]]; then
    azvm_ssh_scope_args
    SSH_CHECK_ARGS=("${SSH_SCOPE_ARGS[@]}" --user "$ADMIN_USER" --port "$SSH_PORT" --identity-file "$SSH_PRIVATE_KEY")
    for vm_name in "${VM_NAMES[@]}"; do SSH_CHECK_ARGS+=(--vm-name "$vm_name"); done
    python3 "$SCRIPT_DIR/azure-vm-ssh-config.py" "$SSH_CHECK_ADD_ACTION" "${SSH_CHECK_ARGS[@]}" >/dev/null ||
        die 'Local SSH config preflight failed; no Azure resources were changed.'
fi
# account list-locations does not accept --subscription. Use the ARM endpoint
# through az rest so validation targets the resolved subscription without
# changing the CLI's default account. Relative URLs also respect its cloud.
LOCATIONS_URL="/subscriptions/$SUBSCRIPTION_ID/locations?api-version=2022-12-01"
# Keep the array nonempty for Bash 4.0-4.3 when nounset is enabled.
LOCATION_URLS_SEEN=('')
LOCATION_FOUND=false
while [[ -n $LOCATIONS_URL ]]; do
    for SEEN_URL in "${LOCATION_URLS_SEEN[@]}"; do
        [[ $LOCATIONS_URL != "$SEEN_URL" ]] || die 'Repeated nextLink in subscription locations response.'
    done
    LOCATION_URLS_SEEN+=("$LOCATIONS_URL")
    LOCATIONS=$(az_cli rest --method get --url "$LOCATIONS_URL" --output json) || \
        die 'Cannot list locations for the selected subscription.'
    jq -e 'type == "object" and
        (.value | type == "array" and all(.[];
            type == "object" and (.name | type == "string"))) and
        (.nextLink == null or (.nextLink | type == "string"))' \
        <<< "$LOCATIONS" >/dev/null || die 'Invalid locations response for the selected subscription.'
    if jq -e --arg location "$LOCATION" 'any(.value[]; .name == $location)' \
        <<< "$LOCATIONS" >/dev/null; then
        LOCATION_FOUND=true
        break
    fi
    LOCATIONS_URL=$(jq -rj '.nextLink // empty' <<< "$LOCATIONS") || \
        die 'Cannot read the next subscription locations page.'
done
[[ $LOCATION_FOUND == true ]] || die "Region $LOCATION is not listed for this subscription."
# Parse one JSON boolean without a trailing newline; avoid raw TSV comparisons.
RG_EXISTS=$(az_cli group exists --name "$RESOURCE_GROUP" --output json) || die 'Cannot determine whether the resource group exists.'
RG_EXISTS=$(jq -erjs 'select(length == 1 and (.[0] | type == "boolean")) | .[0] | tostring' <<< "$RG_EXISTS") || \
    die 'Unexpected response from az group exists; expected a JSON boolean.'
case $RG_EXISTS in
    true)
        RESOURCE_LIST=$(az_cli resource list --resource-group "$RESOURCE_GROUP" --output json) || \
            die 'Cannot list resource-group resources; no changes made.' ;;
    false) RESOURCE_LIST='[]' ;;
    *) die 'Unexpected response from az group exists.' ;;
esac
jq -e 'type == "array"' <<< "$RESOURCE_LIST" >/dev/null || die 'Invalid resource-list response.'
NAMES_JSON=$(printf '%s\n' "${VM_NAMES[@]}" | jq -Rsc 'split("\n")[:-1]') || die 'Cannot build the VM name list.'
PLANNED_RESOURCES=$(jq -nc --argjson names "$NAMES_JSON" '
    [$names[] as $name | [
        {name:$name, type:"microsoft.compute/virtualmachines"},
        {name:($name+"-nic"), type:"microsoft.network/networkinterfaces"},
        {name:($name+"-pip"), type:"microsoft.network/publicipaddresses"},
        {name:($name+"-nsg"), type:"microsoft.network/networksecuritygroups"},
        {name:($name+"-osdisk"), type:"microsoft.compute/disks"}
    ][]]') || die 'Cannot build the resource plan.'
COLLISIONS=$(jq -r --argjson planned "$PLANNED_RESOURCES" '
    .[] | . as $resource |
    select(any($planned[]; .name == ($resource.name | ascii_downcase)
        and .type == ($resource.type | ascii_downcase))) |
    "\(.type)\t\(.name)"' <<< "$RESOURCE_LIST") || die 'Cannot check resource-name collisions.'
[[ -z $COLLISIONS ]] || die "Requested resource names already exist. Choose an unused range or remove the old deployment first:
$COLLISIONS"

NEW_VNET=false
NETWORK_SOURCE=managed
CONTROL_VNET_ID=''
if [[ -n $CONTROL_VM ]]; then
    CONTROL_NETWORK=$(python3 "$SCRIPT_DIR/azure_vm_network.py" --subscription "$SUBSCRIPTION_ID" \
        --resource-group "$CONTROL_RESOURCE_GROUP" --vm-name "$CONTROL_VM") ||
        die 'Cannot resolve the controller network; no Azure resources were changed.'
    CONTROL_VNET_ID=$(jq -erj '.vnet_id | select(type == "string" and length > 0)' <<< "$CONTROL_NETWORK") ||
        die 'Controller network response has no VNet ID.'
    if [[ -z $SUBNET_ID ]]; then
        SUBNET_ID=$(jq -erj '.subnet_id | select(type == "string" and length > 0)' <<< "$CONTROL_NETWORK") ||
            die 'Controller network response has no subnet ID.'
        NETWORK_SOURCE=controller
    fi
fi
if [[ -n $SUBNET_ID ]]; then
    if [[ $NETWORK_SOURCE != controller ]]; then NETWORK_SOURCE=explicit-subnet; fi
    [[ ${SUBNET_ID,,} =~ ^/subscriptions/([^/]+)/resourcegroups/[^/]+/providers/microsoft\.network/virtualnetworks/[^/]+/subnets/[^/]+$ ]] || \
        die '--subnet-id must be a full Azure subnet resource ID.'
    [[ ${BASH_REMATCH[1]} == "${SUBSCRIPTION_ID,,}" ]] || die 'The existing subnet must be in the selected subscription.'
    EXISTING_VNET_ID=${SUBNET_ID%/*}; EXISTING_VNET_ID=${EXISTING_VNET_ID%/*}
    [[ -z $CONTROL_VNET_ID || ${EXISTING_VNET_ID,,} == "${CONTROL_VNET_ID,,}" ]] ||
        die 'The supplied subnet is outside the controller VNet; no Azure resources were changed.'
    VNET_JSON=$(az_cli network vnet show --ids "$EXISTING_VNET_ID" --output json) || die 'Cannot read the existing VNet.'
    jq -e --arg id "$EXISTING_VNET_ID" '
        (.id | type == "string") and (.id | ascii_downcase) == ($id | ascii_downcase)
    ' <<< "$VNET_JSON" >/dev/null || die 'The existing VNet response has an unexpected resource ID.'
    jq -e --arg location "$LOCATION" '.location == $location' <<< "$VNET_JSON" >/dev/null || \
        die 'The existing VNet is not in the requested VM region.'
    SUBNET_JSON=$(az_cli network vnet subnet show --ids "$SUBNET_ID" --output json) || die 'Cannot read the existing subnet.'
    REQUESTED_SUBNET_ID=$SUBNET_ID
    SUBNET_ID=$(jq -erj '.id | select(type == "string" and length > 0)' <<< "$SUBNET_JSON") || die 'The subnet response has no ID.'
    [[ ${SUBNET_ID,,} == "${REQUESTED_SUBNET_ID,,}" ]] || die 'The existing subnet response has an unexpected resource ID.'
else
    EXISTING_VNET_COUNT=$(jq -rj --arg name "$VNET_NAME" '
        [.[] | select((.type | ascii_downcase) == "microsoft.network/virtualnetworks"
            and (.name | ascii_downcase) == $name)] | length' <<< "$RESOURCE_LIST") || die 'Cannot check VNet existence.'
    if [[ $EXISTING_VNET_COUNT == 0 ]]; then
        NEW_VNET=true
        SUBNET_ID="/subscriptions/$SUBSCRIPTION_ID/resourceGroups/$RESOURCE_GROUP/providers/Microsoft.Network/virtualNetworks/$VNET_NAME/subnets/default"
    else
        VNET_JSON=$(az_cli network vnet show --resource-group "$RESOURCE_GROUP" --name "$VNET_NAME" --output json) || \
            die 'Cannot inspect the existing managed VNet.'
        jq -e --arg manager "$MANAGER" --arg prefix "$PREFIX" --arg location "$LOCATION" --arg cidr "$VNET_CIDR" '
            .tags.managedBy == $manager and .tags.vmBatch == $prefix and
            .location == $location and .addressSpace.addressPrefixes == [$cidr]
        ' <<< "$VNET_JSON" >/dev/null || \
            die 'The existing VNet has different ownership, region or address space. Choose another prefix or use --subnet-id.'
        SUBNET_JSON=$(jq -ec '.subnets[] | select(.name == "default")' <<< "$VNET_JSON") || \
            die 'The managed VNet has no default subnet. Inspect it or use --subnet-id.'
        jq -e --arg cidr "$SUBNET_CIDR" '
            .addressPrefix == $cidr or .addressPrefixes == [$cidr]
        ' <<< "$SUBNET_JSON" >/dev/null || die 'The managed subnet has a different address range.'
        SUBNET_ID=$(jq -erj '.id | select(type == "string" and length > 0)' <<< "$SUBNET_JSON") || die 'The managed subnet has no ID.'
    fi
fi

check_vcpu_quota

printf 'Subscription: %s (%s)\n' "$SUBSCRIPTION_NAME" "$SUBSCRIPTION_ID"
printf 'Resource group: %s (%s)\n' "$RESOURCE_GROUP" "$(if [[ $RG_EXISTS == true ]]; then printf existing; else printf 'will create'; fi)"
printf 'Region: %s\nImage: %s\nSize: %s\nAdmin: %s\n' "$LOCATION" "$IMAGE" "$SIZE" "$ADMIN_USER"
printf 'OS disk: %s GiB, %s; security: %s\n' "$OS_DISK_SIZE" "$STORAGE_SKU" "$SECURITY_TYPE"
printf 'Accelerated networking: %s\n' "$ACCELERATED_NETWORKING"
printf 'SSH key: %s\n' "$KEY_SUMMARY"
if [[ $UPDATE_SSH_CONFIG == true ]]; then
    if [[ $SSH_CONFIG_MODE == split ]]; then
        printf 'SSH configs: public=%s; private=%s; Include destination=%s\n' "$SSH_PUBLIC_CONFIG_FILE" "$SSH_PRIVATE_CONFIG_FILE" "$SSH_CONFIG_FILE"
    else
        printf 'SSH config: %s; address=%s\n' "$SSH_CONFIG_FILE" "$SSH_ADDRESS_MODE"
    fi
    printf 'SSH client port=%s; IdentityFile=%s\n' "$SSH_PORT" "$SSH_PRIVATE_KEY"
    printf 'Host aliases will be the VM names; entries are added after each successful creation.\n'
fi
printf 'Subnet: %s\n' "$SUBNET_ID"
printf 'Network source: %s\n' "$NETWORK_SOURCE"
if [[ -n $CONTROL_VM ]]; then
    printf 'Controller: %s (%s); VNet: %s (preserved)\n' "$CONTROL_VM" "$CONTROL_RESOURCE_GROUP" "$CONTROL_VNET_ID"
fi
if [[ $NEW_VNET == true ]]; then printf 'Create shared VNet: %s (%s), subnet default (%s)\n' "$VNET_NAME" "$VNET_CIDR" "$SUBNET_CIDR"; fi
printf 'Public IPs: %s; added SSH allow rule: %s\n' "$PUBLIC_IP" "${SSH_SOURCE:-none (default VNet rules still apply)}"
printf 'Ownership: managedBy=%s vmBatch=%s; per-VM resources also have vmName\n' "$MANAGER" "$PREFIX"
printf '\nPlanned VM resources:\n'
for vm_name in "${VM_NAMES[@]}"; do
    printf '  %s | %s-nic | %s-nsg | %s-osdisk' "$vm_name" "$vm_name" "$vm_name" "$vm_name"
    if [[ $PUBLIC_IP == true ]]; then printf ' | %s-pip' "$vm_name"; fi
    printf '\n'
done
if [[ $DRY_RUN == true ]]; then
    printf '\nDry run complete. No Azure resources or local SSH files were changed.\n'
    printf 'Current family and regional vCPU quotas passed for the entire new batch.\n'
    printf 'Quota is not reserved. Azure still checks capacity, policy, image compatibility and other limits during creation.\n'
    exit 0
fi

# A managed OS disk can be untagged if VM creation fails partway. Tag it only
# when an owned VM actually references this exact expected disk resource ID.
tag_os_disk() {
    local name=$1 vm_json expected_id actual_id
    expected_id="/subscriptions/$SUBSCRIPTION_ID/resourceGroups/$RESOURCE_GROUP/providers/Microsoft.Compute/disks/$name-osdisk"
    vm_json=$(az_cli vm show --resource-group "$RESOURCE_GROUP" --name "$name" --output json) || return 1
    actual_id=$(jq -erj --arg manager "$MANAGER" --arg prefix "$PREFIX" --arg name "$name" --arg expected "$expected_id" '
        select(.tags.managedBy == $manager and .tags.vmBatch == $prefix and .tags.vmName == $name) |
        .storageProfile.osDisk.managedDisk.id |
        select(type == "string" and ascii_downcase == ($expected | ascii_downcase))
    ' <<< "$vm_json") || return 1
    az_cli tag update --resource-id "$actual_id" --operation Merge \
        --tags "managedBy=$MANAGER" "vmBatch=$PREFIX" "vmName=$name" --output none || return 1
}

read_vm_addresses() {
    local name=$1 address_json
    address_json=$(az_cli network nic show --resource-group "$RESOURCE_GROUP" --name "$name-nic" \
        --query 'ipConfigurations[0].privateIPAddress' --output json) || die "Cannot read the private IP for $name."
    PRIVATE_ADDRESS=$(jq -erjs 'select(length == 1) | .[0] | select(type == "string" and length > 0)' <<< "$address_json") ||
        die "No valid private IP was returned for $name."
    PUBLIC_ADDRESS='-'
    if [[ $PUBLIC_IP == true ]]; then
        address_json=$(az_cli network public-ip show --resource-group "$RESOURCE_GROUP" --name "$name-pip" \
            --query ipAddress --output json) || die "Cannot read the public IP for $name."
        PUBLIC_ADDRESS=$(jq -erjs 'select(length == 1) | .[0] | select(type == "string" and length > 0)' <<< "$address_json") ||
            die "No valid public IP was returned for $name."
    fi
    VM_PRIVATE_ADDRESSES[$name]=$PRIVATE_ADDRESS
    VM_PUBLIC_ADDRESSES[$name]=$PUBLIC_ADDRESS
}

MUTATION_STARTED=true
if [[ $RG_EXISTS == false ]]; then
    az_cli group create --name "$RESOURCE_GROUP" --location "$LOCATION" --output none || die 'Resource-group creation failed.'
fi
if [[ $NEW_VNET == true ]]; then
    az_cli network vnet create --resource-group "$RESOURCE_GROUP" --name "$VNET_NAME" \
        --location "$LOCATION" --address-prefixes "$VNET_CIDR" \
        --subnet-name default --subnet-prefixes "$SUBNET_CIDR" \
        --tags "managedBy=$MANAGER" "vmBatch=$PREFIX" --output none || die 'VNet creation failed.'
fi

for vm_name in "${VM_NAMES[@]}"; do
    note "Creating $vm_name ..."
    VM_TAGS=("managedBy=$MANAGER" "vmBatch=$PREFIX" "vmName=$vm_name")
    NSG_ID="/subscriptions/$SUBSCRIPTION_ID/resourceGroups/$RESOURCE_GROUP/providers/Microsoft.Network/networkSecurityGroups/$vm_name-nsg"
    NIC_ID="/subscriptions/$SUBSCRIPTION_ID/resourceGroups/$RESOURCE_GROUP/providers/Microsoft.Network/networkInterfaces/$vm_name-nic"
    PIP_ID="/subscriptions/$SUBSCRIPTION_ID/resourceGroups/$RESOURCE_GROUP/providers/Microsoft.Network/publicIPAddresses/$vm_name-pip"
    az_cli network nsg create --resource-group "$RESOURCE_GROUP" --name "$vm_name-nsg" \
        --location "$LOCATION" --tags "${VM_TAGS[@]}" --output none || die "NSG creation failed for $vm_name."
    if [[ -n $SSH_SOURCE ]]; then
        az_cli network nsg rule create --resource-group "$RESOURCE_GROUP" --nsg-name "$vm_name-nsg" \
            --name AllowSSH --priority 1000 --direction Inbound --access Allow --protocol Tcp \
            --source-address-prefixes "$SSH_SOURCE" --source-port-ranges '*' \
            --destination-address-prefixes '*' --destination-port-ranges 22 --output none || \
            die "SSH rule creation failed for $vm_name."
    fi
    NIC_ARGS=(network nic create --resource-group "$RESOURCE_GROUP" --name "$vm_name-nic"
        --location "$LOCATION" --subnet "$SUBNET_ID" --network-security-group "$NSG_ID"
        --accelerated-networking "$ACCELERATED_NETWORKING"
        --tags "${VM_TAGS[@]}" --output none)
    if [[ $PUBLIC_IP == true ]]; then
        az_cli network public-ip create --resource-group "$RESOURCE_GROUP" --name "$vm_name-pip" \
            --location "$LOCATION" --sku Standard --allocation-method Static --version IPv4 \
            --tags "${VM_TAGS[@]}" --output none || die "Public IP creation failed for $vm_name."
        NIC_ARGS+=(--public-ip-address "$PIP_ID")
    fi
    az_cli "${NIC_ARGS[@]}" || die "NIC creation failed for $vm_name."
    VM_ARGS=(vm create --resource-group "$RESOURCE_GROUP" --name "$vm_name"
        --location "$LOCATION" --image "$IMAGE" --size "$SIZE"
        --admin-username "$ADMIN_USER" --authentication-type ssh --ssh-key-values "$KEY_FILE"
        --nics "$NIC_ID" --os-disk-name "$vm_name-osdisk" --os-disk-size-gb "$OS_DISK_SIZE"
        --storage-sku "$STORAGE_SKU" --os-disk-delete-option Delete --nic-delete-option Delete
        --security-type "$SECURITY_TYPE" --tags "${VM_TAGS[@]}" --output none)
    if [[ $SECURITY_TYPE == TrustedLaunch ]]; then
        VM_ARGS+=(--enable-secure-boot true --enable-vtpm true)
    fi
    # Report selected fields from the actual finalized argv, without exposing
    # SSH key material or relying on a separate copy of the size setting.
    {
        printf 'VM create request: subscription=%q' "$SUBSCRIPTION_ID"
        for ((arg_index=2; arg_index<${#VM_ARGS[@]}; arg_index++)); do
            case ${VM_ARGS[arg_index]} in
                --resource-group|--name|--location|--size)
                    printf ' %s=%q' "${VM_ARGS[arg_index]#--}" "${VM_ARGS[arg_index+1]}"
                    ;;
            esac
        done
        printf '\n'
    } >&2
    if ! az_cli "${VM_ARGS[@]}"; then
        tag_os_disk "$vm_name" || note "Could not verify/tag an OS disk for failed VM $vm_name; inspect any untagged leftovers manually."
        die "VM creation failed for $vm_name."
    fi
    tag_os_disk "$vm_name" || die "VM $vm_name was created, but OS disk ownership could not be verified/tagged."
    azvm_hosts_action add "$vm_name" ||
        die "VM $vm_name was created, but its runner hosts entry could not be written."
    read_vm_addresses "$vm_name"
    if [[ $UPDATE_SSH_CONFIG == true ]]; then
        SSH_ADD_ARGS=("${SSH_SCOPE_ARGS[@]}" --vm-name "$vm_name" --user "$ADMIN_USER"
            --port "$SSH_PORT" --identity-file "$SSH_PRIVATE_KEY")
        if [[ $SSH_CONFIG_MODE == split ]]; then
            SSH_ADD_ARGS+=(--hostname "$PUBLIC_ADDRESS" --private-hostname "$PRIVATE_ADDRESS")
        else
            if [[ $SSH_ADDRESS_MODE == public ]]; then SSH_HOSTNAME=$PUBLIC_ADDRESS; else SSH_HOSTNAME=$PRIVATE_ADDRESS; fi
            SSH_ADD_ARGS+=(--hostname "$SSH_HOSTNAME")
        fi
        python3 "$SCRIPT_DIR/azure-vm-ssh-config.py" "$SSH_ADD_ACTION" "${SSH_ADD_ARGS[@]}" >/dev/null ||
            die "VM $vm_name was created, but its local SSH config entry could not be written."
        note "SSH entry ready: ssh $vm_name"
        if [[ $SSH_CONFIG_MODE == split ]]; then note "Private SSH entry ready: ssh $vm_name-private"; fi
    fi
done

printf '\nCreated %s VM(s). Connection information:\n' "$COUNT"
printf '%-30s %-16s %-16s\n' VM PRIVATE_IP PUBLIC_IP
for vm_name in "${VM_NAMES[@]}"; do
    printf '%-30s %-16s %-16s\n' "$vm_name" "${VM_PRIVATE_ADDRESSES[$vm_name]}" "${VM_PUBLIC_ADDRESSES[$vm_name]}"
done
if [[ $UPDATE_SSH_CONFIG == true ]]; then
    printf '\nSSH: ssh %s (or another VM name shown above)\n' "${VM_NAMES[0]}"
    if [[ $SSH_CONFIG_MODE == split ]]; then
        printf 'Private SSH: ssh %s-private\n' "${VM_NAMES[0]}"
        printf 'Direct configs: ssh -F %q %s; ssh -F %q %s-private\n' \
            "$SSH_PUBLIC_CONFIG_FILE" "${VM_NAMES[0]}" "$SSH_PRIVATE_CONFIG_FILE" "${VM_NAMES[0]}"
    fi
else
    printf '\nSSH: ssh -i <your-private-key-file> %s@<reachable-IP>\n' "$ADMIN_USER"
fi
printf 'Completed. Keep the subscription, resource group and name prefix for deletion.\n'
