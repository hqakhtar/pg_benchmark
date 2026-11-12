#!/usr/bin/env python3
"""Plan or explicitly apply control-VNet-to-runner-VNet Azure peerings."""

import argparse
import hashlib
import ipaddress
import itertools
import json
import re
import subprocess
import sys
import uuid

from azure_vm_network import NetworkError as PeeringError, object_data, resource_id, vm_subnet


def fail(message):
    raise PeeringError(message)


class Azure:
    def __init__(self, subscription):
        self.subscription = subscription
        account = object_data(self.run(["account", "show"]), "Azure account")
        try:
            self.subscription = str(uuid.UUID(account["id"]))
        except (KeyError, ValueError, TypeError, AttributeError):
            fail("Azure account response has no valid subscription ID")

    def run(self, args, read=True):
        command = ["az", *args, "--subscription", self.subscription,
                   "--only-show-errors", "--output", "json" if read else "none"]
        try:
            result = subprocess.run(command, capture_output=True, text=True, check=False)
        except OSError as error:
            fail(f"Cannot run Azure CLI: {error}")
        if result.returncode:
            detail = result.stderr.strip() or result.stdout.strip() or "no error text"
            fail(f"Azure CLI failed ({' '.join(args[:4])}): {detail}")
        if not read:
            return None
        try:
            return json.loads(result.stdout)
        except json.JSONDecodeError:
            fail(f"Azure CLI returned invalid JSON for {' '.join(args[:4])}")

    def get(self, command, rid):
        result = object_data(self.run([*command, "show", "--ids", rid]), rid)
        if not isinstance(result.get("id"), str) or result["id"].lower() != rid.lower():
            fail(f"Azure CLI returned a different resource for {rid}")
        return result

    def vm_network(self, group, name):
        address, subnet_id = vm_subnet(self.get, self.subscription, group, name)
        return address, subnet_id.rsplit("/", 2)[0]

    def network(self, rid):
        resource_id(rid, "Microsoft.Network/virtualNetworks", self.subscription)
        network = self.get(["network", "vnet"], rid)
        prefixes = object_data(network.get("addressSpace"), f"{rid} address space").get("addressPrefixes")
        if not isinstance(prefixes, list) or not prefixes or not all(isinstance(p, str) for p in prefixes):
            fail(f"Missing address prefixes for {rid}")
        try:
            return [ipaddress.ip_network(prefix) for prefix in prefixes]
        except ValueError as error:
            fail(f"Invalid address space for {rid}: {error}")

    def peering_args(self, source):
        group, name = resource_id(source, "Microsoft.Network/virtualNetworks", self.subscription)
        return ["--resource-group", group, "--vnet-name", name]


def peering_name(remote):
    return "pg-benchmark-" + hashlib.sha256(remote.lower().encode("utf-8")).hexdigest()[:16]


def plan_peerings(azure, control, runners):
    networks = {}
    for rid in [control, *runners]:
        if rid.lower() not in networks:
            networks[rid.lower()] = (rid, azure.network(rid))
    for (left, left_prefixes), (right, right_prefixes) in itertools.combinations(networks.values(), 2):
        for a, b in itertools.product(left_prefixes, right_prefixes):
            if a.version == b.version and a.overlaps(b):
                fail(f"Overlapping address spaces: {left} ({a}) and {right} ({b}). "
                     "Readdressing is required; no peerings were changed.")

    peerings = {}
    for key, (rid, _) in networks.items():
        items = azure.run(["network", "vnet", "peering", "list", *azure.peering_args(rid)])
        if not isinstance(items, list):
            fail(f"Invalid peering list for {rid}")
        peerings[key] = [object_data(item, f"{rid} peering") for item in items]
        for item in peerings[key]:
            if not isinstance(item.get("name"), str) or not item["name"]:
                fail(f"Invalid peering name on {rid}")

    plan = []
    for remote, _ in networks.values():
        if remote.lower() == control.lower():
            continue
        for source, target in [(control, remote), (remote, control)]:
            matches = []
            for item in peerings[source.lower()]:
                other = object_data(item.get("remoteVirtualNetwork"), f"{source} remote VNet").get("id")
                resource_id(other, "Microsoft.Network/virtualNetworks", None)
                if other.lower() == target.lower():
                    matches.append(item)
            if len(matches) > 1:
                fail(f"Multiple existing peerings from {source} to {target}; inspect them manually")
            name = peering_name(target)
            if matches:
                existing = matches[0]
                name = existing.get("name")
                if not isinstance(name, str) or not re.fullmatch(r"[A-Za-z0-9_.-]+", name):
                    fail(f"Invalid existing peering name on {source}")
                if (existing.get("allowVirtualNetworkAccess") is not True
                        or existing.get("peeringState") not in ("Connected", "Initiated")
                        or existing.get("provisioningState") != "Succeeded"):
                    fail(f"Existing peering {source}/{name} is disabled or unhealthy; inspect it manually")
            elif any(item.get("name", "").lower() == name.lower() for item in peerings[source.lower()]):
                fail(f"Peering name {name} already targets another VNet on {source}; refusing to overwrite it")
            plan.append((source, target, name, not matches))
    return plan


def validate_args(args):
    for label, value in [("subscription", args.subscription), ("resource group", args.resource_group),
                         ("control resource group", args.control_resource_group)]:
        if not value or value.startswith("-") or any(ord(c) < 32 for c in value):
            fail(f"A nonempty {label} without control characters is required")
    for group in (args.resource_group, args.control_resource_group):
        if not re.fullmatch(r"[A-Za-z0-9._()-]{1,90}", group) or group.endswith("."):
            fail(f"Invalid Azure resource group name: {group}")
    if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.-]{0,63}", args.control_vm):
        fail("Invalid --control-vm name")
    if not re.fullmatch(r"[a-z][a-z0-9-]{0,47}", args.name_prefix):
        fail("--name-prefix must contain lowercase letters, digits or hyphens and start with a letter (maximum 48 characters)")
    if not (1 <= args.start <= 20 and 1 <= args.count <= 20 and args.start + args.count - 1 <= 20):
        fail("--start and --count must select runner indices within 01 through 20")
    if args.digits != 2:
        fail("--digits must be 2")


def main():
    parser = argparse.ArgumentParser(
        prog="peer-azure-vms.sh",
        usage="%(prog)s --env-file PATH --control-vm NAME [options]",
        description="Use --env-file PATH to select the existing Azure batch settings. "
                    "Discover VM NICs and peer the control VNet with each distinct runner VNet. "
                    "Default: read-only plan. No NSG, route, SSH config, or VM changes.")
    parser.add_argument("--control-vm", required=True, help="Existing initiating VM name")
    parser.add_argument("--control-resource-group", help="Default: runner resource group")
    parser.add_argument("--subscription", default="", help="AZVM_SUBSCRIPTION; name or ID")
    parser.add_argument("--resource-group", default="", help="AZVM_RESOURCE_GROUP")
    parser.add_argument("--name-prefix", default="", help="AZVM_NAME_PREFIX; runner VM prefix")
    parser.add_argument("--count", type=int, default=0, help="AZVM_COUNT; up to 20 runners")
    parser.add_argument("--start", type=int, default=1, help="AZVM_START; default 1")
    parser.add_argument("--digits", type=int, default=2, help="AZVM_DIGITS; must be 2")
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--dry-run", action="store_true", help="Read and print the plan only (default)")
    mode.add_argument("--apply", action="store_true", help="Create missing directional peerings and verify Connected")
    args = parser.parse_args()
    if args.control_resource_group is None:
        args.control_resource_group = args.resource_group
    validate_args(args)
    azure = Azure(args.subscription)
    control_ip, control = azure.vm_network(args.control_resource_group, args.control_vm)
    runners = []
    print(f"Subscription: {azure.subscription}")
    print(f"Control VM: {args.control_vm} private={control_ip}\nControl VNet: {control}")
    for number in range(args.start, args.start + args.count):
        name = f"{args.name_prefix}{number:02d}"
        address, network = azure.vm_network(args.resource_group, name)
        runners.append(network)
        print(f"Runner: {name} private={address} vnet={network}")
    plan = plan_peerings(azure, control, runners)
    print("\nVNet-wide connectivity, subject to existing NSGs; not an SSH-only per-VM rule.")
    for source, target, name, create in plan:
        print(f"{'CREATE' if create else 'REUSE'} {source} -> {target} name={name}")
    if not plan:
        print("All selected runners already share the control VNet. No peering is needed.")
        return
    if not args.apply:
        print("DRY RUN: no resources changed. Rerun with --apply to create the missing peerings.")
        return
    try:
        for source, target, name, create in plan:
            if create:
                azure.run(["network", "vnet", "peering", "create", *azure.peering_args(source),
                           "--name", name, "--remote-vnet", target, "--allow-vnet-access"])
        for source, target, name, _ in plan:
            base = ["network", "vnet", "peering"]
            options = [*azure.peering_args(source), "--name", name]
            azure.run([*base, "wait", *options, "--custom",
                       "peeringState=='Connected' && provisioningState=='Succeeded'",
                       "--interval", "5", "--timeout", "60"], read=False)
            result = object_data(azure.run([*base, "show", *options]), name)
            remote = object_data(result.get("remoteVirtualNetwork"), f"{name} remote VNet").get("id")
            if (not isinstance(remote, str) or remote.lower() != target.lower()
                    or result.get("peeringState") != "Connected"
                    or result.get("provisioningState") != "Succeeded"
                    or result.get("allowVirtualNetworkAccess") is not True):
                fail(f"Peering {source}/{name} did not verify as Connected with VNet access enabled")
    except PeeringError:
        print("Apply failed: some peerings may remain. No rollback was attempted. "
              "Inspect the error, then rerun the dry-run plan before retrying.", file=sys.stderr)
        raise
    print("All required peerings verified Connected. Test SSH from the control VM using the private IPs above.")
    print("NSGs, Linux firewalls, SSH keys, and SSH HostName entries were not changed.")


if __name__ == "__main__":
    try:
        main()
    except PeeringError as error:
        print(f"ERROR: {error}", file=sys.stderr)
        raise SystemExit(1)
