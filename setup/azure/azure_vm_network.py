#!/usr/bin/env python3
"""Read a VM's primary NIC subnet without changing Azure resources."""

import argparse
import ipaddress
import json
import re
import subprocess
import sys
import uuid


class NetworkError(Exception):
    pass


def object_data(value, label):
    if not isinstance(value, dict):
        raise NetworkError(f"Invalid {label}: expected a JSON object")
    return value


def resource_id(value, kind, subscription):
    if not isinstance(value, str) or any(ord(char) < 32 for char in value):
        raise NetworkError(f"Invalid {kind} resource ID: {value!r}")
    parts = value.split("/")
    if (len(parts) < 9 or parts[0] or not all(parts[1:]) or (len(parts) - 7) % 2
            or parts[1].lower() != "subscriptions"
            or (subscription is not None and parts[2].lower() != subscription.lower())
            or parts[3].lower() != "resourcegroups" or parts[5].lower() != "providers"
            or "/".join([parts[6], *parts[7::2]]).lower() != kind.lower()):
        raise NetworkError(f"Unexpected {kind} resource ID in selected subscription: {value}")
    return parts[4], parts[-1]


def primary(items, label):
    if not isinstance(items, list) or not items or not all(isinstance(item, dict) for item in items):
        raise NetworkError(f"Invalid {label}: expected a nonempty JSON object array")
    selected = items if len(items) == 1 else [item for item in items if item.get("primary") is True]
    if len(selected) != 1:
        raise NetworkError(f"Cannot identify exactly one primary {label}")
    return selected[0]


def vm_subnet(get, subscription, group, name):
    vm_id = (f"/subscriptions/{subscription}/resourceGroups/{group}"
             f"/providers/Microsoft.Compute/virtualMachines/{name}")
    vm = get(["vm"], vm_id)
    profile = object_data(vm.get("networkProfile"), f"{name} network profile")
    nic_id = primary(profile.get("networkInterfaces"), f"{name} NIC").get("id")
    resource_id(nic_id, "Microsoft.Network/networkInterfaces", subscription)
    nic = get(["network", "nic"], nic_id)
    config = primary(nic.get("ipConfigurations"), f"{name} IP configuration")
    subnet_id = object_data(config.get("subnet"), f"{name} subnet").get("id")
    resource_id(subnet_id, "Microsoft.Network/virtualNetworks/subnets", subscription)
    if not isinstance(config.get("privateIPAddress"), str):
        raise NetworkError(f"Missing or invalid private IP address for {name}")
    try:
        address = str(ipaddress.ip_address(config["privateIPAddress"]))
    except ValueError:
        raise NetworkError(f"Missing or invalid private IP address for {name}") from None
    return address, subnet_id


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--subscription", required=True)
    parser.add_argument("--resource-group", required=True)
    parser.add_argument("--vm-name", required=True)
    args = parser.parse_args()
    try:
        subscription = str(uuid.UUID(args.subscription))
    except ValueError:
        raise NetworkError("Controller lookup requires a resolved subscription UUID") from None
    if (not re.fullmatch(r"[A-Za-z0-9._()-]{1,90}", args.resource_group)
            or args.resource_group.endswith(".")):
        raise NetworkError("Invalid controller resource group")
    if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.-]{0,63}", args.vm_name):
        raise NetworkError("Invalid controller VM name")

    def get(command, rid):
        result = subprocess.run(
            ["az", *command, "show", "--ids", rid, "--subscription", subscription,
             "--only-show-errors", "--output", "json"],
            capture_output=True, text=True, check=False,
        )
        if result.returncode:
            detail = result.stderr.strip() or result.stdout.strip() or "no error text"
            raise NetworkError(f"Cannot read controller resource {rid}: {detail}")
        try:
            data = object_data(json.loads(result.stdout), rid)
        except json.JSONDecodeError:
            raise NetworkError(f"Azure CLI returned invalid JSON for {rid}") from None
        if not isinstance(data.get("id"), str) or data["id"].lower() != rid.lower():
            raise NetworkError(f"Azure CLI returned a different resource for {rid}")
        return data

    address, subnet = vm_subnet(get, subscription, args.resource_group, args.vm_name)
    if ipaddress.ip_address(address).version != 4:
        raise NetworkError("The controller's primary IP configuration must use IPv4")
    print(json.dumps({"private_ip": address, "subnet_id": subnet,
                      "vnet_id": subnet.rsplit("/", 2)[0]}))


if __name__ == "__main__":
    try:
        main()
    except (NetworkError, OSError) as error:
        print(f"ERROR: Controller network: {error}", file=sys.stderr)
        sys.exit(1)
