#!/usr/bin/env python3
"""Offline Azure CLI fixture for wrapper provisioning tests."""

import json
import ipaddress
import os
from pathlib import Path
import sys

args = sys.argv[1:]
state_path = Path(os.environ["TEST_AZURE_STATE"])
state = json.loads(state_path.read_text())
with Path(os.environ["TEST_AZURE_TRACE"]).open("a") as stream:
    stream.write(json.dumps(args) + "\n")
subscription = "00000000-0000-0000-0000-000000000001"
base = f"/subscriptions/{subscription}/resourceGroups/test-rg/providers"


def option(name, default=None):
    return args[args.index(name) + 1] if name in args else default


def save():
    state_path.write_text(json.dumps(state))


def output(value):
    print(json.dumps(value))


def resource(kind, name, **values):
    tags = {}
    if "--tags" in args:
        for value in args[args.index("--tags") + 1:]:
            if value.startswith("--"):
                break
            key, text = value.split("=", 1)
            tags[key] = text
    result = {"id": f"{base}/{kind}/{name}", "name": name, "type": kind, "tags": tags, **values}
    state["resources"].append(result)
    save()
    return result


if args[:2] == ["account", "show"]:
    output(subscription if "--query" in args else {"id": subscription, "name": "Test"})
elif args[:1] == ["rest"]:
    output({"value": [{"name": "eastus"}]})
elif args[:2] == ["group", "exists"]:
    output(state.get("group_exists", True))
elif args[:2] == ["resource", "list"]:
    if state.get("fail_inventory"):
        sys.exit(41)
    group = option("--resource-group")
    output([item for item in state["resources"]
            if group is None or item["id"].split("/")[4].lower() == group.lower()])
elif args[:2] == ["vm", "list-skus"]:
    output([{"name": option("--size"), "resourceType": "virtualMachines",
             "locations": ["eastus"], "family": "testFamily",
             "capabilities": [{"name": "vCPUs", "value": "2"}]}])
elif args[:2] == ["vm", "list-usage"]:
    output([{"name": {"value": name}, "currentValue": 0, "limit": 100}
            for name in ("testFamily", "cores")])
elif args[:3] == ["network", "nic", "create"]:
    name = option("--name")
    subnet_id = option("--subnet")
    subnet = next((item for item in state["resources"] if item["id"] == subnet_id), {})
    network = ipaddress.ip_network(subnet.get("addressPrefix", "10.50.0.0/24"))
    used = {config.get("privateIPAddress") for item in state["resources"]
            for config in item.get("ipConfigurations", [])}
    address = next(str(network.network_address + index) for index in range(4, network.num_addresses - 1)
                   if str(network.network_address + index) not in used)
    config_id = f"{base}/Microsoft.Network/networkInterfaces/{name}/ipConfigurations/ipconfig1"
    config = {"id": config_id, "name": "ipconfig1", "primary": True,
              "privateIPAddress": address, "subnet": {"id": subnet_id}}
    pip_id = option("--public-ip-address")
    if pip_id:
        config["publicIPAddress"] = {"id": pip_id}
        next(item for item in state["resources"] if item["id"] == pip_id)["ipConfiguration"] = {"id": config_id}
    resource("Microsoft.Network/networkInterfaces", name,
             networkSecurityGroup={"id": option("--network-security-group")},
             ipConfigurations=[config])
elif args[:3] == ["network", "nsg", "create"]:
    resource("Microsoft.Network/networkSecurityGroups", option("--name"))
elif args[:4] == ["network", "nsg", "rule", "create"]:
    pass
elif args[:3] == ["network", "public-ip", "create"]:
    address = f"203.0.113.{4 + sum(item.get('type') == 'Microsoft.Network/publicIPAddresses' for item in state['resources'])}"
    resource("Microsoft.Network/publicIPAddresses", option("--name"), ipAddress=address)
elif args[:2] == ["vm", "create"]:
    name = option("--name")
    if state.get("fail_create") == name:
        sys.exit(42)
    disk = resource("Microsoft.Compute/disks", name + "-osdisk")
    resource("Microsoft.Compute/virtualMachines", name,
             storageProfile={"osDisk": {"managedDisk": {"id": disk["id"]}, "deleteOption": "Delete"}},
             networkProfile={"networkInterfaces": [{"id": option("--nics"), "deleteOption": "Delete"}]})
elif args[:2] == ["tag", "update"] or args[:3] == ["network", "vnet", "create"]:
    pass
elif "show" in args:
    if args[:3] == ["network", "nic", "show"] and "--query" in args:
        nic = next(item for item in state["resources"] if item["name"] == option("--name"))
        output(nic["ipConfigurations"][0]["privateIPAddress"])
    elif args[:3] == ["network", "public-ip", "show"] and "--query" in args:
        pip = next(item for item in state["resources"] if item["name"] == option("--name"))
        output(pip["ipAddress"])
    else:
        matches = [item for item in state["resources"]
                   if item["id"] == option("--ids") or item["name"] == option("--name")]
        if not matches:
            sys.exit(43)
        result = dict(matches[0])
        result["id"] = state.get("wrong_show_ids", {}).get(result["id"], result["id"])
        output(result)
elif "delete" in args:
    name = option("--name") or option("--ids", "").rsplit("/", 1)[-1]
    if state.get("fail_delete") == name:
        sys.exit(44)
    removed = {name}
    if args[:2] == ["vm", "delete"]:
        removed.update((name + "-osdisk", name + "-nic"))
    removed_nics = [item["id"] for item in state["resources"]
                    if item["name"] in removed and item.get("type") == "Microsoft.Network/networkInterfaces"]
    for item in state["resources"]:
        config_id = (item.get("ipConfiguration") or {}).get("id", "")
        if any(config_id.startswith(nic + "/") for nic in removed_nics):
            item["ipConfiguration"] = None
    state["resources"] = [item for item in state["resources"] if item["name"] not in removed]
    save()
else:
    print(f"Unsupported mock Azure command: {args}", file=sys.stderr)
    sys.exit(45)
