#!/usr/bin/env python3
"""Offline Azure CLI fixture for wrapper provisioning tests."""

import json
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
    output(True)
elif args[:2] == ["resource", "list"]:
    if state.get("fail_inventory"):
        sys.exit(41)
    output(state["resources"])
elif args[:2] == ["vm", "list-skus"]:
    output([{"name": option("--size"), "resourceType": "virtualMachines",
             "locations": ["eastus"], "family": "testFamily",
             "capabilities": [{"name": "vCPUs", "value": "2"}]}])
elif args[:2] == ["vm", "list-usage"]:
    output([{"name": {"value": name}, "currentValue": 0, "limit": 100}
            for name in ("testFamily", "cores")])
elif args[:3] == ["network", "nic", "create"]:
    name = option("--name")
    resource("Microsoft.Network/networkInterfaces", name,
             networkSecurityGroup={"id": option("--network-security-group")},
             ipConfigurations=[{"privateIPAddress": "10.50.0.4"}])
elif args[:3] == ["network", "nsg", "create"]:
    resource("Microsoft.Network/networkSecurityGroups", option("--name"))
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
        output("10.50.0.4")
    else:
        matches = [item for item in state["resources"]
                   if item["id"] == option("--ids") or item["name"] == option("--name")]
        if not matches:
            sys.exit(43)
        output(matches[0])
elif "delete" in args:
    name = option("--name")
    if state.get("fail_delete") == name:
        sys.exit(44)
    removed = {name}
    if args[:2] == ["vm", "delete"]:
        removed.update((name + "-osdisk", name + "-nic"))
    state["resources"] = [item for item in state["resources"] if item["name"] not in removed]
    save()
else:
    print(f"Unsupported mock Azure command: {args}", file=sys.stderr)
    sys.exit(45)
