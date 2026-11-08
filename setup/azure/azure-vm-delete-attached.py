#!/usr/bin/env python3
"""Explicitly delete basic Azure VMs using their inspected attachment IDs.

This helper is used only by delete-azure-vms.sh --include-unmanaged.  It does
not claim/tag resources or infer orphan names.  A private recovery journal is
written before deletion so a retry can finish cleanup after the VM is gone.
"""

import argparse
import contextlib
import fcntl
import hashlib
import json
import os
from pathlib import Path
import re
import stat
import subprocess
import sys
import tempfile
import uuid


class CleanupError(Exception):
    pass


TYPES = {
    "vm": "Microsoft.Compute/virtualMachines",
    "disk": "Microsoft.Compute/disks",
    "nic": "Microsoft.Network/networkInterfaces",
    "pip": "Microsoft.Network/publicIPAddresses",
    "nsg": "Microsoft.Network/networkSecurityGroups",
}
COMMANDS = {
    "vm": ["vm"], "disk": ["disk"], "nic": ["network", "nic"],
    "pip": ["network", "public-ip"], "nsg": ["network", "nsg"],
}
GUID_FIELDS = {"vm": "vmId", "disk": "uniqueId", "nic": "resourceGuid",
               "pip": "resourceGuid", "nsg": "resourceGuid"}


def fail(message):
    raise CleanupError(message)


def mapping(value, label):
    if not isinstance(value, dict):
        fail(f"Invalid {label}: expected a JSON object")
    return value


def array(value, label):
    if not isinstance(value, list):
        fail(f"Invalid {label}: expected a JSON array")
    return value


def optional_array(obj, key, label):
    value = obj.get(key)
    return [] if value is None else array(value, label)


def text(value, label):
    if not isinstance(value, str) or not value or any(ord(c) < 32 for c in value):
        fail(f"Invalid {label}: expected a nonempty string without control characters")
    return value


def parse_id(value):
    value = text(value, "Azure resource ID")
    parts = value.split("/")
    if (len(parts) < 9 or parts[0] or parts[1].lower() != "subscriptions"
            or parts[3].lower() != "resourcegroups" or parts[5].lower() != "providers"
            or not all(parts[1:]) or (len(parts) - 7) % 2):
        fail(f"Invalid Azure resource ID: {value}")
    try:
        subscription = str(uuid.UUID(parts[2]))
    except ValueError:
        fail(f"Invalid subscription in resource ID: {value}")
    resource_type = "/".join([parts[6]] + parts[7::2])
    return subscription, parts[4], resource_type, parts[-1]


def same_id(left, right):
    return isinstance(left, str) and isinstance(right, str) and left.lower() == right.lower()


def ref_id(value, label, optional=False):
    if value is None:
        if optional:
            return None
        fail(f"Missing {label}")
    obj = mapping(value, label)
    if not obj and optional:
        return None
    rid = text(obj.get("id"), f"{label} ID")
    parse_id(rid)
    return rid


def ref_ids(value, label):
    return [ref_id(item, label) for item in array(value, label)]


def identity(resource, kind):
    value = resource.get(GUID_FIELDS[kind])
    if value is None or value == "":
        return None
    try:
        return str(uuid.UUID(text(value, GUID_FIELDS[kind])))
    except ValueError:
        fail(f"Invalid {GUID_FIELDS[kind]} on {resource.get('id', kind)}")


def delete_option(value, label):
    if value not in (None, "Delete", "Detach"):
        fail(f"Unsupported {label}: {value!r}")


def reject_links(obj, keys, label):
    for key in keys:
        if obj.get(key):
            fail(f"Refusing {label}: unsupported attachment {key}")


class Azure:
    def __init__(self, subscription, resource_group):
        self.subscription = subscription
        self.resource_group = resource_group

    def run(self, args, read=True):
        argv = ["az", *args, "--subscription", self.subscription,
                "--only-show-errors", "--output", "json" if read else "none"]
        try:
            result = subprocess.run(argv, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                                    text=True, check=False)
        except OSError as error:
            fail(f"Cannot run Azure CLI: {error}")
        if result.returncode:
            detail = result.stderr.strip() or result.stdout.strip() or "no error text"
            fail(f"Azure CLI failed ({' '.join(args[:4])}): {detail}")
        if not read:
            return None
        try:
            return json.loads(result.stdout)
        except (ValueError, TypeError):
            fail(f"Azure CLI returned invalid JSON for {' '.join(args[:3])}")

    def check_id(self, rid, kind=None, same_scope=True):
        subscription, group, resource_type, _ = parse_id(rid)
        if same_scope and (subscription != self.subscription or
                           group.lower() != self.resource_group.lower()):
            fail(f"Refusing attachment outside the selected subscription/resource group: {rid}")
        if kind and resource_type.lower() != TYPES[kind].lower():
            fail(f"Unexpected resource type for {kind}: {rid}")
        return rid

    def inventory(self):
        exists = self.run(["group", "exists", "--name", self.resource_group])
        if not isinstance(exists, bool):
            fail("Azure CLI returned a non-boolean resource-group existence result")
        if not exists:
            return {}
        items = array(self.run(["resource", "list", "--resource-group", self.resource_group]),
                      "resource inventory")
        result = {}
        for item in items:
            item = mapping(item, "resource inventory entry")
            rid = self.check_id(item.get("id"))
            resource_type = text(item.get("type"), "resource inventory type")
            if resource_type.lower() != parse_id(rid)[2].lower() or rid.lower() in result:
                fail(f"Invalid or duplicate resource inventory entry: {rid}")
            result[rid.lower()] = item
        return result

    def get(self, kind, rid, inventory=None):
        self.check_id(rid, kind)
        if inventory is None:
            inventory = self.inventory()
        if rid.lower() not in inventory:
            return None
        resource = mapping(self.run([*COMMANDS[kind], "show", "--ids", rid]), kind)
        if not same_id(resource.get("id"), rid):
            fail(f"Azure CLI returned a different resource for {rid}")
        if ("type" in resource and
                text(resource["type"], "resource type").lower() != TYPES[kind].lower()):
            fail(f"Azure CLI returned an unexpected type for {rid}")
        return resource

    def require(self, kind, rid, inventory):
        resource = self.get(kind, rid, inventory)
        if resource is None:
            fail(f"VM attachment is missing from the resource inventory: {rid}")
        return resource

    def delete(self, kind, rid):
        args = [*COMMANDS[kind], "delete", "--ids", rid]
        if kind in ("vm", "disk"):
            args.append("--yes")
        self.run(args, read=False)
        if rid.lower() in self.inventory():
            fail(f"Deletion returned, but the resource is still listed: {rid}; retry after it disappears")


def descriptor(kind, resource, delete=True):
    immutable_id = identity(resource, kind)
    if delete and immutable_id is None:
        fail(f"Cannot prepare recoverable cleanup: {resource['id']} is missing {GUID_FIELDS[kind]}")
    return {"kind": kind, "id": resource["id"], "identity": immutable_id,
            "delete": delete}


def validate_disk(disk, vm_id, detached=False):
    owner = disk.get("managedBy")
    if not detached and owner is None:
        fail(f"OS disk does not report its expected VM attachment: {disk['id']}")
    if owner is not None:
        parse_id(owner)
        if detached or not same_id(owner, vm_id):
            fail(f"Refusing disk attached to a VM: {disk['id']} (managedBy={owner})")
    owners = optional_array(disk, "managedByExtended", "disk managedByExtended")
    for owner in owners:
        parse_id(owner)
        if detached or not same_id(owner, vm_id):
            fail(f"Refusing disk with another attachment: {disk['id']}")
    if disk.get("maxShares") not in (None, 0, 1):
        fail(f"Refusing shared disk: {disk['id']}")


def inspect_nic(nic, vm_id, detached=False):
    owner = ref_id(nic.get("virtualMachine"), "NIC VM", optional=True)
    if not detached and owner is None:
        fail(f"NIC does not report its expected VM attachment: {nic['id']}")
    if owner and (detached or not same_id(owner, vm_id)):
        fail(f"Refusing NIC attached to a VM: {nic['id']} (virtualMachine={owner})")
    reject_links(nic, ("virtualMachineScaleSet", "privateEndpoint", "tapConfigurations",
                      "hostedWorkloads", "migrationPhase"), nic["id"])
    if nic.get("nicType") not in (None, "Standard"):
        fail(f"Refusing unsupported NIC type: {nic['id']}")
    configs = array(nic.get("ipConfigurations"), "NIC IP configurations")
    if len(configs) != 1:
        fail(f"Refusing {nic['id']}: exactly one NIC IP configuration is required")
    config = mapping(configs[0], "NIC IP configuration")
    cid = text(config.get("id"), "NIC IP configuration ID")
    if (not cid.lower().startswith(nic["id"].lower() + "/ipconfigurations/")
            or len(cid.split("/")) != len(nic["id"].split("/")) + 2):
        fail(f"IP configuration does not belong to the expected NIC: {cid}")
    parse_id(cid)
    reject_links(config, ("loadBalancerBackendAddressPools", "loadBalancerInboundNatRules",
                          "applicationGatewayBackendAddressPools", "gatewayLoadBalancer",
                          "privateLinkConnectionProperties", "applicationSecurityGroups"), cid)
    pip = ref_id(config.get("publicIPAddress"), "NIC public IP", optional=True)
    if pip:
        delete_option(config["publicIPAddress"].get("deleteOption"), "NIC public IP deleteOption")
    subnet = ref_id(config.get("subnet"), "NIC subnet", optional=True)
    nsg = ref_id(nic.get("networkSecurityGroup"), "NIC NSG", optional=True)
    return {"ip_configuration": cid, "pip": pip, "subnet": subnet, "nsg": nsg}


def validate_pip(pip, ip_config, detached=False, require_attached=False):
    reject_links(pip, ("natGateway", "linkedPublicIPAddress", "servicePublicIPAddress"), pip["id"])
    owner = ref_id(pip.get("ipConfiguration"), "public IP attachment", optional=True)
    if require_attached and owner is None:
        fail(f"Public IP does not report its expected NIC attachment: {pip['id']}")
    if owner and (detached or not same_id(owner, ip_config)):
        fail(f"Refusing public IP with an attachment: {pip['id']} (ipConfiguration={owner})")


def nsg_retention(nsg, nic_id, detached=False):
    nics = ref_ids(optional_array(nsg, "networkInterfaces", "NSG NIC attachments"), "NSG NIC attachments")
    subnets = ref_ids(optional_array(nsg, "subnets", "NSG subnet attachments"), "NSG subnet attachments")
    flow_logs = optional_array(nsg, "flowLogs", "NSG flow logs")
    if flow_logs:
        return "NSG has flow logs"
    if subnets:
        return "NSG is associated with a subnet"
    if any(detached or not same_id(rid, nic_id) for rid in nics):
        return "NSG is still associated with a NIC"
    return None


def vm_id_for(azure, name):
    return (f"/subscriptions/{azure.subscription}/resourceGroups/{azure.resource_group}"
            f"/providers/Microsoft.Compute/virtualMachines/{name}")


def discover(azure, name, ssh_config, inventory):
    vm_id = vm_id_for(azure, name)
    vm = azure.get("vm", vm_id, inventory)
    plan = {"schema_version": 1, "subscription": azure.subscription,
            "resource_group": azure.resource_group, "vm_name": name,
            "vm": {"kind": "vm", "id": vm_id, "identity": None, "delete": True},
            "resources": [], "retained": [], "attachments": {},
            "ssh_config": ssh_config, "vm_deleted": False}
    if vm is None:
        plan["absent_without_journal"] = True
        return plan
    reject_links(vm, ("virtualMachineScaleSet",), vm_id)
    storage = mapping(vm.get("storageProfile"), "VM storage profile")
    disks = array(storage.get("dataDisks", []), "VM data disks")
    if disks:
        fail(f"Refusing {name}: data disks are present; this mode supports only an OS disk")
    os_disk = mapping(storage.get("osDisk"), "VM OS disk")
    reject_links(os_disk, ("vhd", "diffDiskSettings"), name)
    delete_option(os_disk.get("deleteOption"), "OS disk deleteOption")
    disk_id = azure.check_id(ref_id(os_disk.get("managedDisk"), "managed OS disk"), "disk")
    profile = mapping(vm.get("networkProfile"), "VM network profile")
    nics = array(profile.get("networkInterfaces"), "VM NIC attachments")
    if len(nics) != 1:
        fail(f"Refusing {name}: exactly one NIC is required")
    nic_ref = mapping(nics[0], "VM NIC")
    delete_option(nic_ref.get("deleteOption"), "VM NIC deleteOption")
    nic_id = azure.check_id(ref_id(nic_ref, "VM NIC"), "nic")
    disk = azure.require("disk", disk_id, inventory)
    nic = azure.require("nic", nic_id, inventory)
    validate_disk(disk, vm_id)
    attachments = inspect_nic(nic, vm_id)
    attachments.update(disk=disk_id, nic=nic_id)
    plan["vm"] = descriptor("vm", vm)
    plan["attachments"] = attachments
    plan["resources"].append(descriptor("nic", nic))
    if attachments["pip"]:
        pip_id = azure.check_id(attachments["pip"], "pip")
        pip = azure.require("pip", pip_id, inventory)
        validate_pip(pip, attachments["ip_configuration"], require_attached=True)
        plan["resources"].append(descriptor("pip", pip))
        if pip.get("publicIPPrefix"):
            plan["retained"].append({"id": ref_id(pip["publicIPPrefix"], "public IP prefix"),
                                      "reason": "public IP prefixes are retained"})
    plan["resources"].append(descriptor("disk", disk))
    if attachments["nsg"]:
        nsg_id = azure.check_id(attachments["nsg"], "nsg", same_scope=False)
        sub, group, _, _ = parse_id(nsg_id)
        if sub != azure.subscription or group.lower() != azure.resource_group.lower():
            plan["retained"].append({"id": nsg_id, "reason": "NSG is outside the selected resource group"})
        else:
            nsg = azure.require("nsg", nsg_id, inventory)
            reason = nsg_retention(nsg, nic_id)
            plan["resources"].append(descriptor("nsg", nsg, delete=reason is None))
            if reason:
                plan["retained"].append({"id": nsg_id, "reason": reason})
    if attachments["subnet"]:
        sid = attachments["subnet"]
        if parse_id(sid)[2].lower() != "microsoft.network/virtualnetworks/subnets":
            fail(f"Unexpected subnet ID: {sid}")
        plan["retained"].extend([
            {"id": sid, "reason": "subnets are retained"},
            {"id": sid.rsplit("/", 2)[0], "reason": "VNets are retained"},
        ])
    vm_identity = mapping(vm.get("identity") if vm.get("identity") is not None else {}, "VM identity")
    assigned_value = vm_identity.get("userAssignedIdentities")
    assigned = mapping(assigned_value if assigned_value is not None else {}, "user-assigned identities")
    for rid in assigned:
        parse_id(rid)
        plan["retained"].append({"id": rid, "reason": "user-assigned identities are retained"})
    return plan


def check_identity(record, current, resumed):
    saved = record["identity"]
    actual = identity(current, record["kind"])
    if resumed and (saved is None or actual is None):
        fail(f"Cannot safely resume without a recorded and current immutable identity: {record['id']}")
    if saved is not None and saved != actual:
        fail(f"Resource identity changed; refusing a possibly recreated resource: {record['id']}")


def compare_layout(saved, current):
    if current.get("absent_without_journal"):
        fail(f"VM disappeared after the plan was inspected: {saved['vm']['id']}; rerun to inspect again")
    a = {key: value.lower() if isinstance(value, str) else value
         for key, value in saved["attachments"].items()}
    b = {key: value.lower() if isinstance(value, str) else value
         for key, value in current["attachments"].items()}
    if a != b:
        fail(f"VM attachments changed since the deletion plan was inspected: {saved['vm_name']}")
    current_records = {item["id"].lower(): item for item in [current["vm"], *current["resources"]]}
    for record in [saved["vm"], *saved["resources"]]:
        other = current_records.get(record["id"].lower())
        if other is None or (record["identity"] is not None and record["identity"] != other["identity"]):
            fail(f"Resource changed since the deletion plan was inspected: {record['id']}")


def validate_plan(azure, plan, resumed, inventory):
    vm = azure.get("vm", plan["vm"]["id"], inventory)
    if plan.get("absent_without_journal"):
        if vm is not None:
            fail(f"VM appeared after the plan was inspected: {plan['vm']['id']}; rerun to inspect it")
        return
    if vm is not None:
        if plan.get("vm_deleted"):
            fail(f"VM exists after a recorded deletion; refusing: {plan['vm']['id']}")
        check_identity(plan["vm"], vm, resumed)
        current = discover(azure, plan["vm_name"], plan["ssh_config"], inventory)
        compare_layout(plan, current)
    elif not resumed:
        fail(f"VM disappeared before its recovery plan was saved: {plan['vm']['id']}; rerun")
    for record in plan["resources"]:
        resource = azure.get(record["kind"], record["id"], inventory)
        if resource is None:
            if vm is not None:
                fail(f"VM attachment disappeared: {record['id']}")
            continue
        # Retained NSGs are never candidates for deletion, so no GUID is needed.
        if record["delete"]:
            check_identity(record, resource, resumed)
        if record["kind"] == "disk":
            validate_disk(resource, plan["vm"]["id"], detached=vm is None)
        elif record["kind"] == "nic":
            links = inspect_nic(resource, plan["vm"]["id"], detached=vm is None)
            for key in ("pip", "subnet", "nsg", "ip_configuration"):
                left, right = links[key], plan["attachments"][key]
                if (left or "").lower() != (right or "").lower():
                    fail(f"NIC attachment changed since the deletion plan: {resource['id']}")
        elif record["kind"] == "pip":
            validate_pip(resource, plan["attachments"]["ip_configuration"], require_attached=vm is not None)
    return vm


class Journals:
    def __init__(self, directory, azure, ssh_config):
        self.directory = Path(directory)
        self.azure = azure
        self.ssh_config = ssh_config
        if self.directory.is_symlink():
            fail("State directory must not be a symlink")
        if self.directory.exists() and not self.directory.is_dir():
            fail("State directory is not a directory")

    def path(self, name):
        digest = hashlib.sha256(vm_id_for(self.azure, name).lower().encode()).hexdigest()
        return self.directory / (digest + ".json")

    def read(self, name):
        path = self.path(name)
        try:
            fd = os.open(path, os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0))
        except FileNotFoundError:
            return None
        with os.fdopen(fd, "r", encoding="utf-8") as handle:
            info = os.fstat(handle.fileno())
            if (not stat.S_ISREG(info.st_mode) or info.st_uid != os.getuid()
                    or info.st_mode & 0o077 or info.st_size > 1024 * 1024):
                fail(f"Recovery plan must be a private, regular file owned by this user: {path}")
            try:
                plan = json.load(handle)
            except (ValueError, UnicodeError):
                fail(f"Invalid recovery plan JSON: {path}")
        mapping(plan, "recovery plan")
        expected = (self.azure.subscription, self.azure.resource_group.lower(), name.lower())
        actual = (plan.get("subscription"), str(plan.get("resource_group", "")).lower(),
                  str(plan.get("vm_name", "")).lower())
        if plan.get("schema_version") != 1 or actual != expected:
            fail(f"Recovery plan scope or version does not match this request: {path}")
        if plan.get("ssh_config") != self.ssh_config:
            fail(f"Use the same SSH-config setting recorded in the recovery plan: {path}")
        vm = mapping(plan.get("vm"), "recovery VM")
        if vm.get("kind") != "vm" or not same_id(vm.get("id"), vm_id_for(self.azure, name)):
            fail(f"Invalid VM in recovery plan: {path}")
        records = array(plan.get("resources"), "recovery resources")
        attachments = mapping(plan.get("attachments"), "recovery attachments")
        counts = {kind: 0 for kind in ("disk", "nic", "pip", "nsg")}
        for record in [vm, *records]:
            mapping(record, "recovery resource")
            kind = record.get("kind")
            if kind not in TYPES or (record is not vm and kind == "vm"):
                fail(f"Invalid resource kind in recovery plan: {path}")
            self.azure.check_id(record.get("id"), kind)
            if not isinstance(record.get("delete"), bool):
                fail(f"Invalid deletion policy in recovery plan: {path}")
            if "identity" not in record:
                fail(f"Missing identity field in recovery plan: {path}")
            value = record.get("identity")
            if value is not None:
                try:
                    if str(uuid.UUID(value)) != value:
                        raise ValueError()
                except (ValueError, TypeError, AttributeError):
                    fail(f"Invalid immutable identity in recovery plan: {path}")
            if record is not vm:
                counts[kind] += 1
                if not same_id(record["id"], attachments.get(kind)):
                    fail(f"Recovery resource does not match its attachment: {path}")
                if kind != "nsg" and not record["delete"]:
                    fail(f"Invalid recovery deletion policy: {path}")
        if counts["disk"] != 1 or counts["nic"] != 1 or counts["pip"] > 1 or counts["nsg"] > 1:
            fail(f"Unsupported resource layout in recovery plan: {path}")
        if not vm["delete"]:
            fail(f"Invalid VM deletion policy in recovery plan: {path}")
        if set(attachments) != {"disk", "nic", "pip", "nsg", "subnet", "ip_configuration"}:
            fail(f"Incomplete attachments in recovery plan: {path}")
        for value in attachments.values():
            if value is not None:
                parse_id(value)
        if bool(attachments["pip"]) != bool(counts["pip"]):
            fail(f"Incomplete public IP in recovery plan: {path}")
        for retained in array(plan.get("retained"), "retained resources"):
            mapping(retained, "retained resource")
            parse_id(retained.get("id"))
            text(retained.get("reason"), "retention reason")
        if not isinstance(plan.get("vm_deleted"), bool) or plan.get("absent_without_journal"):
            fail(f"Invalid recovery progress: {path}")
        return plan

    @contextlib.contextmanager
    def lock(self):
        self.directory.mkdir(mode=0o700, parents=True, exist_ok=True)
        info = self.directory.lstat()
        if (not stat.S_ISDIR(info.st_mode) or info.st_uid != os.getuid()
                or info.st_mode & 0o022):
            fail("State directory must be owned by this user and not writable by other users")
        fd = os.open(self.directory / ".lock", os.O_CREAT | os.O_RDWR | getattr(os, "O_NOFOLLOW", 0), 0o600)
        try:
            info = os.fstat(fd)
            if not stat.S_ISREG(info.st_mode) or info.st_uid != os.getuid():
                fail("Invalid recovery-state lock file")
            try:
                fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError:
                fail("Another attachment cleanup is using this state directory; retry after it finishes")
            yield
        finally:
            os.close(fd)

    def write(self, plan):
        path = self.path(plan["vm_name"])
        fd, temporary = tempfile.mkstemp(prefix=".plan-", dir=self.directory)
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as handle:
                os.fchmod(handle.fileno(), 0o600)
                json.dump(plan, handle, indent=2, sort_keys=True)
                handle.write("\n")
                handle.flush()
                os.fsync(handle.fileno())
            os.replace(temporary, path)
            self.sync_directory()
        finally:
            with contextlib.suppress(FileNotFoundError):
                os.unlink(temporary)

    def sync_directory(self):
        fd = os.open(self.directory, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
        try:
            os.fsync(fd)
        finally:
            os.close(fd)

    def remove(self, plan):
        self.path(plan["vm_name"]).unlink()
        self.sync_directory()


def ssh_action(config, azure, names, action):
    if config is None:
        return []
    if isinstance(config, list):
        return sorted({name for path in config for name in ssh_action(path, azure, names, action)})
    helper = Path(__file__).resolve().with_name("azure-vm-ssh-config.py")
    args = [sys.executable, str(helper), action, "--config", config,
            "--subscription", azure.subscription, "--resource-group", azure.resource_group]
    for name in names:
        args.extend(["--vm-name", name])
    result = subprocess.run(args, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, check=False)
    if result.returncode:
        fail(result.stderr.strip() or "SSH config update failed")
    try:
        obj = json.loads(result.stdout)
    except ValueError:
        fail("SSH config helper returned invalid JSON")
    names = array(mapping(obj, "SSH helper result").get("managed_vm_names"), "managed SSH VM names")
    if not all(isinstance(name, str) for name in names):
        fail("SSH config helper returned invalid VM names")
    return names


def remove_managed_ssh(plan, azure):
    name = plan["vm_name"]
    managed = ssh_action(plan["ssh_config"], azure, [name], "check-remove")
    if name in managed:
        ssh_action(plan["ssh_config"], azure, [name], "remove")


def preview(plan, resumed, managed_ssh):
    label = "Resume exact recorded attachments" if resumed else "Inspect actual attachments"
    print(f"\n{plan['vm_name']} — {label}")
    print(f"  VM: {plan['vm']['id']}")
    if plan.get("absent_without_journal"):
        print("  VM is absent; no recovery plan exists. No orphan resources will be guessed.")
    else:
        print("  Delete VM if present; then delete remaining unattached resources:")
        for record in plan["resources"]:
            if record["delete"]:
                suffix = " (only if unused; otherwise retain)" if record["kind"] == "nsg" else ""
                print(f"    {record['kind']}: {record['id']}{suffix}")
        for item in plan["retained"]:
            print(f"  Retain: {item['id']} ({item['reason']})")
    if plan["vm_name"] in managed_ssh:
        configs = plan["ssh_config"] if isinstance(plan["ssh_config"], list) else [plan["ssh_config"]]
        print(f"  Remove this VM's managed SSH entries from {', '.join(configs)} after confirmed VM absence.")


def confirm(prompt):
    try:
        return input(prompt + " [y/N] ").strip().lower() in ("y", "yes")
    except EOFError:
        return False


def execute(azure, journals, plan, resumed, hosts_file=None):
    name = plan["vm_name"]
    if plan.get("absent_without_journal"):
        if azure.get("vm", plan["vm"]["id"]) is not None:
            fail(f"VM appeared before SSH cleanup: {name}; no changes made to that VM")
        hosts_action(hosts_file, "remove", name)
        remove_managed_ssh(plan, azure)
        print(f"{name}: already absent; no dependency names were inferred.")
        return
    ssh_action(plan["ssh_config"], azure, [name], "check-remove")
    current_vm = validate_plan(azure, plan, resumed, azure.inventory())
    if current_vm is not None:
        print(f"Deleting VM: {plan['vm']['id']}")
        azure.delete("vm", plan["vm"]["id"])
    if azure.get("vm", plan["vm"]["id"]) is not None:
        fail(f"VM absence could not be confirmed: {plan['vm']['id']}")
    plan["vm_deleted"] = True
    journals.write(plan)
    hosts_action(hosts_file, "remove", name)
    remove_managed_ssh(plan, azure)
    for record in plan["resources"]:
        if not record["delete"]:
            continue
        kind = record["kind"]
        resource = azure.get(kind, record["id"])
        if resource is None:
            print(f"Already absent: {record['id']}")
            continue
        check_identity(record, resource, resumed)
        if kind == "nic":
            links = inspect_nic(resource, plan["vm"]["id"], detached=True)
            for key in ("pip", "subnet", "nsg", "ip_configuration"):
                if (links[key] or "").lower() != (plan["attachments"][key] or "").lower():
                    fail(f"NIC attachment changed; refusing deletion: {record['id']}")
        elif kind == "pip":
            validate_pip(resource, plan["attachments"]["ip_configuration"], detached=True)
        elif kind == "disk":
            validate_disk(resource, plan["vm"]["id"], detached=True)
        elif kind == "nsg":
            reason = nsg_retention(resource, plan["attachments"]["nic"], detached=True)
            if reason:
                print(f"Retain: {record['id']} ({reason})")
                continue
        print(f"Deleting {kind}: {record['id']}")
        azure.delete(kind, record["id"])
    journals.remove(plan)
    print(f"{name}: cleanup completed. Resource group and shared infrastructure were retained.")


def absolute_path(value, label):
    value = text(value, label)
    if not os.path.isabs(value):
        fail(f"{label} must be absolute")
    return os.path.normpath(value)


def hosts_action(path, action, *names):
    if path is None:
        return
    helper = Path(__file__).resolve().with_name("azure-vm-hosts.py")
    result = subprocess.run([sys.executable, str(helper), action, path, *names], check=False)
    if result.returncode:
        fail(f"Runner hosts operation failed ({action}): {path}")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--subscription", required=True)
    parser.add_argument("--resource-group", required=True)
    parser.add_argument("--vm-name", required=True, action="append")
    parser.add_argument("--state-dir", required=True)
    parser.add_argument("--ssh-config", action="append")
    parser.add_argument("--hosts-file")
    parser.add_argument("--dry-run", action="store_true")
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--interactive", action="store_true")
    mode.add_argument("--yes", action="store_true")
    args = parser.parse_args()
    try:
        subscription = str(uuid.UUID(args.subscription))
    except ValueError:
        fail("--subscription must be a canonical subscription UUID")
    if subscription != args.subscription.lower():
        fail("--subscription must be a canonical subscription UUID")
    if not re.fullmatch(r"[A-Za-z0-9_.()-]{1,90}", args.resource_group):
        fail("Invalid resource group name")
    if any(not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.-]{0,63}", name) for name in args.vm_name):
        fail("Invalid VM name")
    if len({name.lower() for name in args.vm_name}) != len(args.vm_name):
        fail("VM names must be unique")
    state_dir = absolute_path(args.state_dir, "State directory")
    configs = [absolute_path(path, "SSH config path") for path in args.ssh_config or []]
    if len(configs) != len(set(configs)):
        fail("SSH config paths must be unique")
    config = configs if len(configs) > 1 else (configs[0] if configs else None)
    hosts_file = os.path.abspath(args.hosts_file) if args.hosts_file else None
    hosts_action(hosts_file, "check")
    azure = Azure(subscription, args.resource_group)
    journals = Journals(state_dir, azure, config)
    managed = ssh_action(config, azure, args.vm_name, "check-remove")
    inventory = azure.inventory()
    work = []
    for name in args.vm_name:
        saved = journals.read(name)
        if saved is None:
            plan = discover(azure, name, config, inventory)
        else:
            plan = saved
            try:
                validate_plan(azure, plan, True, inventory)
            except (CleanupError, OSError, KeyboardInterrupt):
                print(f"Recovery plan retained: {journals.path(name)}", file=sys.stderr)
                raise
        work.append((plan, saved is not None))
    print(f"Subscription: {subscription}")
    print(f"Resource group: {args.resource_group}")
    print("Attachment-driven deletion plan (OS disk contents will be permanently deleted):")
    for plan, resumed in work:
        preview(plan, resumed, managed)
    print("\nThe resource group, VNets, subnets, user-assigned identities, storage accounts, "
          "snapshots and unrelated resources are retained.")
    if args.dry_run:
        print("Dry run: no Azure resources, SSH config or recovery files were changed.")
        return
    if args.interactive:
        selected = [(plan, resumed) for plan, resumed in work
                    if confirm(f"Delete/finish cleanup for {plan['vm_name']} exactly as listed?")]
    elif args.yes or confirm("Proceed with this exact deletion plan?"):
        selected = work
    else:
        selected = []
    if not selected:
        print("No VMs selected; no changes made.")
        return
    try:
        with journals.lock():
            inventory = azure.inventory()
            for plan, resumed in selected:
                current_journal = journals.read(plan["vm_name"])
                if (resumed and current_journal != plan) or (not resumed and current_journal is not None):
                    fail(f"Recovery plan changed after inspection: {journals.path(plan['vm_name'])}; rerun")
                validate_plan(azure, plan, resumed, inventory)
            ssh_action(config, azure, [plan["vm_name"] for plan, _ in selected], "check-remove")
            for plan, _ in selected:
                if not plan.get("absent_without_journal"):
                    journals.write(plan)
            for plan, resumed in selected:
                execute(azure, journals, plan, resumed, hosts_file)
    except (CleanupError, OSError, KeyboardInterrupt):
        for plan, _ in selected:
            path = journals.path(plan["vm_name"])
            if path.exists():
                print(f"Recovery plan retained: {path}", file=sys.stderr)
        raise


if __name__ == "__main__":
    try:
        main()
    except (CleanupError, OSError, KeyboardInterrupt) as error:
        print(f"ERROR: {error or 'Interrupted; rerun the same explicit cleanup to resume.'}", file=sys.stderr)
        sys.exit(1)
