#!/usr/bin/env python3
"""Offline tests for control-to-runner VNet discovery and peering."""

import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import tempfile
import unittest


ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "setup/azure/peer-azure-vms.sh"
SUBSCRIPTION = "00000000-0000-0000-0000-000000000001"


def rid(kind, name, group="test-rg"):
    return f"/subscriptions/{SUBSCRIPTION}/resourceGroups/{group}/providers/{kind}/{name}"


class PeeringTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory(prefix="pg-benchmark-peering-")
        self.addCleanup(self.temporary.cleanup)
        self.directory = Path(self.temporary.name)
        self.bin = self.directory / "bin"
        self.bin.mkdir()
        shutil.copy(ROOT / "tests/fixtures/fake-azure.py", self.bin / "az")
        (self.bin / "az").chmod(0o700)
        self.trace = self.directory / "trace.jsonl"
        self.state_path = self.directory / "state.json"
        self.env_file = self.directory / "azure.env"
        self.env_file.write_text(
            'AZVM_SUBSCRIPTION="test"\nAZVM_RESOURCE_GROUP="test-rg"\n'
            'AZVM_NAME_PREFIX="runner-"\nAZVM_COUNT="2"\n'
        )
        self.environment = {
            **os.environ, "PATH": str(self.bin) + os.pathsep + os.environ["PATH"],
            "HOME": str(self.directory / "home"), "TEST_AZURE_STATE": str(self.state_path),
            "TEST_AZURE_TRACE": str(self.trace),
        }
        self.state = {"resources": [], "peerings": {}}
        self.control_net = self.add_network("control-vnet", "10.40.0.0/16")
        self.runner_net = self.add_network("runner-vnet", "10.50.0.0/16")
        self.add_vm("control", self.control_net, "10.40.0.4")
        for number in range(1, 21):
            self.add_vm(f"runner-{number:02d}", self.runner_net, f"10.50.0.{number + 3}")
        self.save()

    def save(self):
        self.state_path.write_text(json.dumps(self.state))

    def load(self):
        self.state = json.loads(self.state_path.read_text())

    def add_network(self, name, prefix):
        network = rid("Microsoft.Network/virtualNetworks", name, "network-rg")
        self.state["resources"].append(
            {"id": network, "name": name, "addressSpace": {"addressPrefixes": [prefix]}}
        )
        return network

    def add_vm(self, name, network, address):
        nic = rid("Microsoft.Network/networkInterfaces", name + "-nic")
        self.state["resources"].extend([
            {"id": rid("Microsoft.Compute/virtualMachines", name), "name": name,
             "networkProfile": {"networkInterfaces": [{"id": nic}]}},
            {"id": nic, "name": name + "-nic", "ipConfigurations": [
                {"privateIPAddress": address, "subnet": {"id": network + "/subnets/default"}}
            ]},
        ])

    def resource(self, name):
        return next(item for item in self.state["resources"] if item["name"] == name)

    def existing(self, source, target, name="existing", **changes):
        item = {"name": name, "remoteVirtualNetwork": {"id": target},
                "allowVirtualNetworkAccess": True, "provisioningState": "Succeeded",
                "peeringState": "Connected", **changes}
        self.state["peerings"].setdefault(source.lower(), []).append(item)
        return item

    def calls(self):
        return [json.loads(line) for line in self.trace.read_text().splitlines()] if self.trace.exists() else []

    def creates(self):
        return [call for call in self.calls() if call[:4] == ["network", "vnet", "peering", "create"]]

    def invoke(self, *args, success=True):
        result = subprocess.run(
            ["bash", str(SCRIPT), "--env-file", str(self.env_file), "--control-vm", "control", *args],
            env=self.environment, text=True, capture_output=True,
        )
        output = result.stdout + result.stderr
        if success:
            self.assertEqual(result.returncode, 0, output)
        else:
            self.assertNotEqual(result.returncode, 0, output)
        return output

    def test_default_and_explicit_dry_runs_make_no_changes(self):
        before = self.state_path.read_bytes()
        for args in ((), ("--dry-run",)):
            output = self.invoke(*args)
            self.assertIn("DRY RUN", output)
            self.assertEqual(output.count("\nCREATE "), 2)
            self.assertIn("private=10.50.0.4", output)
            self.assertEqual(self.state_path.read_bytes(), before)
        self.assertEqual(self.creates(), [])
        self.assertFalse((self.directory / "home").exists())

    def test_all_twenty_shared_vnet_runners_need_only_two_links(self):
        output = self.invoke("--count", "20", "--apply")
        self.assertEqual(output.count("Runner:"), 20)
        self.assertEqual(len(self.creates()), 2)
        self.assertIn("All required peerings verified Connected", output)
        for command in self.creates():
            self.assertIn("--allow-vnet-access", command)
            self.assertNotIn("--allow-forwarded-traffic", command)
            self.assertNotIn("--allow-gateway-transit", command)
            self.assertNotIn("--use-remote-gateways", command)
        self.assertFalse(any(call[:2] == ["account", "set"] or call[:2] == ["vm", "create"]
                             or call[:2] == ["network", "nsg"] for call in self.calls()))

    def test_rerun_reuses_existing_links(self):
        self.invoke("--apply")
        output = self.invoke("--apply")
        self.assertEqual(len(self.creates()), 2)
        self.assertEqual(output.count("\nREUSE "), 2)
        self.assertNotIn("\nCREATE ", output)

    def test_initiated_one_way_peering_is_completed(self):
        self.existing(self.control_net, self.runner_net, peeringState="Initiated")
        self.save()
        self.invoke("--apply")
        self.assertEqual(len(self.creates()), 1)

    def test_only_control_to_spoke_links_are_created(self):
        third = self.add_network("other-runners", "10.60.0.0/16")
        config = self.resource("runner-03-nic")["ipConfigurations"][0]
        config["subnet"]["id"] = third + "/subnets/default"
        config["privateIPAddress"] = "10.60.0.4"
        self.save()
        self.invoke("--count", "3", "--apply")
        self.assertEqual(len(self.creates()), 4)
        self.load()
        for source, entries in self.state["peerings"].items():
            for entry in entries:
                self.assertTrue(source == self.control_net.lower()
                                or entry["remoteVirtualNetwork"]["id"] == self.control_net)

    def test_same_vnet_requires_no_peering(self):
        for number in (1, 2):
            config = self.resource(f"runner-{number:02d}-nic")["ipConfigurations"][0]
            config["subnet"]["id"] = self.control_net + "/subnets/default"
        self.save()
        output = self.invoke("--apply")
        self.assertIn("No peering is needed", output)
        self.assertEqual(self.creates(), [])

    def test_control_and_runner_overlap_fails_before_writes(self):
        self.resource("runner-vnet")["addressSpace"]["addressPrefixes"] = ["10.40.0.0/16"]
        self.save()
        output = self.invoke("--apply", success=False)
        self.assertIn("Overlapping address spaces", output)
        self.assertEqual(self.creates(), [])

    def test_overlapping_runner_vnets_are_rejected(self):
        other = self.add_network("overlapping-spoke", "10.50.0.0/24")
        self.resource("runner-02-nic")["ipConfigurations"][0]["subnet"]["id"] = other + "/subnets/default"
        self.save()
        output = self.invoke("--apply", success=False)
        self.assertIn("Overlapping address spaces", output)
        self.assertEqual(self.creates(), [])

    def test_missing_later_runner_prevents_all_writes(self):
        self.state["resources"].remove(self.resource("runner-02"))
        self.save()
        self.invoke("--apply", success=False)
        self.assertEqual(self.creates(), [])

    def test_invalid_network_metadata_fails_without_writes(self):
        self.resource("runner-02-nic")["ipConfigurations"][0]["privateIPAddress"] = 1
        self.save()
        output = self.invoke("--apply", success=False)
        self.assertIn("invalid private IP address", output)
        self.assertEqual(self.creates(), [])

    def test_disabled_or_disconnected_existing_peering_is_not_overwritten(self):
        for changes in ({"allowVirtualNetworkAccess": False}, {"peeringState": "Disconnected"}):
            with self.subTest(changes=changes):
                self.state["peerings"] = {}
                self.existing(self.control_net, self.runner_net, **changes)
                self.save()
                output = self.invoke("--apply", success=False)
                self.assertIn("disabled or unhealthy", output)
                self.assertEqual(self.creates(), [])

    def test_generated_name_collision_is_not_overwritten(self):
        other = self.add_network("unrelated", "10.70.0.0/16")
        name = "pg-benchmark-" + hashlib.sha256(self.runner_net.lower().encode()).hexdigest()[:16]
        self.existing(self.control_net, other, name=name)
        self.save()
        output = self.invoke("--apply", success=False)
        self.assertIn("refusing to overwrite", output)
        self.assertEqual(self.creates(), [])

    def test_unrelated_cross_subscription_peering_is_preserved(self):
        remote = self.runner_net.replace(SUBSCRIPTION, "00000000-0000-0000-0000-000000000002")
        self.existing(self.control_net, remote)
        self.save()
        self.invoke("--apply")
        self.assertEqual(len(self.creates()), 2)
        self.load()
        self.assertEqual(self.state["peerings"][self.control_net.lower()][0]["remoteVirtualNetwork"]["id"], remote)

    def test_partial_apply_failure_can_be_replanned_and_retried(self):
        self.state["fail_peering_create"] = 2
        self.save()
        output = self.invoke("--apply", success=False)
        self.assertIn("some peerings may remain", output)
        self.load()
        self.assertEqual(sum(map(len, self.state["peerings"].values())), 1)
        self.assertIn("\nREUSE ", self.invoke("--dry-run"))
        self.invoke("--apply")
        self.load()
        self.assertEqual(sum(map(len, self.state["peerings"].values())), 2)

    def test_failed_connection_verification_cannot_report_success(self):
        self.state["fail_peering_wait"] = True
        self.save()
        output = self.invoke("--apply", success=False)
        self.assertNotIn("All required peerings verified Connected", output)
        self.assertIn("Synthetic peering connection wait failure", output)

    def test_control_vm_in_another_resource_group_is_supported(self):
        self.resource("control")["id"] = rid("Microsoft.Compute/virtualMachines", "control", "control-rg")
        self.save()
        self.invoke("--control-resource-group", "control-rg", "--apply")
        self.assertEqual(len(self.creates()), 2)

    def test_ambiguous_primary_nic_fails_without_writes(self):
        refs = self.resource("control")["networkProfile"]["networkInterfaces"]
        refs.append({"id": rid("Microsoft.Network/networkInterfaces", "secondary")})
        self.save()
        output = self.invoke("--apply", success=False)
        self.assertIn("exactly one primary", output)
        self.assertEqual(self.creates(), [])

    def test_invalid_selection_fails_before_azure_calls(self):
        for args in (("--count", "0"), ("--count", "21"), ("--count", "2", "--start", "20"),
                     ("--name-prefix", "Bad"), ("--control-vm", "bad;name"),
                     ("--dry-run", "--apply")):
            with self.subTest(args=args):
                self.invoke(*args, success=False)
                self.assertEqual(self.calls(), [])

    def test_help_requires_neither_environment_file_nor_azure_login(self):
        result = subprocess.run(["bash", str(SCRIPT), "--help"], env=self.environment,
                                text=True, capture_output=True)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("--control-vm", result.stdout)
        self.assertEqual(self.calls(), [])


if __name__ == "__main__":
    unittest.main()
