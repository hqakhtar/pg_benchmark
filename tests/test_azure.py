#!/usr/bin/env python3
"""Offline regression tests for Azure wrapper actions and runner inventories."""

import contextlib
import importlib.util
import io
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import unittest
from unittest import mock


ROOT = Path(__file__).resolve().parents[1]
HELPER = ROOT / "setup/azure/azure-vm-hosts.py"
spec = importlib.util.spec_from_file_location("azure_hosts", HELPER)
hosts_helper = importlib.util.module_from_spec(spec)
spec.loader.exec_module(hosts_helper)


class AzureTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory(prefix="pg-benchmark-azure-")
        self.addCleanup(self.temporary.cleanup)
        self.directory = Path(self.temporary.name)
        self.checkout = self.directory / "checkout 'quoted"
        self.checkout.mkdir()
        (self.checkout / "lib").mkdir()
        shutil.copy(ROOT / "wrapper.sh", self.checkout)
        shutil.copy(ROOT / "lib/common.sh", self.checkout / "lib")
        shutil.copytree(ROOT / "setup/azure", self.checkout / "setup/azure")
        self.caller = self.directory / "caller"
        self.caller.mkdir()
        self.home = self.directory / "home"
        (self.home / ".ssh").mkdir(parents=True, mode=0o700)
        self.key = self.home / ".ssh/id_ed25519"
        subprocess.run(["ssh-keygen", "-q", "-t", "ed25519", "-N", "", "-f", str(self.key)], check=True)
        self.bin = self.directory / "bin"
        self.bin.mkdir()
        shutil.copy(ROOT / "tests/fixtures/fake-azure.py", self.bin / "az")
        (self.bin / "az").chmod(0o700)
        self.state_path = self.directory / "state.json"
        self.write_state(resources=[])
        self.trace = self.directory / "trace.jsonl"
        self.hosts = self.checkout / "hosts.txt"
        self.env_file = self.checkout / "setup/azure/azure-vms.env"
        self.env_file.write_text(
            'AZVM_SUBSCRIPTION="test"\nAZVM_RESOURCE_GROUP="test-rg"\n'
            'AZVM_LOCATION="eastus"\nAZVM_NAME_PREFIX="test-vm-r"\n'
            'AZVM_COUNT="2"\nAZVM_START="1"\nAZVM_PUBLIC_IP="false"\n'
            'AZVM_UPDATE_SSH_CONFIG="true"\nAZVM_SSH_CONFIG_MODE="single"\n'
            f'AZVM_SSH_KEY="{self.key}.pub"\nAZVM_SSH_PRIVATE_KEY="{self.key}"\n'
        )
        self.environment = {
            **os.environ, "PATH": str(self.bin) + os.pathsep + os.environ["PATH"],
            "HOME": str(self.home), "TEST_AZURE_STATE": str(self.state_path),
            "TEST_AZURE_TRACE": str(self.trace),
            "XDG_STATE_HOME": str(self.home / ".local/state"),
            "CONNECTION_ENV_FILE": "/nonexistent/poison.env",
            "BENCHMARK_ENV_FILE": "/nonexistent/poison.env",
            "RUN_ENV_FILE": "/nonexistent/poison.env",
        }

    def write_state(self, **changes):
        state = json.loads(self.state_path.read_text()) if self.state_path.exists() else {}
        state.update(changes)
        self.state_path.write_text(json.dumps(state))

    def invoke(self, *args, status=0, input=""):
        result = subprocess.run(
            ["bash", str(self.checkout / "wrapper.sh"), *args],
            cwd=self.caller, env=self.environment, input=input,
            stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True,
        )
        if status == 0:
            self.assertEqual(result.returncode, 0, result.stdout)
        else:
            self.assertNotEqual(result.returncode, 0, result.stdout)
        return result

    def test_create_and_delete_preserve_unrelated_hosts_and_comments(self):
        original = b"# Existing runners\r\nother-host # keep\r\n\r\n"
        self.hosts.write_bytes(original)
        self.invoke("--azure-create")
        self.assertEqual(self.hosts.read_bytes(), original + b"test-vm-r01\ntest-vm-r02\n")
        config = (self.home / ".ssh/config").read_text()
        self.assertIn("Host test-vm-r01\n", config)
        self.invoke("--azure-delete", "--", "--count", "1", "--yes")
        self.assertEqual(self.hosts.read_bytes(), original + b"test-vm-r02\n")
        self.assertFalse((self.caller / "hosts.txt").exists())

    def test_missing_hosts_file_is_created_privately(self):
        self.invoke("--azure-create", "--", "--start", "3", "--count", "1")
        self.assertEqual(self.hosts.read_text(), "test-vm-r03\n")
        self.assertEqual(self.hosts.stat().st_mode & 0o777, 0o600)

    def test_dry_runs_do_not_modify_local_files(self):
        self.invoke("--azure-create", "--", "--dry-run")
        self.assertFalse(self.hosts.exists())
        self.assertFalse(Path(str(self.hosts) + ".lock").exists())
        self.hosts.write_text("# Keep\nother-host\ntest-vm-r01\n")
        before = self.hosts.read_bytes()
        self.invoke("--azure-delete", "--", "--dry-run")
        self.assertEqual(self.hosts.read_bytes(), before)
        self.assertFalse((self.home / ".ssh/config").exists())
        self.assertEqual(json.loads(self.state_path.read_text())["resources"], [])

    def test_partial_create_tracks_only_successful_vms(self):
        self.hosts.write_text("other-host\n")
        self.write_state(fail_create="test-vm-r02")
        self.invoke("--azure-create", status=1)
        self.assertEqual(self.hosts.read_text(), "other-host\ntest-vm-r01\n")

    def test_failed_delete_preserves_hosts(self):
        self.invoke("--azure-create", "--", "--count", "1")
        before = self.hosts.read_bytes()
        self.write_state(fail_delete="test-vm-r01")
        self.invoke("--azure-delete", "--", "--count", "1", "--yes", status=1)
        self.assertEqual(self.hosts.read_bytes(), before)

    def test_skipped_and_cancelled_deletions_preserve_hosts(self):
        self.hosts.write_text("other-host\ntest-vm-r01\ntest-vm-r02\n")
        before = self.hosts.read_bytes()
        self.invoke("--azure-delete", "--", "--interactive", input="n\nn\n")
        self.assertEqual(self.hosts.read_bytes(), before)
        self.invoke("--azure-delete")
        self.assertEqual(self.hosts.read_bytes(), before)

    def test_absent_vms_are_removed_only_after_confirmation(self):
        self.hosts.write_text("other-host\ntest-vm-r01\ntest-vm-r02\n")
        self.invoke("--azure-delete", "--", "--count", "1", "--yes")
        self.assertEqual(self.hosts.read_text(), "other-host\ntest-vm-r02\n")

    def test_unmanaged_absent_deletion_updates_hosts(self):
        self.hosts.write_text("other-host\ntest-vm-r01\n")
        self.invoke("--azure-delete", "--", "--include-unmanaged", "--yes")
        self.assertEqual(self.hosts.read_text(), "other-host\n")

    def test_unmanaged_dry_runs_and_skips_preserve_hosts(self):
        self.hosts.write_text("other-host\ntest-vm-r01\n")
        before = self.hosts.read_bytes()
        self.invoke("--azure-delete", "--", "--include-unmanaged", "--dry-run")
        self.assertEqual(self.hosts.read_bytes(), before)
        self.invoke("--azure-delete", "--", "--include-unmanaged", "--interactive", input="n\nn\n")
        self.assertEqual(self.hosts.read_bytes(), before)

    def test_interactive_delete_removes_only_approved_alias(self):
        self.hosts.write_text("other-host\ntest-vm-r01\ntest-vm-r02\n")
        self.invoke("--azure-delete", "--", "--interactive", input="y\nn\n")
        self.assertEqual(self.hosts.read_text(), "other-host\ntest-vm-r02\n")

    def test_failed_inventory_read_never_removes_hosts(self):
        self.hosts.write_text("test-vm-r01\n")
        self.write_state(fail_inventory=True)
        self.invoke("--azure-delete", "--", "--yes", status=1)
        self.assertEqual(self.hosts.read_text(), "test-vm-r01\n")

    def test_custom_env_file_is_forwarded_without_shell_evaluation(self):
        selected = self.caller / "custom 'settings.env"
        selected.write_text(self.env_file.read_text().replace('AZVM_COUNT="2"', 'AZVM_COUNT="1"'))
        self.env_file.unlink()
        self.invoke("--azure-create", "--", "--env-file", selected.name)
        self.assertEqual(self.hosts.read_text(), "test-vm-r01\n")

    def test_invalid_action_combinations_do_not_invoke_azure(self):
        for args in (
            ("--azure-create", "hammerdb"),
            ("--azure-create", "--setup"),
            ("--azure-delete", "--host", "host"),
            ("--azure-create", "--run-env", "config"),
            ("--azure-create", "--", "--hosts-file", "other.txt"),
        ):
            with self.subTest(args=args):
                self.invoke(*args, status=1)
        self.assertFalse(self.trace.exists())

    def test_help_requires_no_configuration_or_azure(self):
        self.env_file.unlink()
        for action in ("--azure-create", "--azure-delete"):
            result = self.invoke(action, "--", "--help")
            self.assertIn("--hosts-file", result.stdout)
        self.assertFalse(self.trace.exists())
        self.assertFalse(self.hosts.exists())

    def test_invalid_inventory_fails_before_azure_changes(self):
        self.hosts.write_text("[runners]\n")
        self.invoke("--azure-create", status=1)
        self.assertFalse(self.trace.exists())

    def test_host_helper_is_idempotent_and_preserves_no_final_newline(self):
        self.hosts.write_text("# Keep\nother-host")
        hosts_helper.update_hosts("add", self.hosts, ["test-vm-r01"])
        expected = "# Keep\nother-host\ntest-vm-r01\n"
        self.assertEqual(self.hosts.read_text(), expected)
        hosts_helper.update_hosts("add", self.hosts, ["test-vm-r01"])
        self.assertEqual(self.hosts.read_text(), expected)
        hosts_helper.update_hosts("remove", self.hosts, ["test-vm-r01"])
        self.assertEqual(self.hosts.read_text(), "# Keep\nother-host\n")

    def test_missing_file_is_not_created_by_delete(self):
        self.invoke("--azure-delete", "--", "--yes")
        self.assertFalse(self.hosts.exists())

    def test_symlink_inventory_is_rejected(self):
        target = self.directory / "target"
        target.write_text("other-host\n")
        self.hosts.symlink_to(target)
        self.invoke("--azure-create", status=1)
        self.assertEqual(target.read_text(), "other-host\n")
        self.assertFalse(self.trace.exists())

    def test_concurrent_additions_preserve_all_hosts_and_permissions(self):
        self.hosts.write_text("other-host\n")
        self.hosts.chmod(0o640)
        names = [f"test-vm-r{index:02d}" for index in range(1, 11)]
        processes = [
            subprocess.Popen(
                [sys.executable, str(HELPER), "add", str(self.hosts), name],
                stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
            ) for name in names
        ]
        for process in processes:
            stdout, stderr = process.communicate(timeout=10)
            self.assertEqual(process.returncode, 0, stdout + stderr)
        self.assertEqual(set(self.hosts.read_text().splitlines()), {"other-host", *names})
        self.assertEqual(self.hosts.stat().st_mode & 0o777, 0o640)

    def configure_split(self, controller=True, custom_paths=True):
        contents = self.env_file.read_text().replace('AZVM_PUBLIC_IP="false"', 'AZVM_PUBLIC_IP="true"')
        contents = contents.replace('AZVM_SSH_CONFIG_MODE="single"', 'AZVM_SSH_CONFIG_MODE="split"')
        contents += 'AZVM_SSH_SOURCE="203.0.113.0/24"\n'
        if custom_paths:
            contents += (f'AZVM_SSH_PUBLIC_CONFIG_FILE="{self.home}/.ssh/pg-benchmark-public.conf"\n'
                         f'AZVM_SSH_PRIVATE_CONFIG_FILE="{self.home}/.ssh/pg-benchmark-private.conf"\n')
        if controller:
            contents += 'AZVM_CONTROL_VM="controller"\nAZVM_CONTROL_RESOURCE_GROUP="control-rg"\n'
        self.env_file.write_text(contents)
        return self.add_controller() if controller else []

    def add_controller(self):
        subscription = "00000000-0000-0000-0000-000000000001"
        network = f"/subscriptions/{subscription}/resourceGroups/network-rg/providers/Microsoft.Network/virtualNetworks/controller-vnet"
        nic = f"/subscriptions/{subscription}/resourceGroups/control-rg/providers/Microsoft.Network/networkInterfaces/controller-nic"
        vm = f"/subscriptions/{subscription}/resourceGroups/control-rg/providers/Microsoft.Compute/virtualMachines/controller"
        resources = [
            {"id": network, "name": "controller-vnet", "type": "Microsoft.Network/virtualNetworks",
             "location": "eastus", "tags": {}, "addressSpace": {"addressPrefixes": ["10.40.0.0/16"]}},
            {"id": network + "/subnets/default", "name": "default",
             "type": "Microsoft.Network/virtualNetworks/subnets", "addressPrefix": "10.40.0.0/24"},
            {"id": nic, "name": "controller-nic", "type": "Microsoft.Network/networkInterfaces",
             "ipConfigurations": [{"primary": True, "privateIPAddress": "10.40.0.4",
                                   "subnet": {"id": network + "/subnets/default"}}]},
            {"id": vm, "name": "controller", "type": "Microsoft.Compute/virtualMachines",
             "networkProfile": {"networkInterfaces": [{"id": nic, "primary": True}]}},
        ]
        state = json.loads(self.state_path.read_text())
        self.write_state(resources=state["resources"] + resources)
        return resources

    def azure_calls(self):
        return [json.loads(line) for line in self.trace.read_text().splitlines()] if self.trace.exists() else []

    def assert_no_azure_creates(self):
        self.assertFalse([call for call in self.azure_calls() if "create" in call])

    def ssh_parameters(self, alias, config=None):
        result = subprocess.run(
            ["ssh", "-G", "-F", str(config or self.home / ".ssh/config"),
             "-o", "CanonicalizeHostname=no", alias],
            env=self.environment, text=True, capture_output=True,
        )
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        return dict(line.split(" ", 1) for line in result.stdout.splitlines() if " " in line)

    def paired_helper(self, action, name="test-vm-r01", status=0, *args):
        result = subprocess.run(
            [sys.executable, str(self.checkout / "setup/azure/azure-vm-ssh-config.py"), action,
             "--config", str(self.home / ".ssh/config"),
             "--public-config", str(self.home / ".ssh/pg-benchmark-public.conf"),
             "--private-config", str(self.home / ".ssh/pg-benchmark-private.conf"),
             "--subscription", "00000000-0000-0000-0000-000000000001",
             "--resource-group", "test-rg", "--vm-name", name, *args],
            env=self.environment, text=True, capture_output=True,
        )
        if status == 0:
            self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        else:
            self.assertNotEqual(result.returncode, 0, result.stdout + result.stderr)
        return result

    def test_controller_subnet_dual_access_and_include_only_cleanup(self):
        controller = self.configure_split()
        main = self.home / ".ssh/config"
        original = b"# Keep my config\r\nHost *\r\n    User other\r\nHost unrelated\r\n    HostName 127.0.0.1\r\n"
        main.write_bytes(original)
        public = self.home / ".ssh/pg-benchmark-public.conf"
        private = self.home / ".ssh/pg-benchmark-private.conf"
        result = self.invoke("--azure-create")
        expected = (f'Include "{public}"\r\nInclude "{private}"\r\n').encode() + original
        self.assertEqual(main.read_bytes(), expected)
        self.assertIn("Network source: controller", result.stdout)
        resources = json.loads(self.state_path.read_text())["resources"]
        self.assertEqual([item for item in resources if item in controller], controller)
        subnet = controller[1]["id"]
        for number in (1, 2):
            name = f"test-vm-r{number:02d}"
            nic = next(item for item in resources if item["name"] == name + "-nic")
            pip = next(item for item in resources if item["name"] == name + "-pip")
            self.assertEqual(nic["ipConfigurations"][0]["subnet"]["id"], subnet)
            self.assertEqual(self.ssh_parameters(name)["hostname"], pip["ipAddress"])
            self.assertEqual(self.ssh_parameters(name + "-private")["hostname"],
                             nic["ipConfigurations"][0]["privateIPAddress"])
            for alias, config in ((name, public), (name + "-private", private)):
                settings = self.ssh_parameters(alias, config)
                self.assertEqual(settings["user"], "azuresu")
                self.assertEqual(settings["port"], "22")
                self.assertEqual(settings["identityfile"], str(self.key))
                self.assertEqual(settings["identitiesonly"], "yes")
        self.assertEqual(public.stat().st_mode & 0o777, 0o600)
        self.assertEqual(private.stat().st_mode & 0o777, 0o600)
        self.assertEqual(self.hosts.read_text(), "test-vm-r01\ntest-vm-r02\n")
        self.assertFalse([call for call in self.azure_calls() if call[:3] == ["network", "vnet", "create"]])
        self.assertFalse([call for call in self.azure_calls() if "peering" in call])
        rules = [call for call in self.azure_calls() if call[:4] == ["network", "nsg", "rule", "create"]]
        self.assertEqual(len(rules), 2)
        for rule in rules:
            self.assertEqual(rule[rule.index("--source-address-prefixes") + 1], "203.0.113.0/24")
            self.assertEqual(rule[rule.index("--destination-port-ranges") + 1], "22")
        self.invoke("--azure-delete", "--", "--count", "1", "--yes")
        self.assertEqual(main.read_bytes(), expected)
        self.assertNotIn("Host test-vm-r01\n", public.read_text())
        self.assertNotIn("Host test-vm-r01-private\n", private.read_text())
        self.assertIn("Host test-vm-r02-private\n", private.read_text())
        remaining = json.loads(self.state_path.read_text())["resources"]
        self.assertEqual([item for item in remaining if item in controller], controller)

    def test_custom_split_paths_and_repeated_includes(self):
        self.configure_split()
        directory = self.directory / 'custom "quoted % configs'
        public = directory / "public.conf"
        private = directory / "private.conf"
        main = self.directory / "main config"
        original = b"Host unrelated\n    HostName 127.0.0.1\n"
        main.write_bytes(original)
        options = ("--ssh-config-file", str(main), "--ssh-public-config-file", str(public),
                   "--ssh-private-config-file", str(private))
        self.invoke("--azure-create", "--", *options)
        before = main.read_bytes()
        self.assertTrue(before.endswith(original))
        self.assertEqual(self.ssh_parameters("test-vm-r01", main)["hostname"], "203.0.113.4")
        self.assertEqual(self.ssh_parameters("test-vm-r01-private", main)["hostname"], "10.40.0.5")
        self.invoke("--azure-create", "--", *options, "--start", "3", "--count", "1")
        self.assertEqual(main.read_bytes(), before)
        self.assertEqual(main.read_text().count("Include "), 2)
        self.assertEqual(self.ssh_parameters("test-vm-r03-private", main)["hostname"], "10.40.0.7")
        self.assertFalse((self.home / ".ssh/config").exists())

    def test_default_configs_are_flat_in_wrapper_working_directory(self):
        self.configure_split(custom_paths=False)
        self.environment["AZVM_WORK_DIR"] = "/nonexistent/inherited-directory"
        public = self.caller / "sshconfig-pg-benchmark-public.conf"
        private = self.caller / "sshconfig-pg-benchmark-private.conf"
        main = self.home / ".ssh/config"
        original = b"# Keep existing config\nHost unrelated\n    HostName 127.0.0.1\n"
        main.write_bytes(original)
        self.invoke("--azure-create", "--", "--count", "1")
        expected = f'Include "{public}"\nInclude "{private}"\n'.encode() + original
        self.assertEqual(main.read_bytes(), expected)
        self.assertEqual(self.ssh_parameters("test-vm-r01")["hostname"], "203.0.113.4")
        self.assertEqual(self.ssh_parameters("test-vm-r01-private")["hostname"], "10.40.0.5")
        for file in (public, private):
            self.assertTrue(file.is_file())
            self.assertEqual(file.stat().st_mode & 0o777, 0o600)
            self.assertFalse((self.checkout / file.name).exists())
            self.assertFalse((self.env_file.parent / file.name).exists())
            self.assertFalse((self.home / ".ssh" / file.name).exists())
        self.assertFalse((self.caller / ".ssh").exists())
        self.assertFalse((self.caller / "sshconfig-pg-benchmark").exists())
        self.invoke("--azure-delete", "--", "--count", "1", "--yes")
        self.assertEqual(public.read_bytes(), b"")
        self.assertEqual(private.read_bytes(), b"")
        self.assertEqual(main.read_bytes(), expected)

    def test_standalone_working_dir_defaults_and_explicit_cleanup_elsewhere(self):
        self.configure_split(custom_paths=False)
        working = self.directory / 'working "directory % with spaces'
        elsewhere = self.directory / "other caller"
        working.mkdir()
        elsewhere.mkdir()
        public = working / "sshconfig-pg-benchmark-public.conf"
        private = working / "sshconfig-pg-benchmark-private.conf"

        def run_script(action, cwd, *options):
            result = subprocess.run(
                ["bash", str(self.checkout / f"setup/azure/{action}-azure-vms.sh"),
                 "--env-file", str(self.env_file), "--count", "1", *options],
                cwd=cwd, env=self.environment, text=True, capture_output=True,
            )
            self.assertEqual(result.returncode, 0, result.stdout + result.stderr)

        run_script("create", working)
        self.assertEqual(self.ssh_parameters("test-vm-r01")["hostname"], "203.0.113.4")
        self.assertEqual(self.ssh_parameters("test-vm-r01-private")["hostname"], "10.40.0.5")
        main = self.home / ".ssh/config"
        before = main.read_bytes()
        run_script("delete", elsewhere, "--yes", "--ssh-public-config-file", str(public),
                   "--ssh-private-config-file", str(private))
        self.assertEqual(public.read_bytes(), b"")
        self.assertEqual(private.read_bytes(), b"")
        self.assertEqual(main.read_bytes(), before)
        self.assertFalse((elsewhere / public.name).exists())
        self.assertFalse((elsewhere / private.name).exists())
        self.assertFalse(self.hosts.exists())

    def test_working_directory_defaults_are_not_created_by_dry_run(self):
        self.configure_split(custom_paths=False)
        before = self.state_path.read_bytes()
        self.invoke("--azure-create", "--", "--dry-run")
        self.assertEqual(list(self.caller.iterdir()), [])
        self.assertEqual(self.state_path.read_bytes(), before)
        self.assertFalse((self.home / ".ssh/config").exists())

    def test_explicit_relative_output_paths_still_use_environment_file_directory(self):
        self.configure_split(custom_paths=False)
        public = self.env_file.parent / "public-custom.conf"
        private = self.env_file.parent / "private-custom.conf"
        self.invoke("--azure-create", "--", "--count", "1", "--ssh-public-config-file", public.name,
                    "--ssh-private-config-file", private.name)
        self.assertTrue(public.is_file())
        self.assertTrue(private.is_file())
        self.assertFalse((self.caller / "sshconfig-pg-benchmark-public.conf").exists())
        self.assertFalse((self.caller / "sshconfig-pg-benchmark-private.conf").exists())
        self.assertEqual(self.ssh_parameters("test-vm-r01", public)["hostname"], "203.0.113.4")
        self.assertEqual(self.ssh_parameters("test-vm-r01-private", private)["hostname"], "10.40.0.5")

    def test_existing_global_include_and_unmanaged_output_content_are_preserved(self):
        self.configure_split()
        public = self.home / ".ssh/pg-benchmark-public.conf"
        private = self.home / ".ssh/pg-benchmark-private.conf"
        public_original = b"# Public notes\nHost unrelated-public\n    HostName 127.0.0.1\n"
        private_original = b"# Private notes\nHost unrelated-private\n    HostName 127.0.0.2\n"
        public.write_bytes(public_original)
        private.write_bytes(private_original)
        main = self.home / ".ssh/config"
        original = f'Include "{self.home}/.ssh/pg-benchmark-*.conf"\nHost unrelated\n    User other\n'.encode()
        main.write_bytes(original)
        self.invoke("--azure-create", "--", "--count", "1")
        self.assertEqual(main.read_bytes(), original)
        self.assertTrue(public.read_bytes().endswith(public_original))
        self.assertTrue(private.read_bytes().endswith(private_original))
        self.assertEqual(self.ssh_parameters("test-vm-r01")["hostname"], "203.0.113.4")
        self.assertEqual(self.ssh_parameters("test-vm-r01-private")["hostname"], "10.40.0.5")

    def test_conditional_include_does_not_replace_global_runner_includes(self):
        self.configure_split()
        main = self.home / ".ssh/config"
        original = f'Host unrelated\n    Include "{self.home}/.ssh/pg-benchmark-*.conf"\n'.encode()
        main.write_bytes(original)
        self.invoke("--azure-create", "--", "--count", "1")
        self.assertTrue(main.read_bytes().endswith(original))
        self.assertEqual(main.read_text().count("Include "), 3)
        self.assertEqual(self.ssh_parameters("test-vm-r01")["hostname"], "203.0.113.4")
        self.assertEqual(self.ssh_parameters("test-vm-r01-private")["hostname"], "10.40.0.5")

    def test_explicit_subnet_in_controller_vnet_is_allowed(self):
        controller = self.configure_split()
        subnet = {**controller[1], "name": "runners",
                  "id": controller[0]["id"] + "/subnets/runners", "addressPrefix": "10.40.1.0/24"}
        self.write_state(resources=controller + [subnet])
        self.env_file.write_text(self.env_file.read_text() + 'AZVM_VNET_CIDR="unused"\nAZVM_SUBNET_CIDR="unused"\n')
        self.invoke("--azure-create", "--", "--subnet-id", subnet["id"])
        nics = [item for item in json.loads(self.state_path.read_text())["resources"]
                if item["name"].startswith("test-vm-r") and item["name"].endswith("-nic")]
        self.assertEqual(len(nics), 2)
        self.assertTrue(all(item["ipConfigurations"][0]["subnet"]["id"] == subnet["id"] for item in nics))

    def test_controller_uses_primary_nic_and_ip_not_first_entries(self):
        controller = self.configure_split()
        secondary = json.loads(json.dumps(controller[2]))
        secondary["id"] += "-secondary"
        secondary["name"] += "-secondary"
        secondary["ipConfigurations"][0]["subnet"]["id"] = controller[0]["id"] + "/subnets/secondary"
        secondary["ipConfigurations"][0]["privateIPAddress"] = "10.40.2.4"
        controller[3]["networkProfile"]["networkInterfaces"].insert(0, {"id": secondary["id"]})
        config = json.loads(json.dumps(controller[2]["ipConfigurations"][0]))
        config.pop("primary")
        config["subnet"]["id"] = controller[0]["id"] + "/subnets/secondary"
        controller[2]["ipConfigurations"].insert(0, config)
        self.write_state(resources=controller + [secondary])
        self.invoke("--azure-create")
        reads = [call for call in self.azure_calls() if call[:2] == ["vm", "show"]
                 and "--ids" in call and call[call.index("--ids") + 1] == controller[3]["id"]]
        self.assertEqual(len(reads), 1)
        nics = [item for item in json.loads(self.state_path.read_text())["resources"]
                if item["name"].startswith("test-vm-r") and item["name"].endswith("-nic")]
        self.assertTrue(all(item["ipConfigurations"][0]["subnet"]["id"] == controller[1]["id"] for item in nics))

    def test_controller_resource_group_defaults_to_runner_group(self):
        controller = self.configure_split()
        contents = self.env_file.read_text().replace('AZVM_CONTROL_RESOURCE_GROUP="control-rg"\n', "")
        self.env_file.write_text(contents)
        controller = json.loads(json.dumps(controller).replace("/control-rg/", "/test-rg/"))
        self.write_state(resources=controller)
        self.invoke("--azure-create", "--", "--count", "1")
        self.assertEqual(self.ssh_parameters("test-vm-r01-private")["hostname"], "10.40.0.5")

    def test_without_controller_default_split_preserves_managed_network_selection(self):
        self.configure_split(controller=False)
        self.env_file.write_text(self.env_file.read_text().replace('AZVM_SSH_CONFIG_MODE="split"\n', ""))
        result = self.invoke("--azure-create", "--", "--count", "1")
        self.assertIn("Network source: managed", result.stdout)
        self.assertEqual(self.ssh_parameters("test-vm-r01")["hostname"], "203.0.113.4")
        self.assertEqual(self.ssh_parameters("test-vm-r01-private")["hostname"], "10.50.0.4")

    def test_explicit_subnet_outside_controller_vnet_fails_without_writes(self):
        controller = self.configure_split()
        subnet = controller[1]["id"].replace("/controller-vnet/", "/other-vnet/")
        before = self.state_path.read_bytes()
        result = self.invoke("--azure-create", "--", "--subnet-id", subnet, status=1)
        self.assertIn("outside the controller VNet", result.stdout)
        self.assertEqual(self.state_path.read_bytes(), before)
        self.assert_no_azure_creates()
        self.assertFalse((self.home / ".ssh/config").exists())

    def test_invalid_controller_networks_stop_before_mutations(self):
        controller = self.configure_split()
        cases = ("missing", "ambiguous-nic", "ambiguous-ip", "foreign-nic", "foreign-subnet", "ipv6", "region")
        for case in cases:
            with self.subTest(case=case):
                resources = json.loads(json.dumps(controller))
                if case == "missing":
                    resources.pop()
                elif case == "ambiguous-nic":
                    refs = resources[3]["networkProfile"]["networkInterfaces"]
                    refs[0].pop("primary")
                    refs.append({"id": resources[2]["id"] + "-other"})
                elif case == "ambiguous-ip":
                    configs = resources[2]["ipConfigurations"]
                    configs[0].pop("primary")
                    configs.append(dict(configs[0]))
                elif case == "foreign-nic":
                    refs = resources[3]["networkProfile"]["networkInterfaces"]
                    refs[0]["id"] = refs[0]["id"].replace("000000000001", "000000000002")
                elif case == "foreign-subnet":
                    config = resources[2]["ipConfigurations"][0]
                    config["subnet"]["id"] = config["subnet"]["id"].replace("000000000001", "000000000002")
                elif case == "ipv6":
                    resources[2]["ipConfigurations"][0]["privateIPAddress"] = "fd00::4"
                else:
                    resources[0]["location"] = "westus"
                self.write_state(resources=resources)
                before = self.state_path.read_bytes()
                self.invoke("--azure-create", status=1)
                self.assertEqual(self.state_path.read_bytes(), before)
                self.assert_no_azure_creates()
                self.assertFalse((self.home / ".ssh/config").exists())

    def test_unexpected_network_response_ids_cannot_change_runner_vnet(self):
        controller = self.configure_split()
        for resource in controller[:2]:
            with self.subTest(resource=resource["name"]):
                self.write_state(wrong_show_ids={resource["id"]: resource["id"].replace("controller-vnet", "other-vnet")})
                before = self.state_path.read_bytes()
                result = self.invoke("--azure-create", status=1)
                self.assertIn("unexpected resource ID", result.stdout)
                self.assertEqual(self.state_path.read_bytes(), before)
                self.assert_no_azure_creates()

    def test_controller_mode_rejects_settings_that_break_dual_access(self):
        self.configure_split()
        for options in (("--no-public-ip",), ("--update-ssh-config", "false"),
                        ("--ssh-config-mode", "single"), ("--ssh-address", "private"),
                        ("--control-vm", "invalid;name")):
            with self.subTest(options=options):
                self.invoke("--azure-create", "--", *options, status=1)
                self.assertFalse(self.trace.exists())

    def test_controller_enables_ssh_updates_when_setting_is_omitted(self):
        self.configure_split()
        self.env_file.write_text(self.env_file.read_text().replace('AZVM_UPDATE_SSH_CONFIG="true"\n', "")
                                 .replace('AZVM_SSH_CONFIG_MODE="split"\n', ""))
        self.invoke("--azure-create", "--", "--count", "1")
        self.assertIn("Host test-vm-r01-private\n", (self.home / ".ssh/pg-benchmark-private.conf").read_text())
        self.invoke("--azure-delete", "--", "--count", "1", "--yes")
        self.assertEqual((self.home / ".ssh/pg-benchmark-private.conf").read_bytes(), b"")

    def test_split_dry_run_does_not_create_configs_directories_or_locks(self):
        self.configure_split()
        directory = self.directory / "not-created"
        before = self.state_path.read_bytes()
        self.invoke("--azure-create", "--", "--dry-run", "--ssh-config-file", str(directory / "main"),
                    "--ssh-public-config-file", str(directory / "public"),
                    "--ssh-private-config-file", str(directory / "private"))
        self.assertFalse(directory.exists())
        self.assertEqual(self.state_path.read_bytes(), before)
        self.assertFalse(self.hosts.exists())

    def test_split_conflicts_in_main_and_included_configs_are_rejected(self):
        self.configure_split()
        main = self.home / ".ssh/config"
        extra = self.home / ".ssh/other.conf"
        for alias, included in (("test-vm-r01", False), ("test-vm-r01-private", True)):
            with self.subTest(alias=alias):
                content = f"Host {alias}\n    HostName 127.0.0.1\n"
                if included:
                    extra.write_text(content)
                    main.write_text(f'Include "{extra}"\n')
                else:
                    main.write_text(content)
                before = main.read_bytes()
                result = self.invoke("--azure-create", status=1)
                self.assertIn("already exists", result.stdout)
                self.assertEqual(main.read_bytes(), before)
                self.assert_no_azure_creates()
                self.assertFalse((self.home / ".ssh/pg-benchmark-public.conf").exists())

    def test_split_config_and_lock_symlinks_fail_before_azure_writes(self):
        self.configure_split()
        target = self.directory / "untouched"
        target.write_text("# Keep\n")
        for filename in ("pg-benchmark-public.conf", "pg-benchmark-private.conf.azvm.lock"):
            with self.subTest(filename=filename):
                path = self.home / ".ssh" / filename
                path.symlink_to(target)
                self.invoke("--azure-create", status=1)
                self.assertEqual(target.read_text(), "# Keep\n")
                self.assert_no_azure_creates()
                path.unlink()

    def test_split_paths_cannot_alias_main_other_output_or_private_key(self):
        self.configure_split()
        for destination in (self.home / ".ssh/config", self.home / ".ssh/pg-benchmark-private.conf", self.key):
            with self.subTest(destination=destination):
                key = self.key.read_bytes()
                self.invoke("--azure-create", "--", "--ssh-public-config-file", str(destination), status=1)
                self.assertEqual(self.key.read_bytes(), key)
                self.assert_no_azure_creates()

    def test_split_failed_deletion_preserves_both_files(self):
        self.configure_split()
        self.invoke("--azure-create", "--", "--count", "1")
        files = [self.home / ".ssh" / name for name in ("config", "pg-benchmark-public.conf", "pg-benchmark-private.conf")]
        before = [path.read_bytes() for path in files]
        self.write_state(fail_delete="test-vm-r01")
        self.invoke("--azure-delete", "--", "--count", "1", "--yes", status=1)
        self.assertEqual([path.read_bytes() for path in files], before)

    def test_split_dry_and_skipped_deletions_preserve_both_files(self):
        self.configure_split()
        self.invoke("--azure-create", "--", "--count", "1")
        files = [self.home / ".ssh" / name for name in ("config", "pg-benchmark-public.conf", "pg-benchmark-private.conf")]
        before = [path.read_bytes() for path in files]
        self.invoke("--azure-delete", "--", "--count", "1", "--dry-run")
        self.invoke("--azure-delete", "--", "--count", "1", "--interactive", input="n\n")
        self.assertEqual([path.read_bytes() for path in files], before)

    def test_unmanaged_absent_deletion_removes_both_configs_and_preserves_includes(self):
        controller = self.configure_split()
        self.invoke("--azure-create", "--", "--count", "1")
        main = self.home / ".ssh/config"
        before = main.read_bytes()
        self.write_state(resources=controller)
        self.invoke("--azure-delete", "--", "--count", "1", "--include-unmanaged", "--yes")
        self.assertEqual(main.read_bytes(), before)
        for filename in ("pg-benchmark-public.conf", "pg-benchmark-private.conf"):
            self.assertEqual((self.home / ".ssh" / filename).read_bytes(), b"")
        self.assertEqual(json.loads(self.state_path.read_text())["resources"], controller)

    def make_runner_recoverable(self, name="test-vm-r01"):
        state = json.loads(self.state_path.read_text())
        vm_id = next(item["id"] for item in state["resources"] if item["name"] == name)
        fields = {"Microsoft.Compute/virtualMachines": "vmId", "Microsoft.Compute/disks": "uniqueId",
                  "Microsoft.Network/networkInterfaces": "resourceGuid",
                  "Microsoft.Network/publicIPAddresses": "resourceGuid",
                  "Microsoft.Network/networkSecurityGroups": "resourceGuid"}
        for number, resource in enumerate(state["resources"], 1):
            if resource["name"] == name or resource["name"].startswith(name + "-"):
                resource[fields[resource["type"]]] = f"10000000-0000-0000-0000-{number:012d}"
                if resource["type"] == "Microsoft.Compute/disks":
                    resource["managedBy"] = vm_id
                elif resource["type"] == "Microsoft.Network/networkInterfaces":
                    resource["virtualMachine"] = {"id": vm_id}
        self.write_state(resources=state["resources"])

    def test_attachment_driven_deletion_removes_both_config_entries(self):
        controller = self.configure_split()
        self.invoke("--azure-create", "--", "--count", "1")
        self.make_runner_recoverable()
        main = self.home / ".ssh/config"
        before = main.read_bytes()
        self.invoke("--azure-delete", "--", "--count", "1", "--include-unmanaged", "--yes")
        self.assertEqual(main.read_bytes(), before)
        self.assertEqual((self.home / ".ssh/pg-benchmark-public.conf").read_bytes(), b"")
        self.assertEqual((self.home / ".ssh/pg-benchmark-private.conf").read_bytes(), b"")
        self.assertEqual(json.loads(self.state_path.read_text())["resources"], controller)

    def test_deletion_recovery_binds_both_config_paths_and_can_resume(self):
        controller = self.configure_split()
        self.invoke("--azure-create", "--", "--count", "1")
        self.make_runner_recoverable()
        main = self.home / ".ssh/config"
        before = main.read_bytes()
        journals = self.directory / "journals"
        options = ("--count", "1", "--include-unmanaged", "--yes", "--delete-state-dir", str(journals))
        self.write_state(fail_delete="test-vm-r01-pip")
        self.invoke("--azure-delete", "--", *options, status=1)
        files = list(journals.glob("*.json"))
        self.assertEqual(len(files), 1)
        plan = json.loads(files[0].read_text())
        self.assertEqual(plan["ssh_config"], [str(self.home / ".ssh/pg-benchmark-public.conf"),
                                             str(self.home / ".ssh/pg-benchmark-private.conf")])
        changed_private = self.home / ".ssh/new-private.conf"
        result = self.invoke("--azure-delete", "--", *options, "--ssh-private-config-file",
                             str(changed_private), status=1)
        self.assertIn("same SSH-config setting", result.stdout)
        self.assertFalse(changed_private.exists())
        self.write_state(fail_delete=None)
        self.invoke("--azure-delete", "--", *options)
        self.assertFalse(list(journals.glob("*.json")))
        self.assertEqual(main.read_bytes(), before)
        self.assertEqual(json.loads(self.state_path.read_text())["resources"], controller)

    def test_absent_group_deletion_cleans_both_stale_configs(self):
        controller = self.configure_split()
        self.invoke("--azure-create", "--", "--count", "1")
        before = (self.home / ".ssh/config").read_bytes()
        self.write_state(resources=controller, group_exists=False)
        self.invoke("--azure-delete", "--", "--count", "1", "--yes")
        self.assertEqual((self.home / ".ssh/config").read_bytes(), before)
        self.assertEqual((self.home / ".ssh/pg-benchmark-public.conf").read_bytes(), b"")
        self.assertEqual((self.home / ".ssh/pg-benchmark-private.conf").read_bytes(), b"")

    def test_existing_managed_main_config_is_preserved_and_still_readable(self):
        self.invoke("--azure-create", "--", "--count", "1")
        main = self.home / ".ssh/config"
        original = main.read_bytes()
        self.configure_split()
        self.invoke("--azure-create", "--", "--count", "1", "--start", "2")
        self.assertTrue(main.read_bytes().endswith(original))
        self.assertEqual(main.read_text().count("Include "), 2)
        result = subprocess.run(
            [sys.executable, str(self.checkout / "setup/azure/azure-vm-ssh-config.py"), "check-remove",
             "--config", str(main), "--subscription", "00000000-0000-0000-0000-000000000001",
             "--resource-group", "test-rg", "--vm-name", "test-vm-r01"],
            env=self.environment, text=True, capture_output=True,
        )
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(json.loads(result.stdout)["managed_vm_names"], ["test-vm-r01"])

    def test_paired_alias_metadata_retains_original_vm_ownership(self):
        name = "r" * 64
        self.paired_helper("add-pair", name, 0, "--user", "azuresu", "--port", "22",
                           "--identity-file", str(self.key), "--hostname", "203.0.113.4",
                           "--private-hostname", "10.40.0.5")
        self.assertEqual(self.ssh_parameters(name + "-private")["hostname"], "10.40.0.5")
        state = self.paired_helper("check-remove-pair", name)
        self.assertEqual(json.loads(state.stdout)["managed_vm_names"], [name])
        self.paired_helper("remove-pair", name)
        self.assertEqual((self.home / ".ssh/pg-benchmark-private.conf").read_bytes(), b"")

    def test_paired_invalid_addresses_fail_without_local_changes(self):
        self.paired_helper("add-pair", "test-vm-r01", 1, "--user", "azuresu", "--port", "22",
                           "--identity-file", str(self.key), "--hostname", "203.0.113.4,10.40.0.5",
                           "--private-hostname", "10.40.0.5")
        self.assertEqual(sorted(path.name for path in (self.home / ".ssh").iterdir()),
                         ["id_ed25519", "id_ed25519.pub"])

    def test_missing_pair_removal_creates_no_files_or_locks(self):
        self.paired_helper("remove-pair")
        self.assertEqual(sorted(path.name for path in (self.home / ".ssh").iterdir()),
                         ["id_ed25519", "id_ed25519.pub"])

    def test_pair_uses_same_lock_order_and_includes_for_directory_aliases(self):
        helper_spec = importlib.util.spec_from_file_location(
            "azure_ssh_alias_tests", self.checkout / "setup/azure/azure-vm-ssh-config.py")
        helper = importlib.util.module_from_spec(helper_spec)
        helper_spec.loader.exec_module(helper)
        scope = ("00000000-0000-0000-0000-000000000001", "test-rg")
        original_lock = helper.mutation_lock
        for label in ("a-configs", "a-configs % with spaces"):
            with self.subTest(label=label):
                root = self.directory / label
                real = root / "a-real"
                main_dir = root / "b-main"
                alias = root / "z-alias"
                real.mkdir(parents=True)
                main_dir.mkdir()
                alias.symlink_to(real, target_is_directory=True)
                main = main_dir / "config"
                orders = []
                before = None
                for number, parent in enumerate((real, alias), 1):
                    order = []

                    @contextlib.contextmanager
                    def recording_lock(path):
                        order.append(path.resolve())
                        with original_lock(path):
                            yield

                    args = helper.argparse.Namespace(
                        action="add-pair", public_config=str(parent / "public.conf"),
                        private_config=str(parent / "private.conf"), user="azuresu", port=22,
                        hostname=f"203.0.113.{number + 3}", private_hostname=f"10.40.0.{number + 4}")
                    with mock.patch.object(helper, "mutation_lock", recording_lock), contextlib.redirect_stdout(io.StringIO()):
                        helper.pair_action(args, main, scope, [f"test-vm-r{number:02d}"], str(self.key))
                    orders.append(order)
                    if before is None:
                        before = main.read_bytes()
                self.assertEqual(orders[0], orders[1])
                self.assertEqual(main.read_bytes(), before)

    def test_pair_preflight_rejects_hard_links_to_keys_or_other_configs(self):
        main = self.home / ".ssh/config"
        public = self.home / ".ssh/pg-benchmark-public.conf"
        private = self.home / ".ssh/pg-benchmark-private.conf"
        main.write_bytes(b"# Main config\n")
        private.write_bytes(b"# Private config\n")
        for source in (self.key, main, private):
            with self.subTest(source=source.name):
                before = source.read_bytes()
                os.link(source, public)
                try:
                    self.paired_helper("check-add-pair", "test-vm-r01", 1, "--user", "azuresu",
                                       "--port", "22", "--identity-file", str(self.key))
                    self.assertEqual(source.read_bytes(), before)
                finally:
                    public.unlink()


if __name__ == "__main__":
    unittest.main()
