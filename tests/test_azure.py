#!/usr/bin/env python3
"""Offline regression tests for Azure wrapper actions and runner inventories."""

import importlib.util
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import unittest


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
            'AZVM_UPDATE_SSH_CONFIG="true"\n'
            f'AZVM_SSH_KEY="{self.key}.pub"\nAZVM_SSH_PRIVATE_KEY="{self.key}"\n'
        )
        self.environment = {
            **os.environ, "PATH": str(self.bin) + os.pathsep + os.environ["PATH"],
            "HOME": str(self.home), "TEST_AZURE_STATE": str(self.state_path),
            "TEST_AZURE_TRACE": str(self.trace),
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


if __name__ == "__main__":
    unittest.main()
