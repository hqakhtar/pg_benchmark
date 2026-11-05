#!/usr/bin/env python3
"""Test login-limit probes without sudo, PAM changes, or runner connections."""

import os
from pathlib import Path
import subprocess
import unittest

import yaml


ROOT = Path(__file__).resolve().parents[1]
PLAYBOOK = ROOT / "setup/ansible/runner_setup.yml"
ULIMIT_FIXTURE = """
ulimit() {
    case "$1" in
        -Sn) printf '%s\\n' "$TEST_NOFILE_SOFT" ;;
        -Hn) printf '%s\\n' "$TEST_NOFILE_HARD" ;;
        *) return 2 ;;
    esac
}
export -f ulimit
"""


class RunnerLimitTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        play = yaml.safe_load(PLAYBOOK.read_text())[0]
        block = next(
            task["block"] for task in play["tasks"]
            if task["name"] == "Configure HammerDB runner file-descriptor limits"
        )
        task = next(
            task for task in block
            if task.get("register") == "hammerdb_local_session_limits"
        )
        cls.probes = task["loop"]

    def run_probe(self, probe, soft="65536", hard="1048576", script=None):
        argv = probe["argv"]
        if probe["name"] == "sudo":
            self.assertEqual(argv[:4], ["/usr/bin/sudo", "-n", "-i", "-u"])
            command = [*argv[5:-1], script if script is not None else argv[-1]]
            # sudo -i backslash-escapes non-alphanumeric command characters.
            command = " ".join(
                "".join(
                    character if character.isascii() and (
                        character.isalnum() or character in "_-$"
                    ) else "\\" + character
                    for character in argument
                )
                for argument in command
            )
        else:
            self.assertEqual(
                argv[:5],
                ["/usr/bin/su", "--login", "--shell", "/bin/bash", "--command"],
            )
            command = script if script is not None else argv[5]
        return subprocess.run(
            ["/bin/bash", "-c", ULIMIT_FIXTURE + command],
            env={**os.environ, "TEST_NOFILE_SOFT": soft, "TEST_NOFILE_HARD": hard},
            capture_output=True, text=True, timeout=5,
        )

    def test_local_probes_emit_two_exact_limit_lines(self):
        for probe in self.probes:
            with self.subTest(session=probe["name"]):
                result = self.run_probe(probe)
                self.assertEqual(result.returncode, 0, result.stderr)
                self.assertEqual(result.stdout.splitlines(), [
                    "HAMMERDB_NOFILE_SOFT=65536",
                    "HAMMERDB_NOFILE_HARD=1048576",
                ])

    def test_probes_report_actual_values_instead_of_expected_constants(self):
        for probe in self.probes:
            with self.subTest(session=probe["name"]):
                result = self.run_probe(probe, soft="1024", hard="65536")
                self.assertEqual(result.returncode, 0, result.stderr)
                self.assertEqual(result.stdout.splitlines(), [
                    "HAMMERDB_NOFILE_SOFT=1024",
                    "HAMMERDB_NOFILE_HARD=65536",
                ])

    def test_original_multiline_sudo_probe_reproduces_reported_corruption(self):
        probe = next(probe for probe in self.probes if probe["name"] == "sudo")
        original = (
            "printf 'HAMMERDB_NOFILE_SOFT=%s\\n' \"$(ulimit -Sn)\"\n"
            "printf 'HAMMERDB_NOFILE_HARD=%s\\n' \"$(ulimit -Hn)\"\n"
        )
        result = self.run_probe(probe, script=original)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(result.stdout.splitlines(), [
            "HAMMERDB_NOFILE_SOFT=65536printf",
            r"HAMMERDB_NOFILE_SOFT=HAMMERDB_NOFILE_HARD=%s\n",
            "HAMMERDB_NOFILE_SOFT=1048576",
        ])


if __name__ == "__main__":
    unittest.main()
