#!/usr/bin/env python3
"""Offline tests for the standalone worker failure monitor."""

import importlib.util
import os
from pathlib import Path
import shlex
import shutil
import subprocess
import sys
import tempfile
import unittest


ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "monitor_workers.py"
spec = importlib.util.spec_from_file_location("monitor_workers", SCRIPT)
monitor_workers = importlib.util.module_from_spec(spec)
spec.loader.exec_module(monitor_workers)


class MonitorTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory(prefix="pg-benchmark-monitor-")
        self.addCleanup(self.temporary.cleanup)
        self.directory = Path(self.temporary.name)
        self.monitor = monitor_workers.Monitor([self.directory])

    def test_native_failure_messages(self):
        messages = [
            "Error in Virtual User 48: server closed the connection unexpectedly",
            "Vuser 64:FINISHED FAILED",
            "Error:Build virtual users must be less than or equal to number of warehouses",
            "FATAL: remaining connection slots are reserved",
            "PANIC: could not write to file",
            "Connection reset by 192.0.2.1 port 22",
            "ssh: connect to host 192.0.2.1 port 22: Connection timed out",
            "Permission denied (publickey).",
            "Host key verification failed.",
            "ssh: Could not resolve hostname runner: Name or service not known",
            "client_loop: send disconnect: Broken pipe",
            "Segmentation fault (core dumped)",
            "Killed by signal 9.",
            "Vuser 3:aborted",
        ]
        path = self.directory / "data.runner.log"
        for message in messages:
            with self.subTest(message=message):
                path.write_text(message + "\n")
                monitor = monitor_workers.Monitor([self.directory])
                with self.assertRaises(monitor_workers.WorkerFailure) as result:
                    monitor.scan()
                self.assertIn(f"{path}:1:", str(result.exception))

    def test_normal_logs_and_success_statuses(self):
        (self.directory / "output.log").write_text(
            "Changed connection:pg_host from localhost to runner\n"
            "Changed tpcc:pg_raiseerror from false to true\n"
            "Vuser 1:FINISHED SUCCESS\n"
            "TEST RESULT : System achieved 10 NOPM from 20 PostgreSQL TPM\n"
        )
        (self.directory / "output.log.status").write_text("elapsed_seconds=4\nexit_code=0\n")
        (self.directory / "status.json").write_text('{"exit_code": 0}')
        (self.directory / "run.json").write_text('{"exit_code": 0, "status": "succeeded"}')
        (self.directory / "connection.env").write_text("ERROR: must not be read")
        request = self.directory / ".request-123"
        request.mkdir()
        (request / "private.log").write_text("ERROR: must not be read")
        self.monitor.scan()
        self.monitor.scan()

    def test_all_status_formats_and_nonzero_exit_codes(self):
        for name, content in [
            ("data.runner.log.status", "elapsed_seconds=1\nexit_code=255\n"),
            ("status.json", '{"exit_code": 137}'),
            ("run.json", '{"exit_code": 1, "status": "failed"}'),
            ("run.json", '{"exit_code": 0, "status": "interrupted"}'),
        ]:
            with self.subTest(name=name, content=content):
                path = self.directory / name
                path.write_text(content)
                with self.assertRaises(monitor_workers.WorkerFailure):
                    self.monitor.scan()
                path.unlink()

    def test_incremental_append_and_partial_lines(self):
        path = self.directory / "output.log"
        path.write_bytes(b"healthy\nVuser 9:FINISHED FA")
        self.monitor.scan()
        first_offset = self.monitor.logs[path][1]
        self.monitor.scan()
        self.assertEqual(self.monitor.logs[path][1], first_offset)
        with path.open("ab") as stream:
            stream.write(b"ILED")
        with self.assertRaises(monitor_workers.WorkerFailure) as result:
            self.monitor.scan()
        self.assertIn(f"Source: {path}:2:", str(result.exception))
        self.assertIn("Diagnostic: Vuser 9:FINISHED FAILED", str(result.exception))

    def test_runner_and_phase_for_loader_logs_and_statuses(self):
        runner = "hamid-citus-hammerdb-1pb-r13.example.com"
        for phase in ("preflight", "cleanup", "bootstrap", "data", "finalize"):
            for suffix in (".log", ".log.status"):
                with self.subTest(phase=phase, suffix=suffix):
                    path = self.directory / f"{phase}.{runner}{suffix}"
                    path.write_text(
                        "exit_code=255\n" if suffix.endswith(".status")
                        else "Vuser 131:Connection to database failed\n"
                    )
                    with self.assertRaises(monitor_workers.WorkerFailure) as result:
                        self.monitor.scan()
                    message = str(result.exception)
                    self.assertIn(f"Runner: {runner}\n", message)
                    self.assertIn(f"Phase: {phase}\n", message)
                    self.assertIn(f"Source: {path}", message)
                    path.unlink()

    def test_runner_not_inferred_from_unrelated_log_names(self):
        for name in ("output.log", "hammerdb.log", "postgresql.log", "status.json"):
            with self.subTest(name=name):
                path = self.directory / name
                path.write_text('{"exit_code": 1}' if name.endswith(".json") else "ERROR\n")
                with self.assertRaises(monitor_workers.WorkerFailure) as result:
                    self.monitor.scan()
                self.assertIn(
                    "Runner: unknown (not recorded in this artifact)",
                    str(result.exception),
                )
                path.unlink()

    def test_cli_escapes_progress_controls_to_preserve_runner_and_source(self):
        path = self.directory / "data.runner-13.log"
        path.write_bytes(
            b"loading\x1b[2K\rVuser 131:Connection to database failed\n"
        )
        result = subprocess.run(
            [sys.executable, str(SCRIPT), str(self.directory)],
            capture_output=True, timeout=5,
        )
        self.assertEqual(result.returncode, 1)
        stderr = result.stderr.decode()
        self.assertIn("WORKER FAILURE:\nRunner: runner-13\nPhase: data\n", stderr)
        self.assertIn(f"Source: {path}:1:\n", stderr)
        self.assertIn(
            r"Diagnostic: loading\x1b[2K\rVuser 131:Connection to database failed",
            stderr,
        )
        self.assertNotIn("\r", stderr)
        self.assertNotIn("\x1b", stderr)

    def test_new_directories_and_multiple_roots(self):
        self.monitor.scan()
        second = self.directory / "iteration-2"
        second.mkdir()
        self.monitor.directories.append(second)
        (second / "hammerdb.log").write_text("Error in Virtual User 7: lost connection\n")
        with self.assertRaises(monitor_workers.WorkerFailure):
            self.monitor.scan()

    def test_truncation_and_replacement(self):
        path = self.directory / "output.log"
        path.write_text("healthy progress\n" * 10)
        self.monitor.scan()
        path.write_text("ERROR: truncated\n")
        with self.assertRaises(monitor_workers.WorkerFailure):
            self.monitor.scan()
        path.write_text("healthy\n")
        self.monitor.scan()
        replacement = self.directory / "replacement"
        replacement.write_text("ERROR: replaced\n")
        replacement.replace(path)
        with self.assertRaises(monitor_workers.WorkerFailure):
            self.monitor.scan()

    def test_status_updates_and_partial_writes(self):
        path = self.directory / "status.json"
        path.write_text('{"exit_code":')
        self.monitor.scan()
        path.write_text('{"exit_code": 0}')
        self.monitor.scan()
        path.write_text('{"exit_code": 42}')
        with self.assertRaises(monitor_workers.WorkerFailure):
            self.monitor.scan()

    def test_invalid_status_is_reported_after_retry(self):
        for name, content in [
            ("status.json", "{"),
            ("status.json", "[]"),
            ("status.json", '{"exit_code": "bad"}'),
            ("data.runner.log.status", "elapsed_seconds=1\n"),
        ]:
            with self.subTest(name=name, content=content):
                path = self.directory / name
                path.write_text(content)
                monitor = monitor_workers.Monitor([self.directory])
                monitor.scan()
                with self.assertRaisesRegex(ValueError, "Invalid status file"):
                    monitor.scan()
                path.unlink()

    def test_custom_pattern_and_non_utf8(self):
        (self.directory / "output.log").write_bytes(b"\xff\nworker disappeared\n")
        self.monitor.scan()
        monitor = monitor_workers.Monitor([self.directory], [r"worker disappeared"])
        with self.assertRaises(monitor_workers.WorkerFailure):
            monitor.scan()

    def test_cli_watches_until_new_worker_fails(self):
        process = subprocess.Popen(
            [sys.executable, str(SCRIPT), str(self.directory), "--interval", "0.02"],
            stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
        )
        try:
            self.assertIn("Monitoring:", process.stdout.readline())
            (self.directory / "data.runner.log.status").write_text(
                "elapsed_seconds=1\nexit_code=255\n"
            )
            stdout, stderr = process.communicate(timeout=5)
            self.assertEqual(process.returncode, 1, (stdout, stderr))
            self.assertIn("WORKER FAILURE:", stderr)
            self.assertIn("data.runner.log.status", stderr)
        finally:
            if process.poll() is None:
                process.terminate()
                process.communicate(timeout=5)

    def test_cli_invalid_inputs(self):
        for arguments in [
            [str(self.directory / "missing")],
            [str(self.directory), "--interval", "0"],
            [str(self.directory), "--interval", "nan"],
            [str(self.directory), "--interval", "inf"],
            [str(self.directory), "--failure-pattern", "["],
        ]:
            with self.subTest(arguments=arguments):
                result = subprocess.run(
                    [sys.executable, str(SCRIPT), *arguments],
                    capture_output=True, text=True, timeout=5,
                )
                self.assertEqual(result.returncode, 2, result.stderr)

    def test_wrapper_monitor_needs_no_runtime_config(self):
        path = self.directory / "run with spaces"
        path.mkdir()
        (path / "output.log").write_text("site-specific worker lost\n")
        result = subprocess.run(
            [
                "bash", str(ROOT / "wrapper.sh"), "--monitor", str(path),
                "--", "--interval", "0.01", "--failure-pattern", "worker lost",
            ],
            env={
                **os.environ,
                "CONNECTION_ENV_FILE": "/nonexistent/connection.env",
                "BENCHMARK_ENV_FILE": "/nonexistent/benchmark.env",
                "RUN_ENV_FILE": "/nonexistent/run.env",
            },
            cwd=self.directory, capture_output=True, text=True, timeout=5,
        )
        self.assertEqual(result.returncode, 1, result.stderr)
        self.assertIn(f"{path / 'output.log'}:1:", result.stderr)
        self.assertIn("WORKER FAILURE:", result.stderr)

    def test_wrapper_forwards_multiple_directories_and_monitor_errors(self):
        empty = self.directory / "empty"
        empty.mkdir()
        (self.directory / "output.log").write_text("FINISHED FAILED\n")
        result = subprocess.run(
            ["bash", str(ROOT / "wrapper.sh"), "--monitor", str(empty), str(self.directory)],
            capture_output=True, text=True, timeout=5,
        )
        self.assertEqual(result.returncode, 1, result.stderr)
        result = subprocess.run(
            [
                "bash", str(ROOT / "wrapper.sh"), "--monitor", str(empty),
                "--", "--failure-pattern", "[",
            ],
            capture_output=True, text=True, timeout=5,
        )
        self.assertEqual(result.returncode, 2, result.stderr)
        self.assertIn("MONITOR ERROR:", result.stderr)

    def test_wrapper_rejects_conflicting_options(self):
        for arguments in [
            ["hammerdb", "--monitor", str(self.directory)],
            ["--monitor", str(self.directory), "--load"],
            ["--monitor", str(self.directory), "--host", "runner"],
            ["--monitor", str(self.directory), "--run-env", "private.env"],
            ["--monitor", "--connection-env", "private.env"],
            ["--monitor", "--benchmark-env", "private.env"],
        ]:
            with self.subTest(arguments=arguments):
                result = subprocess.run(
                    ["bash", str(ROOT / "wrapper.sh"), *arguments],
                    capture_output=True, text=True, timeout=5,
                )
                self.assertEqual(result.returncode, 2, result.stderr)

    def configured_checkout(self):
        checkout = self.directory / "checkout"
        (checkout / "lib").mkdir(parents=True)
        shutil.copy(ROOT / "wrapper.sh", checkout)
        shutil.copy(SCRIPT, checkout)
        shutil.copy(ROOT / "lib/common.sh", checkout / "lib")
        return checkout

    def write_run_config(self, path, **settings):
        path.write_text("".join(
            f"{name}={shlex.quote(str(value))}\n"
            for name, value in settings.items()
        ))

    def make_run(self, root, name, failure=True, manifest=True):
        path = root / name
        path.mkdir(parents=True)
        if manifest:
            (path / "run.json").write_text('{"status": "running", "exit_code": 0}')
        if failure:
            (path / "output.log").write_text("Vuser 7:FINISHED FAILED\n")
        return path

    def test_selects_newest_configured_run_not_directory_mtime(self):
        runs = self.directory / "results"
        logs = self.directory / "logs"
        old = self.make_run(runs, "tpcc-20261104T100000Z-1-1")
        latest = self.make_run(runs, "tpcc-20261105T100000Z-2-2")
        self.make_run(runs, "other-20261106T100000Z-3-3")
        self.make_run(runs, "tpcc-20261107T100000Z-4-4", manifest=False)
        self.make_run(logs, "load-20261104T100000Z-5-5", manifest=False)
        os.utime(old, (2000000000, 2000000000))
        self.assertEqual(
            monitor_workers.newest_directory(runs, "tpcc", logs), latest
        )
        load = self.make_run(logs, "load-20261106T100000Z-6-6", manifest=False)
        self.assertEqual(monitor_workers.newest_directory(runs, "tpcc", logs), load)

    def test_print_load_start_uses_newest_timestamp_without_scanning_failures(self):
        logs = self.directory / "load logs"
        old = self.make_run(logs, "load-20261104T100000Z-1-1", manifest=False)
        latest = self.make_run(logs, "load-20261105T100000Z-2-2", manifest=False)
        os.utime(old, (2000000000, 2000000000))
        result = subprocess.run(
            [sys.executable, str(SCRIPT), "--load-log-root", str(logs),
             "--print-started-at"],
            capture_output=True, text=True, timeout=5,
        )
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(result.stdout, "20261105T100000Z\n")
        self.assertIn(f"Load logs: {latest}", result.stderr)

    def test_print_load_start_accepts_explicit_directory(self):
        load = self.make_run(
            self.directory, "load-20261104T100000Z-1-1", manifest=False,
        )
        result = subprocess.run(
            [sys.executable, str(SCRIPT), str(load), "--print-started-at"],
            capture_output=True, text=True, timeout=5,
        )
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(result.stdout, "20261104T100000Z\n")

    def test_print_load_start_rejects_missing_custom_multiple_and_invalid_dates(self):
        custom = self.make_run(self.directory, "custom logs", manifest=False)
        invalid = self.make_run(
            self.directory, "load-20260230T100000Z-1-1", manifest=False,
        )
        valid = self.make_run(
            self.directory, "load-20261104T100000Z-1-1", manifest=False,
        )
        for options in (
            [str(self.directory / "missing")],
            [str(custom)],
            [str(invalid)],
            [str(valid), str(custom)],
            ["--load-log-root", str(self.directory / "missing")],
        ):
            with self.subTest(options=options):
                result = subprocess.run(
                    [sys.executable, str(SCRIPT), *options, "--print-started-at"],
                    capture_output=True, text=True, timeout=5,
                )
                self.assertEqual(result.returncode, 2, result.stderr)
                self.assertEqual(result.stdout, "")

    def test_default_wrapper_config_auto_selects_local_benchmark(self):
        checkout = self.configured_checkout()
        latest = self.make_run(
            self.directory / "custom results", "tpcc-20261105T100000Z-1-1"
        )
        self.write_run_config(
            checkout / "run.env",
            RUN_OUTPUT_ROOT="./custom results",
            RUN_LABEL="tpcc",
            RUN_LOAD_LOG_ROOT="./custom logs",
        )
        environment = dict(os.environ)
        for key in ("RUN_ENV_FILE", "LOG_DIR"):
            environment.pop(key, None)
        environment["CONNECTION_ENV_FILE"] = "/missing/connection.env"
        result = subprocess.run(
            ["bash", str(checkout / "wrapper.sh"), "--monitor"],
            env=environment, cwd=self.directory,
            capture_output=True, text=True, timeout=5,
        )
        self.assertEqual(result.returncode, 1, result.stderr)
        self.assertIn(f"Monitoring: {latest}", result.stdout)
        self.assertIn(str(latest / "output.log"), result.stderr)

    def test_alternate_run_config_selectors_and_load_log_root(self):
        checkout = self.configured_checkout()
        self.make_run(self.directory / "results", "tpcc-20261104T100000Z-1-1")
        latest = self.make_run(
            self.directory / "load logs", "load-20261105T100000Z-2-2",
            manifest=False,
        )
        config = self.directory / "loading.env"
        self.write_run_config(
            config,
            RUN_OUTPUT_ROOT="./results",
            RUN_LABEL="tpcc",
            RUN_LOAD_LOG_ROOT="./load logs",
        )
        for options in ([], ["--run-env", str(config)]):
            with self.subTest(options=options):
                environment = {**os.environ, "RUN_ENV_FILE": str(config)}
                environment.pop("LOG_DIR", None)
                if options:
                    environment["RUN_ENV_FILE"] = "/missing/overridden.env"
                result = subprocess.run(
                    [
                        "bash", str(checkout / "wrapper.sh"), "--monitor",
                        *options, "--", "--interval", "0.01",
                    ],
                    env=environment, cwd=self.directory,
                    capture_output=True, text=True, timeout=5,
                )
                self.assertEqual(result.returncode, 1, result.stderr)
                self.assertIn(f"Monitoring: {latest}", result.stdout)

    def test_exact_log_dir_from_run_configuration(self):
        checkout = self.configured_checkout()
        logs = self.directory / "exact logs"
        logs.mkdir()
        (logs / "data.runner.log.status").write_text("exit_code=255\n")
        config = self.directory / "loading.env"
        self.write_run_config(
            config, RUN_OUTPUT_ROOT="./remote-results", RUN_LABEL="tpcc",
            LOG_DIR=logs,
        )
        result = subprocess.run(
            [
                "bash", str(checkout / "wrapper.sh"), "--monitor",
                "--run-env", str(config),
            ],
            cwd=self.directory, capture_output=True, text=True, timeout=5,
        )
        self.assertEqual(result.returncode, 1, result.stderr)
        self.assertIn(str(logs / "data.runner.log.status"), result.stderr)

    def test_auto_selection_rejects_missing_or_sample_configuration(self):
        checkout = self.configured_checkout()
        config = self.directory / "private.env"
        for settings in ({}, {"RUN_OUTPUT_ROOT": "./results", "RUN_LABEL": "tpcc"}):
            with self.subTest(settings=settings):
                self.write_run_config(config, **settings)
                environment = {
                    key: value for key, value in os.environ.items()
                    if key not in ("RUN_OUTPUT_ROOT", "RUN_LABEL", "LOG_DIR")
                }
                result = subprocess.run(
                    [
                        "bash", str(checkout / "wrapper.sh"), "--monitor",
                        "--run-env", str(config),
                    ],
                    env=environment, cwd=self.directory,
                    capture_output=True, text=True, timeout=5,
                )
                self.assertNotEqual(result.returncode, 0)
                self.assertIn("requires" if not settings else "No matching run", result.stderr)
        sample = self.directory / "run.env.sample"
        self.write_run_config(sample, RUN_OUTPUT_ROOT="./results", RUN_LABEL="tpcc")
        alias = self.directory / "alias.env"
        alias.symlink_to(sample)
        for path in (sample, alias, self.directory / "missing.env"):
            result = subprocess.run(
                [
                    "bash", str(checkout / "wrapper.sh"), "--monitor",
                    "--run-env", str(path),
                ],
                capture_output=True, text=True, timeout=5,
            )
            self.assertNotEqual(result.returncode, 0)
            self.assertIn("Sample" if path == sample else
                          "sample" if path == alias else "readable", result.stderr)

    def test_discovery_locks_onto_selected_run(self):
        root = self.directory / "results"
        selected = self.make_run(
            root, "tpcc-20261105T100000Z-1-1", failure=False
        )
        process = subprocess.Popen(
            [
                sys.executable, str(SCRIPT), "--run-output-root", str(root),
                "--run-label", "tpcc", "--interval", "0.02",
            ],
            stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
        )
        try:
            self.assertIn(str(selected), process.stdout.readline())
            self.make_run(root, "tpcc-20261106T100000Z-2-2")
            (selected / "output.log").write_text("ERROR: selected worker failed\n")
            stdout, stderr = process.communicate(timeout=5)
            self.assertEqual(process.returncode, 1, (stdout, stderr))
            self.assertIn(str(selected / "output.log"), stderr)
        finally:
            if process.poll() is None:
                process.terminate()
                process.communicate(timeout=5)

    def test_missing_root_is_not_silently_ignored(self):
        self.monitor.directories = [self.directory / "missing"]
        with self.assertRaises(OSError):
            self.monitor.scan()


if __name__ == "__main__":
    unittest.main()
