#!/usr/bin/env python3
"""Watch loader or benchmark artifacts without interfering with the run."""

import argparse
from datetime import datetime
import json
import math
import os
from pathlib import Path
import re
import sys
import time


FAILURE_PATTERN = (
    r"\b(?:error|fatal|panic|failed|aborted)\b"
    r"|\b(?:segmentation fault|core dumped|broken pipe)\b"
    r"|\bconnection (?:reset|refused|timed out|closed)\b"
    r"|\bserver closed the connection\b"
    r"|\b(?:permission denied|no route to host|network is unreachable)\b"
    r"|\b(?:could not resolve hostname|host key verification failed)\b"
    r"|\b(?:killed by signal|terminated by signal)\b"
)


class WorkerFailure(Exception):
    """A worker reported a failure."""

    def __init__(self, path, diagnostic, line=None):
        match = re.fullmatch(
            r"(preflight|cleanup|bootstrap|data|finalize)\.(.+)\.log(?:\.status)?",
            path.name,
        )
        runner = match[2] if match else "unknown (not recorded in this artifact)"
        fields = [f"Runner: {runner}"]
        if match:
            fields.append(f"Phase: {match[1]}")
        source = f"{path}:{line}:" if line is not None else str(path)
        fields.extend((f"Source: {source}", f"Diagnostic: {diagnostic}"))
        # HammerDB progress can contain CR/ANSI controls that overwrite labels.
        escapes = {"\r": r"\r", "\n": r"\n", "\t": r"\t"}
        message = "\n".join(
            "".join(
                escapes.get(character, f"\\x{ord(character):02x}")
                if ord(character) < 32 or ord(character) == 127 else character
                for character in field
            )
            for field in fields
        )
        super().__init__(message)


class Monitor:
    def __init__(self, directories, patterns=()):
        self.directories = directories
        self.failure = re.compile(
            "|".join(f"(?:{pattern})" for pattern in [FAILURE_PATTERN, *patterns]),
            re.IGNORECASE,
        )
        self.logs = {}
        self.statuses = {}
        self.incomplete = {}

    @staticmethod
    def walk_error(error):
        raise error

    def files(self):
        for directory in self.directories:
            if not directory.is_dir():
                raise OSError(f"Not a readable directory: {directory}")
            for root, directories, files in os.walk(directory, onerror=self.walk_error):
                directories[:] = sorted(
                    name for name in directories if not name.startswith(".")
                )
                for name in sorted(files):
                    if name.endswith((".log", ".log.status")) or name in (
                        "run.json", "status.json",
                    ):
                        yield Path(root) / name

    def check_log(self, path):
        with path.open("rb") as stream:
            info = os.fstat(stream.fileno())
            identity = (info.st_dev, info.st_ino)
            previous, offset, number, pending = self.logs.get(
                path, (identity, 0, 1, b"")
            )
            if previous != identity or info.st_size < offset:
                offset, number, pending = 0, 1, b""
            stream.seek(offset)
            for raw in stream:
                line = pending + raw
                text = line.decode("utf-8", errors="replace")
                if self.failure.search(text):
                    raise WorkerFailure(path, text.rstrip("\r\n"), number)
                if line.endswith(b"\n"):
                    number += 1
                    pending = b""
                else:
                    pending = line
            self.logs[path] = (identity, stream.tell(), number, pending)

    def check_status(self, path):
        raw = path.read_text(encoding="utf-8")
        if self.statuses.get(path) == raw:
            return
        try:
            if path.name.endswith(".log.status"):
                fields = dict(line.split("=", 1) for line in raw.splitlines())
                code = int(fields["exit_code"])
                status = ""
            else:
                fields = json.loads(raw)
                code = fields["exit_code"]
                if type(code) is not int:
                    raise ValueError("exit_code must be an integer")
                status = fields.get("status", "")
        except (ValueError, KeyError, TypeError) as error:
            # Stage/SSH status files are not written atomically. Retry once.
            if self.incomplete.get(path) == raw:
                raise ValueError(f"Invalid status file {path}: {error}") from error
            self.incomplete[path] = raw
            return
        self.incomplete.pop(path, None)
        if code != 0 or status in ("failed", "interrupted"):
            raise WorkerFailure(
                path, f"exit_code={code}, status={status or 'finished'}"
            )
        self.statuses[path] = raw

    def scan(self):
        for path in self.files():
            try:
                if path.name.endswith(".log"):
                    self.check_log(path)
                else:
                    self.check_status(path)
            except FileNotFoundError:
                # Files can be replaced or removed between discovery and open.
                continue


def positive_seconds(value):
    seconds = float(value)
    if not math.isfinite(seconds) or seconds <= 0:
        raise argparse.ArgumentTypeError("must be a finite number greater than zero")
    return seconds


def newest_directory(run_root, label, load_root):
    candidates = []
    for root, prefix, manifest in (
        (run_root, f"{label}-", True),
        (load_root, "load-", False),
    ):
        if root is None or not root.exists():
            continue
        if not root.is_dir():
            raise ValueError(f"Not a directory: {root}")
        for path in root.iterdir():
            if not path.is_dir() or not path.name.startswith(prefix):
                continue
            suffix = path.name[len(prefix):]
            match = re.fullmatch(r"(\d{8}T\d{6}Z)-\d+-\d+", suffix)
            if match is None or (manifest and not (path / "run.json").is_file()):
                continue
            # Directory mtime changes during a run; rank by its start timestamp.
            candidates.append((match[1], path.stat().st_mtime_ns, str(path), path))
    if not candidates:
        raise ValueError(
            "No matching run found in the configured output/log roots; "
            "start a run first or pass its directory explicitly"
        )
    return max(candidates)[-1].resolve()


def load_started_at(directory):
    match = re.fullmatch(r"load-(\d{8}T\d{6}Z)-\d+-\d+", directory.name)
    if match is None:
        raise ValueError(f"Load directory has no start timestamp: {directory}")
    datetime.strptime(match[1], "%Y%m%dT%H%M%SZ")
    return match[1]


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "directories", nargs="*", type=Path,
        help="specific load log or benchmark run directories (searched recursively)",
    )
    parser.add_argument("--run-output-root", type=Path, help="configured benchmark root")
    parser.add_argument("--run-label", help="configured benchmark run label")
    parser.add_argument("--load-log-root", type=Path, help="configured load log root")
    parser.add_argument(
        "--print-started-at", action="store_true",
        help="print the selected load directory's UTC start timestamp and exit",
    )
    parser.add_argument(
        "--interval", type=positive_seconds, default=2.0,
        help="poll interval in seconds (default: 2)",
    )
    parser.add_argument(
        "--failure-pattern", action="append", default=[],
        help="additional case-insensitive Python regular expression; repeatable",
    )
    args = parser.parse_args(argv)
    discovery = args.run_output_root is not None or args.load_log_root is not None
    if args.directories and discovery:
        parser.error("choose explicit directories or configured roots, not both")
    if not args.directories and not discovery:
        parser.error("provide directories or configured roots (wrapper.sh --monitor reads run.env)")
    if args.run_output_root is not None and not args.run_label:
        parser.error("--run-output-root requires --run-label")
    try:
        directories = args.directories
        if discovery:
            directories = [
                newest_directory(
                    args.run_output_root, args.run_label, args.load_log_root
                )
            ]
        monitor = Monitor(
            [path.resolve() for path in directories], args.failure_pattern
        )
        for directory in monitor.directories:
            if not directory.is_dir():
                parser.error(f"not a directory: {directory}")
        if args.print_started_at:
            if len(monitor.directories) != 1:
                parser.error("--print-started-at requires exactly one load directory")
            directory = monitor.directories[0]
            started_at = load_started_at(directory)
            print(f"Load logs: {directory} (started at {started_at})", file=sys.stderr)
            print(started_at)
            return 0
        print(
            "Monitoring: " + ", ".join(map(str, monitor.directories))
            + f" (every {args.interval:g}s; Ctrl-C to stop)",
            flush=True,
        )
        while True:
            monitor.scan()
            time.sleep(args.interval)
    except WorkerFailure as error:
        print(f"WORKER FAILURE:\n{error}", file=sys.stderr, flush=True)
        return 1
    except (OSError, ValueError, re.error) as error:
        print(f"MONITOR ERROR: {error}", file=sys.stderr, flush=True)
        return 2
    except KeyboardInterrupt:
        return 130


if __name__ == "__main__":
    sys.exit(main())
