#!/usr/bin/env python3
"""Preserve a plain runner inventory while adding or removing Azure VM aliases."""

import argparse
import fcntl
import os
from pathlib import Path
import re
import stat
import sys
import tempfile


def read_hosts(path):
    if path.is_symlink():
        raise ValueError(f"Runner hosts file must not be a symlink: {path}")
    if not path.exists():
        return "", set()
    if not path.is_file():
        raise ValueError(f"Runner hosts file must be a regular file: {path}")
    with path.open(encoding="utf-8", newline="") as stream:
        contents = stream.read()
    hosts = set()
    for line in contents.splitlines():
        host = line.split("#", 1)[0].strip()
        if not host:
            continue
        # Match lib/common.sh's plain runner-host format.
        if not re.fullmatch(r"[A-Za-z0-9_][A-Za-z0-9_.:@%\-]*", host):
            raise ValueError(f"Invalid runner host in {path}: {host}")
        if host in hosts:
            raise ValueError(f"Duplicate runner host in {path}: {host}")
        hosts.add(host)
    return contents, hosts


def update_hosts(action, path, names):
    contents, _ = read_hosts(path)
    if not path.parent.is_dir() or not os.access(path.parent, os.W_OK):
        raise ValueError(f"Runner hosts directory must exist and be writable: {path.parent}")
    if action == "check":
        return
    # Keep a stable lock across atomic replacements, including concurrent batches.
    descriptor = os.open(str(path) + ".lock", os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW, 0o600)
    with os.fdopen(descriptor, "a") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        contents, hosts = read_hosts(path)
        if action == "add":
            missing = [name for name in dict.fromkeys(names) if name not in hosts]
            updated = contents
            if missing:
                if updated and not updated.endswith(("\n", "\r")):
                    updated += "\n"
                updated += "".join(name + "\n" for name in missing)
        else:
            removed = set(names)
            updated = "".join(
                line for line in contents.splitlines(keepends=True)
                if line.split("#", 1)[0].strip() not in removed
            )
        if updated == contents:
            return
        mode = stat.S_IMODE(path.stat().st_mode) if path.exists() else 0o600
        descriptor, temporary = tempfile.mkstemp(prefix=path.name + ".", dir=path.parent)
        try:
            with os.fdopen(descriptor, "w", encoding="utf-8", newline="") as stream:
                os.fchmod(stream.fileno(), mode)
                stream.write(updated)
                stream.flush()
                os.fsync(stream.fileno())
            os.replace(temporary, path)
        finally:
            if os.path.exists(temporary):
                os.unlink(temporary)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=("check", "add", "remove"))
    parser.add_argument("path")
    parser.add_argument("names", nargs="*")
    args = parser.parse_args()
    if any(not re.fullmatch(r"[a-z][a-z0-9-]{0,63}", name) for name in args.names):
        parser.error("Expected plain Azure VM names")
    update_hosts(args.action, Path(os.path.abspath(args.path)), args.names)


if __name__ == "__main__":
    try:
        main()
    except (OSError, ValueError, UnicodeError) as error:
        print(f"ERROR: Cannot update runner hosts: {error}", file=sys.stderr)
        sys.exit(1)
