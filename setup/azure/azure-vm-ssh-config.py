#!/usr/bin/env python3
"""Manage only the Azure VM batch's explicitly owned entries in an SSH config.

check-add/check-remove never create directories, files, or locks. Mutations use
an advisory lock, re-read the config under that lock, then replace it atomically.
No SSH connections, Match commands, or private-key reads are performed.
"""
import argparse
import contextlib
import errno
import fcntl
import glob
import ipaddress
import json
import os
from pathlib import Path
import pwd
import re
import shlex
import stat
import sys
import tempfile
import uuid


BEGIN = b"# BEGIN AZURE-VM-BATCH SSH CONFIG v1\n"
END = b"# END AZURE-VM-BATCH SSH CONFIG v1\n"
HEADER = b"# Managed by azure-vm-batch-v1; edit connection settings in the env file.\n"
RECORD = b"# AZVM-ENTRY "
FIELDS = {"subscription", "resource_group", "vm_name", "hostname", "port", "user", "identity_file"}


class ConfigError(Exception):
    pass


def safe_text(value, description):
    if not isinstance(value, str) or not value:
        raise ConfigError(f"{description} must be a nonempty string")
    if any(ord(char) < 32 or ord(char) == 127 or 0xD800 <= ord(char) <= 0xDFFF for char in value):
        raise ConfigError(f"{description} must not contain control characters")
    return value


def absolute_path(value, description):
    safe_text(value, description)
    if not os.path.isabs(value):
        raise ConfigError(f"{description} must be an absolute path: {value!r}")
    return Path(os.path.abspath(value))


def normalize_scope(subscription, resource_group):
    try:
        subscription = str(uuid.UUID(subscription))
    except (ValueError, AttributeError, TypeError):
        raise ConfigError("subscription must be a resolved subscription UUID") from None
    safe_text(resource_group, "resource group")
    if not re.fullmatch(r"[a-zA-Z0-9._()-]{1,90}", resource_group) or resource_group.endswith("."):
        raise ConfigError("invalid resource group name")
    return subscription, resource_group.lower()


def validate_name(name):
    if not isinstance(name, str) or not re.fullmatch(r"[a-z][a-z0-9-]{0,63}", name):
        raise ConfigError("VM/SSH alias must contain lowercase letters, digits or hyphens, start with a letter, and be at most 64 characters")
    return name


def validate_connection(user, port, identity_file, require_key):
    if not isinstance(user, str) or not re.fullmatch(r"[a-z_][a-z0-9_-]{0,31}", user):
        raise ConfigError("invalid SSH username")
    if isinstance(port, bool) or not isinstance(port, int) or not 1 <= port <= 65535:
        raise ConfigError("SSH port must be an integer between 1 and 65535")
    path = absolute_path(identity_file, "SSH private-key path")
    if "${" in str(path):
        raise ConfigError("SSH private-key path cannot contain an SSH environment-variable expansion (${...})")
    if require_key and (not path.is_file() or not os.access(path, os.R_OK)):
        raise ConfigError(f"SSH private-key path is not a readable regular file: {path}")
    return str(path)


def validate_record(record):
    if not isinstance(record, dict) or set(record) != FIELDS:
        raise ConfigError("managed SSH entry has invalid metadata fields")
    subscription, group = normalize_scope(record["subscription"], record["resource_group"])
    validate_name(record["vm_name"])
    identity = validate_connection(record["user"], record["port"], record["identity_file"], False)
    try:
        hostname = str(ipaddress.ip_address(record["hostname"]))
    except (ValueError, TypeError):
        raise ConfigError("managed SSH entry has an invalid IP address") from None
    normalized = dict(record, subscription=subscription, resource_group=group,
                      identity_file=identity, hostname=hostname)
    if record != normalized:
        raise ConfigError("managed SSH entry metadata is not in its original canonical form")
    return normalized


def quote_identity(path):
    # SSH expands percent tokens even in quotes. %% represents a literal %.
    return '"' + path.replace("\\", "\\\\").replace('"', '\\"').replace("%", "%%") + '"'


def render(records):
    if not records:
        return b""
    lines = [BEGIN, HEADER]
    for item in sorted(records, key=lambda entry: entry["vm_name"]):
        metadata = json.dumps(item, sort_keys=True, separators=(",", ":"), ensure_ascii=True)
        body = (f"Host {item['vm_name']}\n"
                f"    HostName {item['hostname']}\n"
                f"    Port {item['port']}\n"
                f"    User {item['user']}\n"
                f"    IdentityFile {quote_identity(item['identity_file'])}\n"
                "    IdentitiesOnly yes\n\n")
        lines.extend((RECORD + metadata.encode("ascii") + b"\n", body.encode("utf-8")))
    # Restore global matching before the user's original leading directives.
    lines.extend((b"Host *\n", END))
    return b"".join(lines)


def split_config(data):
    marker_lines = [line for line in data.splitlines()
                    if line.startswith((b"# BEGIN AZURE-VM-BATCH SSH CONFIG", b"# END AZURE-VM-BATCH SSH CONFIG"))]
    if not marker_lines:
        return [], data
    if (not data.startswith(BEGIN) or marker_lines != [BEGIN.rstrip(b"\n"), END.rstrip(b"\n")]
            or data.count(END) != 1):
        raise ConfigError("managed SSH section is misplaced, duplicated or malformed; preserve it and inspect the config manually")
    end = data.index(END) + len(END)
    section, remainder = data[:end], data[end:]
    records = []
    for line in section.splitlines():
        if line.startswith(RECORD):
            try:
                record = json.loads(line[len(RECORD):])
            except (ValueError, UnicodeDecodeError):
                raise ConfigError("managed SSH entry metadata is malformed") from None
            records.append(validate_record(record))
    aliases = [item["vm_name"] for item in records]
    if not records or len(aliases) != len(set(aliases)) or render(records) != section:
        raise ConfigError("managed SSH section was edited or contains duplicate entries; no changes made")
    return records, remainder


def file_signature(info):
    return info.st_dev, info.st_ino, info.st_size, info.st_mtime_ns, info.st_ctime_ns


def read_config(path):
    flags = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0) | getattr(os, "O_NONBLOCK", 0)
    try:
        descriptor = os.open(path, flags)
    except FileNotFoundError:
        if path.is_symlink():
            raise ConfigError(f"SSH config must not be a symlink: {path}") from None
        return b"", None
    except OSError as error:
        if error.errno == errno.ELOOP:
            raise ConfigError(f"SSH config must not be a symlink: {path}") from None
        raise
    with os.fdopen(descriptor, "rb") as stream:
        before = os.fstat(stream.fileno())
        if not stat.S_ISREG(before.st_mode):
            raise ConfigError(f"SSH config is not a regular file: {path}")
        data = stream.read()
        after = os.fstat(stream.fileno())
        if file_signature(before) != file_signature(after):
            raise ConfigError("SSH config changed while it was being read; retry")
    return data, file_signature(after)


def check_destination(path):
    # A read-only best-effort permission check; mutations check again under lock.
    if path.is_symlink():
        raise ConfigError(f"SSH config must not be a symlink: {path}")
    parent = path.parent
    while not parent.exists():
        parent = parent.parent
    if not parent.is_dir() or not os.access(parent, os.W_OK | os.X_OK):
        raise ConfigError(f"SSH config directory is not writable: {parent}")


def include_path(token):
    # Include accepts environment variables and tokens. Expand only values that
    # are independent of SSH's conditional evaluation; never execute Match exec.
    def environment(match):
        name = match.group(1)
        if name not in os.environ:
            raise ConfigError(f"cannot inspect Include with unset environment variable {name}")
        return os.environ[name]
    home = Path(pwd.getpwuid(os.getuid()).pw_dir)
    token = re.sub(r"\$\{([A-Za-z_][A-Za-z0-9_]*)\}", environment, token)
    if "${" in token:
        raise ConfigError("cannot safely inspect this SSH Include environment expansion")
    expanded = []
    index = 0
    while index < len(token):
        if token[index] != "%":
            expanded.append(token[index]); index += 1; continue
        if index + 1 >= len(token) or token[index + 1] not in ("%", "d"):
            raise ConfigError("cannot safely inspect an SSH Include with host-dependent or unknown percent tokens")
        expanded.append("%" if token[index + 1] == "%" else str(home))
        index += 2
    token = "".join(expanded)
    if token == "~" or token.startswith("~/"):
        token = str(home) + token[1:]
    elif token.startswith("~"):
        raise ConfigError("cannot safely inspect an SSH Include referring to another user's home")
    if not os.path.isabs(token):
        # OpenSSH resolves user-config relative Includes underneath ~/.ssh.
        token = str(home / ".ssh" / token)
    return token


def check_manual_aliases(data, selected_names, config_path):
    visited = set()
    selected = {name.lower() for name in selected_names}

    def scan(contents, origin, depth):
        if depth > 16:
            raise ConfigError("SSH Include nesting is too deep to inspect safely")
        for line in contents.decode("utf-8", errors="replace").splitlines():
            match = re.match(r"\s*([A-Za-z][A-Za-z0-9]*)\s*(?:=\s*|\s+|$)(.*)", line)
            if not match or match.group(1).lower() not in ("host", "include"):
                continue
            try:
                words = shlex.split(match.group(2), comments=True, posix=True)
            except ValueError:
                raise ConfigError(f"cannot parse Host/Include directive in {origin}") from None
            if match.group(1).lower() == "host":
                for word in words:
                    if word.lower() in selected:
                        raise ConfigError(f"unmanaged SSH Host alias {word!r} already exists in {origin}; choose another VM prefix or resolve that entry manually")
            else:
                for token in words:
                    for filename in sorted(glob.glob(include_path(token))):
                        path = Path(filename)
                        real = str(path.resolve())
                        if real in visited:
                            continue
                        if len(visited) >= 256:
                            raise ConfigError("too many SSH Include files to inspect safely")
                        visited.add(real)
                        if not path.is_file():
                            raise ConfigError(f"SSH Include is not a regular readable file: {path}")
                        scan(path.read_bytes(), path, depth + 1)

    visited.add(str(config_path.resolve()))
    scan(data, config_path, 0)


def owned_names(records, scope, names):
    wanted = set(names)
    return [item["vm_name"] for item in records
            if (item["subscription"], item["resource_group"]) == scope and item["vm_name"] in wanted]


def check_add(records, remainder, scope, names, config):
    for item in records:
        if item["vm_name"] in names and (item["subscription"], item["resource_group"]) != scope:
            raise ConfigError(f"SSH alias {item['vm_name']} belongs to another subscription/resource group")
    check_manual_aliases(remainder, names, config)


@contextlib.contextmanager
def mutation_lock(config):
    os.umask(0o077)
    config.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    lock = config.with_name(config.name + ".azvm.lock")
    try:
        descriptor = os.open(lock, os.O_RDWR | os.O_CREAT | getattr(os, "O_NOFOLLOW", 0), 0o600)
    except OSError as error:
        if error.errno == errno.ELOOP:
            raise ConfigError(f"SSH config lock must not be a symlink: {lock}") from None
        raise
    try:
        info = os.fstat(descriptor)
        if not stat.S_ISREG(info.st_mode) or info.st_nlink != 1:
            raise ConfigError(f"SSH config lock is not a single regular file: {lock}")
        os.fchmod(descriptor, 0o600)
        fcntl.flock(descriptor, fcntl.LOCK_EX)
        yield
    finally:
        os.close(descriptor)


def atomic_write(config, data, original, signature):
    descriptor, filename = tempfile.mkstemp(prefix="." + config.name + ".azvm-", dir=config.parent)
    try:
        with os.fdopen(descriptor, "wb") as stream:
            os.fchmod(stream.fileno(), 0o600)
            stream.write(data)
            stream.flush()
            os.fsync(stream.fileno())
        current, current_signature = read_config(config)
        if current != original or current_signature != signature:
            raise ConfigError("SSH config changed concurrently; no replacement was made, retry")
        os.replace(filename, config)
        directory = os.open(config.parent, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
        try:
            try:
                os.fsync(directory)
            except OSError as error:
                if error.errno not in (errno.EINVAL, errno.ENOTSUP):
                    raise
        finally:
            os.close(directory)
    finally:
        with contextlib.suppress(FileNotFoundError):
            os.unlink(filename)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=("check-add", "check-remove", "add", "remove"))
    parser.add_argument("--config", required=True)
    parser.add_argument("--subscription", required=True)
    parser.add_argument("--resource-group", required=True)
    parser.add_argument("--vm-name", required=True, action="append")
    parser.add_argument("--user")
    parser.add_argument("--port", type=int)
    parser.add_argument("--identity-file")
    parser.add_argument("--hostname")
    args = parser.parse_args()
    config = absolute_path(args.config, "SSH config path")
    scope = normalize_scope(args.subscription, args.resource_group)
    names = [validate_name(name) for name in args.vm_name]
    if len(set(names)) != len(names):
        raise ConfigError("VM names must be unique")
    if args.action in ("add", "remove") and len(names) != 1:
        raise ConfigError("add/remove accepts exactly one --vm-name")
    if args.action in ("add", "check-add"):
        identity = validate_connection(args.user, args.port, args.identity_file, True)
    check_destination(config)
    original, signature = read_config(config)
    records, remainder = split_config(original)
    if args.action == "check-add":
        check_add(records, remainder, scope, names, config)
    if args.action.startswith("check-"):
        print(json.dumps({"managed_vm_names": owned_names(records, scope, names)}))
        return
    if args.action == "remove" and signature is None:
        print(json.dumps({"changed": False, "managed_vm_names": []}))
        return
    if args.action == "add":
        try:
            hostname = str(ipaddress.ip_address(args.hostname))
        except (ValueError, TypeError):
            raise ConfigError("--hostname must be a valid IPv4 or IPv6 address") from None
    with mutation_lock(config):
        original, signature = read_config(config)
        records, remainder = split_config(original)
        if args.action == "add":
            check_add(records, remainder, scope, names, config)
            records = [item for item in records if item["vm_name"] != names[0]]
            records.append(dict(subscription=scope[0], resource_group=scope[1], vm_name=names[0],
                                hostname=hostname, port=args.port, user=args.user, identity_file=identity))
        else:
            records = [item for item in records
                       if not (item["vm_name"] == names[0] and (item["subscription"], item["resource_group"]) == scope)]
        result = render(records) + remainder
        changed = result != original
        if changed:
            atomic_write(config, result, original, signature)
        print(json.dumps({"changed": changed, "managed_vm_names": owned_names(records, scope, names)}))


if __name__ == "__main__":
    try:
        main()
    except (ConfigError, OSError) as error:
        print(f"ERROR: SSH config: {error}", file=sys.stderr)
        sys.exit(1)
