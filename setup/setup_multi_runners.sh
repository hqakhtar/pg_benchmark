#!/usr/bin/env bash
set -u

if [ "$#" -ne 2 ]; then
  echo "Usage: $0 <hosts_file> <log_folder>"
  echo "Example: $0 /path/to/hosts.txt /path/to/logs"
  exit 1
fi

HOSTS_FILE="$1"
LOG_DIR="$2"

# Directory where this orchestration script resides,
# regardless of where you call it from.
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

SETUP_SCRIPT="$SCRIPT_DIR/setup_runner_ubuntu.sh"

if [ ! -f "$HOSTS_FILE" ]; then
  echo "Error: hosts file not found: $HOSTS_FILE"
  exit 1
fi

if [ ! -f "$SETUP_SCRIPT" ]; then
  echo "Error: setup script not found: $SETUP_SCRIPT"
  exit 1
fi

mkdir -p "$LOG_DIR"

pids=()

while IFS= read -r host || [ -n "$host" ]; do
  # Skip blank lines and comments
  [[ -z "$host" || "$host" =~ ^[[:space:]]*# ]] && continue

  safe_host="$(echo "$host" | tr -c 'a-zA-Z0-9._-' '_')"
  log_file="$LOG_DIR/${safe_host}.log"

  echo "Starting setup on $host"
  echo "Log: $log_file"

  ssh -o StrictHostKeyChecking=accept-new \
      "$host" \
      'bash -s' \
      < "$SETUP_SCRIPT" > "$log_file" 2>&1 &

  pids+=("$!")
done < "$HOSTS_FILE"

failed=0

for pid in "${pids[@]}"; do
  if ! wait "$pid"; then
    failed=1
  fi
done

if [ "$failed" -eq 0 ]; then
  echo "All VM setup jobs completed successfully."
else
  echo "One or more VM setup jobs failed. Check logs in: $LOG_DIR"
  exit 1
fi

