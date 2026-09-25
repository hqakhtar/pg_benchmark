#!/usr/bin/env bash
set -u

if [ "$#" -ne 3 ];
then
  echo "Usage: $0 <hosts_file> <log_folder> <script to run>"
  echo "Example: $0 /path/to/hosts.txt /path/to/logs /path/to/bash_script"
  exit 1
fi

HOSTS_FILE="$1"
LOG_DIR="$2"
SETUP_SCRIPT="$3"

# Directory where this orchestration script resides,
# regardless of where you call it from.
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

if [ ! -f "$HOSTS_FILE" ];
then
  echo "Error: hosts file not found: $HOSTS_FILE"
  exit 1
fi

if [ ! -f "$SETUP_SCRIPT" ];
then
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

  echo "Starting running script $SETUP_SCRIPT on $host"
  echo "Log: $log_file"

  # Ship the script over stdin; each host gets an independent job and log.
  ssh -o StrictHostKeyChecking=accept-new \
      "$host" \
      'bash -s' \
      < "$SETUP_SCRIPT" > "$log_file" 2>&1 &

  pids+=("$!")
done < "$HOSTS_FILE"

failed=0

# Wait for all jobs so a single failed runner cannot yield overall success.
for pid in "${pids[@]}"; do
  if ! wait "$pid";
  then
    failed=1
  fi

done

if [ "$failed" -eq 0 ];
then
  echo "All VM setup jobs completed successfully."
else
  echo "One or more VM setup jobs failed. Check logs in: $LOG_DIR"
  exit 1
fi
