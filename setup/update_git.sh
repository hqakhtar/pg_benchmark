#!/usr/bin/env bash
set -euo pipefail

cd pg_benchmark
git checkout .
git pull --ff-only
