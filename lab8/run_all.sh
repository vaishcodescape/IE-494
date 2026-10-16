#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd "$(dirname "$0")" && pwd)"
cd "$ROOT"
DATA="${1:-$ROOT/data}"
DB="$ROOT/db"
OUT="$ROOT/output"
mvn -q -DskipTests package
rm -rf "$OUT"
mkdir -p "$OUT"
mvn -q exec:java \
  -Dexec.mainClass=pmj.bdp.rocks.lab8.Lab08 \
  -Dexec.args="all $DATA $DB $OUT" \
  | tee "$OUT/run_log.txt"
