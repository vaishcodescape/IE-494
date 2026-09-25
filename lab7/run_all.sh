#!/usr/bin/env bash
# Lab 07: multivariate linear regression (normal equation) + Spark SQL predict UDF.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")" && pwd)"
cd "$ROOT"

DATA="${1:-$ROOT/data/houses.csv}"
OUT="$ROOT/output"

echo "==> Building..."
mvn -q -DskipTests package

rm -rf "$OUT"
mkdir -p "$OUT"

echo
echo "==> Lab 07  data=$DATA  out=$OUT"
mvn -q exec:java \
  -Dexec.mainClass=pmj.bdp.spark.lab7.Lab07 \
  -Dexec.args="$DATA $OUT" | tee "$OUT/run_log.txt"

echo
echo "Done. Output: $OUT"
