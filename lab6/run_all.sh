#!/usr/bin/env bash
# Lab 06: full cube lattice on Spark RDDs, queries on SalesCube (DataFrame API).
set -euo pipefail

ROOT="$(cd "$(dirname "$0")" && pwd)"
cd "$ROOT"

DATA="${1:-$ROOT/data}"
CUBE="$ROOT/SalesCube"

echo "==> Building..."
mvn -q -DskipTests package

rm -rf "$CUBE" "$ROOT/output"

echo
echo "==> Lab 06  data=$DATA  cube=$CUBE"
mvn -q exec:java \
  -Dexec.mainClass=pmj.bdp.spark.lab6.Lab06 \
  -Dexec.args="$DATA $CUBE"

echo
echo "Done. Parquet cube: $CUBE"
echo "Query tables: $ROOT/output"
