#!/usr/bin/env bash
# Build and run Lab 04 Spark RDD jobs locally. Do not use DataFrames/SQL.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")" && pwd)"
cd "$ROOT"

EMP="$ROOT/data/employee.csv"
LOG="$ROOT/data/web_access_log.txt"

echo "==> Building..."
mvn -q -DskipTests package

echo
echo "==> Exercise 1: employee.csv"
mvn -q exec:java \
  -Dexec.mainClass=pmj.bdp.spark.lab4.EmployeeRdd \
  -Dexec.args="$EMP"

echo
echo "==> Exercise 2: web_access_log.txt"
mvn -q exec:java \
  -Dexec.mainClass=pmj.bdp.spark.lab4.WebLogRdd \
  -Dexec.args="$LOG"

echo
echo "Done. Copy the printed output into the PDF lab report."
