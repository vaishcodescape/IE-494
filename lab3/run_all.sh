#!/usr/bin/env bash
# Build and run Lab 03 MapReduce jobs locally.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")" && pwd)"
cd "$ROOT"

VISITORS="$ROOT/data/daily-website-visitors.csv"
RETAIL="$ROOT/data/retail.dat"
OUT="$ROOT/output"
JAR="$ROOT/target/lab3.jar"
MINSUP="${MINSUP:-1000}"

echo "==> Building jar..."
mvn -q -DskipTests package

rm -rf "$OUT"
mkdir -p "$OUT"

echo "==> Exercise 1: monthly moving average (window=3) ..."
hadoop jar "$JAR" pmj.bdp.mr.lab3.MonthlyMovingAverage \
  "$VISITORS" "$OUT/q1_moving_average"
echo "---- $OUT/q1_moving_average ----"
cat "$OUT/q1_moving_average"/part-*
echo

echo "==> Exercise 2: MR-Apriori (minSupport=$MINSUP) ..."
hadoop jar "$JAR" pmj.bdp.mr.lab3.MRApriori \
  "$RETAIL" "$OUT/q2_apriori" "$MINSUP"
echo "---- frequent itemsets (k>=2) ----"
cat "$OUT/q2_apriori/frequent-itemsets.txt"
echo

echo "All jobs finished. Outputs under $OUT"
