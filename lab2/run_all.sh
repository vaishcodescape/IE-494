#!/usr/bin/env bash
# Build and run all Lab 02 MapReduce jobs locally with Hadoop.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")" && pwd)"
cd "$ROOT"

ORDERS="$ROOT/data/orders.csv"
WEBLOG="$ROOT/data/web_access_log.txt"
OUT="$ROOT/output"
JAR="$ROOT/target/lab2.jar"

# Jobs default to local MapReduce + file:/// (see Configuration in each main).
# Optional: export HADOOP_CONF_DIR="$ROOT/conf" for the same via XML.

echo "==> Building jar..."
mvn -q -DskipTests package

rm -rf "$OUT"
mkdir -p "$OUT"

run_job() {
  local main="$1"
  local input="$2"
  local outdir="$3"
  echo "==> Running $main ..."
  hadoop jar "$JAR" "$main" "$input" "$outdir"
  echo "---- $outdir ----"
  # local mode may write part-r-00000 or part-m-00000
  local part
  part="$(ls "$outdir"/part-* 2>/dev/null | head -1)"
  head -20 "$part"
  echo
}

run_job pmj.bdp.mr.lab2.SalesChannelCount  "$ORDERS" "$OUT/q1_channel_count"
run_job pmj.bdp.mr.lab2.SalesChannelAmount "$ORDERS" "$OUT/q2_channel_amount"
run_job pmj.bdp.mr.lab2.YearlyOrders       "$ORDERS" "$OUT/q3_yearly_orders"
run_job pmj.bdp.mr.lab2.MonthlyWebStats    "$WEBLOG" "$OUT/q4_monthly_web"
run_job pmj.bdp.mr.lab2.Error404           "$WEBLOG" "$OUT/q5_error404"

echo "All jobs finished. Outputs under $OUT"