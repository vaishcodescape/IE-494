#!/usr/bin/env bash
# IE-494 Lab 10: DynamoDB Local + TPC-H SF1 (CUSTOMER, ORDERS, LINEITEM).
#   bash run_all.sh [custkey] [orderkey] [year]
# Defaults: customer 121 (15 orders, 3 in 1993 and 5 in 1994), its order 5512869 (7 line items), year 1994.
set -euo pipefail
ROOT="$(cd "$(dirname "$0")" && pwd)"
cd "$ROOT"
DATA="${DATA:-$ROOT/../lab8/data}"          # customer.tbl, orders.tbl, lineitem.tbl (dbgen -s 1)
DBGEN="$ROOT/../lab8/tpch-kit/dbgen"
DDB="$ROOT/dynamodb_local"                  # unzipped dynamodb_local_latest
OUT="$ROOT/output"
CUST="${1:-121}"
ORDER="${2:-5512869}"
YEAR="${3:-1994}"
mkdir -p "$OUT" "$ROOT/db"

# 1. DynamoDB Local on :8000 (same command as the course setup sheet, plus a data directory)
if ! curl -s -o /dev/null http://localhost:8000/; then
  (cd "$DDB" && nohup java -Djava.library.path=./DynamoDBLocal_lib -jar DynamoDBLocal.jar \
      -sharedDb -port 8000 -dbPath "$ROOT/db" > "$OUT/dynamodb_local.log" 2>&1 &)
  sleep 4
fi

# 2. TPC-H SF1 tables, generated with the dbgen of Lab 08 if they are missing
if [ ! -f "$DATA/lineitem.tbl" ]; then
  mkdir -p "$DATA"
  (cd "$DBGEN" && DSS_PATH="$DATA" ./dbgen -q -s 1 -T c -f && DSS_PATH="$DATA" ./dbgen -q -s 1 -T o -f)
fi

# 3. build and run
mvn -q -DskipTests package
# exec:exec forks a plain JVM with the project classpath; under exec:java the SDK runs inside
# Maven's plugin classloader, which failed to load an SDK exception class once during this lab.
run() {
  mvn -q exec:exec -Dexec.executable=java \
      -Dexec.args="-cp %classpath pmj.bdp.dynamo.lab10.Lab10 $*"
}
run create --drop            2>&1 | tee "$OUT/01_create.txt"
run load "$DATA"             2>&1 | tee "$OUT/02_load.txt"
run describe                 2>&1 | tee "$OUT/03_describe.txt"
run queries "$CUST" "$ORDER" "$YEAR" 2>&1 | tee "$OUT/04_queries.txt"
cat "$OUT"/0[1-4]_*.txt > "$OUT/run_log.txt"
