#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd "$(dirname "$0")" && pwd)"
cd "$ROOT"
LAB8="$(cd "$ROOT/../lab8" && pwd)"
CORDERS="${1:-$LAB8/db/CORDERS_zstd}"
DATA="$ROOT/data"
DB="$ROOT/db"
OUT="$ROOT/output"

# Lab 08 output CORDERS is the input. Rebuild it if it is missing.
if [ ! -f "$CORDERS/CURRENT" ]; then
  (cd "$LAB8" && mvn -q -DskipTests package \
    && mvn -q exec:java -Dexec.mainClass=pmj.bdp.rocks.lab8.Lab08 \
         -Dexec.args="load $LAB8/data $LAB8/db $DATA/lab8_run" \
    && mvn -q exec:java -Dexec.mainClass=pmj.bdp.rocks.lab8.Lab08 \
         -Dexec.args="merge $LAB8/db zstd $DATA/lab8_run")
fi

mvn -q -DskipTests package
rm -rf "$OUT"
mkdir -p "$OUT" "$DATA"
# exec:exec forks a plain JVM. Under exec:java, rocksdbjni sits in Maven's
# classloader and the listener callbacks (native threads) cannot find its classes.
mvn -q exec:exec -Dexec.executable=java \
  -Dexec.args="--enable-native-access=ALL-UNNAMED -cp %classpath pmj.bdp.rocks.lab9.Lab09 all $CORDERS $DATA $DB $OUT" \
  | tee "$OUT/run_log.txt"
