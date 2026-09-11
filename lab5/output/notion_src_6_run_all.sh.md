<details>
<summary>run_all.sh (full)</summary>
	```bash
	#!/usr/bin/env bash
	# Build and run Lab 05 YAFIM (Spark RDD). Do not use DataFrames/SQL.
	set -euo pipefail
	
	ROOT="$(cd "$(dirname "$0")" && pwd)"
	cd "$ROOT"
	
	RETAIL="$ROOT/data/retail.dat"
	OUT="$ROOT/output/yafim"
	MINSUP="${MINSUP:-1000}"
	
	echo "==> Building..."
	mvn -q -DskipTests package
	
	rm -rf "$OUT"
	
	echo
	echo "==> YAFIM on retail.dat  minSupport=$MINSUP"
	mvn -q exec:java \
	  -Dexec.mainClass=pmj.bdp.spark.lab5.Yafim \
	  -Dexec.args="$RETAIL $MINSUP $OUT"
	
	echo
	echo "---- summary ----"
	wc -l "$OUT"/L*.txt "$OUT"/frequent-itemsets.txt
	echo
	echo "Done. Outputs under $OUT"
	```
</details>