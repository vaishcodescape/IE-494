---
## Source code
All of `lab5/`. Spark RDD only.

<details>
<summary>Yafim.java (full)</summary>
	```java
	package pmj.bdp.spark.lab5;

	import java.io.BufferedWriter;
	import java.io.IOException;
	import java.nio.file.Files;
	import java.nio.file.Path;
	import java.util.ArrayList;
	import java.util.Comparator;
	import java.util.List;

	import org.apache.spark.api.java.JavaPairRDD;
	import org.apache.spark.api.java.JavaRDD;
	import org.apache.spark.api.java.JavaSparkContext;
	import org.apache.spark.broadcast.Broadcast;

	import scala.Tuple2;

	/**
	 * YAFIM — Yet Another Frequent Itemset Mining
	 * (Qiu, Gu, Yuan, Huang; IEEE IPDPSW 2014).
	 *
	 * Spark RDD only (no DataFrame / SQL). Two phases from the paper:
	 *   Algo-2  frequent 1-itemsets L1  (flatMap + reduceByKey + minsup filter)
	 *   Algo-3  frequent k-itemsets, k ≥ 2 (apriori-gen, broadcast hash tree,
	 *           subset() on each cached transaction, reduceByKey)
	 *
	 * Usage: Yafim &lt;transactions&gt; [minSupport] [outputDir]
	 * Defaults: minSupport=1000, outputDir=output/yafim
	 */
	public class Yafim {

	    public static void main(String[] args) throws IOException {
	        if (args.length < 1) {
	            System.err.println("Usage: Yafim <transactions> [minSupport] [outputDir]");
	            System.exit(1);
	        }
	        String input = args[0];
	        int minSupport = args.length > 1 ? Integer.parseInt(args[1]) : 1000;
	        Path outDir = Path.of(args.length > 2 ? args[2] : "output/yafim");

	        JavaSparkContext sc = SparkUtil.localContext("Lab5 YAFIM");
	        try {
	            run(sc, input, minSupport, outDir);
	        } finally {
	            sc.stop();
	            sc.close();
	        }
	    }

	    static void run(JavaSparkContext sc, String input, int minSupport, Path outDir)
	            throws IOException {
	        Files.createDirectories(outDir);

	        JavaRDD<int[]> transactions = sc.textFile(input)
	                .map(Itemset::parseTransaction)
	                .filter(t -> t.length > 0)
	                .cache();

	        long n = transactions.count();
	        System.out.println("transactions = " + n);
	        System.out.println("minSupport   = " + minSupport
	                + "  (" + String.format("%.4f", minSupport / (double) n) + " of n)");

	        List<Tuple2<String, Integer>> allFrequent = new ArrayList<>();

	        // ---- Algo-2: Phase I, frequent 1-itemsets ----
	        System.out.println();
	        System.out.println("======== YAFIM Algo-2  L1 ========");
	        List<Tuple2<String, Integer>> l1kv = algo2Frequent1(transactions, minSupport);
	        List<int[]> lk = keysToItemsets(l1kv);
	        System.out.println("|L1| = " + lk.size());
	        writeLevel(outDir.resolve("L1.txt"), l1kv);
	        allFrequent.addAll(l1kv);

	        // ---- Algo-3: Phase II, k >= 2 ----
	        for (int k = 2; ; k++) {
	            System.out.println();
	            System.out.println("======== YAFIM Algo-3  L" + k + " ========");
	            List<int[]> ck = Itemset.generateCandidates(lk);
	            System.out.println("|C" + k + "| = " + ck.size());
	            if (ck.isEmpty()) {
	                break;
	            }

	            List<Tuple2<String, Integer>> lkkv = algo3FrequentK(sc, transactions, ck, minSupport);
	            lk = keysToItemsets(lkkv);
	            System.out.println("|L" + k + "| = " + lk.size());
	            if (lk.isEmpty()) {
	                break;
	            }
	            writeLevel(outDir.resolve("L" + k + ".txt"), lkkv);
	            allFrequent.addAll(lkkv);
	        }

	        Path combined = outDir.resolve("frequent-itemsets.txt");
	        writeLevel(combined, allFrequent);
	        System.out.println();
	        System.out.println("Wrote " + allFrequent.size() + " frequent itemsets to " + combined);
	    }

	    /**
	     * Paper Algorithm 2 — Phase I.
	     * Transactions RDD already cached. flatMap items to (item, 1),
	     * reduceByKey, keep counts ≥ minsup.
	     */
	    static List<Tuple2<String, Integer>> algo2Frequent1(
	            JavaRDD<int[]> transactions, int minSupport) {
	        JavaPairRDD<Integer, Integer> counts = transactions
	                .flatMapToPair(t -> {
	                    List<Tuple2<Integer, Integer>> out = new ArrayList<>(t.length);
	                    for (int item : t) {
	                        out.add(new Tuple2<>(item, 1));
	                    }
	                    return out.iterator();
	                })
	                .reduceByKey(Integer::sum)
	                .filter(kv -> kv._2 >= minSupport);

	        List<Tuple2<String, Integer>> rows = new ArrayList<>();
	        for (Tuple2<Integer, Integer> kv : counts.collect()) {
	            rows.add(new Tuple2<>(Integer.toString(kv._1), kv._2));
	        }
	        rows.sort(Comparator.comparing(t -> Integer.parseInt(t._1)));
	        return rows;
	    }

	    /**
	     * Paper Algorithm 3 — Phase II for one k.
	     * Build / broadcast a hash tree of Ck, run subset() on each transaction,
	     * reduceByKey, keep counts ≥ minsup.
	     */
	    static List<Tuple2<String, Integer>> algo3FrequentK(
	            JavaSparkContext sc,
	            JavaRDD<int[]> transactions,
	            List<int[]> candidates,
	            int minSupport) {

	        Broadcast<HashTree> treeBc = sc.broadcast(new HashTree(candidates));
	        try {
	            JavaPairRDD<String, Integer> counts = transactions
	                    .flatMapToPair(t -> {
	                        List<int[]> hits = treeBc.value().findCandidates(t);
	                        List<Tuple2<String, Integer>> out = new ArrayList<>(hits.size());
	                        for (int[] c : hits) {
	                            out.add(new Tuple2<>(Itemset.toKey(c), 1));
	                        }
	                        return out.iterator();
	                    })
	                    .reduceByKey(Integer::sum)
	                    .filter(kv -> kv._2 >= minSupport);

	            List<Tuple2<String, Integer>> rows = new ArrayList<>(counts.collect());
	            rows.sort(Comparator.comparing(t -> t._1));
	            return rows;
	        } finally {
	            treeBc.destroy();
	        }
	    }

	    private static List<int[]> keysToItemsets(List<Tuple2<String, Integer>> rows) {
	        List<int[]> list = new ArrayList<>(rows.size());
	        for (Tuple2<String, Integer> row : rows) {
	            list.add(Itemset.parseKey(row._1));
	        }
	        return list;
	    }

	    private static void writeLevel(Path file, List<Tuple2<String, Integer>> rows)
	            throws IOException {
	        try (BufferedWriter w = Files.newBufferedWriter(file)) {
	            for (Tuple2<String, Integer> row : rows) {
	                w.write(row._1);
	                w.write('\t');
	                w.write(Integer.toString(row._2));
	                w.newLine();
	            }
	        }
	    }
	}
	```
</details>

<details>
<summary>HashTree.java (full)</summary>
	```java
	package pmj.bdp.spark.lab5;

	import java.io.Serializable;
	import java.util.ArrayList;
	import java.util.LinkedHashSet;
	import java.util.List;

	/**
	 * Apriori hash tree used by YAFIM Algo-3 {@code subset()} to find which
	 * candidate k-itemsets a transaction contains (Qiu et al., IPDPSW 2014).
	 *
	 * Interior nodes hash on the next item; leaves store candidates. A leaf
	 * splits once it exceeds {@link #MAX_LEAF} and depth &lt; k.
	 */
	public final class HashTree implements Serializable {

	    private static final long serialVersionUID = 1L;
	    private static final int BRANCH = 11;
	    private static final int MAX_LEAF = 4;

	    private final Node root;
	    private final int k;

	    public HashTree(List<int[]> candidates) {
	        if (candidates == null || candidates.isEmpty()) {
	            throw new IllegalArgumentException("candidates must be non-empty");
	        }
	        this.k = candidates.get(0).length;
	        this.root = new Node(true);
	        for (int[] c : candidates) {
	            insert(root, c, 0);
	        }
	    }

	    /**
	     * Candidates contained in the (sorted, unique) transaction.
	     * Each candidate is returned at most once: hash collisions can visit the
	     * same leaf via multiple item prefixes, which would otherwise inflate support.
	     */
	    public List<int[]> findCandidates(int[] transaction) {
	        LinkedHashSet<String> seen = new LinkedHashSet<>();
	        List<int[]> hits = new ArrayList<>();
	        search(root, transaction, 0, 0, seen, hits);
	        return hits;
	    }

	    private void insert(Node node, int[] itemset, int depth) {
	        if (node.leaf) {
	            node.candidates.add(itemset);
	            if (node.candidates.size() > MAX_LEAF && depth < k) {
	                split(node, depth);
	            }
	            return;
	        }
	        int bucket = bucket(itemset[depth]);
	        if (node.children[bucket] == null) {
	            node.children[bucket] = new Node(true);
	        }
	        insert(node.children[bucket], itemset, depth + 1);
	    }

	    private void split(Node leaf, int depth) {
	        List<int[]> old = leaf.candidates;
	        leaf.leaf = false;
	        leaf.candidates = null;
	        leaf.children = new Node[BRANCH];
	        for (int[] c : old) {
	            insert(leaf, c, depth);
	        }
	    }

	    private void search(Node node, int[] t, int depth, int start,
	                        LinkedHashSet<String> seen, List<int[]> hits) {
	        if (node == null) {
	            return;
	        }
	        if (node.leaf) {
	            for (int[] cand : node.candidates) {
	                if (Itemset.containsAll(t, cand) && seen.add(Itemset.toKey(cand))) {
	                    hits.add(cand);
	                }
	            }
	            return;
	        }
	        int remaining = k - depth;
	        for (int i = start; i <= t.length - remaining; i++) {
	            Node child = node.children[bucket(t[i])];
	            if (child != null) {
	                search(child, t, depth + 1, i + 1, seen, hits);
	            }
	        }
	    }

	    private static int bucket(int item) {
	        int m = item % BRANCH;
	        return m < 0 ? m + BRANCH : m;
	    }

	    private static final class Node implements Serializable {
	        private static final long serialVersionUID = 1L;
	        boolean leaf;
	        List<int[]> candidates;
	        Node[] children;

	        Node(boolean leaf) {
	            this.leaf = leaf;
	            if (leaf) {
	                this.candidates = new ArrayList<>();
	            } else {
	                this.children = new Node[BRANCH];
	            }
	        }
	    }
	}
	```
</details>

<details>
<summary>Itemset.java (full)</summary>
	```java
	package pmj.bdp.spark.lab5;

	import java.util.ArrayList;
	import java.util.Arrays;
	import java.util.HashSet;
	import java.util.List;
	import java.util.Set;

	/**
	 * Sorted integer itemset helpers (Apriori join + prune).
	 */
	public final class Itemset {

	    private Itemset() {
	    }

	    public static int[] parseTransaction(String line) {
	        if (line == null || line.isBlank()) {
	            return new int[0];
	        }
	        String[] tok = line.trim().split("\\s+");
	        int[] raw = new int[tok.length];
	        int n = 0;
	        for (String t : tok) {
	            if (t.isEmpty()) {
	                continue;
	            }
	            raw[n++] = Integer.parseInt(t);
	        }
	        if (n == 0) {
	            return new int[0];
	        }
	        Arrays.sort(raw, 0, n);
	        int w = 1;
	        for (int i = 1; i < n; i++) {
	            if (raw[i] != raw[w - 1]) {
	                raw[w++] = raw[i];
	            }
	        }
	        return Arrays.copyOf(raw, w);
	    }

	    public static String toKey(int[] items) {
	        StringBuilder sb = new StringBuilder();
	        for (int i = 0; i < items.length; i++) {
	            if (i > 0) {
	                sb.append(',');
	            }
	            sb.append(items[i]);
	        }
	        return sb.toString();
	    }

	    public static int[] parseKey(String key) {
	        String[] parts = key.split(",");
	        int[] items = new int[parts.length];
	        for (int i = 0; i < parts.length; i++) {
	            items[i] = Integer.parseInt(parts[i].trim());
	        }
	        return items;
	    }

	    /** Both arrays sorted. */
	    public static boolean containsAll(int[] transaction, int[] candidate) {
	        int i = 0;
	        int j = 0;
	        while (i < transaction.length && j < candidate.length) {
	            if (transaction[i] == candidate[j]) {
	                i++;
	                j++;
	            } else if (transaction[i] < candidate[j]) {
	                i++;
	            } else {
	                return false;
	            }
	        }
	        return j == candidate.length;
	    }

	    /**
	     * Apriori-gen: self-join L_{k-1} then prune candidates whose
	     * (k-1)-subsets are not all frequent.
	     */
	    public static List<int[]> generateCandidates(List<int[]> previous) {
	        List<int[]> candidates = new ArrayList<>();
	        if (previous.isEmpty()) {
	            return candidates;
	        }
	        int kMinus1 = previous.get(0).length;
	        Set<String> frequent = new HashSet<>();
	        for (int[] itemset : previous) {
	            frequent.add(toKey(itemset));
	        }

	        for (int i = 0; i < previous.size(); i++) {
	            int[] a = previous.get(i);
	            for (int j = i + 1; j < previous.size(); j++) {
	                int[] b = previous.get(j);
	                if (!prefixEqual(a, b, kMinus1 - 1)) {
	                    continue;
	                }
	                int lastA = a[kMinus1 - 1];
	                int lastB = b[kMinus1 - 1];
	                if (lastA == lastB) {
	                    continue;
	                }
	                int[] cand = new int[kMinus1 + 1];
	                System.arraycopy(a, 0, cand, 0, kMinus1 - 1);
	                if (lastA < lastB) {
	                    cand[kMinus1 - 1] = lastA;
	                    cand[kMinus1] = lastB;
	                } else {
	                    cand[kMinus1 - 1] = lastB;
	                    cand[kMinus1] = lastA;
	                }
	                if (allSubsetsFrequent(cand, frequent)) {
	                    candidates.add(cand);
	                }
	            }
	        }
	        return candidates;
	    }

	    private static boolean prefixEqual(int[] a, int[] b, int len) {
	        for (int i = 0; i < len; i++) {
	            if (a[i] != b[i]) {
	                return false;
	            }
	        }
	        return true;
	    }

	    private static boolean allSubsetsFrequent(int[] candidate, Set<String> frequent) {
	        int k = candidate.length;
	        int[] subset = new int[k - 1];
	        for (int drop = 0; drop < k; drop++) {
	            int idx = 0;
	            for (int i = 0; i < k; i++) {
	                if (i != drop) {
	                    subset[idx++] = candidate[i];
	                }
	            }
	            if (!frequent.contains(toKey(subset))) {
	                return false;
	            }
	        }
	        return true;
	    }
	}
	```
</details>

<details>
<summary>SparkUtil.java (full)</summary>
	```java
	package pmj.bdp.spark.lab5;

	import org.apache.spark.SparkConf;
	import org.apache.spark.api.java.JavaSparkContext;

	public final class SparkUtil {

	    private SparkUtil() {
	    }

	    public static JavaSparkContext localContext(String appName) {
	        SparkConf conf = new SparkConf()
	                .setAppName(appName)
	                .setMaster("local[*]")
	                .set("spark.ui.enabled", "false")
	                .set("spark.driver.host", "127.0.0.1");
	        JavaSparkContext sc = new JavaSparkContext(conf);
	        sc.setLogLevel("ERROR");
	        return sc;
	    }
	}
	```
</details>

<details>
<summary>pom.xml (full)</summary>
	```xml
	<?xml version="1.0" encoding="UTF-8"?>
	<project xmlns="http://maven.apache.org/POM/4.0.0"
	         xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
	         xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 http://maven.apache.org/xsd/maven-4.0.0.xsd">

	    <modelVersion>4.0.0</modelVersion>

	    <groupId>pmj.bdp.spark</groupId>
	    <artifactId>lab5</artifactId>
	    <version>1.0</version>
	    <packaging>jar</packaging>

	    <properties>
	        <project.build.sourceEncoding>UTF-8</project.build.sourceEncoding>
	        <maven.compiler.release>17</maven.compiler.release>
	        <spark.version>4.2.0</spark.version>
	    </properties>

	    <dependencies>
	        <dependency>
	            <groupId>org.apache.spark</groupId>
	            <artifactId>spark-core_2.13</artifactId>
	            <version>${spark.version}</version>
	        </dependency>
	    </dependencies>

	    <build>
	        <finalName>lab5</finalName>
	        <plugins>
	            <plugin>
	                <groupId>org.codehaus.mojo</groupId>
	                <artifactId>exec-maven-plugin</artifactId>
	                <version>3.5.0</version>
	            </plugin>
	        </plugins>
	    </build>

	</project>
	```
</details>

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

---
## PDF export
In Notion: **Share → Export → PDF** (or ••• menu → Export). Keep “include subpages” off; this is a standalone private page.
