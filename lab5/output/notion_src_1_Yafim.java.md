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