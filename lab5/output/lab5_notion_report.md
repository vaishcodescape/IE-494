<callout icon="⚡" color="blue_bg">
	**IE-494 · Big Data Processing · Autumn 2026**
	Instructor: PM Jat (`pm_jat@dau.ac.in`)
	Student: Aditya Vaish · ID `202401401`
	Submitted: <mention-date start="2026-09-11"/>
	**Constraint:** Spark **RDD API only**. No DataFrames, Datasets, or Spark SQL.
</callout>
<table_of_contents/>
---
## Assignment
Implement **basic YAFIM** (Yet Another Frequent Itemset Mining) on Spark RDDs:
- Paper Algorithm 2 — frequent 1-itemsets
- Paper Algorithm 3 — frequent k-itemsets for k ≥ 2 (apriori-gen + broadcast hash tree + `subset()`)
- Mine the FIMI `retail.dat` dataset ([http://fimi.uantwerpen.be/data/retail.dat](http://fimi.uantwerpen.be/data/retail.dat)) with `minSupport = 1000`

**Citation:** Hongjian Qiu, Rong Gu, Chunfeng Yuan, Yihua Huang, *YAFIM: A Parallel Frequent Itemset Mining Algorithm with Spark*, IEEE International Parallel & Distributed Processing Symposium Workshops (**IPDPSW 2014**).

## Dataset
`retail.dat` is a market-basket file from the FIMI repository: one transaction per line, **space-separated integer item IDs**.

<table fit-page-width="true" header-row="true">
	<tr>
		<td>**property**</td>
		<td>**value**</td>
	</tr>
	<tr>
		<td>transactions (baskets)</td>
		<td>88,162</td>
	</tr>
	<tr>
		<td>encoding</td>
		<td>space-separated item IDs, one basket per line</td>
	</tr>
	<tr>
		<td>minSupport</td>
		<td>1000</td>
	</tr>
	<tr>
		<td>relative support</td>
		<td>1000 / 88162 ≈ 1.13%</td>
	</tr>
	<tr>
		<td>Spark master</td>
		<td>`local[*]` (Spark 4.2.0, `spark-core_2.13`, Java 17)</td>
	</tr>
	<tr>
		<td>API</td>
		<td>Java RDD only — no DataFrame / Dataset / Spark SQL</td>
	</tr>
</table>

A tiny `data/toy.dat` (5 baskets) is used as a sanity check with `minSupport = 3`.

---
## Algorithm
YAFIM is Apriori on Spark RDDs. The transaction RDD is **parsed once, cached**, and reused for every pass.

### Phase I — Algorithm 2 (frequent 1-itemsets)
1. `flatMap` each cached transaction to `(item, 1)` pairs.
2. `reduceByKey` to support counts.
3. Keep items with count ≥ `minSupport`. That list is `L1`.

### Phase II — Algorithm 3 (k ≥ 2)
For k = 2, 3, … until `Ck` or `Lk` is empty:
1. **apriori-gen:** self-join frequent (k−1)-itemsets on a shared (k−2)-prefix, then prune any k-candidate whose (k−1)-subsets are not all frequent.
2. Build an in-memory **hash tree** of `Ck` (branch factor 11, leaf split after 4 candidates) and **broadcast** it.
3. Each partition runs `subset()` / `HashTree.findCandidates` on every cached transaction: walk the tree by hashing successive items, and at leaves test `containsAll`.
4. `flatMap` matching candidates to `(key, 1)`, `reduceByKey`, keep counts ≥ `minSupport` → `Lk`.

`SparkContext.collect()` returns an unmodifiable list, so results are copied into `new ArrayList<>(...)` before `List.sort`.

### Hash-tree support counting (bug fix)
Hashing two different items into the same bucket can visit a leaf **more than once** for the same transaction. Without dedupe, the same candidate was emitted multiple times per basket, which **inflated support**. Example before the fix: pair `39,48` counted **75856** (YAFIM) vs **29142** (lab3 MR-Apriori), and `|L2|` was **227** instead of **49**.

`findCandidates` now records keys in a `LinkedHashSet` and returns each candidate **once** per transaction. After the fix, YAFIM matches lab3 on every itemset key and count.

---
## How to run
From `lab5/`. `mvn exec:java` does **not** recompile unless you also `package` / `compile`.
```bash
cd lab5
bash run_all.sh
```
Equivalent Maven commands:
```bash
cd lab5
mvn -DskipTests package

# retail.dat, minSupport=1000
mvn exec:java \
  -Dexec.mainClass=pmj.bdp.spark.lab5.Yafim \
  -Dexec.args="$(pwd)/data/retail.dat 1000 $(pwd)/output/yafim"

# toy sanity check, minSupport=3
mvn exec:java \
  -Dexec.mainClass=pmj.bdp.spark.lab5.Yafim \
  -Dexec.args="$(pwd)/data/toy.dat 3 $(pwd)/output/toy"
```
Outputs: `output/yafim/L1.txt` … `L4.txt` and `frequent-itemsets.txt` (tab-separated `itemset\tsupport`).

---
## Evaluation
### Toy sanity check (`toy.dat`, minSupport = 3)
5 transactions. Expected: L1 = items 1, 2, 3; three frequent pairs; L3 empty (the triple 1,2,3 has support 2).

<table fit-page-width="true" header-row="true">
	<tr>
		<td>**itemset**</td>
		<td>**support**</td>
	</tr>
	<tr>
		<td>1</td>
		<td>4</td>
	</tr>
	<tr>
		<td>2</td>
		<td>4</td>
	</tr>
	<tr>
		<td>3</td>
		<td>4</td>
	</tr>
	<tr>
		<td>1,2</td>
		<td>3</td>
	</tr>
	<tr>
		<td>1,3</td>
		<td>3</td>
	</tr>
	<tr>
		<td>2,3</td>
		<td>3</td>
	</tr>
</table>

L1 = 3, C2 = 3, L2 = 3, C3 = 1, L3 = 0. Matches the hand count.

### Retail (`retail.dat`, minSupport = 1000)
**transactions = 88162**, relative minsup ≈ **1.13%**.

<table fit-page-width="true" header-row="true">
	<tr>
		<td>**level**</td>
		<td>**YAFIM Lk count**</td>
		<td>**lab3 MR-Apriori Lk count**</td>
		<td>**YAFIM Ck count**</td>
		<td>**match?**</td>
	</tr>
	<tr>
		<td>L1</td>
		<td>56</td>
		<td>56</td>
		<td>— (Algo-2)</td>
		<td>yes</td>
	</tr>
	<tr>
		<td>L2</td>
		<td>49</td>
		<td>49</td>
		<td>1540</td>
		<td>yes</td>
	</tr>
	<tr>
		<td>L3</td>
		<td>24</td>
		<td>24</td>
		<td>31</td>
		<td>yes</td>
	</tr>
	<tr>
		<td>L4</td>
		<td>6</td>
		<td>6</td>
		<td>6</td>
		<td>yes</td>
	</tr>
	<tr>
		<td>L5</td>
		<td>0 (stop)</td>
		<td>0</td>
		<td>0</td>
		<td>yes</td>
	</tr>
</table>

Total frequent itemsets written: **135** (56 + 49 + 24 + 6). Lab3’s combined `frequent-itemsets.txt` lists **k ≥ 2 only** (79 itemsets). Every k ≥ 2 key and support in YAFIM equals lab3 (Python diff: 0 missing keys, 0 count mismatches).

Check pair **39,48**:

<table fit-page-width="true" header-row="true">
	<tr>
		<td>**source**</td>
		<td>**support of 39,48**</td>
	</tr>
	<tr>
		<td>YAFIM after HashTree dedupe</td>
		<td>29142</td>
	</tr>
	<tr>
		<td>lab3 MR-Apriori</td>
		<td>29142</td>
	</tr>
	<tr>
		<td>YAFIM before dedupe (buggy)</td>
		<td>75856</td>
	</tr>
</table>

Singleton supports also match (`39` → 50675, `48` → 42135).

#### L4 (all 6 frequent 4-itemsets)

<table fit-page-width="true" header-row="true">
	<tr>
		<td>**itemset**</td>
		<td>**support**</td>
	</tr>
	<tr>
		<td>32,38,39,48</td>
		<td>1236</td>
	</tr>
	<tr>
		<td>32,39,41,48</td>
		<td>1646</td>
	</tr>
	<tr>
		<td>36,38,39,48</td>
		<td>1080</td>
	</tr>
	<tr>
		<td>38,39,41,48</td>
		<td>1991</td>
	</tr>
	<tr>
		<td>38,39,48,110</td>
		<td>1031</td>
	</tr>
	<tr>
		<td>38,39,48,170</td>
		<td>1193</td>
	</tr>
</table>

#### L3 (all 24 frequent 3-itemsets)

<table fit-page-width="true" header-row="true">
	<tr>
		<td>**itemset**</td>
		<td>**support**</td>
	</tr>
	<tr>
		<td>32,38,39</td>
		<td>1840</td>
	</tr>
	<tr>
		<td>32,38,48</td>
		<td>1646</td>
	</tr>
	<tr>
		<td>32,39,41</td>
		<td>2359</td>
	</tr>
	<tr>
		<td>32,39,48</td>
		<td>5402</td>
	</tr>
	<tr>
		<td>32,41,48</td>
		<td>2063</td>
	</tr>
	<tr>
		<td>36,38,39</td>
		<td>1945</td>
	</tr>
	<tr>
		<td>36,38,48</td>
		<td>1360</td>
	</tr>
	<tr>
		<td>36,39,48</td>
		<td>1116</td>
	</tr>
	<tr>
		<td>38,39,110</td>
		<td>1740</td>
	</tr>
	<tr>
		<td>38,39,170</td>
		<td>2019</td>
	</tr>
	<tr>
		<td>38,39,41</td>
		<td>3051</td>
	</tr>
	<tr>
		<td>38,39,48</td>
		<td>6102</td>
	</tr>
	<tr>
		<td>38,41,48</td>
		<td>2374</td>
	</tr>
	<tr>
		<td>38,48,110</td>
		<td>1361</td>
	</tr>
	<tr>
		<td>38,48,170</td>
		<td>1538</td>
	</tr>
	<tr>
		<td>39,41,48</td>
		<td>7366</td>
	</tr>
	<tr>
		<td>39,48,110</td>
		<td>1037</td>
	</tr>
	<tr>
		<td>39,48,170</td>
		<td>1206</td>
	</tr>
	<tr>
		<td>39,48,225</td>
		<td>1400</td>
	</tr>
	<tr>
		<td>39,48,237</td>
		<td>1244</td>
	</tr>
	<tr>
		<td>39,48,310</td>
		<td>1347</td>
	</tr>
	<tr>
		<td>39,48,475</td>
		<td>1092</td>
	</tr>
	<tr>
		<td>39,48,65</td>
		<td>1797</td>
	</tr>
	<tr>
		<td>39,48,89</td>
		<td>2125</td>
	</tr>
</table>

#### L2 (all 49 frequent 2-itemsets)

<table fit-page-width="true" header-row="true">
	<tr>
		<td>**itemset**</td>
		<td>**support**</td>
	</tr>
	<tr>
		<td>32,38</td>
		<td>2833</td>
	</tr>
	<tr>
		<td>32,39</td>
		<td>8455</td>
	</tr>
	<tr>
		<td>32,41</td>
		<td>3196</td>
	</tr>
	<tr>
		<td>32,48</td>
		<td>8034</td>
	</tr>
	<tr>
		<td>36,38</td>
		<td>2790</td>
	</tr>
	<tr>
		<td>36,39</td>
		<td>2037</td>
	</tr>
	<tr>
		<td>36,48</td>
		<td>1416</td>
	</tr>
	<tr>
		<td>37,38</td>
		<td>1046</td>
	</tr>
	<tr>
		<td>38,110</td>
		<td>2725</td>
	</tr>
	<tr>
		<td>38,170</td>
		<td>3031</td>
	</tr>
	<tr>
		<td>38,286</td>
		<td>1116</td>
	</tr>
	<tr>
		<td>38,39</td>
		<td>10345</td>
	</tr>
	<tr>
		<td>38,41</td>
		<td>3897</td>
	</tr>
	<tr>
		<td>38,48</td>
		<td>7944</td>
	</tr>
	<tr>
		<td>39,101</td>
		<td>1400</td>
	</tr>
	<tr>
		<td>39,110</td>
		<td>1759</td>
	</tr>
	<tr>
		<td>39,1327</td>
		<td>1156</td>
	</tr>
	<tr>
		<td>39,147</td>
		<td>1137</td>
	</tr>
	<tr>
		<td>39,170</td>
		<td>2059</td>
	</tr>
	<tr>
		<td>39,2238</td>
		<td>1287</td>
	</tr>
	<tr>
		<td>39,225</td>
		<td>2351</td>
	</tr>
	<tr>
		<td>39,237</td>
		<td>1929</td>
	</tr>
	<tr>
		<td>39,255</td>
		<td>1057</td>
	</tr>
	<tr>
		<td>39,270</td>
		<td>1194</td>
	</tr>
	<tr>
		<td>39,271</td>
		<td>1434</td>
	</tr>
	<tr>
		<td>39,310</td>
		<td>1852</td>
	</tr>
	<tr>
		<td>39,41</td>
		<td>11414</td>
	</tr>
	<tr>
		<td>39,413</td>
		<td>1130</td>
	</tr>
	<tr>
		<td>39,438</td>
		<td>1260</td>
	</tr>
	<tr>
		<td>39,475</td>
		<td>1500</td>
	</tr>
	<tr>
		<td>39,48</td>
		<td>29142</td>
	</tr>
	<tr>
		<td>39,65</td>
		<td>2787</td>
	</tr>
	<tr>
		<td>39,79</td>
		<td>1111</td>
	</tr>
	<tr>
		<td>39,89</td>
		<td>2749</td>
	</tr>
	<tr>
		<td>41,48</td>
		<td>9018</td>
	</tr>
	<tr>
		<td>48,101</td>
		<td>1311</td>
	</tr>
	<tr>
		<td>48,110</td>
		<td>1380</td>
	</tr>
	<tr>
		<td>48,147</td>
		<td>1036</td>
	</tr>
	<tr>
		<td>48,170</td>
		<td>1557</td>
	</tr>
	<tr>
		<td>48,225</td>
		<td>1736</td>
	</tr>
	<tr>
		<td>48,237</td>
		<td>1682</td>
	</tr>
	<tr>
		<td>48,255</td>
		<td>1057</td>
	</tr>
	<tr>
		<td>48,271</td>
		<td>1090</td>
	</tr>
	<tr>
		<td>48,310</td>
		<td>1692</td>
	</tr>
	<tr>
		<td>48,413</td>
		<td>1135</td>
	</tr>
	<tr>
		<td>48,438</td>
		<td>1025</td>
	</tr>
	<tr>
		<td>48,475</td>
		<td>1428</td>
	</tr>
	<tr>
		<td>48,65</td>
		<td>2529</td>
	</tr>
	<tr>
		<td>48,89</td>
		<td>2798</td>
	</tr>
</table>

<details>
<summary>L1 — all 56 frequent 1-itemsets</summary>

	<table fit-page-width="true" header-row="true">
		<tr>
			<td>**item**</td>
			<td>**support**</td>
		</tr>
		<tr>
			<td>9</td>
			<td>1372</td>
		</tr>
		<tr>
			<td>19</td>
			<td>1005</td>
		</tr>
		<tr>
			<td>32</td>
			<td>15167</td>
		</tr>
		<tr>
			<td>36</td>
			<td>2936</td>
		</tr>
		<tr>
			<td>37</td>
			<td>1074</td>
		</tr>
		<tr>
			<td>38</td>
			<td>15596</td>
		</tr>
		<tr>
			<td>39</td>
			<td>50675</td>
		</tr>
		<tr>
			<td>41</td>
			<td>14945</td>
		</tr>
		<tr>
			<td>48</td>
			<td>42135</td>
		</tr>
		<tr>
			<td>49</td>
			<td>1120</td>
		</tr>
		<tr>
			<td>60</td>
			<td>1489</td>
		</tr>
		<tr>
			<td>65</td>
			<td>4472</td>
		</tr>
		<tr>
			<td>78</td>
			<td>1060</td>
		</tr>
		<tr>
			<td>79</td>
			<td>1600</td>
		</tr>
		<tr>
			<td>89</td>
			<td>3837</td>
		</tr>
		<tr>
			<td>101</td>
			<td>2237</td>
		</tr>
		<tr>
			<td>110</td>
			<td>2794</td>
		</tr>
		<tr>
			<td>117</td>
			<td>1026</td>
		</tr>
		<tr>
			<td>123</td>
			<td>1302</td>
		</tr>
		<tr>
			<td>147</td>
			<td>1779</td>
		</tr>
		<tr>
			<td>161</td>
			<td>1010</td>
		</tr>
		<tr>
			<td>170</td>
			<td>3099</td>
		</tr>
		<tr>
			<td>185</td>
			<td>1376</td>
		</tr>
		<tr>
			<td>201</td>
			<td>1133</td>
		</tr>
		<tr>
			<td>225</td>
			<td>3257</td>
		</tr>
		<tr>
			<td>237</td>
			<td>3032</td>
		</tr>
		<tr>
			<td>249</td>
			<td>1160</td>
		</tr>
		<tr>
			<td>255</td>
			<td>1474</td>
		</tr>
		<tr>
			<td>270</td>
			<td>1734</td>
		</tr>
		<tr>
			<td>271</td>
			<td>2094</td>
		</tr>
		<tr>
			<td>286</td>
			<td>1183</td>
		</tr>
		<tr>
			<td>301</td>
			<td>1204</td>
		</tr>
		<tr>
			<td>310</td>
			<td>2594</td>
		</tr>
		<tr>
			<td>338</td>
			<td>1274</td>
		</tr>
		<tr>
			<td>413</td>
			<td>1880</td>
		</tr>
		<tr>
			<td>438</td>
			<td>1863</td>
		</tr>
		<tr>
			<td>475</td>
			<td>2167</td>
		</tr>
		<tr>
			<td>533</td>
			<td>1487</td>
		</tr>
		<tr>
			<td>548</td>
			<td>1137</td>
		</tr>
		<tr>
			<td>589</td>
			<td>1119</td>
		</tr>
		<tr>
			<td>592</td>
			<td>1227</td>
		</tr>
		<tr>
			<td>604</td>
			<td>1209</td>
		</tr>
		<tr>
			<td>677</td>
			<td>1110</td>
		</tr>
		<tr>
			<td>740</td>
			<td>1181</td>
		</tr>
		<tr>
			<td>824</td>
			<td>1210</td>
		</tr>
		<tr>
			<td>1004</td>
			<td>1102</td>
		</tr>
		<tr>
			<td>1146</td>
			<td>1426</td>
		</tr>
		<tr>
			<td>1327</td>
			<td>1786</td>
		</tr>
		<tr>
			<td>1393</td>
			<td>1161</td>
		</tr>
		<tr>
			<td>2238</td>
			<td>1715</td>
		</tr>
		<tr>
			<td>12925</td>
			<td>1467</td>
		</tr>
		<tr>
			<td>13041</td>
			<td>1051</td>
		</tr>
		<tr>
			<td>14098</td>
			<td>1291</td>
		</tr>
		<tr>
			<td>15832</td>
			<td>1143</td>
		</tr>
		<tr>
			<td>16010</td>
			<td>1316</td>
		</tr>
		<tr>
			<td>16217</td>
			<td>1166</td>
		</tr>
	</table>

</details>

### Terminal summary (retail)
Spark driver INFO lines omitted; application stdout:
```text
transactions = 88162
minSupport   = 1000  (0.0113 of n)

======== YAFIM Algo-2  L1 ========
|L1| = 56

======== YAFIM Algo-3  L2 ========
|C2| = 1540
|L2| = 49

======== YAFIM Algo-3  L3 ========
|C3| = 31
|L3| = 24

======== YAFIM Algo-3  L4 ========
|C4| = 6
|L4| = 6

======== YAFIM Algo-3  L5 ========
|C5| = 0

Wrote 135 frequent itemsets to output/yafim/frequent-itemsets.txt
```
```text
56 output/yafim/L1.txt
49 output/yafim/L2.txt
24 output/yafim/L3.txt
 6 output/yafim/L4.txt
135 output/yafim/frequent-itemsets.txt
```

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
