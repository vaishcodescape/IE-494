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
