IE-494 · Big Data Processing · Autumn 2026
Instructor: PM Jat (`pm_jat@dau.ac.in`)
Student: Aditya Vaish · ID `202401401`
Submitted: <mention-date start="2026-09-18"/>
Part 1: Spark **RDD** full cube (lattice of cuboids), materialized as parquet `SalesCube`.
Part 2: slice/dice queries on `SalesCube` with the **DataFrame** API.
<table_of_contents/>
---
## Assignment
Implement a data cube as a lattice of cuboids in Spark, using the lecture sales dataset.

1. Compute the **full cube** as a lattice using **Spark RDDs** (not the DataFrame `cube()` operator) and materialize it in parquet as `SalesCube`.
2. Answer the following from `SalesCube` using the Spark **DataFrame** API:
	1. Total sales (amount) of all items of category `Tshirts`. Project: `item`, `sum(amount)`.
	2. Total sales of all items of category `Tshirts` at `store1`. Project: `item`, `sum(amount)`.
	3. Total sales of customer `cust1`. Project: `sum(amount)`.
	4. Total sales of customer `cust1` from items of category `Tshirts`. Project: `item`, `sum(amount)`.
	5. Total sales of all stores of state `WA` for category `Tshirts`. Project: `store`, `item`, `sum(amount)`.
	6. Gender-wise total sales of items in category `Jacket`. Project: `gender`, `item`, `sum(amount)`.

## Dataset
Denormalized sales facts in `lab6/data/sales.csv`. Measure is `amount`. Dimensions:

<table fit-page-width="true" header-row="true">
	<tr>
		<td>**dimension**</td>
		<td>**role**</td>
	</tr>
	<tr>
		<td>item</td>
		<td>product</td>
	</tr>
	<tr>
		<td>category</td>
		<td>item type (`Tshirts`, `Jacket`, `Hoodies`)</td>
	</tr>
	<tr>
		<td>store</td>
		<td>store id (`store1`, `store2`, `store3`)</td>
	</tr>
	<tr>
		<td>state</td>
		<td>store state (`WA`, `CA`)</td>
	</tr>
	<tr>
		<td>customer</td>
		<td>customer id (`cust1`, …)</td>
	</tr>
	<tr>
		<td>gender</td>
		<td>customer gender (`M`, `F`)</td>
	</tr>
	<tr>
		<td>amount</td>
		<td>measure (sum)</td>
	</tr>
</table>

<table fit-page-width="true" header-row="true">
	<tr>
		<td>**property**</td>
		<td>**value**</td>
	</tr>
	<tr>
		<td>facts</td>
		<td>10</td>
	</tr>
	<tr>
		<td>dimensions</td>
		<td>6 (`item`, `category`, `store`, `state`, `customer`, `gender`)</td>
	</tr>
	<tr>
		<td>cuboids in lattice</td>
		<td>2^6 = 64</td>
	</tr>
	<tr>
		<td>materialized cells</td>
		<td>515</td>
	</tr>
	<tr>
		<td>Spark</td>
		<td>4.2.0 (`spark-core_2.13`, `spark-sql_2.13`), Java 17, master `local[*]`</td>
	</tr>
	<tr>
		<td>cube storage</td>
		<td>parquet directory `lab6/SalesCube`</td>
	</tr>
	<tr>
		<td>ALL sentinel</td>
		<td>`*` (rolled-up dimension)</td>
	</tr>
</table>

Input rows:

<table fit-page-width="true" header-row="true">
	<tr>
		<td>**item**</td>
		<td>**category**</td>
		<td>**store**</td>
		<td>**state**</td>
		<td>**customer**</td>
		<td>**gender**</td>
		<td>**amount**</td>
	</tr>
	<tr>
		<td>blue-tshirt</td>
		<td>Tshirts</td>
		<td>store1</td>
		<td>WA</td>
		<td>cust1</td>
		<td>M</td>
		<td>120</td>
	</tr>
	<tr>
		<td>red-tshirt</td>
		<td>Tshirts</td>
		<td>store1</td>
		<td>WA</td>
		<td>cust2</td>
		<td>F</td>
		<td>80</td>
	</tr>
	<tr>
		<td>green-tshirt</td>
		<td>Tshirts</td>
		<td>store2</td>
		<td>WA</td>
		<td>cust1</td>
		<td>M</td>
		<td>60</td>
	</tr>
	<tr>
		<td>white-tshirt</td>
		<td>Tshirts</td>
		<td>store3</td>
		<td>CA</td>
		<td>cust3</td>
		<td>F</td>
		<td>40</td>
	</tr>
	<tr>
		<td>navy-tshirt</td>
		<td>Tshirts</td>
		<td>store1</td>
		<td>WA</td>
		<td>cust1</td>
		<td>M</td>
		<td>45</td>
	</tr>
	<tr>
		<td>leather-jacket</td>
		<td>Jacket</td>
		<td>store1</td>
		<td>WA</td>
		<td>cust1</td>
		<td>M</td>
		<td>300</td>
	</tr>
	<tr>
		<td>denim-jacket</td>
		<td>Jacket</td>
		<td>store2</td>
		<td>CA</td>
		<td>cust2</td>
		<td>F</td>
		<td>220</td>
	</tr>
	<tr>
		<td>rain-jacket</td>
		<td>Jacket</td>
		<td>store1</td>
		<td>WA</td>
		<td>cust4</td>
		<td>F</td>
		<td>180</td>
	</tr>
	<tr>
		<td>bomber-jacket</td>
		<td>Jacket</td>
		<td>store3</td>
		<td>CA</td>
		<td>cust1</td>
		<td>M</td>
		<td>250</td>
	</tr>
	<tr>
		<td>hoodie</td>
		<td>Hoodies</td>
		<td>store1</td>
		<td>WA</td>
		<td>cust1</td>
		<td>M</td>
		<td>90</td>
	</tr>
</table>

---
## Method
### Part 1 — RDD lattice
Each cached fact is expanded to all 64 grouping-sets. Bit *i* of the mask keeps dimension *i*; a cleared bit replaces it with `*`. `reduceByKey` sums `amount`. The resulting `(CubeKey, amount)` pairs are written as parquet `SalesCube`.

```java
final int nCuboids = 1 << CubeKey.NDIM; // 64
sales.flatMapToPair(s -> {
    List<Tuple2<CubeKey, Double>> cells = new ArrayList<>(nCuboids);
    for (int mask = 0; mask < nCuboids; mask++) {
        cells.add(new Tuple2<>(CubeKey.fromSale(s, mask), s.amount));
    }
    return cells.iterator();
}).reduceByKey(Double::sum);
```

The DataFrame `cube()` operator is not used to compute the lattice.

### Part 2 — DataFrame lookups
Queries do not re-aggregate the facts. They select the cuboid whose free dimensions match the requested projection (those columns are not `*`) and whose other columns are `*`, then filter the remaining predicates (`Tshirts`, `store1`, `cust1`, `WA`, `Jacket`).

<table fit-page-width="true" header-row="true">
	<tr>
		<td>**query**</td>
		<td>**cuboid (non-ALL dimensions)**</td>
		<td>**filter**</td>
	</tr>
	<tr>
		<td>Q1</td>
		<td>`item`, `category`</td>
		<td>`category = Tshirts`</td>
	</tr>
	<tr>
		<td>Q2</td>
		<td>`item`, `category`, `store`</td>
		<td>`category = Tshirts` and `store = store1`</td>
	</tr>
	<tr>
		<td>Q3</td>
		<td>`customer`</td>
		<td>`customer = cust1`</td>
	</tr>
	<tr>
		<td>Q4</td>
		<td>`item`, `category`, `customer`</td>
		<td>`customer = cust1` and `category = Tshirts`</td>
	</tr>
	<tr>
		<td>Q5</td>
		<td>`item`, `category`, `store`, `state`</td>
		<td>`state = WA` and `category = Tshirts`</td>
	</tr>
	<tr>
		<td>Q6</td>
		<td>`item`, `category`, `gender`</td>
		<td>`category = Jacket`</td>
	</tr>
</table>

---
## How to run
From `lab6/`. `mvn exec:java` does not recompile unless `package` / `compile` is also run.

```bash
cd lab6
bash run_all.sh
```

Equivalent Maven:

```bash
cd lab6
mvn -DskipTests package
mvn exec:java \
  -Dexec.mainClass=pmj.bdp.spark.lab6.Lab06 \
  -Dexec.args="$(pwd)/data $(pwd)/SalesCube"
```

Outputs: parquet `SalesCube/` and tab-separated query files under `output/`.

---
## Lattice summary
`k` is the number of non-ALL dimensions in a cell (granularity of that cuboid). The apex cuboid (`k = 0`) is one cell: total sales over all facts. The base cuboid (`k = 6`) has one cell per fact (10).

<table fit-page-width="true" header-row="true">
	<tr>
		<td>**k**</td>
		<td>**cells**</td>
	</tr>
	<tr>
		<td>0</td>
		<td>1</td>
	</tr>
	<tr>
		<td>1</td>
		<td>24</td>
	</tr>
	<tr>
		<td>2</td>
		<td>105</td>
	</tr>
	<tr>
		<td>3</td>
		<td>174</td>
	</tr>
	<tr>
		<td>4</td>
		<td>142</td>
	</tr>
	<tr>
		<td>5</td>
		<td>59</td>
	</tr>
	<tr>
		<td>6</td>
		<td>10</td>
	</tr>
	<tr>
		<td>total</td>
		<td>515</td>
	</tr>
</table>

cuboids in lattice = 64 (2^6)

SalesCube schema: `item`, `category`, `store`, `state`, `customer`, `gender`, `amount`.

---
## Query results
### Q1. Tshirts by item
Project: `item`, `sum(amount)`. Cuboid (`item`, `category`).

<table fit-page-width="true" header-row="true">
	<tr>
		<td>**item**</td>
		<td>**amount**</td>
	</tr>
	<tr>
		<td>blue-tshirt</td>
		<td>120</td>
	</tr>
	<tr>
		<td>green-tshirt</td>
		<td>60</td>
	</tr>
	<tr>
		<td>navy-tshirt</td>
		<td>45</td>
	</tr>
	<tr>
		<td>red-tshirt</td>
		<td>80</td>
	</tr>
	<tr>
		<td>white-tshirt</td>
		<td>40</td>
	</tr>
</table>

Sum of Q1 amounts = 345, which matches all Tshirts facts (120+80+60+40+45).

### Q2. Tshirts at store1 by item
Project: `item`, `sum(amount)`. Cuboid (`item`, `category`, `store`).

<table fit-page-width="true" header-row="true">
	<tr>
		<td>**item**</td>
		<td>**amount**</td>
	</tr>
	<tr>
		<td>blue-tshirt</td>
		<td>120</td>
	</tr>
	<tr>
		<td>navy-tshirt</td>
		<td>45</td>
	</tr>
	<tr>
		<td>red-tshirt</td>
		<td>80</td>
	</tr>
</table>

Sum = 245 (store1 Tshirts only; green-tshirt at store2 and white-tshirt at store3 are excluded).

### Q3. Total sales of cust1
Project: `sum(amount)`. Cuboid (`customer`).

<table fit-page-width="true" header-row="true">
	<tr>
		<td>**amount**</td>
	</tr>
	<tr>
		<td>865</td>
	</tr>
</table>

cust1 facts: 120+60+45+300+250+90 = 865.

### Q4. cust1 Tshirts by item
Project: `item`, `sum(amount)`. Cuboid (`item`, `category`, `customer`).

<table fit-page-width="true" header-row="true">
	<tr>
		<td>**item**</td>
		<td>**amount**</td>
	</tr>
	<tr>
		<td>blue-tshirt</td>
		<td>120</td>
	</tr>
	<tr>
		<td>green-tshirt</td>
		<td>60</td>
	</tr>
	<tr>
		<td>navy-tshirt</td>
		<td>45</td>
	</tr>
</table>

Sum = 225.

### Q5. WA Tshirts by store, item
Project: `store`, `item`, `sum(amount)`. Cuboid (`item`, `category`, `store`, `state`).

<table fit-page-width="true" header-row="true">
	<tr>
		<td>**store**</td>
		<td>**item**</td>
		<td>**amount**</td>
	</tr>
	<tr>
		<td>store1</td>
		<td>blue-tshirt</td>
		<td>120</td>
	</tr>
	<tr>
		<td>store1</td>
		<td>navy-tshirt</td>
		<td>45</td>
	</tr>
	<tr>
		<td>store1</td>
		<td>red-tshirt</td>
		<td>80</td>
	</tr>
	<tr>
		<td>store2</td>
		<td>green-tshirt</td>
		<td>60</td>
	</tr>
</table>

white-tshirt is CA and is excluded. Sum = 305.

### Q6. Jacket by gender, item
Project: `gender`, `item`, `sum(amount)`. Cuboid (`item`, `category`, `gender`).

<table fit-page-width="true" header-row="true">
	<tr>
		<td>**gender**</td>
		<td>**item**</td>
		<td>**amount**</td>
	</tr>
	<tr>
		<td>F</td>
		<td>denim-jacket</td>
		<td>220</td>
	</tr>
	<tr>
		<td>F</td>
		<td>rain-jacket</td>
		<td>180</td>
	</tr>
	<tr>
		<td>M</td>
		<td>bomber-jacket</td>
		<td>250</td>
	</tr>
	<tr>
		<td>M</td>
		<td>leather-jacket</td>
		<td>300</td>
	</tr>
</table>

Sum = 950.

---
## Terminal output
```text
loading denormalized sales from sales.csv
sales facts = 10
materialized SalesCube -> /Users/avaish/IE-494/lab6/SalesCube

======== lattice summary (cells per cuboid granularity) ========
k	#cells
0	1
1	24
2	105
3	174
4	142
5	59
6	10
cuboids in lattice = 64  (2^6)
materialized cells = 515

SalesCube cells = 515
```

Query `show()` tables match the result tables above.

---
## Source code
All of `lab6/`. Part 1 is RDD-only for cube construction. Part 2 reads the parquet cube with DataFrames.

<details>
<summary>Lab06.java</summary>
	```java
	package pmj.bdp.spark.lab6;
	
	import java.io.IOException;
	import java.nio.file.Files;
	import java.nio.file.Path;
	import java.util.ArrayList;
	import java.util.Comparator;
	import java.util.List;
	
	import org.apache.spark.api.java.JavaPairRDD;
	import org.apache.spark.api.java.JavaRDD;
	import org.apache.spark.api.java.JavaSparkContext;
	import org.apache.spark.sql.Dataset;
	import org.apache.spark.sql.Row;
	import org.apache.spark.sql.RowFactory;
	import org.apache.spark.sql.SparkSession;
	import org.apache.spark.sql.types.DataTypes;
	import org.apache.spark.sql.types.StructType;
	
	import scala.Tuple2;
	
	/**
	 * Lab 06 — data cube as a lattice of cuboids.
	 *
	 * 1. Full cube on Spark RDDs (2^6 grouping-sets), materialized as parquet {@code SalesCube}.
	 * 2. Slice/dice lookups on that parquet with the DataFrame API.
	 *
	 * Usage: Lab06 &lt;dataDir&gt; [salesCubeDir]
	 */
	public class Lab06 {
	
	    public static void main(String[] args) throws IOException {
	        if (args.length < 1) {
	            System.err.println("Usage: Lab06 <dataDir> [salesCubeDir]");
	            System.exit(1);
	        }
	        Path dataDir = Path.of(args[0]);
	        Path cubeDir = Path.of(args.length > 1 ? args[1] : "SalesCube");
	
	        SparkSession spark = SparkUtil.localSession("Lab6 SalesCube");
	        JavaSparkContext sc = SparkUtil.javaContext(spark);
	        try {
	            JavaRDD<Sale> sales = SalesLoader.load(sc, dataDir).cache();
	            long n = sales.count();
	            System.out.println("sales facts = " + n);
	            if (n == 0) {
	                throw new IllegalStateException("no sales rows loaded from " + dataDir);
	            }
	
	            JavaPairRDD<CubeKey, Double> lattice = buildLattice(sales).cache();
	            materializeParquet(spark, lattice, cubeDir);
	            printLatticeSummary(lattice);
	
	            Dataset<Row> cube = spark.read().parquet(cubeDir.toString());
	            cube.createOrReplaceTempView("SalesCube");
	            System.out.println();
	            System.out.println("SalesCube schema:");
	            cube.printSchema();
	            System.out.println("SalesCube cells = " + cube.count());
	
	            Path outDir = cubeDir.toAbsolutePath().getParent() == null
	                    ? Path.of("output")
	                    : cubeDir.toAbsolutePath().getParent().resolve("output");
	            Files.createDirectories(outDir);
	            SalesCubeQuery.runAll(cube, outDir);
	        } finally {
	            sc.close();
	            spark.stop();
	        }
	    }
	
	    /**
	     * Naive full-cube: each fact emits every cuboid (all 2^{6} masks), then
	     * {@code reduceByKey} sums amount. {@code *} is ALL.
	     */
	    static JavaPairRDD<CubeKey, Double> buildLattice(JavaRDD<Sale> sales) {
	        final int nCuboids = 1 << CubeKey.NDIM;
	        return sales.flatMapToPair(s -> {
	            List<Tuple2<CubeKey, Double>> cells = new ArrayList<>(nCuboids);
	            for (int mask = 0; mask < nCuboids; mask++) {
	                cells.add(new Tuple2<>(CubeKey.fromSale(s, mask), s.amount));
	            }
	            return cells.iterator();
	        }).reduceByKey(Double::sum);
	    }
	
	    static void materializeParquet(SparkSession spark,
	                                   JavaPairRDD<CubeKey, Double> lattice,
	                                   Path cubeDir) throws IOException {
	        if (Files.exists(cubeDir)) {
	            deleteRecursively(cubeDir);
	        }
	
	        StructType schema = new StructType()
	                .add("item", DataTypes.StringType, false)
	                .add("category", DataTypes.StringType, false)
	                .add("store", DataTypes.StringType, false)
	                .add("state", DataTypes.StringType, false)
	                .add("customer", DataTypes.StringType, false)
	                .add("gender", DataTypes.StringType, false)
	                .add("amount", DataTypes.DoubleType, false);
	
	        JavaRDD<Row> rows = lattice.map(t -> RowFactory.create(
	                t._1.get(CubeKey.ITEM),
	                t._1.get(CubeKey.CATEGORY),
	                t._1.get(CubeKey.STORE),
	                t._1.get(CubeKey.STATE),
	                t._1.get(CubeKey.CUSTOMER),
	                t._1.get(CubeKey.GENDER),
	                t._2));
	
	        spark.createDataFrame(rows, schema)
	                .write()
	                .mode("overwrite")
	                .parquet(cubeDir.toString());
	        System.out.println("materialized SalesCube -> " + cubeDir.toAbsolutePath());
	    }
	
	    static void printLatticeSummary(JavaPairRDD<CubeKey, Double> lattice) {
	        SparkUtil.banner("lattice summary (cells per cuboid granularity)");
	        List<Tuple2<Integer, Integer>> byDim = new ArrayList<>(lattice
	                .mapToPair(t -> new Tuple2<>(t._1.granularity(), 1))
	                .reduceByKey(Integer::sum)
	                .collect());
	        byDim.sort(Comparator.comparingInt(t -> t._1));
	        System.out.println("k\t#cells");
	        int total = 0;
	        for (Tuple2<Integer, Integer> t : byDim) {
	            System.out.println(t._1 + "\t" + t._2);
	            total += t._2;
	        }
	        System.out.println("cuboids in lattice = " + (1 << CubeKey.NDIM)
	                + "  (2^" + CubeKey.NDIM + ")");
	        System.out.println("materialized cells = " + total);
	    }
	
	    private static void deleteRecursively(Path p) throws IOException {
	        if (!Files.exists(p)) {
	            return;
	        }
	        try (var walk = Files.walk(p)) {
	            walk.sorted(Comparator.reverseOrder()).forEach(q -> {
	                try {
	                    Files.deleteIfExists(q);
	                } catch (IOException e) {
	                    throw new RuntimeException(e);
	                }
	            });
	        }
	    }
	}
	```
</details>

<details>
<summary>SalesCubeQuery.java</summary>
	```java
	package pmj.bdp.spark.lab6;
	
	import java.io.BufferedWriter;
	import java.io.IOException;
	import java.nio.charset.StandardCharsets;
	import java.nio.file.Files;
	import java.nio.file.Path;
	import java.util.List;
	
	import org.apache.spark.sql.Column;
	import org.apache.spark.sql.Dataset;
	import org.apache.spark.sql.Row;
	
	import static org.apache.spark.sql.functions.col;
	
	/**
	 * Slice/dice the materialized {@code SalesCube} with the DataFrame API.
	 * {@code *} means ALL (rolled-up dimension). Queries pick the cuboid whose
	 * free dimensions match the requested projection.
	 */
	public final class SalesCubeQuery {
	
	    private SalesCubeQuery() {
	    }
	
	    public static void runAll(Dataset<Row> cube, Path outDir) throws IOException {
	        Files.createDirectories(outDir);
	
	        q1TshirtsByItem(cube, outDir);
	        q2TshirtsAtStore1(cube, outDir);
	        q3Cust1Total(cube, outDir);
	        q4Cust1Tshirts(cube, outDir);
	        q5WaTshirtsByStoreItem(cube, outDir);
	        q6JacketByGenderItem(cube, outDir);
	    }
	
	    /** Q1. Total sales of all items of category Tshirts. Project: item, sum(amount). */
	    static void q1TshirtsByItem(Dataset<Row> cube, Path outDir) throws IOException {
	        SparkUtil.banner("Q1  Tshirts by item");
	        Dataset<Row> r = cuboid(cube,
	                allExcept("item", "category"))
	                .filter(col("category").equalTo("Tshirts"));
	        showAndSave(r.select("item", "amount"), outDir.resolve("q1_tshirts_by_item.txt"));
	    }
	
	    /** Q2. Tshirts at store1. Project: item, sum(amount). */
	    static void q2TshirtsAtStore1(Dataset<Row> cube, Path outDir) throws IOException {
	        SparkUtil.banner("Q2  Tshirts at store1 by item");
	        Dataset<Row> r = cuboid(cube,
	                allExcept("item", "category", "store"))
	                .filter(col("category").equalTo("Tshirts")
	                        .and(col("store").equalTo("store1")));
	        showAndSave(r.select("item", "amount"), outDir.resolve("q2_tshirts_store1.txt"));
	    }
	
	    /** Q3. Total sales of customer cust1. Project: sum(amount). */
	    static void q3Cust1Total(Dataset<Row> cube, Path outDir) throws IOException {
	        SparkUtil.banner("Q3  total sales of cust1");
	        Dataset<Row> r = cuboid(cube, allExcept("customer"))
	                .filter(col("customer").equalTo("cust1"))
	                .select("amount");
	        showAndSave(r, outDir.resolve("q3_cust1_total.txt"));
	    }
	
	    /** Q4. cust1 sales of Tshirts. Project: item, sum(amount). */
	    static void q4Cust1Tshirts(Dataset<Row> cube, Path outDir) throws IOException {
	        SparkUtil.banner("Q4  cust1 Tshirts by item");
	        Dataset<Row> r = cuboid(cube,
	                allExcept("item", "category", "customer"))
	                .filter(col("customer").equalTo("cust1")
	                        .and(col("category").equalTo("Tshirts")));
	        showAndSave(r.select("item", "amount"), outDir.resolve("q4_cust1_tshirts.txt"));
	    }
	
	    /** Q5. WA stores, Tshirts. Project: store, item, sum(amount). */
	    static void q5WaTshirtsByStoreItem(Dataset<Row> cube, Path outDir) throws IOException {
	        SparkUtil.banner("Q5  WA Tshirts by store, item");
	        Dataset<Row> r = cuboid(cube,
	                allExcept("item", "category", "store", "state"))
	                .filter(col("state").equalTo("WA")
	                        .and(col("category").equalTo("Tshirts")));
	        showAndSave(r.select("store", "item", "amount"),
	                outDir.resolve("q5_wa_tshirts_store_item.txt"));
	    }
	
	    /** Q6. Gender-wise Jacket sales. Project: gender, item, sum(amount). */
	    static void q6JacketByGenderItem(Dataset<Row> cube, Path outDir) throws IOException {
	        SparkUtil.banner("Q6  Jacket by gender, item");
	        Dataset<Row> r = cuboid(cube,
	                allExcept("item", "category", "gender"))
	                .filter(col("category").equalTo("Jacket"));
	        showAndSave(r.select("gender", "item", "amount"),
	                outDir.resolve("q6_jacket_gender_item.txt"));
	    }
	
	    /**
	     * Keep only the cuboid whose rolled-up dimensions are {@code *} and whose
	     * {@code keep} dimensions are not {@code *}.
	     */
	    static Dataset<Row> cuboid(Dataset<Row> cube, String[] keep) {
	        Column pred = null;
	        java.util.Set<String> kept = java.util.Set.of(keep);
	        for (String name : CubeKey.NAMES) {
	            Column c = kept.contains(name)
	                    ? col(name).notEqual(CubeKey.ALL)
	                    : col(name).equalTo(CubeKey.ALL);
	            pred = pred == null ? c : pred.and(c);
	        }
	        return cube.filter(pred);
	    }
	
	    static String[] allExcept(String... keep) {
	        return keep;
	    }
	
	    static void showAndSave(Dataset<Row> df, Path file) throws IOException {
	        Dataset<Row> ordered = df;
	        String[] cols = df.columns();
	        if (cols.length > 0) {
	            Column[] order = new Column[cols.length];
	            for (int i = 0; i < cols.length; i++) {
	                order[i] = col(cols[i]);
	            }
	            ordered = df.orderBy(order);
	        }
	        ordered.show(100, false);
	        List<Row> rows = ordered.collectAsList();
	        try (BufferedWriter w = Files.newBufferedWriter(file, StandardCharsets.UTF_8)) {
	            w.write(String.join("\t", ordered.columns()));
	            w.newLine();
	            for (Row r : rows) {
	                StringBuilder sb = new StringBuilder();
	                for (int i = 0; i < r.size(); i++) {
	                    if (i > 0) {
	                        sb.append('\t');
	                    }
	                    Object v = r.get(i);
	                    if (v instanceof Double d) {
	                        if (d == Math.rint(d)) {
	                            sb.append(String.format("%.0f", d));
	                        } else {
	                            sb.append(String.format("%.2f", d));
	                        }
	                    } else {
	                        sb.append(v);
	                    }
	                }
	                w.write(sb.toString());
	                w.newLine();
	            }
	        }
	        System.out.println("wrote " + file);
	    }
	}
	```
</details>

<details>
<summary>CubeKey.java</summary>
	```java
	package pmj.bdp.spark.lab6;
	
	import java.io.Serializable;
	import java.util.Arrays;
	import java.util.Objects;
	
	/**
	 * One cell in the cube lattice. Aggregated dimensions are {@link #ALL}.
	 *
	 * Dimension order matches the lectures' sales cube:
	 * item, category, store, state, customer, gender.
	 */
	public final class CubeKey implements Serializable {
	
	    private static final long serialVersionUID = 1L;
	
	    public static final String ALL = "*";
	    public static final int NDIM = 6;
	
	    public static final int ITEM = 0;
	    public static final int CATEGORY = 1;
	    public static final int STORE = 2;
	    public static final int STATE = 3;
	    public static final int CUSTOMER = 4;
	    public static final int GENDER = 5;
	
	    public static final String[] NAMES = {
	            "item", "category", "store", "state", "customer", "gender"
	    };
	
	    public final String[] dims;
	
	    public CubeKey(String[] dims) {
	        this.dims = dims;
	    }
	
	    /**
	     * Emit the cuboid identified by {@code mask}: bit i set means keep dimension i,
	     * bit i clear means roll it up to {@link #ALL}.
	     */
	    public static CubeKey fromSale(Sale s, int mask) {
	        String[] d = new String[NDIM];
	        for (int i = 0; i < NDIM; i++) {
	            d[i] = (mask & (1 << i)) != 0 ? s.dimension(i) : ALL;
	        }
	        return new CubeKey(d);
	    }
	
	    public String get(int i) {
	        return dims[i];
	    }
	
	    /** Number of non-ALL dimensions — the cuboid's dimensionality. */
	    public int granularity() {
	        int n = 0;
	        for (String d : dims) {
	            if (!ALL.equals(d)) {
	                n++;
	            }
	        }
	        return n;
	    }
	
	    @Override
	    public boolean equals(Object o) {
	        if (this == o) {
	            return true;
	        }
	        if (!(o instanceof CubeKey other)) {
	            return false;
	        }
	        return Arrays.equals(dims, other.dims);
	    }
	
	    @Override
	    public int hashCode() {
	        return Arrays.hashCode(dims);
	    }
	
	    @Override
	    public String toString() {
	        return Objects.toString(Arrays.toString(dims));
	    }
	}
	```
</details>

<details>
<summary>Sale.java</summary>
	```java
	package pmj.bdp.spark.lab6;
	
	import java.io.Serializable;
	
	/**
	 * One denormalized sales fact used as the cube's base cuboid:
	 * item, category, store, state, customer, gender, amount.
	 */
	public final class Sale implements Serializable {
	
	    private static final long serialVersionUID = 1L;
	
	    public final String item;
	    public final String category;
	    public final String store;
	    public final String state;
	    public final String customer;
	    public final String gender;
	    public final double amount;
	
	    public Sale(String item, String category, String store, String state,
	                String customer, String gender, double amount) {
	        this.item = item;
	        this.category = category;
	        this.store = store;
	        this.state = state;
	        this.customer = customer;
	        this.gender = gender;
	        this.amount = amount;
	    }
	
	    public String dimension(int i) {
	        return switch (i) {
	            case CubeKey.ITEM -> item;
	            case CubeKey.CATEGORY -> category;
	            case CubeKey.STORE -> store;
	            case CubeKey.STATE -> state;
	            case CubeKey.CUSTOMER -> customer;
	            case CubeKey.GENDER -> gender;
	            default -> throw new IllegalArgumentException("dim " + i);
	        };
	    }
	}
	```
</details>

<details>
<summary>SalesLoader.java</summary>
	```java
	package pmj.bdp.spark.lab6;
	
	import java.io.IOException;
	import java.nio.file.Files;
	import java.nio.file.Path;
	import java.util.ArrayList;
	import java.util.HashMap;
	import java.util.List;
	import java.util.Locale;
	import java.util.Map;
	import java.util.stream.Stream;
	
	import org.apache.spark.api.java.JavaPairRDD;
	import org.apache.spark.api.java.JavaRDD;
	import org.apache.spark.api.java.JavaSparkContext;
	
	import scala.Tuple2;
	
	/**
	 * Load the lecture sales dataset. Accepts either:
	 * <ul>
	 *   <li>one denormalized CSV with columns item, category, store, state,
	 *       customer, gender, amount (header names are matched loosely), or</li>
	 *   <li>a star schema: sales/orders + items + stores + customers, joined on
	 *       item / store / customer.</li>
	 * </ul>
	 */
	public final class SalesLoader {
	
	    private SalesLoader() {
	    }
	
	    public static JavaRDD<Sale> load(JavaSparkContext sc, Path dataDir) throws IOException {
	        if (Files.isRegularFile(dataDir)) {
	            return loadDenormalized(sc, dataDir);
	        }
	        if (!Files.isDirectory(dataDir)) {
	            throw new IOException("data path not found: " + dataDir);
	        }
	
	        List<Path> csvs = listCsv(dataDir);
	        if (csvs.isEmpty()) {
	            throw new IOException("no CSV files under " + dataDir);
	        }
	
	        Path denorm = pick(csvs, "sales", "sale", "factsales", "fact_sales", "dataset");
	        if (denorm != null && hasAllBaseColumns(denorm)) {
	            System.out.println("loading denormalized sales from " + denorm.getFileName());
	            return loadDenormalized(sc, denorm);
	        }
	
	        Path sales = pick(csvs, "sales", "orders", "fact", "transactions");
	        Path items = pick(csvs, "items", "item", "products", "product");
	        Path stores = pick(csvs, "stores", "store", "locations", "location");
	        Path customers = pick(csvs, "customers", "customer", "cust");
	        if (sales != null && items != null && stores != null && customers != null) {
	            System.out.println("loading star schema:");
	            System.out.println("  sales     " + sales.getFileName());
	            System.out.println("  items     " + items.getFileName());
	            System.out.println("  stores    " + stores.getFileName());
	            System.out.println("  customers " + customers.getFileName());
	            return loadStar(sc, sales, items, stores, customers);
	        }
	
	        if (denorm != null) {
	            System.out.println("loading CSV as denormalized sales: " + denorm.getFileName());
	            return loadDenormalized(sc, denorm);
	        }
	        if (csvs.size() == 1) {
	            return loadDenormalized(sc, csvs.get(0));
	        }
	        throw new IOException("could not identify sales dataset in " + dataDir
	                + " (files: " + csvs + ")");
	    }
	
	    static JavaRDD<Sale> loadDenormalized(JavaSparkContext sc, Path file) throws IOException {
	        Header hdr = Header.read(file);
	        JavaRDD<String> lines = sc.textFile(file.toString());
	        if (hdr.hasHeader) {
	            String headerLine = hdr.line;
	            lines = lines.filter(l -> !l.equals(headerLine));
	        }
	        Map<String, Integer> cols = hdr.cols;
	        int iItem = require(cols, "item", "itemid", "item_id", "product", "productid", "sku");
	        int iCat = require(cols, "category", "cat", "item_category", "product_category");
	        int iStore = require(cols, "store", "storeid", "store_id", "shop");
	        int iState = require(cols, "state", "store_state", "region");
	        int iCust = require(cols, "customer", "cust", "custid", "customer_id", "cust_id");
	        int iGender = require(cols, "gender", "sex");
	        int iAmt = require(cols, "amount", "sales", "price", "amt", "total", "qtyamount");
	        return lines.map(line -> {
	            String[] f = split(line);
	            if (f.length == 0) {
	                return null;
	            }
	            try {
	                return new Sale(
	                        get(f, iItem), get(f, iCat), get(f, iStore), get(f, iState),
	                        get(f, iCust), get(f, iGender), parseAmount(get(f, iAmt)));
	            } catch (RuntimeException e) {
	                return null;
	            }
	        }).filter(s -> s != null);
	    }
	
	    static JavaRDD<Sale> loadStar(JavaSparkContext sc, Path sales, Path items,
	                                  Path stores, Path customers) throws IOException {
	        JavaPairRDD<String, String[]> itemDim = dimByKey(sc, items,
	                new String[]{"item", "itemid", "item_id", "product", "productid"},
	                new String[]{"category", "cat", "item_category", "product_category"});
	        JavaPairRDD<String, String[]> storeDim = dimByKey(sc, stores,
	                new String[]{"store", "storeid", "store_id", "shop"},
	                new String[]{"state", "store_state", "region"});
	        JavaPairRDD<String, String[]> custDim = dimByKey(sc, customers,
	                new String[]{"customer", "cust", "custid", "customer_id", "cust_id"},
	                new String[]{"gender", "sex"});
	
	        Header sh = Header.read(sales);
	        Map<String, Integer> cols = sh.cols;
	        int iItem = require(cols, "item", "itemid", "item_id", "product", "productid");
	        int iStore = require(cols, "store", "storeid", "store_id", "shop");
	        int iCust = require(cols, "customer", "cust", "custid", "customer_id", "cust_id");
	        int iAmt = require(cols, "amount", "sales", "price", "amt", "total");
	        String headerLine = sh.line;
	        boolean skipHeader = sh.hasHeader;
	
	        JavaPairRDD<String, Tuple2<String, Tuple2<String, Double>>> facts = sc.textFile(sales.toString())
	                .filter(l -> !skipHeader || !l.equals(headerLine))
	                .mapToPair(line -> {
	                    String[] f = split(line);
	                    if (f.length == 0) {
	                        return null;
	                    }
	                    try {
	                        String item = get(f, iItem);
	                        String store = get(f, iStore);
	                        String cust = get(f, iCust);
	                        double amt = parseAmount(get(f, iAmt));
	                        return new Tuple2<>(item, new Tuple2<>(store, new Tuple2<>(cust, amt)));
	                    } catch (RuntimeException e) {
	                        return null;
	                    }
	                })
	                .filter(t -> t != null);
	
	        return facts.join(itemDim)
	                .mapToPair(t -> {
	                    String item = t._1;
	                    String store = t._2._1._1;
	                    String cust = t._2._1._2._1;
	                    double amt = t._2._1._2._2;
	                    String category = t._2._2[0];
	                    return new Tuple2<>(store,
	                            new Tuple2<>(item, new Tuple2<>(category, new Tuple2<>(cust, amt))));
	                })
	                .join(storeDim)
	                .mapToPair(t -> {
	                    String store = t._1;
	                    String item = t._2._1._1;
	                    String category = t._2._1._2._1;
	                    String cust = t._2._1._2._2._1;
	                    double amt = t._2._1._2._2._2;
	                    String state = t._2._2[0];
	                    return new Tuple2<>(cust, new Tuple2<>(
	                            new String[]{item, category, store, state}, amt));
	                })
	                .join(custDim)
	                .map(t -> {
	                    String customer = t._1;
	                    String[] d = t._2._1._1;
	                    double amt = t._2._1._2;
	                    String gender = t._2._2[0];
	                    return new Sale(d[0], d[1], d[2], d[3], customer, gender, amt);
	                });
	    }
	
	    private static JavaPairRDD<String, String[]> dimByKey(JavaSparkContext sc, Path file,
	                                                          String[] keyAliases, String[] valueAliases)
	            throws IOException {
	        Header hdr = Header.read(file);
	        int k = require(hdr.cols, keyAliases);
	        int v = require(hdr.cols, valueAliases);
	        String headerLine = hdr.line;
	        boolean skipHeader = hdr.hasHeader;
	        return sc.textFile(file.toString())
	                .filter(l -> !skipHeader || !l.equals(headerLine))
	                .mapToPair(line -> {
	                    String[] f = split(line);
	                    if (f.length == 0) {
	                        return null;
	                    }
	                    return new Tuple2<>(get(f, k), new String[]{get(f, v)});
	                })
	                .filter(t -> t != null)
	                .reduceByKey((a, b) -> a);
	    }
	
	    private static List<Path> listCsv(Path dir) throws IOException {
	        List<Path> out = new ArrayList<>();
	        try (Stream<Path> walk = Files.list(dir)) {
	            walk.filter(p -> {
	                String n = p.getFileName().toString().toLowerCase(Locale.ROOT);
	                return Files.isRegularFile(p) && (n.endsWith(".csv") || n.endsWith(".txt"));
	            }).forEach(out::add);
	        }
	        return out;
	    }
	
	    private static Path pick(List<Path> files, String... stems) {
	        for (String stem : stems) {
	            for (Path p : files) {
	                String n = p.getFileName().toString().toLowerCase(Locale.ROOT)
	                        .replace(".csv", "").replace(".txt", "")
	                        .replace("-", "").replace("_", "");
	                String s = stem.replace("-", "").replace("_", "");
	                if (n.equals(s) || n.startsWith(s)) {
	                    return p;
	                }
	            }
	        }
	        return null;
	    }
	
	    private static boolean hasAllBaseColumns(Path file) throws IOException {
	        Header hdr = Header.read(file);
	        try {
	            require(hdr.cols, "item", "itemid", "item_id", "product", "productid", "sku");
	            require(hdr.cols, "category", "cat", "item_category", "product_category");
	            require(hdr.cols, "store", "storeid", "store_id", "shop");
	            require(hdr.cols, "state", "store_state", "region");
	            require(hdr.cols, "customer", "cust", "custid", "customer_id", "cust_id");
	            require(hdr.cols, "gender", "sex");
	            require(hdr.cols, "amount", "sales", "price", "amt", "total");
	            return true;
	        } catch (IllegalArgumentException e) {
	            return false;
	        }
	    }
	
	    static final class Header {
	        final String line;
	        final boolean hasHeader;
	        final Map<String, Integer> cols;
	
	        Header(String line, boolean hasHeader, Map<String, Integer> cols) {
	            this.line = line;
	            this.hasHeader = hasHeader;
	            this.cols = cols;
	        }
	
	        static Header read(Path file) throws IOException {
	            String first;
	            try (Stream<String> lines = Files.lines(file)) {
	                first = lines.filter(l -> l != null && !l.isBlank()).findFirst().orElse("");
	            }
	            first = first.replace("\uFEFF", "").trim();
	            String[] f = split(first);
	            Map<String, Integer> cols = new HashMap<>();
	            boolean header = looksLikeHeader(f);
	            if (header) {
	                for (int i = 0; i < f.length; i++) {
	                    cols.put(norm(f[i]), i);
	                }
	            } else {
	                for (int i = 0; i < f.length; i++) {
	                    cols.put(String.valueOf(i), i);
	                }
	            }
	            return new Header(first, header, cols);
	        }
	    }
	
	    private static boolean looksLikeHeader(String[] f) {
	        if (f.length == 0) {
	            return false;
	        }
	        int letters = 0;
	        for (String s : f) {
	            if (s.chars().anyMatch(Character::isLetter)) {
	                letters++;
	            }
	        }
	        return letters >= Math.max(2, f.length / 2);
	    }
	
	    private static int require(Map<String, Integer> cols, String... aliases) {
	        for (String a : aliases) {
	            Integer i = cols.get(norm(a));
	            if (i != null) {
	                return i;
	            }
	        }
	        // positional fallback when there is no header: try well-known 7-col layout
	        if (cols.containsKey("0") && cols.size() >= 7) {
	            Map<String, Integer> pos = Map.of(
	                    "item", 0, "category", 1, "store", 2, "state", 3,
	                    "customer", 4, "gender", 5, "amount", 6);
	            for (String a : aliases) {
	                Integer i = pos.get(norm(a));
	                if (i != null && i < cols.size()) {
	                    return i;
	                }
	            }
	        }
	        throw new IllegalArgumentException("missing column, tried " + String.join(",", aliases)
	                + " in " + cols.keySet());
	    }
	
	    private static String get(String[] f, int i) {
	        if (i < 0 || i >= f.length) {
	            throw new IllegalArgumentException("col " + i);
	        }
	        return f[i].trim();
	    }
	
	    static String[] split(String line) {
	        if (line == null) {
	            return new String[0];
	        }
	        String raw = line.replace("\uFEFF", "").trim();
	        if (raw.isEmpty()) {
	            return new String[0];
	        }
	        String delim = raw.contains("\t") && !raw.contains(",") ? "\t" : ",";
	        return raw.split(delim, -1);
	    }
	
	    static String norm(String s) {
	        return s.toLowerCase(Locale.ROOT).replace(" ", "").replace("-", "").replace("_", "");
	    }
	
	    static double parseAmount(String s) {
	        String t = s.trim().replace("$", "").replace(",", "");
	        return Double.parseDouble(t);
	    }
	}
	```
</details>

<details>
<summary>SparkUtil.java</summary>
	```java
	package pmj.bdp.spark.lab6;
	
	import org.apache.spark.api.java.JavaSparkContext;
	import org.apache.spark.sql.SparkSession;
	
	public final class SparkUtil {
	
	    private SparkUtil() {
	    }
	
	    public static SparkSession localSession(String appName) {
	        SparkSession spark = SparkSession.builder()
	                .appName(appName)
	                .master("local[*]")
	                .config("spark.ui.enabled", "false")
	                .config("spark.driver.host", "127.0.0.1")
	                .config("spark.sql.session.timeZone", "UTC")
	                .getOrCreate();
	        spark.sparkContext().setLogLevel("ERROR");
	        return spark;
	    }
	
	    public static JavaSparkContext javaContext(SparkSession spark) {
	        return JavaSparkContext.fromSparkContext(spark.sparkContext());
	    }
	
	    public static void banner(String title) {
	        System.out.println();
	        System.out.println("======== " + title + " ========");
	    }
	}
	```
</details>

<details>
<summary>pom.xml</summary>
	```xml
	<?xml version="1.0" encoding="UTF-8"?>
	<project xmlns="http://maven.apache.org/POM/4.0.0"
	         xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
	         xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 http://maven.apache.org/xsd/maven-4.0.0.xsd">
	
	    <modelVersion>4.0.0</modelVersion>
	
	    <groupId>pmj.bdp.spark</groupId>
	    <artifactId>lab6</artifactId>
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
	        <dependency>
	            <groupId>org.apache.spark</groupId>
	            <artifactId>spark-sql_2.13</artifactId>
	            <version>${spark.version}</version>
	        </dependency>
	    </dependencies>
	
	    <build>
	        <finalName>lab6</finalName>
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
<summary>run_all.sh</summary>
	```bash
	#!/usr/bin/env bash
	# Lab 06: full cube lattice on Spark RDDs, queries on SalesCube (DataFrame API).
	set -euo pipefail
	
	ROOT="$(cd "$(dirname "$0")" && pwd)"
	cd "$ROOT"
	
	DATA="${1:-$ROOT/data}"
	CUBE="$ROOT/SalesCube"
	
	echo "==> Building..."
	mvn -q -DskipTests package
	
	rm -rf "$CUBE" "$ROOT/output"
	
	echo
	echo "==> Lab 06  data=$DATA  cube=$CUBE"
	mvn -q exec:java \
	  -Dexec.mainClass=pmj.bdp.spark.lab6.Lab06 \
	  -Dexec.args="$DATA $CUBE"
	
	echo
	echo "Done. Parquet cube: $CUBE"
	echo "Query tables: $ROOT/output"
	```
</details>

<details>
<summary>data/sales.csv</summary>
	```csv
	item,category,store,state,customer,gender,amount
	blue-tshirt,Tshirts,store1,WA,cust1,M,120
	red-tshirt,Tshirts,store1,WA,cust2,F,80
	green-tshirt,Tshirts,store2,WA,cust1,M,60
	white-tshirt,Tshirts,store3,CA,cust3,F,40
	navy-tshirt,Tshirts,store1,WA,cust1,M,45
	leather-jacket,Jacket,store1,WA,cust1,M,300
	denim-jacket,Jacket,store2,CA,cust2,F,220
	rain-jacket,Jacket,store1,WA,cust4,F,180
	bomber-jacket,Jacket,store3,CA,cust1,M,250
	hoodie,Hoodies,store1,WA,cust1,M,90
	```
</details>
