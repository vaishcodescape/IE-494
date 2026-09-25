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
