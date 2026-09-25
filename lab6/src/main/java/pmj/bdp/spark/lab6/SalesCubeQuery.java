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
