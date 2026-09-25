package pmj.bdp.spark.lab7;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Locale;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class Lab07 {

    public static void main(String[] args) throws IOException {
        Path dataDir = Path.of(args.length > 0 ? args[0] : "data");
        Path csv = Files.isRegularFile(dataDir)
                ? dataDir
                : dataDir.resolve("houses.csv");
        Path outDir = Path.of(args.length > 1 ? args[1] : "output");
        Files.createDirectories(outDir);

        SparkSession spark = SparkUtil.localSession("Lab7 Linear Regression");
        try {
            Dataset<Row> houses = HousesLoader.load(spark, csv).cache();
            long n = houses.count();
            System.out.println("houses rows = " + n);
            houses.createOrReplaceTempView("houses");

            SparkUtil.banner("schema");
            houses.printSchema();

            List<Row> rows = houses.orderBy("id").collectAsList();
            int m = rows.size();
            double[][] x = new double[m][LinearRegression.N_FEATURES];
            double[] y = new double[m];
            for (int i = 0; i < m; i++) {
                Row r = rows.get(i);
                x[i][0] = r.getDouble(r.fieldIndex("x1"));
                x[i][1] = r.getDouble(r.fieldIndex("x2"));
                x[i][2] = r.getDouble(r.fieldIndex("x3"));
                x[i][3] = r.getDouble(r.fieldIndex("x4"));
                x[i][4] = r.getDouble(r.fieldIndex("x5"));
                x[i][5] = r.getDouble(r.fieldIndex("x6"));
                y[i] = r.getDouble(r.fieldIndex("y"));
            }

            SparkUtil.banner("fit (normal equation)");
            double[] theta = LinearRegression.fit(x, y);
            String[] names = {
                    "theta0 (intercept)", "theta1 (x1 date)", "theta2 (x2 age)",
                    "theta3 (x3 MRT m)", "theta4 (x4 stores)", "theta5 (x5 lat)",
                    "theta6 (x6 lon)"
            };
            for (int i = 0; i < theta.length; i++) {
                System.out.printf(Locale.ROOT, "%s\t%.10f%n", names[i], theta[i]);
            }
            writeTheta(outDir.resolve("theta.txt"), names, theta);

            LinearRegression.registerPredict(spark, theta);

            SparkUtil.banner("SQL  SELECT predict(x1..x6) FROM houses WHERE id = 1234");
            Dataset<Row> q1234 = spark.sql(
                    "SELECT id, x1, x2, x3, x4, x5, x6, y, "
                            + "predict(x1, x2, x3, x4, x5, x6) AS yhat "
                            + "FROM houses WHERE id = 1234");
            q1234.show(false);
            writeQuery(outDir.resolve("predict_id_1234.txt"), q1234);
            System.out.println("rows for id=1234: " + q1234.count()
                    + "  (UCI ids are 1..414; 1234 is not in the dataset)");

            SparkUtil.banner("SQL  SELECT predict(x1..x6) FROM houses WHERE id = 123");
            Dataset<Row> q123 = spark.sql(
                    "SELECT id, x1, x2, x3, x4, x5, x6, y, "
                            + "predict(x1, x2, x3, x4, x5, x6) AS yhat "
                            + "FROM houses WHERE id = 123");
            q123.show(false);
            writeQuery(outDir.resolve("predict_id_123.txt"), q123);

            SparkUtil.banner("SQL  all predictions + RMSE");
            Dataset<Row> pred = spark.sql(
                    "SELECT id, y, predict(x1, x2, x3, x4, x5, x6) AS yhat, "
                            + "(y - predict(x1, x2, x3, x4, x5, x6)) AS residual "
                            + "FROM houses");
            pred.orderBy("id").show(12, false);
            writeQuery(outDir.resolve("predictions.txt"), pred.orderBy("id"));

            Dataset<Row> rmseDf = spark.sql(
                    "SELECT COUNT(*) AS n, "
                            + "SQRT(AVG(POW(y - predict(x1, x2, x3, x4, x5, x6), 2))) AS rmse, "
                            + "AVG(ABS(y - predict(x1, x2, x3, x4, x5, x6))) AS mae "
                            + "FROM houses");
            rmseDf.show(false);
            writeQuery(outDir.resolve("rmse.txt"), rmseDf);

            Row rmseRow = rmseDf.first();
            System.out.printf(Locale.ROOT, "RMSE = %.6f%n", rmseRow.getDouble(rmseRow.fieldIndex("rmse")));
        } finally {
            spark.stop();
        }
    }

    static void writeTheta(Path file, String[] names, double[] theta) throws IOException {
        try (BufferedWriter w = Files.newBufferedWriter(file, StandardCharsets.UTF_8)) {
            w.write("term\tweight");
            w.newLine();
            for (int i = 0; i < theta.length; i++) {
                w.write(names[i] + "\t" + String.format(Locale.ROOT, "%.10f", theta[i]));
                w.newLine();
            }
        }
        System.out.println("wrote " + file);
    }

    static void writeQuery(Path file, Dataset<Row> df) throws IOException {
        String[] cols = df.columns();
        List<Row> rows = df.collectAsList();
        try (BufferedWriter w = Files.newBufferedWriter(file, StandardCharsets.UTF_8)) {
            w.write(String.join("\t", cols));
            w.newLine();
            for (Row r : rows) {
                StringBuilder sb = new StringBuilder();
                for (int i = 0; i < r.size(); i++) {
                    if (i > 0) {
                        sb.append('\t');
                    }
                    Object v = r.get(i);
                    if (v instanceof Double d) {
                        sb.append(String.format(Locale.ROOT, "%.6f", d));
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
