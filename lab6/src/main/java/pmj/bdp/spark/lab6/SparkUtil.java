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
