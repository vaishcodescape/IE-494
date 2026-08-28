package pmj.bdp.spark.lab4;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

/**
 * Exercise 2 — web_access_log.txt, RDD only.
 *
 * Combined log format: User-Agent is the last quoted field
 * (https://httpd.apache.org/docs/2.4/logs.html#accesslog).
 */
public class WebLogRdd {

    private static final double BYTES_PER_MB = 1024.0 * 1024.0;

    public static void main(String[] args) {
        String input = args.length > 0 ? args[0] : "data/web_access_log.txt";

        JavaSparkContext sc = SparkUtil.localContext("Lab4 Web Log RDD");
        try {
            JavaRDD<AccessLog> logs = sc.textFile(input)
                    .map(AccessLog::parse)
                    .filter(r -> r != null)
                    .cache();

            monthlySummary(logs);
            mostPopularBrowser(logs);
        } finally {
            sc.stop();
            sc.close();
        }
    }

    /**
     * &lt;Year-Month, Number of Requests, Download Size (MB)&gt;
     * e.g. Dec-2015, Jan-2016, ...
     */
    static void monthlySummary(JavaRDD<AccessLog> logs) {
        SparkUtil.banner("Q1  monthly requests and download size (MB)");

        JavaPairRDD<String, Tuple2<Long, Long>> monthly = logs
                .mapToPair(r -> new Tuple2<>(r.yearMonth(),
                        new Tuple2<>(1L, r.bytes)))
                .reduceByKey((a, b) -> new Tuple2<>(a._1 + b._1, a._2 + b._2));

        List<Tuple2<String, Tuple2<Long, Long>>> rows = new ArrayList<>(monthly.collect());
        rows.sort(Comparator.comparingInt(t -> AccessLog.sortKey(t._1)));

        System.out.println("Year-Month\tRequests\tDownloadMB");
        for (Tuple2<String, Tuple2<Long, Long>> t : rows) {
            double mb = t._2._2 / BYTES_PER_MB;
            System.out.printf("%s\t%d\t%.2f%n", t._1, t._2._1, mb);
        }
    }

    /** Browser that issued the most requests (from the User-Agent field). */
    static void mostPopularBrowser(JavaRDD<AccessLog> logs) {
        SparkUtil.banner("Q2  most popular browser (User-Agent)");

        List<Tuple2<String, Integer>> families = new ArrayList<>(logs
                .mapToPair(r -> new Tuple2<>(AccessLog.browserFamily(r.userAgent), 1))
                .reduceByKey(Integer::sum)
                .collect());
        families.sort(Comparator.comparing((Tuple2<String, Integer> t) -> t._2).reversed());

        System.out.println("browser\trequests");
        families.forEach(t -> System.out.println(t._1 + "\t" + t._2));
        Tuple2<String, Integer> top = families.get(0);
        System.out.println("MOST POPULAR BROWSER = " + top._1
                + "  (" + top._2 + " requests)");

        SparkUtil.banner("Q2b  top exact User-Agent strings");
        List<Tuple2<String, Integer>> agents = new ArrayList<>(logs
                .mapToPair(r -> new Tuple2<>(r.userAgent, 1))
                .reduceByKey(Integer::sum)
                .collect());
        agents.sort(Comparator.comparing((Tuple2<String, Integer> t) -> t._2).reversed());
        int n = Math.min(5, agents.size());
        for (int i = 0; i < n; i++) {
            Tuple2<String, Integer> t = agents.get(i);
            System.out.println(t._2 + "\t" + t._1);
        }
    }

}
