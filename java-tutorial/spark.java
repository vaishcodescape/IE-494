// Apache Spark implementation in Java
import org.apache.spark.api.java.JavaRDD;


public class SparkExample {
    public static void main(String[] args) {
        // Initialize Spark context
        SparkConf conf = new SparkConf().setAppName("Spark Example").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);

        // Create an RDD from a list of integers
        List<Integer> data = Arrays.asList(1, 2, 3, 4, 5);
        JavaRDD<Integer> rdd = sc.parallelize(data);

        // Perform a transformation (e.g., map)
        JavaRDD<Integer> squaredRdd = rdd.map(x -> x * x);

        // Collect the results and print them
        List<Integer> result = squaredRdd.collect();
        System.out.println("Squared values: " + result);

        // Stop the Spark context
        sc.stop();
    }
}