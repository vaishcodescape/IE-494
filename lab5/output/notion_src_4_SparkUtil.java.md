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