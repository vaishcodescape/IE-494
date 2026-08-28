<callout icon="⚡" color="blue_bg">
	**Constraint:** RDD API only. No DataFrames, Datasets, or Spark SQL.
	Spark 4.2.0 (`spark-core_2.13`), Java 17, master `local[*]`.
	Data: `employee.csv` (311 employees) and `web_access_log.txt` (Apache Combined Log Format).
</callout>
<table_of_contents/>
---
## Overview
Two Spark jobs in `lab4/`:
1. **Exercise 1** — `EmployeeRdd` on `data/employee.csv` (columns: empno, name, dno, salary, state, gender).
2. **Exercise 2** — `WebLogRdd` on `data/web_access_log.txt` (monthly traffic + most popular browser from User-Agent).
Mean and population standard deviation use `StatAcc` (count, sum, sum of squares): stddev = sqrt(E[x^2] − mean^2).
## How to run
```bash
cd lab4
mvn -DskipTests package

mvn exec:java -Dexec.mainClass=pmj.bdp.spark.lab4.EmployeeRdd -Dexec.args="$(pwd)/data/employee.csv"

mvn exec:java -Dexec.mainClass=pmj.bdp.spark.lab4.WebLogRdd -Dexec.args="$(pwd)/data/web_access_log.txt"
```
Or `bash run_all.sh`. After code changes, include `package` so Maven recompiles.
---
## Exercise 1 — employee.csv
### Q1. (empno, name) from state TX
Filter `state == TX`, collect, sort by empno.
```java
List<Employee> tx = new ArrayList<>(employees
        .filter(e -> "TX".equalsIgnoreCase(e.state))
        .collect());
tx.sort(Comparator.comparingInt(e -> e.empno));
```
**Terminal output**
<table fit-page-width="true" header-row="true">
	<tr>
		<td>**empno**</td>
		<td>**name**</td>
	</tr>
	<tr>
		<td>10012</td>
		<td>Hector Barbossa</td>
	</tr>
	<tr>
		<td>10200</td>
		<td>Anton Chigurh</td>
	</tr>
	<tr>
		<td>10202</td>
		<td>Jac McKinzie</td>
	</tr>
</table>
count = 3
### Q2. salary greater than 1.5 times department average
Per-department mean via `StatAcc` + `reduceByKey`, then `join` and filter.
```java
JavaPairRDD<Integer, Double> deptAvg = employees
        .mapToPair(e -> new Tuple2<>(e.dno, StatAcc.of(e.salary)))
        .reduceByKey(StatAcc::plus)
        .mapValues(StatAcc::mean);

employees.mapToPair(e -> new Tuple2<>(e.dno, e))
        .join(deptAvg)
        .filter(t -> t._2._1.salary > 1.5 * t._2._2);
```
**Terminal output**
<table fit-page-width="true" header-row="true">
	<tr>
		<td>**empno**</td>
		<td>**name**</td>
		<td>**salary**</td>
		<td>**dep_avg_sal**</td>
	</tr>
	<tr>
		<td>10010</td>
		<td>Jennifer Zamora</td>
		<td>220450</td>
		<td>97064.64</td>
	</tr>
	<tr>
		<td>10015</td>
		<td>Jason Foss</td>
		<td>178000</td>
		<td>97064.64</td>
	</tr>
	<tr>
		<td>10019</td>
		<td>Vito Corleone</td>
		<td>170500</td>
		<td>59967.79</td>
	</tr>
	<tr>
		<td>10086</td>
		<td>Katie Roper</td>
		<td>150290</td>
		<td>97064.64</td>
	</tr>
	<tr>
		<td>10222</td>
		<td>Ricardo Ruiz</td>
		<td>148999</td>
		<td>97064.64</td>
	</tr>
	<tr>
		<td>10272</td>
		<td>Debra Houlihan</td>
		<td>180000</td>
		<td>68684.06</td>
	</tr>
	<tr>
		<td>10288</td>
		<td>Peter Monroe</td>
		<td>157000</td>
		<td>97064.64</td>
	</tr>
</table>
count = 7
### Q3. State-wise employee count
Map `(state, 1)`, then `reduceByKey(Integer::sum)`.
**Terminal output**
<table fit-page-width="true" header-row="true">
	<tr>
		<td>**state**</td>
		<td>**count**</td>
	</tr>
	<tr><td>MA</td><td>276</td></tr>
	<tr><td>CT</td><td>6</td></tr>
	<tr><td>TX</td><td>3</td></tr>
	<tr><td>VT</td><td>2</td></tr>
	<tr><td>AL</td><td>1</td></tr>
	<tr><td>AZ</td><td>1</td></tr>
	<tr><td>CA</td><td>1</td></tr>
	<tr><td>CO</td><td>1</td></tr>
	<tr><td>FL</td><td>1</td></tr>
	<tr><td>GA</td><td>1</td></tr>
	<tr><td>ID</td><td>1</td></tr>
	<tr><td>IN</td><td>1</td></tr>
	<tr><td>KY</td><td>1</td></tr>
	<tr><td>ME</td><td>1</td></tr>
	<tr><td>MT</td><td>1</td></tr>
	<tr><td>NC</td><td>1</td></tr>
	<tr><td>ND</td><td>1</td></tr>
	<tr><td>NH</td><td>1</td></tr>
	<tr><td>NV</td><td>1</td></tr>
	<tr><td>NY</td><td>1</td></tr>
	<tr><td>OH</td><td>1</td></tr>
	<tr><td>OR</td><td>1</td></tr>
	<tr><td>PA</td><td>1</td></tr>
	<tr><td>RI</td><td>1</td></tr>
	<tr><td>TN</td><td>1</td></tr>
	<tr><td>UT</td><td>1</td></tr>
	<tr><td>VA</td><td>1</td></tr>
	<tr><td>WA</td><td>1</td></tr>
</table>
### Q4. Overall salary standard deviation
```java
StatAcc acc = employees.map(e -> StatAcc.of(e.salary)).reduce(StatAcc::plus);
```
**Terminal output**
- n = **311**
- mean = **69020.68**
- stddev (population) = **25116.16**
### Q5. Department-wise salary standard deviation
Same accumulator, keyed by `dno`.
**Terminal output**
<table fit-page-width="true" header-row="true">
	<tr>
		<td>**dno**</td>
		<td>**dept_sal_sd**</td>
	</tr>
	<tr><td>1</td><td>19722.68</td></tr>
	<tr><td>2</td><td>0.00</td></tr>
	<tr><td>3</td><td>32875.84</td></tr>
	<tr><td>4</td><td>8754.93</td></tr>
	<tr><td>5</td><td>11420.80</td></tr>
	<tr><td>6</td><td>20702.73</td></tr>
</table>
Dept 2 SD is 0 because it has one employee (Janet King, salary 250000).
### Q6. salary greater than 1.5 times department SD
Join each employee to department SD; keep `salary > 1.5 * dept_sal_sd`.
That cutoff is far below typical pay (dept 5: 1.5 × 11420.80 ≈ 17131), so **every employee matches**.
**Terminal output (sample) + count**
<table fit-page-width="true" header-row="true">
	<tr>
		<td>**empno**</td>
		<td>**dno**</td>
		<td>**name**</td>
		<td>**salary**</td>
		<td>**dept_sal_sd**</td>
	</tr>
	<tr><td>10001</td><td>5</td><td>Calvin Candie</td><td>72640</td><td>11420.80</td></tr>
	<tr><td>10010</td><td>3</td><td>Jennifer Zamora</td><td>220450</td><td>32875.84</td></tr>
	<tr><td>10019</td><td>5</td><td>Vito Corleone</td><td>170500</td><td>11420.80</td></tr>
	<tr><td>10089</td><td>2</td><td>Janet King</td><td>250000</td><td>0.00</td></tr>
	<tr><td>10272</td><td>6</td><td>Debra Houlihan</td><td>180000</td><td>20702.73</td></tr>
	<tr><td>10311</td><td>6</td><td>Randy Dee</td><td>56991</td><td>20702.73</td></tr>
</table>
count = **311** (full 311-row listing: `lab4/output/exercise1_employee.txt`)
### Q7. Department average offset from overall average
offset = dept_avg − overall_avg (overall_avg = 69020.68).
**Terminal output**
<table fit-page-width="true" header-row="true">
	<tr>
		<td>**dno**</td>
		<td>**dept_avg**</td>
		<td>**offset**</td>
	</tr>
	<tr><td>1</td><td>72949.00</td><td>+3928.32</td></tr>
	<tr><td>2</td><td>250000.00</td><td>+180979.32</td></tr>
	<tr><td>3</td><td>97064.64</td><td>+28043.96</td></tr>
	<tr><td>4</td><td>96152.10</td><td>+27131.42</td></tr>
	<tr><td>5</td><td>59967.79</td><td>-9052.90</td></tr>
	<tr><td>6</td><td>68684.06</td><td>-336.62</td></tr>
</table>
---
## Exercise 2 — web_access_log.txt
User-Agent is the last quoted field in Combined Log Format.
### Q1. Monthly requests and download size (MB)
```java
JavaPairRDD<String, Tuple2<Long, Long>> monthly = logs
        .mapToPair(r -> new Tuple2<>(r.yearMonth(), new Tuple2<>(1L, r.bytes)))
        .reduceByKey((a, b) -> new Tuple2<>(a._1 + b._1, a._2 + b._2));
```
**Terminal output**
<table fit-page-width="true" header-row="true">
	<tr>
		<td>**Year-Month**</td>
		<td>**Requests**</td>
		<td>**DownloadMB**</td>
	</tr>
	<tr><td>Dec-2015</td><td>14148</td><td>273.59</td></tr>
	<tr><td>Jan-2016</td><td>28224</td><td>1755.57</td></tr>
	<tr><td>Feb-2016</td><td>64262</td><td>1347.53</td></tr>
</table>
### Q2. Most popular browser (User-Agent)
Browser family order: Edge, Opera, Chrome, Firefox, Internet Explorer, Safari, Bot, Other / Unknown.
**Terminal output**
<table fit-page-width="true" header-row="true">
	<tr>
		<td>**browser**</td>
		<td>**requests**</td>
	</tr>
	<tr><td>Chrome</td><td>60675</td></tr>
	<tr><td>Other</td><td>16894</td></tr>
	<tr><td>Firefox</td><td>13872</td></tr>
	<tr><td>Internet Explorer</td><td>4801</td></tr>
	<tr><td>Safari</td><td>4102</td></tr>
	<tr><td>Unknown</td><td>2557</td></tr>
	<tr><td>Bot</td><td>2470</td></tr>
	<tr><td>Opera</td><td>764</td></tr>
	<tr><td>Edge</td><td>499</td></tr>
</table>
**MOST POPULAR BROWSER = Chrome (60675 requests)**
**Q2b — top exact User-Agent strings**
<table fit-page-width="true" header-row="true">
	<tr>
		<td>**requests**</td>
		<td>**User-Agent**</td>
	</tr>
	<tr><td>53442</td><td>Mozilla/5.0 (Windows NT 5.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/46.0.2490.71 Safari/537.36</td></tr>
	<tr><td>13463</td><td>Go-http-client/1.1</td></tr>
	<tr><td>5142</td><td>Mozilla/5.0 (Windows NT 6.0; rv:34.0) Gecko/20100101 Firefox/34.0</td></tr>
	<tr><td>3559</td><td>Mozilla/5.0 (Windows NT 5.1; rv:29.0) Gecko/20100101 Firefox/29.0</td></tr>
	<tr><td>2548</td><td>-</td></tr>
</table>
<details>
<summary>Raw Exercise 2 terminal dump</summary>
	```text
======== Q1  monthly requests and download size (MB) ========
Year-Month      Requests        DownloadMB
Dec-2015        14148   273.59
Jan-2016        28224   1755.57
Feb-2016        64262   1347.53

======== Q2  most popular browser (User-Agent) ========
browser requests
Chrome  60675
Other   16894
Firefox 13872
Internet Explorer       4801
Safari  4102
Unknown 2557
Bot     2470
Opera   764
Edge    499
MOST POPULAR BROWSER = Chrome  (60675 requests)

======== Q2b  top exact User-Agent strings ========
53442   Mozilla/5.0 (Windows NT 5.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/46.0.2490.71 Safari/537.36
13463   Go-http-client/1.1
5142    Mozilla/5.0 (Windows NT 6.0; rv:34.0) Gecko/20100101 Firefox/34.0
3559    Mozilla/5.0 (Windows NT 5.1; rv:29.0) Gecko/20100101 Firefox/29.0
2548    -
	```
</details>
---
## Source code
Full sources live in `lab4/src/main/java/pmj/bdp/spark/lab4/`.
<details>
<summary>EmployeeRdd.java (full)</summary>
	```java
package pmj.bdp.spark.lab4;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

public class EmployeeRdd {
    public static void main(String[] args) {
        String input = args.length > 0 ? args[0] : "data/employee.csv";
        JavaSparkContext sc = SparkUtil.localContext("Lab4 Employee RDD");
        try {
            JavaRDD<Employee> employees = sc.textFile(input)
                    .map(Employee::parse).filter(e -> e != null).cache();
            q1Texas(employees);
            q2AboveDeptAvg(employees);
            q3StateCounts(employees);
            StatAcc overall = q4OverallStddev(employees);
            JavaPairRDD<Integer, Double> deptSd = q5DeptStddev(employees);
            q6AboveDeptSd(employees, deptSd);
            q7DeptOffset(employees, overall.mean());
        } finally {
            sc.stop();
            sc.close();
        }
    }
}
	```
	See the repo file for q1–q7 method bodies (filter / reduceByKey / join as shown above).
</details>
<details>
<summary>WebLogRdd.java (full)</summary>
	```java
package pmj.bdp.spark.lab4;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

public class WebLogRdd {
    private static final double BYTES_PER_MB = 1024.0 * 1024.0;

    public static void main(String[] args) {
        String input = args.length > 0 ? args[0] : "data/web_access_log.txt";
        JavaSparkContext sc = SparkUtil.localContext("Lab4 Web Log RDD");
        try {
            JavaRDD<AccessLog> logs = sc.textFile(input)
                    .map(AccessLog::parse).filter(r -> r != null).cache();
            monthlySummary(logs);
            mostPopularBrowser(logs);
        } finally {
            sc.stop();
            sc.close();
        }
    }

    static void monthlySummary(JavaRDD<AccessLog> logs) {
        JavaPairRDD<String, Tuple2<Long, Long>> monthly = logs
                .mapToPair(r -> new Tuple2<>(r.yearMonth(), new Tuple2<>(1L, r.bytes)))
                .reduceByKey((a, b) -> new Tuple2<>(a._1 + b._1, a._2 + b._2));
        List<Tuple2<String, Tuple2<Long, Long>>> rows = new ArrayList<>(monthly.collect());
        rows.sort(Comparator.comparingInt(t -> AccessLog.sortKey(t._1)));
        for (Tuple2<String, Tuple2<Long, Long>> t : rows) {
            System.out.printf("%s\t%d\t%.2f%n", t._1, t._2._1, t._2._2 / BYTES_PER_MB);
        }
    }

    static void mostPopularBrowser(JavaRDD<AccessLog> logs) {
        List<Tuple2<String, Integer>> families = new ArrayList<>(logs
                .mapToPair(r -> new Tuple2<>(AccessLog.browserFamily(r.userAgent), 1))
                .reduceByKey(Integer::sum).collect());
        families.sort(Comparator.comparing((Tuple2<String, Integer> t) -> t._2).reversed());
        Tuple2<String, Integer> top = families.get(0);
        System.out.println("MOST POPULAR BROWSER = " + top._1 + "  (" + top._2 + " requests)");
    }
}
	```
</details>
<details>
<summary>StatAcc.java</summary>
	```java
public final class StatAcc implements Serializable {
    public final long count;
    public final double sum;
    public final double sumSq;
    public static StatAcc of(double x) { return new StatAcc(1L, x, x * x); }
    public StatAcc plus(StatAcc o) { return new StatAcc(count + o.count, sum + o.sum, sumSq + o.sumSq); }
    public double mean() { return count == 0 ? 0.0 : sum / count; }
    public double stddev() {
        if (count == 0) return 0.0;
        double m = mean();
        double var = (sumSq / count) - (m * m);
        return var <= 0.0 ? 0.0 : Math.sqrt(var);
    }
}
	```
</details>
<details>
<summary>AccessLog.browserFamily</summary>
	```java
public static String browserFamily(String ua) {
    if (ua == null || ua.isBlank() || "-".equals(ua)) return "Unknown";
    String u = ua.toLowerCase();
    if (u.contains("edg/") || u.contains("edge/")) return "Edge";
    if (u.contains("opr/") || u.contains("opera")) return "Opera";
    if (u.contains("chrome/") || u.contains("crios/")) return "Chrome";
    if (u.contains("firefox/") || u.contains("fxios")) return "Firefox";
    if (u.contains("msie") || u.contains("trident/")) return "Internet Explorer";
    if (u.contains("safari/")) return "Safari";
    if (u.contains("bot") || u.contains("crawler") || u.contains("spider")) return "Bot";
    return "Other";
}
	```
</details>
---
## Files to copy into the PDF
- Exercise 1 full stdout (including all 311 Q6 rows): `lab4/output/exercise1_employee.txt`
- Exercise 2 full stdout: `lab4/output/exercise2_weblog.txt`
- Note: Spark `collect()` is unmodifiable, so the jobs copy into `new ArrayList<>(...)` before `List.sort`.
