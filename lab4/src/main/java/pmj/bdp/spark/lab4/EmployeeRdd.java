package pmj.bdp.spark.lab4;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

/**
 * Exercise 1 — employee.csv, RDD only (no DataFrame / SQL).
 *
 * CSV: empno, name, dno, salary, state, gender
 */
public class EmployeeRdd {

    public static void main(String[] args) {
        String input = args.length > 0 ? args[0] : "data/employee.csv";

        JavaSparkContext sc = SparkUtil.localContext("Lab4 Employee RDD");
        try {
            JavaRDD<Employee> employees = sc.textFile(input)
                    .map(Employee::parse)
                    .filter(e -> e != null)
                    .cache();

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

    /** (empno, name) of employees from state TX. */
    static void q1Texas(JavaRDD<Employee> employees) {
        SparkUtil.banner("Q1  (empno, name) from state TX");
        List<Employee> tx = new ArrayList<>(employees
                .filter(e -> "TX".equalsIgnoreCase(e.state))
                .collect());
        tx.sort(Comparator.comparingInt(e -> e.empno));
        System.out.println("empno\tname");
        for (Employee e : tx) {
            System.out.println(e.empno + "\t" + e.name);
        }
        System.out.println("count = " + tx.size());
    }

    /**
     * (empno, name, salary, dep_avg_sal) where salary &gt; 1.5 * department average.
     */
    static void q2AboveDeptAvg(JavaRDD<Employee> employees) {
        SparkUtil.banner("Q2  salary > 1.5 * department average");
        JavaPairRDD<Integer, StatAcc> deptStats = employees
                .mapToPair(e -> new Tuple2<>(e.dno, StatAcc.of(e.salary)))
                .reduceByKey(StatAcc::plus)
                .cache();

        JavaPairRDD<Integer, Double> deptAvg = deptStats.mapValues(StatAcc::mean);

        List<String> rows = new ArrayList<>(employees
                .mapToPair(e -> new Tuple2<>(e.dno, e))
                .join(deptAvg)
                .filter(t -> t._2._1.salary > 1.5 * t._2._2)
                .map(t -> {
                    Employee e = t._2._1;
                    double avg = t._2._2;
                    return String.format("%d\t%s\t%.0f\t%.2f",
                            e.empno, e.name, e.salary, avg);
                })
                .collect());

        rows.sort(String::compareTo);
        System.out.println("empno\tname\tsalary\tdep_avg_sal");
        rows.forEach(System.out::println);
        System.out.println("count = " + rows.size());
    }

    /** State-wise employee count. */
    static void q3StateCounts(JavaRDD<Employee> employees) {
        SparkUtil.banner("Q3  state-wise employee count");
        List<Tuple2<String, Integer>> counts = new ArrayList<>(employees
                .mapToPair(e -> new Tuple2<>(e.state, 1))
                .reduceByKey(Integer::sum)
                .collect());
        counts.sort(Comparator.comparing((Tuple2<String, Integer> t) -> t._2).reversed()
                .thenComparing(t -> t._1));
        System.out.println("state\tcount");
        counts.forEach(t -> System.out.println(t._1 + "\t" + t._2));
    }

    /** Population standard deviation of salary (all employees). */
    static StatAcc q4OverallStddev(JavaRDD<Employee> employees) {
        SparkUtil.banner("Q4  overall salary standard deviation");
        StatAcc acc = employees
                .map(e -> StatAcc.of(e.salary))
                .reduce(StatAcc::plus);
        System.out.printf("n = %d%n", acc.count);
        System.out.printf("mean = %.2f%n", acc.mean());
        System.out.printf("stddev (population) = %.2f%n", acc.stddev());
        return acc;
    }

    /** Department-wise population standard deviation of salary. */
    static JavaPairRDD<Integer, Double> q5DeptStddev(JavaRDD<Employee> employees) {
        SparkUtil.banner("Q5  department-wise salary standard deviation");
        JavaPairRDD<Integer, Double> deptSd = employees
                .mapToPair(e -> new Tuple2<>(e.dno, StatAcc.of(e.salary)))
                .reduceByKey(StatAcc::plus)
                .mapValues(StatAcc::stddev)
                .cache();

        List<Tuple2<Integer, Double>> rows = new ArrayList<>(deptSd.collect());
        rows.sort(Comparator.comparingInt(t -> t._1));
        System.out.println("dno\tdept_sal_sd");
        rows.forEach(t -> System.out.printf("%d\t%.2f%n", t._1, t._2));
        return deptSd;
    }

    /**
     * (empno, dno, name, salary, dept_sal_sd) where salary &gt; 1.5 * department SD.
     */
    static void q6AboveDeptSd(JavaRDD<Employee> employees,
                              JavaPairRDD<Integer, Double> deptSd) {
        SparkUtil.banner("Q6  salary > 1.5 * department SD");
        List<String> rows = new ArrayList<>(employees
                .mapToPair(e -> new Tuple2<>(e.dno, e))
                .join(deptSd)
                .filter(t -> t._2._1.salary > 1.5 * t._2._2)
                .map(t -> {
                    Employee e = t._2._1;
                    double sd = t._2._2;
                    return String.format("%d\t%d\t%s\t%.0f\t%.2f",
                            e.empno, e.dno, e.name, e.salary, sd);
                })
                .collect());
        rows.sort(String::compareTo);
        System.out.println("empno\tdno\tname\tsalary\tdept_sal_sd");
        rows.forEach(System.out::println);
        System.out.println("count = " + rows.size());
    }

    /** Offset of each department average from the overall average. */
    static void q7DeptOffset(JavaRDD<Employee> employees, double overallAvg) {
        SparkUtil.banner("Q7  department average offset from overall average");
        List<Tuple2<Integer, Double>> avgs = new ArrayList<>(employees
                .mapToPair(e -> new Tuple2<>(e.dno, StatAcc.of(e.salary)))
                .reduceByKey(StatAcc::plus)
                .mapValues(StatAcc::mean)
                .collect());
        avgs.sort(Comparator.comparingInt(t -> t._1));
        System.out.printf("overall_avg = %.2f%n", overallAvg);
        System.out.println("dno\tdept_avg\toffset (dept_avg - overall_avg)");
        for (Tuple2<Integer, Double> t : avgs) {
            double offset = t._2 - overallAvg;
            System.out.printf("%d\t%.2f\t%+.2f%n", t._1, t._2, offset);
        }
    }
}
