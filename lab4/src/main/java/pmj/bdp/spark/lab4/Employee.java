package pmj.bdp.spark.lab4;

import java.io.Serializable;

/**
 * One row of employee.csv:
 * empno, name, dno, salary, state, gender
 */
public final class Employee implements Serializable {

    private static final long serialVersionUID = 1L;

    public final int empno;
    public final String name;
    public final int dno;
    public final double salary;
    public final String state;
    public final String gender;

    public Employee(int empno, String name, int dno, double salary,
                    String state, String gender) {
        this.empno = empno;
        this.name = name;
        this.dno = dno;
        this.salary = salary;
        this.state = state;
        this.gender = gender;
    }

    public static Employee parse(String line) {
        if (line == null || line.isBlank()) {
            return null;
        }
        String raw = line.replace("\uFEFF", "").trim();
        String[] f = raw.split(",", -1);
        if (f.length < 6) {
            return null;
        }
        if (f[0].equalsIgnoreCase("empno") || f[0].equalsIgnoreCase("emp-no")) {
            return null;
        }
        try {
            return new Employee(
                    Integer.parseInt(f[0].trim()),
                    f[1].trim(),
                    Integer.parseInt(f[2].trim()),
                    Double.parseDouble(f[3].trim()),
                    f[4].trim(),
                    f[5].trim());
        } catch (NumberFormatException e) {
            return null;
        }
    }
}
