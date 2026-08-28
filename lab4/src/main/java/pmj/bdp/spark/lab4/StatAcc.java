package pmj.bdp.spark.lab4;

import java.io.Serializable;

/** Running count / sum / sum-of-squares for mean and population SD. */
public final class StatAcc implements Serializable {

    private static final long serialVersionUID = 1L;

    public final long count;
    public final double sum;
    public final double sumSq;

    public StatAcc(long count, double sum, double sumSq) {
        this.count = count;
        this.sum = sum;
        this.sumSq = sumSq;
    }

    public static StatAcc of(double x) {
        return new StatAcc(1L, x, x * x);
    }

    public StatAcc plus(StatAcc other) {
        return new StatAcc(count + other.count, sum + other.sum, sumSq + other.sumSq);
    }

    public double mean() {
        return count == 0 ? 0.0 : sum / count;
    }

    /** Population standard deviation: sqrt(E[x^2] - mean^2). */
    public double stddev() {
        if (count == 0) {
            return 0.0;
        }
        double mean = mean();
        double var = (sumSq / count) - (mean * mean);
        return var <= 0.0 ? 0.0 : Math.sqrt(var);
    }
}
