package pmj.bdp.spark.lab7;

import org.apache.commons.math3.linear.Array2DRowRealMatrix;
import org.apache.commons.math3.linear.ArrayRealVector;
import org.apache.commons.math3.linear.LUDecomposition;
import org.apache.commons.math3.linear.RealMatrix;
import org.apache.commons.math3.linear.RealVector;
import org.apache.commons.math3.linear.SingularMatrixException;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.api.java.UDF6;
import org.apache.spark.sql.types.DataTypes;

/**
 * Multivariate linear regression via the normal equation
 * theta = (X^T X)^{-1} X^T y
 * using Apache Commons Math for the matrix inverse.
 * Column 0 of X is the intercept (all ones).
 */
public final class LinearRegression {

    public static final int N_FEATURES = 6;

    private LinearRegression() {
    }

    /**
     * Estimates the weight vector theta of length n+1 (intercept first).
     *
     * @param x m by n matrix of independent variables (no intercept column)
     * @param y length-m vector of dependent values
     */
    public static double[] fit(double[][] x, double[] y) {
        if (x.length == 0) {
            throw new IllegalArgumentException("empty design matrix");
        }
        if (x.length != y.length) {
            throw new IllegalArgumentException("X rows " + x.length + " != y length " + y.length);
        }
        int m = x.length;
        int n = x[0].length;
        double[][] xa = new double[m][n + 1];
        for (int i = 0; i < m; i++) {
            xa[i][0] = 1.0;
            System.arraycopy(x[i], 0, xa[i], 1, n);
        }

        RealMatrix X = new Array2DRowRealMatrix(xa, false);
        RealMatrix Xt = X.transpose();
        RealMatrix xtx = Xt.multiply(X);
        RealMatrix xtxInv;
        try {
            xtxInv = new LUDecomposition(xtx).getSolver().getInverse();
        } catch (SingularMatrixException e) {
            throw new IllegalStateException("X^T X is singular; cannot invert", e);
        }
        RealVector yv = new ArrayRealVector(y, false);
        RealVector theta = xtxInv.multiply(Xt).operate(yv);
        return theta.toArray();
    }

    /**
     * Predicted y-hat = theta · [1, x].
     */
    public static double predict(double[] theta, double[] x) {
        if (theta.length != x.length + 1) {
            throw new IllegalArgumentException(
                    "theta length " + theta.length + " != 1 + |x| " + x.length);
        }
        double yhat = theta[0];
        for (int i = 0; i < x.length; i++) {
            yhat += theta[i + 1] * x[i];
        }
        return yhat;
    }

    /**
     * Register Spark SQL function predict(x1, x2, x3, x4, x5, x6).
     * Spark SQL does not expand * into UDF arguments, so the assignment query
     * SELECT predict(*) FROM houses WHERE id = 1234
     * is run as
     * SELECT predict(x1, x2, x3, x4, x5, x6) FROM houses WHERE id = 1234
     */
    public static void registerPredict(SparkSession spark, double[] theta) {
        final double[] t = theta.clone();
        spark.udf().register("predict",
                (UDF6<Double, Double, Double, Double, Double, Double, Double>)
                        (x1, x2, x3, x4, x5, x6) -> predict(t, new double[]{
                                x1, x2, x3, x4, x5, x6
                        }),
                DataTypes.DoubleType);
    }
}
