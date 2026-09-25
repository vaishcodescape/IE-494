package pmj.bdp.spark.lab6;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Objects;

/**
 * One cell in the cube lattice. Aggregated dimensions are {@link #ALL}.
 *
 * Dimension order matches the lectures' sales cube:
 * item, category, store, state, customer, gender.
 */
public final class CubeKey implements Serializable {

    private static final long serialVersionUID = 1L;

    public static final String ALL = "*";
    public static final int NDIM = 6;

    public static final int ITEM = 0;
    public static final int CATEGORY = 1;
    public static final int STORE = 2;
    public static final int STATE = 3;
    public static final int CUSTOMER = 4;
    public static final int GENDER = 5;

    public static final String[] NAMES = {
            "item", "category", "store", "state", "customer", "gender"
    };

    public final String[] dims;

    public CubeKey(String[] dims) {
        this.dims = dims;
    }

    /**
     * Emit the cuboid identified by {@code mask}: bit i set means keep dimension i,
     * bit i clear means roll it up to {@link #ALL}.
     */
    public static CubeKey fromSale(Sale s, int mask) {
        String[] d = new String[NDIM];
        for (int i = 0; i < NDIM; i++) {
            d[i] = (mask & (1 << i)) != 0 ? s.dimension(i) : ALL;
        }
        return new CubeKey(d);
    }

    public String get(int i) {
        return dims[i];
    }

    /** Number of non-ALL dimensions — the cuboid's dimensionality. */
    public int granularity() {
        int n = 0;
        for (String d : dims) {
            if (!ALL.equals(d)) {
                n++;
            }
        }
        return n;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof CubeKey other)) {
            return false;
        }
        return Arrays.equals(dims, other.dims);
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(dims);
    }

    @Override
    public String toString() {
        return Objects.toString(Arrays.toString(dims));
    }
}
