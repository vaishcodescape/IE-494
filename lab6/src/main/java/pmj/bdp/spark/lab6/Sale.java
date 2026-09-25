package pmj.bdp.spark.lab6;

import java.io.Serializable;

/**
 * One denormalized sales fact used as the cube's base cuboid:
 * item, category, store, state, customer, gender, amount.
 */
public final class Sale implements Serializable {

    private static final long serialVersionUID = 1L;

    public final String item;
    public final String category;
    public final String store;
    public final String state;
    public final String customer;
    public final String gender;
    public final double amount;

    public Sale(String item, String category, String store, String state,
                String customer, String gender, double amount) {
        this.item = item;
        this.category = category;
        this.store = store;
        this.state = state;
        this.customer = customer;
        this.gender = gender;
        this.amount = amount;
    }

    public String dimension(int i) {
        return switch (i) {
            case CubeKey.ITEM -> item;
            case CubeKey.CATEGORY -> category;
            case CubeKey.STORE -> store;
            case CubeKey.STATE -> state;
            case CubeKey.CUSTOMER -> customer;
            case CubeKey.GENDER -> gender;
            default -> throw new IllegalArgumentException("dim " + i);
        };
    }
}
