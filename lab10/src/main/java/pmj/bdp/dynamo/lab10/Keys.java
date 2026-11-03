package pmj.bdp.dynamo.lab10;

import java.util.Locale;

/**
 * Key layout of the single table {@code TPCH}.
 *
 * <pre>
 *   item              pk               sk                               GSI OrderIndex (orderkey)
 *   customer profile  CUST#&lt;custkey&gt;   PROFILE                          -
 *   order + lines     CUST#&lt;custkey&gt;   ORDER#&lt;orderdate&gt;#&lt;orderkey10&gt;    orderkey
 * </pre>
 *
 * Every order of a customer lives in the customer's partition, sorted by order date,
 * so "orders of a customer", "orders of a customer in a year" and "order history"
 * are one Query each. The line items of an order are embedded in the order item.
 */
final class Keys {

    static final String TABLE = "TPCH";
    static final String ORDER_INDEX = "OrderIndex";

    static final String TYPE_CUSTOMER = "CUSTOMER";
    static final String TYPE_ORDER = "ORDER";

    static final String PROFILE_SK = "PROFILE";
    static final String ORDER_PREFIX = "ORDER#";

    private Keys() {
    }

    static String customerPk(long custkey) {
        return "CUST#" + custkey;
    }

    /** ORDER#1996-01-02#0000000001 : date first so the sort key orders by date, key last for uniqueness. */
    static String orderSk(String orderdate, long orderkey) {
        return String.format(Locale.ROOT, "%s%s#%010d", ORDER_PREFIX, orderdate, orderkey);
    }

    /** Prefix shared by all orders of one year: ORDER#1996 */
    static String yearPrefix(int year) {
        return ORDER_PREFIX + year;
    }
}
