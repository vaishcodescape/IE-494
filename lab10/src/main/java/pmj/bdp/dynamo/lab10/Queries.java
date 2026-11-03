package pmj.bdp.dynamo.lab10;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.TreeSet;

import software.amazon.awssdk.enhanced.dynamodb.DynamoDbEnhancedClient;
import software.amazon.awssdk.enhanced.dynamodb.DynamoDbIndex;
import software.amazon.awssdk.enhanced.dynamodb.DynamoDbTable;
import software.amazon.awssdk.enhanced.dynamodb.Key;
import software.amazon.awssdk.enhanced.dynamodb.TableSchema;
import software.amazon.awssdk.enhanced.dynamodb.model.Page;
import software.amazon.awssdk.enhanced.dynamodb.model.QueryConditional;

/**
 * The seven queries of the lab, one method each, all through the high-level (Enhanced) API:
 * DynamoDbTable.getItem / query and DynamoDbIndex.query. No Scan anywhere.
 */
final class Queries {

    private final DynamoDbTable<Customer> customers;
    private final DynamoDbTable<Order> orders;
    private final DynamoDbIndex<Order> orderIndex;
    private int pages;                    // Query pages fetched by the last ordersOf(...) call

    Queries(DynamoDbEnhancedClient enhanced) {
        customers = enhanced.table(Keys.TABLE, TableSchema.fromBean(Customer.class));
        orders = enhanced.table(Keys.TABLE, TableSchema.fromBean(Order.class));
        orderIndex = orders.index(Keys.ORDER_INDEX);
    }

    // ---------------------------------------------------------------- Q1

    /** Q1. Retrieve a customer's profile: one GetItem on (CUST#custkey, PROFILE). */
    Customer q1CustomerProfile(long custkey) {
        section("Q1: retrieve the profile of customer " + custkey);
        String pk = Keys.customerPk(custkey);
        System.out.println("GetItem  pk=" + pk + "  sk=" + Keys.PROFILE_SK);
        long t = System.nanoTime();
        Customer c = customers.getItem(Key.builder().partitionValue(pk).sortValue(Keys.PROFILE_SK).build());
        if (c == null) {
            System.out.println("customer not found");
        } else {
            printCustomer(c);
        }
        done(t, "1 GetItem");
        return c;
    }

    // ---------------------------------------------------------------- Q2

    /** Q2. All orders of a customer: one Query on the customer's partition, sk begins_with ORDER#. */
    List<Order> q2OrdersOfCustomer(long custkey) {
        section("Q2: all orders of customer " + custkey);
        System.out.println("Query  pk=" + Keys.customerPk(custkey) + "  sk begins_with " + Keys.ORDER_PREFIX);
        long t = System.nanoTime();
        List<Order> list = ordersOf(custkey, Keys.ORDER_PREFIX);
        printOrderHeader();
        for (Order o : list) {
            printOrderRow(o);
        }
        System.out.printf(Locale.ROOT, "%d orders%n", list.size());
        done(t, pages + " Query page(s)");
        return list;
    }

    // ---------------------------------------------------------------- Q3

    /**
     * Q3. All line items of an order, given only O_ORDERKEY: Query the GSI OrderIndex
     * (KEYS_ONLY, so it returns the order's pk/sk), then GetItem the order, whose item
     * embeds every line item.
     */
    Order q3LineItemsOfOrder(long orderkey) {
        section("Q3: all line items of order " + orderkey);
        System.out.println("Query  index " + Keys.ORDER_INDEX + "  orderkey=" + orderkey);
        long t = System.nanoTime();
        Order ref = null;
        int indexPages = 0;
        for (Page<Order> page : orderIndex.query(
                QueryConditional.keyEqualTo(Key.builder().partitionValue(orderkey).build()))) {
            indexPages++;
            for (Order o : page.items()) {
                ref = o;
            }
        }
        if (ref == null) {
            System.out.println("order not found");
            done(t, indexPages + " index Query page(s)");
            return null;
        }
        System.out.println("  -> pk=" + ref.getPk() + "  sk=" + ref.getSk());
        System.out.println("GetItem  pk=" + ref.getPk() + "  sk=" + ref.getSk());
        Order full = orders.getItem(Key.builder().partitionValue(ref.getPk()).sortValue(ref.getSk()).build());
        printOrderHeader();
        printOrderRow(full);
        printLineHeader();
        for (LineItem l : full.getLineitems()) {
            printLineRow(l);
        }
        System.out.printf(Locale.ROOT, "%d line items%n", full.getLineitems().size());
        done(t, indexPages + " index Query page(s) + 1 GetItem");
        return full;
    }

    // ---------------------------------------------------------------- Q4

    /**
     * Q4. Everything needed to display a customer's order history: the profile (GetItem)
     * and every order with its embedded line items (one Query on the same partition).
     */
    void q4OrderHistory(long custkey) {
        section("Q4: order history of customer " + custkey + " (profile + each order with its line items)");
        String pk = Keys.customerPk(custkey);
        System.out.println("GetItem  pk=" + pk + "  sk=" + Keys.PROFILE_SK);
        System.out.println("Query    pk=" + pk + "  sk begins_with " + Keys.ORDER_PREFIX);
        long t = System.nanoTime();
        Customer c = customers.getItem(Key.builder().partitionValue(pk).sortValue(Keys.PROFILE_SK).build());
        List<Order> list = ordersOf(custkey, Keys.ORDER_PREFIX);
        if (c != null) {
            System.out.printf(Locale.ROOT, "%s  (custkey %d, %s, nation %d, phone %s, balance %s)%n",
                    c.getName(), c.getCustkey(), c.getMktsegment(), c.getNationkey(), c.getPhone(), c.getAcctbal());
        }
        long lines = 0;
        BigDecimal total = BigDecimal.ZERO;
        for (Order o : list) {
            System.out.printf(Locale.ROOT, "%nORDER %d  %s  status %s  %s  clerk %s  total %s%n",
                    o.getOrderkey(), o.getOrderdate(), o.getOrderstatus(), o.getOrderpriority(),
                    o.getClerk(), o.getTotalprice());
            for (LineItem l : o.getLineitems()) {
                System.out.printf(Locale.ROOT,
                        "    line %d  part %-7d supp %-5d qty %3s  ext %10s  disc %s  tax %s  %s%s  ship %s %s%n",
                        l.getLinenumber(), l.getPartkey(), l.getSuppkey(), l.getQuantity(), l.getExtendedprice(),
                        l.getDiscount(), l.getTax(), l.getReturnflag(), l.getLinestatus(), l.getShipdate(),
                        l.getShipmode());
                lines++;
            }
            total = total.add(o.getTotalprice());
        }
        System.out.printf(Locale.ROOT, "%n%d orders, %d line items, lifetime order value %s%n",
                list.size(), lines, total);
        done(t, "1 GetItem + " + pages + " Query page(s)");
    }

    // ---------------------------------------------------------------- Q5

    /** Q5. All items ordered by a customer in a year: one Query, sk begins_with ORDER#&lt;year&gt;. */
    List<LineItem> q5ItemsOrderedInYear(long custkey, int year) {
        section("Q5: items ordered by customer " + custkey + " in " + year);
        System.out.println("Query  pk=" + Keys.customerPk(custkey) + "  sk begins_with " + Keys.yearPrefix(year));
        long t = System.nanoTime();
        List<Order> list = ordersOf(custkey, Keys.yearPrefix(year));
        List<LineItem> items = new ArrayList<>();
        Set<Long> parts = new TreeSet<>();
        System.out.printf(Locale.ROOT, "%-10s %-10s %-4s %-8s %-7s %5s %12s%n",
                "orderdate", "orderkey", "line", "partkey", "suppkey", "qty", "extprice");
        for (Order o : list) {
            for (LineItem l : o.getLineitems()) {
                System.out.printf(Locale.ROOT, "%-10s %-10d %-4d %-8d %-7d %5s %12s%n",
                        o.getOrderdate(), o.getOrderkey(), l.getLinenumber(), l.getPartkey(), l.getSuppkey(),
                        l.getQuantity(), l.getExtendedprice());
                items.add(l);
                parts.add(l.getPartkey());
            }
        }
        System.out.printf(Locale.ROOT, "%d orders in %d, %d line items, %d distinct parts: %s%n",
                list.size(), year, items.size(), parts.size(), parts);
        done(t, pages + " Query page(s)");
        return items;
    }

    // ---------------------------------------------------------------- Q6

    /**
     * Q6. Items the customer ordered in (year-1) but not in year: two Queries
     * (one per year prefix) and a set difference on the part keys.
     */
    Set<Long> q6ItemsOfPreviousYearNotThisYear(long custkey, int year) {
        int previous = year - 1;
        section("Q6: items customer " + custkey + " ordered in " + previous + " but not in " + year);
        String pk = Keys.customerPk(custkey);
        System.out.println("Query  pk=" + pk + "  sk begins_with " + Keys.yearPrefix(previous));
        System.out.println("Query  pk=" + pk + "  sk begins_with " + Keys.yearPrefix(year));
        long t = System.nanoTime();
        List<Order> prevOrders = ordersOf(custkey, Keys.yearPrefix(previous));
        int prevPages = pages;
        List<Order> thisOrders = ordersOf(custkey, Keys.yearPrefix(year));
        Set<Long> prevParts = partsOf(prevOrders);
        Set<Long> thisParts = partsOf(thisOrders);
        Set<Long> onlyPrevious = new TreeSet<>(prevParts);
        onlyPrevious.removeAll(thisParts);
        System.out.printf(Locale.ROOT, "parts ordered in %d : %d distinct %s%n", previous, prevParts.size(), prevParts);
        System.out.printf(Locale.ROOT, "parts ordered in %d : %d distinct %s%n", year, thisParts.size(), thisParts);
        System.out.printf(Locale.ROOT, "in %d but not in %d: %d parts%n", previous, year, onlyPrevious.size());
        System.out.printf(Locale.ROOT, "%-8s %-10s %-10s %-4s %5s %12s%n",
                "partkey", "orderdate", "orderkey", "line", "qty", "extprice");
        for (Order o : prevOrders) {
            for (LineItem l : o.getLineitems()) {
                if (onlyPrevious.contains(l.getPartkey())) {
                    System.out.printf(Locale.ROOT, "%-8d %-10s %-10d %-4d %5s %12s%n",
                            l.getPartkey(), o.getOrderdate(), o.getOrderkey(), l.getLinenumber(),
                            l.getQuantity(), l.getExtendedprice());
                }
            }
        }
        done(t, (prevPages + pages) + " Query page(s)");
        return onlyPrevious;
    }

    // ---------------------------------------------------------------- Q7

    /** Q7. Total value of a customer's orders in a given year: one Query, sum of O_TOTALPRICE. */
    BigDecimal q7TotalValueInYear(long custkey, int year) {
        section("Q7: total value of the orders of customer " + custkey + " in " + year);
        System.out.println("Query  pk=" + Keys.customerPk(custkey) + "  sk begins_with " + Keys.yearPrefix(year));
        long t = System.nanoTime();
        List<Order> list = ordersOf(custkey, Keys.yearPrefix(year));
        BigDecimal total = BigDecimal.ZERO;
        BigDecimal fromLines = BigDecimal.ZERO;
        System.out.printf(Locale.ROOT, "%-10s %-10s %-6s %12s%n", "orderdate", "orderkey", "status", "totalprice");
        for (Order o : list) {
            System.out.printf(Locale.ROOT, "%-10s %-10d %-6s %12s%n",
                    o.getOrderdate(), o.getOrderkey(), o.getOrderstatus(), o.getTotalprice());
            total = total.add(o.getTotalprice());
            for (LineItem l : o.getLineitems()) {
                fromLines = fromLines.add(l.netValue());
            }
        }
        System.out.printf(Locale.ROOT, "%d orders in %d, total value %s%n", list.size(), year, total);
        System.out.printf(Locale.ROOT, "cross-check from line items, sum of extendedprice*(1-discount)*(1+tax) = %s%n",
                fromLines.setScale(2, java.math.RoundingMode.HALF_UP));
        done(t, pages + " Query page(s)");
        return total;
    }

    // ---------------------------------------------------------------- helpers

    /** All orders of a customer whose sort key begins with the prefix, oldest first. */
    private List<Order> ordersOf(long custkey, String skPrefix) {
        QueryConditional cond = QueryConditional.sortBeginsWith(
                Key.builder().partitionValue(Keys.customerPk(custkey)).sortValue(skPrefix).build());
        List<Order> out = new ArrayList<>();
        pages = 0;
        for (Page<Order> page : orders.query(cond)) {
            pages++;
            out.addAll(page.items());
        }
        return out;
    }

    private static Set<Long> partsOf(List<Order> list) {
        Set<Long> parts = new TreeSet<>();
        for (Order o : list) {
            for (LineItem l : o.getLineitems()) {
                parts.add(l.getPartkey());
            }
        }
        return parts;
    }

    private static void printCustomer(Customer c) {
        System.out.println("custkey    : " + c.getCustkey());
        System.out.println("name       : " + c.getName());
        System.out.println("address    : " + c.getAddress());
        System.out.println("nationkey  : " + c.getNationkey());
        System.out.println("phone      : " + c.getPhone());
        System.out.println("acctbal    : " + c.getAcctbal());
        System.out.println("mktsegment : " + c.getMktsegment());
        System.out.println("comment    : " + c.getComment());
    }

    private static void printOrderHeader() {
        System.out.printf(Locale.ROOT, "%-10s %-10s %-6s %-15s %-15s %12s %5s%n",
                "orderdate", "orderkey", "status", "priority", "clerk", "totalprice", "lines");
    }

    private static void printOrderRow(Order o) {
        System.out.printf(Locale.ROOT, "%-10s %-10d %-6s %-15s %-15s %12s %5d%n",
                o.getOrderdate(), o.getOrderkey(), o.getOrderstatus(), o.getOrderpriority(), o.getClerk(),
                o.getTotalprice(), o.getLineitems() == null ? 0 : o.getLineitems().size());
    }

    private static void printLineHeader() {
        System.out.printf(Locale.ROOT, "%-4s %-8s %-7s %5s %12s %5s %5s %-2s %-2s %-10s %-10s %-10s %-17s %-7s%n",
                "line", "partkey", "suppkey", "qty", "extprice", "disc", "tax", "rf", "ls",
                "shipdate", "commitdate", "receipt", "shipinstruct", "mode");
    }

    private static void printLineRow(LineItem l) {
        System.out.printf(Locale.ROOT, "%-4d %-8d %-7d %5s %12s %5s %5s %-2s %-2s %-10s %-10s %-10s %-17s %-7s%n",
                l.getLinenumber(), l.getPartkey(), l.getSuppkey(), l.getQuantity(), l.getExtendedprice(),
                l.getDiscount(), l.getTax(), l.getReturnflag(), l.getLinestatus(), l.getShipdate(),
                l.getCommitdate(), l.getReceiptdate(), l.getShipinstruct(), l.getShipmode());
    }

    private static void section(String title) {
        System.out.println();
        System.out.println("======== " + title + " ========");
    }

    private static void done(long startNanos, String requests) {
        System.out.printf(Locale.ROOT, "(%s, %.1f ms)%n", requests, (System.nanoTime() - startNanos) / 1e6);
    }
}
