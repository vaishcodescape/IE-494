## Source code

All of `lab10/`, package `pmj.bdp.dynamo.lab10`.

<toggle heading="h3">
Lab10.java

```java
package pmj.bdp.dynamo.lab10;

import java.nio.file.Path;

import software.amazon.awssdk.enhanced.dynamodb.DynamoDbEnhancedClient;
import software.amazon.awssdk.enhanced.dynamodb.DynamoDbTable;
import software.amazon.awssdk.enhanced.dynamodb.TableSchema;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;

/**
 * Lab 10: TPC-H CUSTOMER / ORDERS / LINEITEM in DynamoDB (Enhanced Client).
 *
 * <pre>
 *   java ... Lab10 create [--drop]                 create table TPCH + GSI OrderIndex
 *   java ... Lab10 load dataDir                    populate from customer.tbl, orders.tbl, lineitem.tbl
 *   java ... Lab10 describe                        describe-table
 *   java ... Lab10 q1 custkey                      customer profile
 *   java ... Lab10 q2 custkey                      all orders of a customer
 *   java ... Lab10 q3 orderkey                     all line items of an order
 *   java ... Lab10 q4 custkey                      order history (profile + orders + line items)
 *   java ... Lab10 q5 custkey year                 items ordered in a year
 *   java ... Lab10 q6 custkey year                 items ordered in year-1 but not in year
 *   java ... Lab10 q7 custkey year                 total value of the orders of a year
 *   java ... Lab10 queries custkey orderkey year   q1 .. q7
 *   java ... Lab10 all dataDir custkey orderkey year
 * </pre>
 */
public class Lab10 {

    public static void main(String[] args) throws Exception {
        System.setProperty("org.slf4j.simpleLogger.defaultLogLevel", "warn");
        if (args.length < 1) {
            usage();
            return;
        }
        try (DynamoDbClient client = Dynamo.client()) {
            DynamoDbEnhancedClient enhanced = Dynamo.enhanced(client);
            DynamoDbTable<Customer> customers = enhanced.table(Keys.TABLE, TableSchema.fromBean(Customer.class));
            DynamoDbTable<Order> orders = enhanced.table(Keys.TABLE, TableSchema.fromBean(Order.class));
            Queries q = new Queries(enhanced);
            switch (args[0]) {
                case "create" -> Schema.create(client, orders, args.length > 1 && args[1].equals("--drop"));
                case "describe" -> describe();
                case "load" -> TpchLoader.load(enhanced, customers, orders, Path.of(args[1])).print();
                case "q1" -> q.q1CustomerProfile(Long.parseLong(args[1]));
                case "q2" -> q.q2OrdersOfCustomer(Long.parseLong(args[1]));
                case "q3" -> q.q3LineItemsOfOrder(Long.parseLong(args[1]));
                case "q4" -> q.q4OrderHistory(Long.parseLong(args[1]));
                case "q5" -> q.q5ItemsOrderedInYear(Long.parseLong(args[1]), Integer.parseInt(args[2]));
                case "q6" -> q.q6ItemsOfPreviousYearNotThisYear(Long.parseLong(args[1]), Integer.parseInt(args[2]));
                case "q7" -> q.q7TotalValueInYear(Long.parseLong(args[1]), Integer.parseInt(args[2]));
                case "queries" -> queries(q, Long.parseLong(args[1]), Long.parseLong(args[2]), Integer.parseInt(args[3]));
                case "all" -> {
                    Schema.create(client, orders, true);
                    TpchLoader.load(enhanced, customers, orders, Path.of(args[1])).print();
                    describe();
                    queries(q, Long.parseLong(args[2]), Long.parseLong(args[3]), Integer.parseInt(args[4]));
                }
                default -> usage();
            }
        }
    }

    /** describe-table with the long-timeout client (DynamoDB Local counts all items for it). */
    static void describe() {
        try (DynamoDbClient slow = Dynamo.longTimeoutClient()) {
            Schema.describe(slow);
        }
    }

    /** Runs the seven query functions for one customer, one of its orders and one year. */
    static void queries(Queries q, long custkey, long orderkey, int year) {
        q.q1CustomerProfile(custkey);
        q.q2OrdersOfCustomer(custkey);
        q.q3LineItemsOfOrder(orderkey);
        q.q4OrderHistory(custkey);
        q.q5ItemsOrderedInYear(custkey, year);
        q.q6ItemsOfPreviousYearNotThisYear(custkey, year);
        q.q7TotalValueInYear(custkey, year);
    }

    private static void usage() {
        System.out.println("usage: Lab10 create [--drop] | load dataDir | describe | q1..q7 ... | queries custkey orderkey year"
                + " | all dataDir custkey orderkey year");
    }
}
```
</toggle>

<toggle heading="h3">
Queries.java

```java
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
```
</toggle>

<toggle heading="h3">
TpchLoader.java

```java
package pmj.bdp.dynamo.lab10;

import java.io.BufferedReader;
import java.io.IOException;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;

import software.amazon.awssdk.enhanced.dynamodb.DynamoDbEnhancedClient;
import software.amazon.awssdk.enhanced.dynamodb.DynamoDbTable;
import software.amazon.awssdk.enhanced.dynamodb.model.BatchWriteItemEnhancedRequest;
import software.amazon.awssdk.enhanced.dynamodb.model.BatchWriteResult;
import software.amazon.awssdk.enhanced.dynamodb.model.WriteBatch;

/**
 * Populates the TPCH table from customer.tbl, orders.tbl and lineitem.tbl.
 *
 * customer.tbl rows become profile items. orders.tbl and lineitem.tbl are both written by dbgen
 * in ascending O_ORDERKEY order, so they are merge-joined in one streaming pass: each order is
 * read, its line items are taken from the head of lineitem.tbl, and one order item (with the
 * line items embedded) is produced. Items are written with BatchWriteItem (25 per request)
 * through the Enhanced Client, several requests in flight at a time.
 */
final class TpchLoader {

    static final int BATCH_SIZE = 25;                       // BatchWriteItem limit
    static final int THREADS = Integer.getInteger("ddb.threads", 8);
    static final int CUSTOMER_FIELDS = 8;
    static final int ORDER_FIELDS = 9;
    static final int LINE_FIELDS = 16;

    private TpchLoader() {
    }

    record Report(Path customerTbl, Path ordersTbl, Path lineTbl,
                  long customers, long orders, long lineItems,
                  long customerItems, long orderItems, long retries,
                  double customerSeconds, double orderSeconds) {

        void print() {
            System.out.println();
            System.out.println("======== load summary ========");
            System.out.printf(Locale.ROOT, "customer.tbl rows   : %,d%n", customers);
            System.out.printf(Locale.ROOT, "orders.tbl rows     : %,d%n", orders);
            System.out.printf(Locale.ROOT, "lineitem.tbl rows   : %,d (embedded in their orders)%n", lineItems);
            System.out.printf(Locale.ROOT, "profile items put   : %,d in %.1f s (%,.0f items/s)%n",
                    customerItems, customerSeconds, customerItems / customerSeconds);
            System.out.printf(Locale.ROOT, "order items put     : %,d in %.1f s (%,.0f items/s)%n",
                    orderItems, orderSeconds, orderItems / orderSeconds);
            System.out.printf(Locale.ROOT, "items in table      : %,d%n", customerItems + orderItems);
            System.out.printf(Locale.ROOT, "batch retries       : %,d (unprocessed items re-sent)%n", retries);
            System.out.printf(Locale.ROOT, "writer threads      : %d, %d items per BatchWriteItem%n", THREADS, BATCH_SIZE);
        }
    }

    static Report load(DynamoDbEnhancedClient enhanced, DynamoDbTable<Customer> customerTable,
                       DynamoDbTable<Order> orderTable, Path dataDir) throws Exception {
        Path customerTbl = findTable(dataDir, "customer");
        Path ordersTbl = findTable(dataDir, "orders");
        Path lineTbl = findTable(dataDir, "lineitem");
        System.out.println("customer.tbl = " + customerTbl + " (" + Files.size(customerTbl) + " bytes)");
        System.out.println("orders.tbl   = " + ordersTbl + " (" + Files.size(ordersTbl) + " bytes)");
        System.out.println("lineitem.tbl = " + lineTbl + " (" + Files.size(lineTbl) + " bytes)");

        System.out.println();
        System.out.println("---- phase 1: CUSTOMER -> profile items (pk=CUST#custkey, sk=PROFILE) ----");
        long t0 = System.nanoTime();
        BatchWriter<Customer> cw = new BatchWriter<>(enhanced, customerTable, Customer.class, "CUSTOMER");
        long customers = loadCustomers(customerTbl, cw);
        cw.finish();
        double customerSeconds = (System.nanoTime() - t0) / 1e9;

        System.out.println();
        System.out.println("---- phase 2: ORDERS + LINEITEM -> order items (pk=CUST#custkey, sk=ORDER#date#orderkey) ----");
        long t1 = System.nanoTime();
        BatchWriter<Order> ow = new BatchWriter<>(enhanced, orderTable, Order.class, "ORDER");
        long[] counts = loadOrders(ordersTbl, lineTbl, ow);
        ow.finish();
        double orderSeconds = (System.nanoTime() - t1) / 1e9;

        return new Report(customerTbl, ordersTbl, lineTbl, customers, counts[0], counts[1],
                cw.written(), ow.written(), cw.retries() + ow.retries(), customerSeconds, orderSeconds);
    }

    private static long loadCustomers(Path tbl, BatchWriter<Customer> writer) throws Exception {
        long rows = 0;
        try (BufferedReader reader = Files.newBufferedReader(tbl, StandardCharsets.US_ASCII)) {
            String[] fields;
            while ((fields = nextFields(reader, CUSTOMER_FIELDS)) != null) {
                writer.add(parseCustomer(fields));
                rows++;
            }
        }
        return rows;
    }

    /** Merge-join of orders.tbl with lineitem.tbl; both files are ordered by O_ORDERKEY. */
    private static long[] loadOrders(Path ordersTbl, Path lineTbl, BatchWriter<Order> writer) throws Exception {
        long orders = 0;
        long lines = 0;
        try (BufferedReader orderReader = Files.newBufferedReader(ordersTbl, StandardCharsets.US_ASCII);
             BufferedReader lineReader = Files.newBufferedReader(lineTbl, StandardCharsets.US_ASCII)) {
            String[] pendingLine = nextFields(lineReader, LINE_FIELDS);
            String[] fields;
            while ((fields = nextFields(orderReader, ORDER_FIELDS)) != null) {
                Order order = parseOrder(fields);
                long orderkey = order.getOrderkey();
                List<LineItem> items = new ArrayList<>(7);
                while (pendingLine != null) {
                    long lineOrderkey = Long.parseLong(pendingLine[0]);
                    if (lineOrderkey < orderkey) {
                        throw new IllegalStateException("lineitem.tbl is not ordered by orderkey or has an orphan line: "
                                + "L_ORDERKEY " + lineOrderkey + " seen while at O_ORDERKEY " + orderkey);
                    }
                    if (lineOrderkey > orderkey) {
                        break;
                    }
                    items.add(parseLineItem(pendingLine));
                    lines++;
                    pendingLine = nextFields(lineReader, LINE_FIELDS);
                }
                order.setLineitems(items);
                writer.add(order);
                orders++;
            }
            if (pendingLine != null) {
                throw new IllegalStateException("line items left over after the last order, L_ORDERKEY " + pendingLine[0]);
            }
        }
        return new long[] {orders, lines};
    }

    static Customer parseCustomer(String[] f) {
        long custkey = Long.parseLong(f[0]);
        Customer c = new Customer();
        c.setPk(Keys.customerPk(custkey));
        c.setSk(Keys.PROFILE_SK);
        c.setType(Keys.TYPE_CUSTOMER);
        c.setCustkey(custkey);
        c.setName(f[1]);
        c.setAddress(f[2]);
        c.setNationkey(Integer.parseInt(f[3]));
        c.setPhone(f[4]);
        c.setAcctbal(new BigDecimal(f[5]));
        c.setMktsegment(f[6]);
        c.setComment(f[7]);
        return c;
    }

    static Order parseOrder(String[] f) {
        long orderkey = Long.parseLong(f[0]);
        long custkey = Long.parseLong(f[1]);
        String orderdate = f[4];
        Order o = new Order();
        o.setPk(Keys.customerPk(custkey));
        o.setSk(Keys.orderSk(orderdate, orderkey));
        o.setType(Keys.TYPE_ORDER);
        o.setOrderkey(orderkey);
        o.setCustkey(custkey);
        o.setOrderstatus(f[2]);
        o.setTotalprice(new BigDecimal(f[3]));
        o.setOrderdate(orderdate);
        o.setYear(Integer.parseInt(orderdate.substring(0, 4)));
        o.setOrderpriority(f[5]);
        o.setClerk(f[6]);
        o.setShippriority(Integer.parseInt(f[7]));
        o.setComment(f[8]);
        return o;
    }

    static LineItem parseLineItem(String[] f) {
        LineItem l = new LineItem();
        l.setPartkey(Long.parseLong(f[1]));
        l.setSuppkey(Long.parseLong(f[2]));
        l.setLinenumber(Integer.parseInt(f[3]));
        l.setQuantity(new BigDecimal(f[4]));
        l.setExtendedprice(new BigDecimal(f[5]));
        l.setDiscount(new BigDecimal(f[6]));
        l.setTax(new BigDecimal(f[7]));
        l.setReturnflag(f[8]);
        l.setLinestatus(f[9]);
        l.setShipdate(f[10]);
        l.setCommitdate(f[11]);
        l.setReceiptdate(f[12]);
        l.setShipinstruct(f[13]);
        l.setShipmode(f[14]);
        l.setComment(f[15]);
        return l;
    }

    /** Next non-empty row of a .tbl file split into exactly n fields. */
    private static String[] nextFields(BufferedReader reader, int n) throws IOException {
        String line;
        while ((line = reader.readLine()) != null) {
            if (line.isEmpty()) {
                continue;
            }
            String[] fields = split(line, n);
            if (fields == null) {
                throw new IOException("malformed row (expected " + n + " fields): " + line);
            }
            return fields;
        }
        return null;
    }

    /** TPC-H .tbl rows are pipe-delimited and end with a trailing pipe. */
    static String[] split(String line, int n) {
        String[] out = new String[n];
        int start = 0;
        int found = 0;
        for (int i = 0; i < line.length(); i++) {
            if (line.charAt(i) == '|') {
                if (found == n) {
                    return null;
                }
                out[found++] = line.substring(start, i);
                start = i + 1;
            }
        }
        if (start < line.length()) {            // no trailing pipe
            if (found == n) {
                return null;
            }
            out[found++] = line.substring(start);
        }
        return found == n ? out : null;
    }

    private static Path findTable(Path dataDir, String baseName) throws IOException {
        try (Stream<Path> files = Files.walk(dataDir, 2)) {
            return files.filter(Files::isRegularFile)
                    .filter(p -> p.getFileName().toString().equalsIgnoreCase(baseName + ".tbl"))
                    .findFirst()
                    .orElseThrow(() -> new IOException("could not find " + baseName + ".tbl under " + dataDir));
        }
    }

    /** Groups items into batches of 25 and writes them with BatchWriteItem on a thread pool. */
    static final class BatchWriter<T> {

        private final DynamoDbEnhancedClient enhanced;
        private final DynamoDbTable<T> table;
        private final Class<T> type;
        private final String label;
        private final ExecutorService pool = Executors.newFixedThreadPool(THREADS);
        private final Semaphore inFlight = new Semaphore(THREADS * 4);
        private final AtomicLong written = new AtomicLong();
        private final AtomicLong retries = new AtomicLong();
        private final AtomicReference<Throwable> failure = new AtomicReference<>();
        private final long started = System.nanoTime();
        private List<T> batch = new ArrayList<>(BATCH_SIZE);
        private long submitted;

        BatchWriter(DynamoDbEnhancedClient enhanced, DynamoDbTable<T> table, Class<T> type, String label) {
            this.enhanced = enhanced;
            this.table = table;
            this.type = type;
            this.label = label;
        }

        void add(T item) throws InterruptedException {
            batch.add(item);
            if (batch.size() == BATCH_SIZE) {
                flushBatch();
            }
        }

        private void flushBatch() throws InterruptedException {
            if (batch.isEmpty()) {
                return;
            }
            Throwable f = failure.get();
            if (f != null) {
                throw new IllegalStateException(label + " batch write failed", f);
            }
            List<T> items = batch;
            batch = new ArrayList<>(BATCH_SIZE);
            inFlight.acquire();
            pool.execute(() -> {
                try {
                    write(items);
                } catch (Throwable t) {
                    failure.compareAndSet(null, t);
                } finally {
                    inFlight.release();
                }
            });
            submitted += items.size();
            if (submitted % 100_000 == 0) {
                progress();
            }
        }

        private void write(List<T> items) throws InterruptedException {
            List<T> pending = items;
            int attempt = 0;
            while (!pending.isEmpty()) {
                WriteBatch.Builder<T> wb = WriteBatch.builder(type).mappedTableResource(table);
                for (T item : pending) {
                    wb.addPutItem(item);
                }
                BatchWriteResult result = enhanced.batchWriteItem(
                        BatchWriteItemEnhancedRequest.builder().writeBatches(wb.build()).build());
                List<T> unprocessed = result.unprocessedPutItemsForTable(table);
                written.addAndGet(pending.size() - unprocessed.size());
                if (!unprocessed.isEmpty()) {
                    retries.incrementAndGet();
                    Thread.sleep(Math.min(1000L, 50L << Math.min(attempt++, 4)));
                }
                pending = unprocessed;
            }
        }

        void finish() throws Exception {
            flushBatch();
            pool.shutdown();
            pool.awaitTermination(1, TimeUnit.DAYS);
            Throwable f = failure.get();
            if (f != null) {
                throw new IllegalStateException(label + " batch write failed", f);
            }
            progress();
        }

        long written() {
            return written.get();
        }

        long retries() {
            return retries.get();
        }

        private void progress() {
            double sec = (System.nanoTime() - started) / 1e9;
            System.out.printf(Locale.ROOT, "  %-8s submitted %,10d  written %,10d  %7.1f s  %,7.0f items/s%n",
                    label, submitted, written.get(), sec, written.get() / Math.max(sec, 1e-9));
        }
    }
}
```
</toggle>

<toggle heading="h3">
Schema.java

```java
package pmj.bdp.dynamo.lab10;

import java.util.Locale;

import software.amazon.awssdk.enhanced.dynamodb.DynamoDbTable;
import software.amazon.awssdk.enhanced.dynamodb.model.CreateTableEnhancedRequest;
import software.amazon.awssdk.enhanced.dynamodb.model.EnhancedGlobalSecondaryIndex;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeDefinition;
import software.amazon.awssdk.services.dynamodb.model.GlobalSecondaryIndexDescription;
import software.amazon.awssdk.services.dynamodb.model.KeySchemaElement;
import software.amazon.awssdk.services.dynamodb.model.Projection;
import software.amazon.awssdk.services.dynamodb.model.ProjectionType;
import software.amazon.awssdk.services.dynamodb.model.ResourceNotFoundException;
import software.amazon.awssdk.services.dynamodb.model.TableDescription;

/** Creates and describes the single table TPCH with its global secondary index OrderIndex. */
final class Schema {

    private Schema() {
    }

    static void create(DynamoDbClient client, DynamoDbTable<Order> orderTable, boolean drop) {
        if (exists(client)) {
            if (!drop) {
                System.out.println("table " + Keys.TABLE + " already exists (use --drop to recreate)");
                describe(client);
                return;
            }
            System.out.println("deleting existing table " + Keys.TABLE + " ...");
            client.deleteTable(r -> r.tableName(Keys.TABLE));
            client.waiter().waitUntilTableNotExists(r -> r.tableName(Keys.TABLE));
        }
        System.out.println("creating table " + Keys.TABLE + " (pk S, sk S) with GSI " + Keys.ORDER_INDEX
                + " (orderkey N, KEYS_ONLY), on-demand capacity ...");
        // The Order bean carries the key schema (pk, sk) and the GSI partition key (orderkey).
        orderTable.createTable(CreateTableEnhancedRequest.builder()
                .globalSecondaryIndices(EnhancedGlobalSecondaryIndex.builder()
                        .indexName(Keys.ORDER_INDEX)
                        .projection(Projection.builder().projectionType(ProjectionType.KEYS_ONLY).build())
                        .build())
                .build());
        client.waiter().waitUntilTableExists(r -> r.tableName(Keys.TABLE));
        System.out.println("created.");
        describe(client);
    }

    static boolean exists(DynamoDbClient client) {
        try {
            client.describeTable(r -> r.tableName(Keys.TABLE));
            return true;
        } catch (ResourceNotFoundException e) {
            return false;
        }
    }

    static void describe(DynamoDbClient client) {
        TableDescription t = client.describeTable(r -> r.tableName(Keys.TABLE)).table();
        System.out.println();
        System.out.println("======== describe-table " + t.tableName() + " ========");
        System.out.println("status               : " + t.tableStatusAsString());
        System.out.println("billing              : " + (t.billingModeSummary() == null ? "PROVISIONED"
                : t.billingModeSummary().billingModeAsString()));
        System.out.println("key schema           : " + keySchema(t.keySchema()));
        StringBuilder attrs = new StringBuilder();
        for (AttributeDefinition a : t.attributeDefinitions()) {
            attrs.append(a.attributeName()).append(' ').append(a.attributeTypeAsString()).append("  ");
        }
        System.out.println("attribute definitions: " + attrs.toString().trim());
        for (GlobalSecondaryIndexDescription g : t.globalSecondaryIndexes()) {
            System.out.println("GSI " + g.indexName() + "        : " + keySchema(g.keySchema())
                    + ", projection " + g.projection().projectionTypeAsString()
                    + ", status " + g.indexStatusAsString());
        }
        System.out.printf(Locale.ROOT, "item count           : %,d%n", t.itemCount());
        System.out.printf(Locale.ROOT, "table size           : %,d bytes (%.1f MiB)%n",
                t.tableSizeBytes(), t.tableSizeBytes() / (1024.0 * 1024.0));
    }

    private static String keySchema(java.util.List<KeySchemaElement> schema) {
        StringBuilder sb = new StringBuilder();
        for (KeySchemaElement k : schema) {
            if (sb.length() > 0) {
                sb.append(", ");
            }
            sb.append(k.attributeName()).append(" (").append(k.keyTypeAsString()).append(')');
        }
        return sb.toString();
    }
}
```
</toggle>

<toggle heading="h3">
Keys.java

```java
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
```
</toggle>

<toggle heading="h3">
Customer.java

```java
package pmj.bdp.dynamo.lab10;

import java.math.BigDecimal;

import software.amazon.awssdk.enhanced.dynamodb.mapper.annotations.DynamoDbBean;
import software.amazon.awssdk.enhanced.dynamodb.mapper.annotations.DynamoDbPartitionKey;
import software.amazon.awssdk.enhanced.dynamodb.mapper.annotations.DynamoDbSortKey;

/** Customer profile item (TPC-H CUSTOMER row). pk = CUST#custkey, sk = PROFILE. */
@DynamoDbBean
public class Customer {

    private String pk;
    private String sk;
    private String type;
    private Long custkey;
    private String name;
    private String address;
    private Integer nationkey;
    private String phone;
    private BigDecimal acctbal;
    private String mktsegment;
    private String comment;

    public Customer() {
    }

    @DynamoDbPartitionKey
    public String getPk() {
        return pk;
    }

    public void setPk(String pk) {
        this.pk = pk;
    }

    @DynamoDbSortKey
    public String getSk() {
        return sk;
    }

    public void setSk(String sk) {
        this.sk = sk;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public Long getCustkey() {
        return custkey;
    }

    public void setCustkey(Long custkey) {
        this.custkey = custkey;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getAddress() {
        return address;
    }

    public void setAddress(String address) {
        this.address = address;
    }

    public Integer getNationkey() {
        return nationkey;
    }

    public void setNationkey(Integer nationkey) {
        this.nationkey = nationkey;
    }

    public String getPhone() {
        return phone;
    }

    public void setPhone(String phone) {
        this.phone = phone;
    }

    public BigDecimal getAcctbal() {
        return acctbal;
    }

    public void setAcctbal(BigDecimal acctbal) {
        this.acctbal = acctbal;
    }

    public String getMktsegment() {
        return mktsegment;
    }

    public void setMktsegment(String mktsegment) {
        this.mktsegment = mktsegment;
    }

    public String getComment() {
        return comment;
    }

    public void setComment(String comment) {
        this.comment = comment;
    }
}
```
</toggle>

<toggle heading="h3">
Order.java

```java
package pmj.bdp.dynamo.lab10;

import java.math.BigDecimal;
import java.util.List;

import software.amazon.awssdk.enhanced.dynamodb.mapper.annotations.DynamoDbBean;
import software.amazon.awssdk.enhanced.dynamodb.mapper.annotations.DynamoDbPartitionKey;
import software.amazon.awssdk.enhanced.dynamodb.mapper.annotations.DynamoDbSecondaryPartitionKey;
import software.amazon.awssdk.enhanced.dynamodb.mapper.annotations.DynamoDbSortKey;

/**
 * Order item (TPC-H ORDERS row) with all of its LINEITEM rows embedded.
 * pk = CUST#custkey, sk = ORDER#orderdate#orderkey. orderkey is the partition key
 * of the global secondary index OrderIndex, so an order can also be found by O_ORDERKEY alone.
 */
@DynamoDbBean
public class Order {

    private String pk;
    private String sk;
    private String type;
    private Long orderkey;
    private Long custkey;
    private String orderstatus;
    private BigDecimal totalprice;
    private String orderdate;
    private Integer year;
    private String orderpriority;
    private String clerk;
    private Integer shippriority;
    private String comment;
    private List<LineItem> lineitems;

    public Order() {
    }

    @DynamoDbPartitionKey
    public String getPk() {
        return pk;
    }

    public void setPk(String pk) {
        this.pk = pk;
    }

    @DynamoDbSortKey
    public String getSk() {
        return sk;
    }

    public void setSk(String sk) {
        this.sk = sk;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    @DynamoDbSecondaryPartitionKey(indexNames = Keys.ORDER_INDEX)
    public Long getOrderkey() {
        return orderkey;
    }

    public void setOrderkey(Long orderkey) {
        this.orderkey = orderkey;
    }

    public Long getCustkey() {
        return custkey;
    }

    public void setCustkey(Long custkey) {
        this.custkey = custkey;
    }

    public String getOrderstatus() {
        return orderstatus;
    }

    public void setOrderstatus(String orderstatus) {
        this.orderstatus = orderstatus;
    }

    public BigDecimal getTotalprice() {
        return totalprice;
    }

    public void setTotalprice(BigDecimal totalprice) {
        this.totalprice = totalprice;
    }

    public String getOrderdate() {
        return orderdate;
    }

    public void setOrderdate(String orderdate) {
        this.orderdate = orderdate;
    }

    public Integer getYear() {
        return year;
    }

    public void setYear(Integer year) {
        this.year = year;
    }

    public String getOrderpriority() {
        return orderpriority;
    }

    public void setOrderpriority(String orderpriority) {
        this.orderpriority = orderpriority;
    }

    public String getClerk() {
        return clerk;
    }

    public void setClerk(String clerk) {
        this.clerk = clerk;
    }

    public Integer getShippriority() {
        return shippriority;
    }

    public void setShippriority(Integer shippriority) {
        this.shippriority = shippriority;
    }

    public String getComment() {
        return comment;
    }

    public void setComment(String comment) {
        this.comment = comment;
    }

    public List<LineItem> getLineitems() {
        return lineitems;
    }

    public void setLineitems(List<LineItem> lineitems) {
        this.lineitems = lineitems;
    }
}
```
</toggle>

<toggle heading="h3">
LineItem.java

```java
package pmj.bdp.dynamo.lab10;

import java.math.BigDecimal;

import software.amazon.awssdk.enhanced.dynamodb.mapper.annotations.DynamoDbBean;

/** One TPC-H LINEITEM row, stored as a map inside the order item's "lineitems" list. */
@DynamoDbBean
public class LineItem {

    private Integer linenumber;
    private Long partkey;
    private Long suppkey;
    private BigDecimal quantity;
    private BigDecimal extendedprice;
    private BigDecimal discount;
    private BigDecimal tax;
    private String returnflag;
    private String linestatus;
    private String shipdate;
    private String commitdate;
    private String receiptdate;
    private String shipinstruct;
    private String shipmode;
    private String comment;

    public LineItem() {
    }

    public Integer getLinenumber() {
        return linenumber;
    }

    public void setLinenumber(Integer linenumber) {
        this.linenumber = linenumber;
    }

    public Long getPartkey() {
        return partkey;
    }

    public void setPartkey(Long partkey) {
        this.partkey = partkey;
    }

    public Long getSuppkey() {
        return suppkey;
    }

    public void setSuppkey(Long suppkey) {
        this.suppkey = suppkey;
    }

    public BigDecimal getQuantity() {
        return quantity;
    }

    public void setQuantity(BigDecimal quantity) {
        this.quantity = quantity;
    }

    public BigDecimal getExtendedprice() {
        return extendedprice;
    }

    public void setExtendedprice(BigDecimal extendedprice) {
        this.extendedprice = extendedprice;
    }

    public BigDecimal getDiscount() {
        return discount;
    }

    public void setDiscount(BigDecimal discount) {
        this.discount = discount;
    }

    public BigDecimal getTax() {
        return tax;
    }

    public void setTax(BigDecimal tax) {
        this.tax = tax;
    }

    public String getReturnflag() {
        return returnflag;
    }

    public void setReturnflag(String returnflag) {
        this.returnflag = returnflag;
    }

    public String getLinestatus() {
        return linestatus;
    }

    public void setLinestatus(String linestatus) {
        this.linestatus = linestatus;
    }

    public String getShipdate() {
        return shipdate;
    }

    public void setShipdate(String shipdate) {
        this.shipdate = shipdate;
    }

    public String getCommitdate() {
        return commitdate;
    }

    public void setCommitdate(String commitdate) {
        this.commitdate = commitdate;
    }

    public String getReceiptdate() {
        return receiptdate;
    }

    public void setReceiptdate(String receiptdate) {
        this.receiptdate = receiptdate;
    }

    public String getShipinstruct() {
        return shipinstruct;
    }

    public void setShipinstruct(String shipinstruct) {
        this.shipinstruct = shipinstruct;
    }

    public String getShipmode() {
        return shipmode;
    }

    public void setShipmode(String shipmode) {
        this.shipmode = shipmode;
    }

    public String getComment() {
        return comment;
    }

    public void setComment(String comment) {
        this.comment = comment;
    }

    /** extendedprice * (1 - discount) * (1 + tax): what the customer pays for this line. */
    BigDecimal netValue() {
        return extendedprice.multiply(BigDecimal.ONE.subtract(discount))
                .multiply(BigDecimal.ONE.add(tax));
    }
}
```
</toggle>

<toggle heading="h3">
Dynamo.java

```java
package pmj.bdp.dynamo.lab10;

import java.net.URI;
import java.time.Duration;

import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.awscore.retry.AwsRetryStrategy;
import software.amazon.awssdk.enhanced.dynamodb.DynamoDbEnhancedClient;
import software.amazon.awssdk.http.apache.ApacheHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.DynamoDbClientBuilder;

/**
 * Connection to DynamoDB Local, same settings as the setup sheet
 * (endpoint http://localhost:8000, access key 1234 / secret 1234, region ap-south-1).
 */
final class Dynamo {

    static final String ENDPOINT = System.getProperty("ddb.endpoint", "http://localhost:8000");

    private Dynamo() {
    }

    /** Low-level client: talks to DynamoDB Local instead of AWS. SDK defaults (30 s socket timeout, retries). */
    static DynamoDbClient client() {
        return builder(Duration.ofSeconds(30)).build();
    }

    /**
     * DescribeTable on DynamoDB Local counts every row of its SQLite table, which takes minutes
     * on 1.65 million items, so that call gets a 30-minute timeout and no retries.
     */
    static DynamoDbClient longTimeoutClient() {
        Duration limit = Duration.ofMinutes(30);
        return builder(limit)
                .overrideConfiguration(o -> o.retryStrategy(AwsRetryStrategy.doNotRetry())
                        .apiCallTimeout(limit).apiCallAttemptTimeout(limit))
                .build();
    }

    private static DynamoDbClientBuilder builder(Duration socketTimeout) {
        return DynamoDbClient.builder()
                .endpointOverride(URI.create(ENDPOINT))
                .region(Region.AP_SOUTH_1)
                .credentialsProvider(StaticCredentialsProvider.create(
                        AwsBasicCredentials.create("1234", "1234")))
                .httpClientBuilder(ApacheHttpClient.builder()
                        .connectionTimeout(Duration.ofSeconds(10))
                        .socketTimeout(socketTimeout));
    }

    /** High-level (Enhanced) client built on top of the low-level one. */
    static DynamoDbEnhancedClient enhanced(DynamoDbClient client) {
        return DynamoDbEnhancedClient.builder().dynamoDbClient(client).build();
    }
}
```
</toggle>

<toggle heading="h3">
pom.xml

```xml
<?xml version="1.0" encoding="UTF-8"?>
<project xmlns="http://maven.apache.org/POM/4.0.0"
         xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
         xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 http://maven.apache.org/xsd/maven-4.0.0.xsd">

    <modelVersion>4.0.0</modelVersion>

    <groupId>pmj.bdp.dynamo</groupId>
    <artifactId>lab10</artifactId>
    <version>1.0</version>
    <packaging>jar</packaging>

    <properties>
        <project.build.sourceEncoding>UTF-8</project.build.sourceEncoding>
        <maven.compiler.release>17</maven.compiler.release>
    </properties>

    <!-- Dependencies as given in the Lab 10 sheet -->
    <dependencyManagement>
        <dependencies>
            <dependency>
                <groupId>software.amazon.awssdk</groupId>
                <artifactId>bom</artifactId>
                <version>2.31.0</version>
                <type>pom</type>
                <scope>import</scope>
            </dependency>
        </dependencies>
    </dependencyManagement>

    <dependencies>
        <dependency>
            <groupId>software.amazon.awssdk</groupId>
            <artifactId>dynamodb</artifactId>
        </dependency>
        <dependency>
            <groupId>software.amazon.awssdk</groupId>
            <artifactId>dynamodb-enhanced</artifactId>
        </dependency>
        <dependency> <!-- default HTTP client of the SDK; declared so its timeouts can be set -->
            <groupId>software.amazon.awssdk</groupId>
            <artifactId>apache-client</artifactId>
        </dependency>
        <dependency>
            <groupId>org.slf4j</groupId>
            <artifactId>slf4j-simple</artifactId>
            <version>1.7.36</version>
        </dependency>
    </dependencies>

    <build>
        <finalName>lab10</finalName>
        <plugins>
            <plugin>
                <groupId>org.codehaus.mojo</groupId>
                <artifactId>exec-maven-plugin</artifactId>
                <version>3.5.0</version>
            </plugin>
        </plugins>
    </build>

</project>
```
</toggle>

<toggle heading="h3">
run_all.sh

```bash
#!/usr/bin/env bash
# IE-494 Lab 10: DynamoDB Local + TPC-H SF1 (CUSTOMER, ORDERS, LINEITEM).
#   bash run_all.sh [custkey] [orderkey] [year]
# Defaults: customer 121 (15 orders, 3 in 1993 and 5 in 1994), its order 5512869 (7 line items), year 1994.
set -euo pipefail
ROOT="$(cd "$(dirname "$0")" && pwd)"
cd "$ROOT"
DATA="${DATA:-$ROOT/../lab8/data}"          # customer.tbl, orders.tbl, lineitem.tbl (dbgen -s 1)
DBGEN="$ROOT/../lab8/tpch-kit/dbgen"
DDB="$ROOT/dynamodb_local"                  # unzipped dynamodb_local_latest
OUT="$ROOT/output"
CUST="${1:-121}"
ORDER="${2:-5512869}"
YEAR="${3:-1994}"
mkdir -p "$OUT" "$ROOT/db"

# 1. DynamoDB Local on :8000 (same command as the course setup sheet, plus a data directory)
if ! curl -s -o /dev/null http://localhost:8000/; then
  (cd "$DDB" && nohup java -Djava.library.path=./DynamoDBLocal_lib -jar DynamoDBLocal.jar \
      -sharedDb -port 8000 -dbPath "$ROOT/db" > "$OUT/dynamodb_local.log" 2>&1 &)
  sleep 4
fi

# 2. TPC-H SF1 tables, generated with the dbgen of Lab 08 if they are missing
if [ ! -f "$DATA/lineitem.tbl" ]; then
  mkdir -p "$DATA"
  (cd "$DBGEN" && DSS_PATH="$DATA" ./dbgen -q -s 1 -T c -f && DSS_PATH="$DATA" ./dbgen -q -s 1 -T o -f)
fi

# 3. build and run
mvn -q -DskipTests package
# exec:exec forks a plain JVM with the project classpath; under exec:java the SDK runs inside
# Maven's plugin classloader, which failed to load an SDK exception class once during this lab.
run() {
  mvn -q exec:exec -Dexec.executable=java \
      -Dexec.args="-cp %classpath pmj.bdp.dynamo.lab10.Lab10 $*"
}
run create --drop            2>&1 | tee "$OUT/01_create.txt"
run load "$DATA"             2>&1 | tee "$OUT/02_load.txt"
run describe                 2>&1 | tee "$OUT/03_describe.txt"
run queries "$CUST" "$ORDER" "$YEAR" 2>&1 | tee "$OUT/04_queries.txt"
cat "$OUT"/0[1-4]_*.txt > "$OUT/run_log.txt"
```
</toggle>
