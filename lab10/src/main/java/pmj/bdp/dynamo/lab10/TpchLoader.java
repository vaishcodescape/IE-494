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
