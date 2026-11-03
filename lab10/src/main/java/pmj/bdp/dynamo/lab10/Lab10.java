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
