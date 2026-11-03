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
