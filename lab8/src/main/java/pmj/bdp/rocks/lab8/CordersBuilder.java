package pmj.bdp.rocks.lab8;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Locale;

import org.rocksdb.CompressionType;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;

/**
 * Task 2: merge-join ORDERS with LINEITEM and write one JSON document per
 * order into CORDERS. Task 3 repeats this under none / LZ4 / Zstd.
 */
final class CordersBuilder {

    private CordersBuilder() {
    }

    static MergeReport build(Path ordersDb, Path lineItemDb, Path cordersDb, CompressionType compression)
            throws RocksDBException, IOException {
        RocksStore.deleteDirectory(cordersDb);
        long started = System.nanoTime();
        long ordersIn = 0;
        long lineItemsIn = 0;
        long written = 0;
        long missingOrders = 0;
        long writeNanos;
        String sample = null;

        try (RocksDB orders = RocksStore.openReadOnly(ordersDb);
             RocksDB lines = RocksStore.openReadOnly(lineItemDb);
             RocksDB out = RocksStore.open(cordersDb, compression, true);
             RocksStore.Batch batch = new RocksStore.Batch(out);
             RocksIterator orderIt = orders.newIterator();
             RocksIterator lineIt = lines.newIterator()) {

            orderIt.seekToFirst();
            lineIt.seekToFirst();
            String[] items = new String[16];
            int nItems = 0;

            while (orderIt.isValid()) {
                byte[] orderKey = Arrays.copyOf(orderIt.key(), Keys.ORDER_WIDTH);
                String orderJson = new String(orderIt.value(), StandardCharsets.UTF_8);
                ordersIn++;
                nItems = 0;

                while (lineIt.isValid()) {
                    byte[] lineKey = lineIt.key();
                    int cmp = compareOrderPrefix(lineKey, orderKey);
                    if (cmp > 0) {
                        break;
                    }
                    if (cmp < 0) {
                        // Line item whose order is missing from ORDERS.
                        missingOrders++;
                        lineIt.next();
                        continue;
                    }
                    if (nItems == items.length) {
                        items = Arrays.copyOf(items, items.length * 2);
                    }
                    items[nItems++] = new String(lineIt.value(), StandardCharsets.UTF_8);
                    lineItemsIn++;
                    lineIt.next();
                }

                String merged = Json.embedLineItems(orderJson, items, nItems);
                if (sample == null) {
                    sample = merged;
                }
                batch.put(orderKey, merged.getBytes(StandardCharsets.UTF_8));
                written++;
                orderIt.next();
                if (ordersIn % 500_000 == 0) {
                    double sec = (System.nanoTime() - started) / 1_000_000_000.0;
                    System.out.printf(Locale.ROOT, "  CORDERS orders %,d in %.1fs%n", ordersIn, sec);
                }
            }
            while (lineIt.isValid()) {
                missingOrders++;
                lineIt.next();
            }
            batch.flush();
            writeNanos = batch.writeNanos();
            RocksStore.flushAndCompact(out);
        }

        long totalNanos = System.nanoTime() - started;
        long size = RocksStore.directoryBytes(cordersDb);

        RocksStore.Scan read;
        try (RocksDB db = RocksStore.openReadOnly(cordersDb)) {
            read = RocksStore.scan(db);
        }
        return new MergeReport(compression, cordersDb, ordersIn, lineItemsIn, written,
                missingOrders, writeNanos, totalNanos, size, read, sample);
    }

    /** Compare the order-key prefix of a LINEITEM key with an ORDERS key. */
    static int compareOrderPrefix(byte[] lineKey, byte[] orderKey) {
        int n = Math.min(Keys.ORDER_WIDTH, Math.min(lineKey.length, orderKey.length));
        for (int i = 0; i < n; i++) {
            int d = (lineKey[i] & 0xff) - (orderKey[i] & 0xff);
            if (d != 0) {
                return d;
            }
        }
        return 0;
    }

    record MergeReport(
            CompressionType compression,
            Path database,
            long ordersRead,
            long lineItemsRead,
            long written,
            long orphanLineItems,
            long writeNanos,
            long totalNanos,
            long directoryBytes,
            RocksStore.Scan read,
            String sample) {

        void print() {
            System.out.println();
            System.out.println("======== CORDERS " + compression + " ========");
            System.out.println("rocksdb path       : " + database);
            System.out.printf(Locale.ROOT, "orders read        : %,d%n", ordersRead);
            System.out.printf(Locale.ROOT, "line items embedded: %,d%n", lineItemsRead);
            System.out.printf(Locale.ROOT, "documents written  : %,d%n", written);
            System.out.printf(Locale.ROOT, "orphan line items  : %,d%n", orphanLineItems);
            System.out.printf(Locale.ROOT, "insertion time     : %.3f s%n", writeNanos / 1_000_000_000.0);
            System.out.printf(Locale.ROOT, "total write time   : %.3f s%n", totalNanos / 1_000_000_000.0);
            System.out.printf(Locale.ROOT, "final db size      : %,d bytes (%.1f MiB)%n",
                    directoryBytes, directoryBytes / (1024.0 * 1024.0));
            System.out.printf(Locale.ROOT, "read keys          : %,d%n", read.keys());
            System.out.printf(Locale.ROOT, "read bytes         : %,d%n", read.bytes());
            System.out.printf(Locale.ROOT, "read throughput    : %.1f MB/s%n", read.mbPerSec());
            System.out.println("sample key         : "
                    + (read.firstKey() == null ? "(empty)" : new String(read.firstKey(), StandardCharsets.US_ASCII)));
            System.out.println("sample value       : " + abbreviate(sample));
        }

        private static String abbreviate(String json) {
            if (json == null) {
                return "(none)";
            }
            int cap = 900;
            if (json.length() <= cap) {
                return json;
            }
            return json.substring(0, cap) + "...";
        }
    }
}
