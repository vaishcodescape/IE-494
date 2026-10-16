package pmj.bdp.rocks.lab8;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;

import org.rocksdb.CompressionType;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;

/** Task 1: load ORDERS.TBL and LINEITEM.TBL into RocksDB with WriteBatch. */
final class TblLoader {

    static final int ORDER_FIELDS = 9;
    static final int LINE_FIELDS = 16;

    private TblLoader() {
    }

    static LoadReport loadOrders(Path tbl, Path dbPath) throws IOException, RocksDBException {
        return load(tbl, dbPath, true);
    }

    static LoadReport loadLineItems(Path tbl, Path dbPath) throws IOException, RocksDBException {
        return load(tbl, dbPath, false);
    }

    private static LoadReport load(Path tbl, Path dbPath, boolean orders) throws IOException, RocksDBException {
        RocksStore.deleteDirectory(dbPath);
        // Source tables are stored with LZ4. The compression study is on CORDERS.
        try (RocksDB db = RocksStore.open(dbPath, CompressionType.LZ4_COMPRESSION, true);
             RocksStore.Batch batch = new RocksStore.Batch(db);
             BufferedReader reader = Files.newBufferedReader(tbl, StandardCharsets.US_ASCII)) {
            HashSet<Long> seen = new HashSet<>();
            List<String> duplicateSamples = new ArrayList<>();
            long rows = 0;
            long bad = 0;
            long duplicates = 0;
            String line;
            long started = System.nanoTime();
            while ((line = reader.readLine()) != null) {
                if (line.isEmpty()) {
                    continue;
                }
                String[] fields = split(line, orders ? ORDER_FIELDS : LINE_FIELDS);
                if (fields == null) {
                    bad++;
                    continue;
                }
                rows++;
                long orderKey = Long.parseLong(fields[0].trim());
                byte[] key;
                byte[] value;
                long identity;
                if (orders) {
                    identity = orderKey;
                    key = Keys.orderKey(orderKey);
                    value = Json.order(fields).getBytes(StandardCharsets.UTF_8);
                } else {
                    int lineNumber = Integer.parseInt(fields[3].trim());
                    identity = Keys.pack(orderKey, lineNumber);
                    key = Keys.lineKey(orderKey, lineNumber);
                    value = Json.lineItem(fields).getBytes(StandardCharsets.UTF_8);
                }
                if (!seen.add(identity)) {
                    duplicates++;
                    if (duplicateSamples.size() < 10) {
                        duplicateSamples.add(new String(key, StandardCharsets.US_ASCII));
                    }
                    continue;
                }
                batch.put(key, value);
                if (rows % 500_000 == 0) {
                    double sec = (System.nanoTime() - started) / 1_000_000_000.0;
                    System.out.printf(Locale.ROOT, "  %s rows %,d in %.1fs%n",
                            orders ? "ORDERS" : "LINEITEM", rows, sec);
                }
            }
            batch.flush();
            long writeNanos = batch.writeNanos();
            long written = batch.rows();
            RocksStore.flushAndCompact(db);
            long totalNanos = System.nanoTime() - started;
            return new LoadReport(tbl, dbPath, rows, written, duplicates, bad,
                    writeNanos, totalNanos, RocksStore.directoryBytes(dbPath), duplicateSamples);
        }
    }

    /**
     * TPC-H .tbl rows are pipe-delimited and end with a trailing pipe.
     * Returns the n fields, or null when the row does not have n fields.
     */
    static String[] split(String line, int n) {
        int fields = 1;
        for (int i = 0; i < line.length(); i++) {
            if (line.charAt(i) == '|') {
                fields++;
            }
        }
        // Trailing pipe produces an extra empty field.
        boolean trailing = line.charAt(line.length() - 1) == '|';
        int dataFields = trailing ? fields - 1 : fields;
        if (dataFields != n) {
            return null;
        }
        String[] out = new String[n];
        int start = 0;
        int found = 0;
        for (int i = 0; i < line.length() && found < n; i++) {
            if (line.charAt(i) == '|') {
                out[found++] = line.substring(start, i);
                start = i + 1;
            }
        }
        if (!trailing && found < n) {
            out[found] = line.substring(start);
        }
        return out;
    }

    record LoadReport(
            Path source,
            Path database,
            long rows,
            long written,
            long duplicates,
            long badRows,
            long writeNanos,
            long totalNanos,
            long directoryBytes,
            List<String> duplicateSamples) {

        void print(String title) {
            System.out.println();
            System.out.println("======== " + title + " ========");
            System.out.println("source file     : " + source);
            System.out.println("rocksdb path    : " + database);
            System.out.printf(Locale.ROOT, "rows read       : %,d%n", rows);
            System.out.printf(Locale.ROOT, "rows written    : %,d%n", written);
            System.out.printf(Locale.ROOT, "duplicate keys  : %,d%n", duplicates);
            System.out.printf(Locale.ROOT, "malformed rows  : %,d%n", badRows);
            if (duplicates == 0) {
                System.out.println("duplicate sample: (none)");
            } else {
                System.out.println("duplicate sample: " + String.join(", ", duplicateSamples));
            }
            System.out.printf(Locale.ROOT, "batch write     : %.3f s%n", writeNanos / 1_000_000_000.0);
            System.out.printf(Locale.ROOT, "total load      : %.3f s%n", totalNanos / 1_000_000_000.0);
            System.out.printf(Locale.ROOT, "db size         : %,d bytes (%.1f MiB)%n",
                    directoryBytes, directoryBytes / (1024.0 * 1024.0));
        }
    }
}
