package pmj.bdp.rocks.lab8;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Locale;
import java.util.stream.Stream;

import org.rocksdb.CompressionType;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;

/**
 * Lab 08: TPC-H ORDERS and LINEITEM in RocksJava.
 *
 * <pre>
 *   java ... Lab08 all  dataDir dbDir outputDir
 *   java ... Lab08 load dataDir dbDir outputDir
 *   java ... Lab08 merge dbDir compression outputDir
 * </pre>
 */
public class Lab08 {

    public static void main(String[] args) throws Exception {
        if (args.length < 1) {
            usage();
            return;
        }
        RocksDB.loadLibrary();
        switch (args[0]) {
            case "all" -> all(Path.of(args[1]), Path.of(args[2]), Path.of(args[3]));
            case "load" -> load(Path.of(args[1]), Path.of(args[2]), Path.of(args[3]));
            case "merge" -> {
                Path dbDir = Path.of(args[1]);
                mergeOne(dbDir, args[2], Path.of(args[3]));
            }
            default -> usage();
        }
    }

    private static void all(Path dataDir, Path dbDir, Path outDir) throws IOException, RocksDBException {
        LoadPair loaded = load(dataDir, dbDir, outDir);
        Files.createDirectories(outDir);
        Path metrics = outDir.resolve("compression.tsv");
        try (var writer = Files.newBufferedWriter(metrics, StandardCharsets.UTF_8)) {
            writer.write("compression\tinsertion_s\tfinal_bytes\tread_MBps\ttotal_write_s\torders\tlineitems\n");
            for (String name : new String[] {"none", "lz4", "zstd"}) {
                CordersBuilder.MergeReport report = build(dbDir, name);
                report.print();
                writer.write(String.format(Locale.ROOT,
                        "%s\t%.3f\t%d\t%.1f\t%.3f\t%d\t%d%n",
                        name,
                        report.writeNanos() / 1_000_000_000.0,
                        report.directoryBytes(),
                        report.read().mbPerSec(),
                        report.totalNanos() / 1_000_000_000.0,
                        report.ordersRead(),
                        report.lineItemsRead()));
                writer.flush();
                writeSample(outDir.resolve("sample_corders_" + name + ".json"), report.sample());
            }
        }
        System.out.println();
        System.out.println("orders file   : " + loaded.orders().source());
        System.out.println("lineitem file : " + loaded.lines().source());
        System.out.println("metrics       : " + metrics);
    }

    private static LoadPair load(Path dataDir, Path dbDir, Path outDir) throws IOException, RocksDBException {
        Path ordersTbl = findTable(dataDir, "orders");
        Path lineTbl = findTable(dataDir, "lineitem");
        Files.createDirectories(outDir);
        System.out.println("ORDERS.TBL   = " + ordersTbl + " (" + Files.size(ordersTbl) + " bytes)");
        System.out.println("LINEITEM.TBL = " + lineTbl + " (" + Files.size(lineTbl) + " bytes)");

        TblLoader.LoadReport orders = TblLoader.loadOrders(ordersTbl, dbDir.resolve("ORDERS"));
        orders.print("Task 1 ORDERS");
        printLookup(dbDir.resolve("ORDERS"), Keys.orderKey(1), "ORDERS key 0000000001");

        TblLoader.LoadReport lines = TblLoader.loadLineItems(lineTbl, dbDir.resolve("LINEITEM"));
        lines.print("Task 1 LINEITEM");
        printLookup(dbDir.resolve("LINEITEM"), Keys.lineKey(1, 1), "LINEITEM key 0000000001|01");

        writeDuplicateReport(outDir.resolve("duplicates.txt"), orders, lines);
        return new LoadPair(orders, lines);
    }

    private static CordersBuilder.MergeReport mergeOne(Path dbDir, String compression, Path outDir)
            throws RocksDBException, IOException {
        Files.createDirectories(outDir);
        CordersBuilder.MergeReport report = build(dbDir, compression);
        report.print();
        writeSample(outDir.resolve("sample_corders_" + compression + ".json"), report.sample());
        return report;
    }

    private static CordersBuilder.MergeReport build(Path dbDir, String compression)
            throws RocksDBException, IOException {
        CompressionType type = RocksStore.compression(compression);
        Path dest = dbDir.resolve("CORDERS_" + compression);
        System.out.println();
        System.out.println("building CORDERS compression=" + compression);
        return CordersBuilder.build(dbDir.resolve("ORDERS"), dbDir.resolve("LINEITEM"), dest, type);
    }

    private static void printLookup(Path dbPath, byte[] key, String label) throws RocksDBException {
        try (RocksDB db = RocksStore.openReadOnly(dbPath)) {
            byte[] value = db.get(key);
            System.out.println(label + " -> " + (value == null
                    ? "(missing)"
                    : new String(value, StandardCharsets.UTF_8)));
        }
    }

    private static void writeDuplicateReport(Path path, TblLoader.LoadReport orders, TblLoader.LoadReport lines)
            throws IOException {
        String text = """
                Duplicate key report
                ORDERS rows read: %d
                ORDERS duplicate keys: %d
                ORDERS malformed rows: %d
                ORDERS duplicate samples: %s
                LINEITEM rows read: %d
                LINEITEM duplicate keys: %d
                LINEITEM malformed rows: %d
                LINEITEM duplicate samples: %s
                """.formatted(
                orders.rows(), orders.duplicates(), orders.badRows(),
                orders.duplicates() == 0 ? "(none)" : String.join(", ", orders.duplicateSamples()),
                lines.rows(), lines.duplicates(), lines.badRows(),
                lines.duplicates() == 0 ? "(none)" : String.join(", ", lines.duplicateSamples()));
        Files.writeString(path, text);
        System.out.println();
        System.out.print(text);
    }

    private static void writeSample(Path path, String json) throws IOException {
        if (json != null) {
            Files.writeString(path, json + System.lineSeparator());
        }
    }

    static Path findTable(Path dataDir, String baseName) throws IOException {
        if (!Files.isDirectory(dataDir)) {
            throw new IOException("data directory not found: " + dataDir);
        }
        try (Stream<Path> walk = Files.walk(dataDir)) {
            return walk.filter(Files::isRegularFile)
                    .filter(p -> {
                        String name = p.getFileName().toString().toLowerCase(Locale.ROOT);
                        return name.equals(baseName + ".tbl") || name.equals(baseName + ".tbl.gz");
                    })
                    .findFirst()
                    .orElseThrow(() -> new IOException(
                            "could not find " + baseName + ".tbl under " + dataDir));
        }
    }

    private static void usage() {
        System.err.println("Usage:");
        System.err.println("  Lab08 all   <dataDir> <dbDir> <outputDir>");
        System.err.println("  Lab08 load  <dataDir> <dbDir> <outputDir>");
        System.err.println("  Lab08 merge <dbDir> <none|lz4|zstd> <outputDir>");
    }

    private record LoadPair(TblLoader.LoadReport orders, TblLoader.LoadReport lines) {
    }
}
