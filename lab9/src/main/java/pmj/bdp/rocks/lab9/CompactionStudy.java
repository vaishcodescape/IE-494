package pmj.bdp.rocks.lab9;

import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import java.util.Set;

import org.rocksdb.BlockBasedTableConfig;
import org.rocksdb.CompactionOptionsUniversal;
import org.rocksdb.CompactionStyle;
import org.rocksdb.CompressionType;
import org.rocksdb.FlushOptions;
import org.rocksdb.InfoLogLevel;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.SstFileReader;
import org.rocksdb.TableProperties;
import org.rocksdb.WriteBatch;
import org.rocksdb.WriteOptions;

/**
 * Steps 1 and 2 for one compaction style: load every order from corders.json
 * into a fresh CORDERS database while {@link SstEventListener} logs each flush
 * and compaction, then list the final SSTables and dump their indexes.
 */
final class CompactionStudy {

    // Same settings for both styles; only the compaction style changes.
    static final long WRITE_BUFFER_SIZE = 128L * 1024 * 1024;
    static final int MAX_WRITE_BUFFERS = 2;
    static final int L0_COMPACTION_TRIGGER = 4;
    static final long TARGET_FILE_SIZE = 64L * 1024 * 1024;
    static final long MAX_BYTES_FOR_LEVEL_BASE = 256L * 1024 * 1024;
    static final long BLOCK_SIZE = 64L * 1024;
    static final int FORMAT_VERSION = 5;
    static final int BACKGROUND_JOBS = 2;
    static final int BATCH_ROWS = 2_000;
    static final int INDEX_EXCERPT = 3;

    enum Style {
        LEVELED("leveled"), UNIVERSAL("universal");

        final String dir;

        Style(String dir) {
            this.dir = dir;
        }

        static Style parse(String name) {
            return switch (name.toLowerCase(Locale.ROOT)) {
                case "leveled", "level", "levelled" -> LEVELED;
                case "universal" -> UNIVERSAL;
                default -> throw new IllegalArgumentException("style must be leveled or universal: " + name);
            };
        }
    }

    private CompactionStudy() {
    }

    static Result run(Style style, Path jsonFile, Path dbRoot, Path outRoot)
            throws IOException, RocksDBException, InterruptedException {
        Path dbPath = dbRoot.resolve(style.dir).resolve("CORDERS");
        Path outDir = outRoot.resolve(style.dir);
        deleteDirectory(dbPath);
        Files.createDirectories(dbPath);
        Files.createDirectories(outDir);

        System.out.println();
        System.out.println("======== " + style + " compaction: load corders.json -> " + dbPath + " ========");
        List<SstTables.Sst> finalFiles;
        SstEventListener.Totals totals;
        long orders = 0;
        long loadNanos;
        long settleNanos;

        try (PrintStream events = printStream(outDir.resolve("events.txt"))) {
            events.println(style + " compaction: every flush and compaction while loading corders.json");
            events.println("rocksdb path: " + dbPath);
            events.println(config(style));
            events.println("'*' marks compaction inputs, '+' marks files written (or moved) by the job.");

            try (CompactionOptionsUniversal universal = new CompactionOptionsUniversal();
                 SstEventListener listener = new SstEventListener(events);
                 Options options = options(style, universal, listener);
                 RocksDB db = RocksDB.open(options, dbPath.toString());
                 WriteOptions writeOptions = new WriteOptions().setDisableWAL(true);
                 WriteBatch batch = new WriteBatch();
                 BufferedReader in = CordersJson.open(jsonFile)) {

                long started = System.nanoTime();
                String line;
                int pending = 0;
                while ((line = in.readLine()) != null) {
                    String doc = CordersJson.document(line);
                    if (doc == null) {
                        continue;
                    }
                    batch.put(CordersJson.key(doc), doc.getBytes(StandardCharsets.UTF_8));
                    orders++;
                    if (++pending == BATCH_ROWS) {
                        db.write(writeOptions, batch);
                        batch.clear();
                        pending = 0;
                    }
                    if (orders % 250_000 == 0) {
                        System.out.printf(Locale.ROOT, "  orders %,d in %.1fs, live SSTables %s%n", orders,
                                (System.nanoTime() - started) / 1_000_000_000.0,
                                SstTables.levels(SstTables.snapshot(db)));
                    }
                }
                if (pending > 0) {
                    db.write(writeOptions, batch);
                }
                loadNanos = System.nanoTime() - started;

                // Flush the last memtable, then let background compactions finish on their own.
                long settleStart = System.nanoTime();
                try (FlushOptions flush = new FlushOptions().setWaitForFlush(true)) {
                    db.flush(flush);
                }
                waitForBackgroundWork(db);
                settleNanos = System.nanoTime() - settleStart;

                finalFiles = SstTables.snapshot(db);
                totals = listener.totals();
                synchronized (listener) {
                    events.println();
                    events.println("======== " + style + " totals ========");
                    events.printf(Locale.ROOT, "orders loaded         : %,d%n", orders);
                    totals.print(events);
                }
            }
        }

        totals.print(System.out);
        Result result = new Result(style, dbPath, orders, loadNanos, settleNanos, totals, finalFiles,
                directoryBytes(dbPath));
        writeFinal(result, outDir);
        return result;
    }

    static String config(Style style) {
        return String.format(Locale.ROOT,
                "options: compaction_style=%s%s, write_buffer_size=%d MiB x %d, level0_file_num_compaction_trigger=%d,"
                        + " target_file_size_base=%d MiB, max_bytes_for_level_base=%d MiB, compression=LZ4,"
                        + " block_size=%d KiB, format_version=%d, index compression off, max_background_jobs=%d,"
                        + " WAL off, batch=%,d",
                style == Style.LEVELED ? "kCompactionStyleLevel" : "kCompactionStyleUniversal",
                style == Style.LEVELED ? " (level_compaction_dynamic_level_bytes=true)"
                        : " (size_ratio=1, min_merge_width=2, max_size_amplification_percent=200)",
                WRITE_BUFFER_SIZE >> 20, MAX_WRITE_BUFFERS, L0_COMPACTION_TRIGGER, TARGET_FILE_SIZE >> 20,
                MAX_BYTES_FOR_LEVEL_BASE >> 20, BLOCK_SIZE >> 10, FORMAT_VERSION, BACKGROUND_JOBS, BATCH_ROWS);
    }

    private static Options options(Style style, CompactionOptionsUniversal universal, SstEventListener listener) {
        BlockBasedTableConfig table = new BlockBasedTableConfig()
                .setBlockSize(BLOCK_SIZE)
                .setFormatVersion(FORMAT_VERSION)
                .setEnableIndexCompression(false);
        Options options = new Options()
                .setCreateIfMissing(true)
                .setCompressionType(CompressionType.LZ4_COMPRESSION)
                .setWriteBufferSize(WRITE_BUFFER_SIZE)
                .setMaxWriteBufferNumber(MAX_WRITE_BUFFERS)
                .setLevel0FileNumCompactionTrigger(L0_COMPACTION_TRIGGER)
                .setTargetFileSizeBase(TARGET_FILE_SIZE)
                .setMaxBytesForLevelBase(MAX_BYTES_FOR_LEVEL_BASE)
                .setMaxBackgroundJobs(BACKGROUND_JOBS)
                .setTableFormatConfig(table)
                .setListeners(List.of(listener))
                .setInfoLogLevel(InfoLogLevel.ERROR_LEVEL);
        if (style == Style.LEVELED) {
            options.setCompactionStyle(CompactionStyle.LEVEL);
            options.setLevelCompactionDynamicLevelBytes(true);
        } else {
            universal.setSizeRatio(1);
            universal.setMinMergeWidth(2);
            universal.setMaxSizeAmplificationPercent(200);
            options.setCompactionStyle(CompactionStyle.UNIVERSAL);
            options.setCompactionOptionsUniversal(universal);
        }
        return options;
    }

    /** RocksJava has no WaitForCompact, so poll until nothing is running or pending. */
    private static void waitForBackgroundWork(RocksDB db) throws RocksDBException, InterruptedException {
        int quiet = 0;
        while (quiet < 5) {
            Thread.sleep(200);
            boolean busy = db.getLongProperty("rocksdb.num-running-compactions") > 0
                    || db.getLongProperty("rocksdb.num-running-flushes") > 0
                    || db.getLongProperty("rocksdb.compaction-pending") > 0
                    || db.getLongProperty("rocksdb.mem-table-flush-pending") > 0;
            quiet = busy ? 0 : quiet + 1;
        }
    }

    /** Step 2: final SSTables, their table properties, and their index blocks. */
    private static void writeFinal(Result r, Path outDir) throws IOException, RocksDBException {
        try (PrintStream fin = printStream(outDir.resolve("final_sstables.txt"));
             PrintStream dump = printStream(outDir.resolve("index_dump.txt"));
             Options options = new Options()) {
            String title = "======== " + r.style() + " final SSTables (" + r.dbPath() + ") ========";
            fin.println(title);
            fin.printf(Locale.ROOT, "orders loaded %,d, load %.3f s, flush + settle %.3f s, db directory %,d bytes%n",
                    r.orders(), r.loadNanos() / 1e9, r.settleNanos() / 1e9, r.directoryBytes());
            SstTables.print(fin, "SSTables final", r.finalFiles(), Set.of(), ' ');
            fin.println();
            fin.println("Table properties and index block of each SSTable (first/last " + INDEX_EXCERPT
                    + " index entries; all entries are in index_dump.txt):");
            dump.println(title);
            dump.println("Every index entry: separator key -> data block handle (offset, size).");

            for (SstTables.Sst f : r.finalFiles()) {
                Path file = r.dbPath().resolve(f.name());
                TableProperties p;
                try (SstFileReader reader = new SstFileReader(options)) {
                    reader.open(file.toString());
                    p = reader.getTableProperties();
                }
                SstIndex.Index index = SstIndex.read(file, p.getIndexKeyIsUserKey() != 0,
                        p.getIndexValueIsDeltaEncoded() != 0);
                String check = check(index, p);
                String head = String.format(Locale.ROOT,
                        "%s  L%d  %,d bytes  keys %s .. %s", f.name(), f.level(), f.size(), f.smallestKey(),
                        f.largestKey());
                String props = String.format(Locale.ROOT,
                        "entries %,d, data blocks %,d, data %,d B, index %,d B, raw key %,d B, raw value %,d B,"
                                + " %s, format_version %d",
                        p.getNumEntries(), p.getNumDataBlocks(), p.getDataSize(), p.getIndexSize(),
                        p.getRawKeySize(), p.getRawValueSize(), p.getCompressionName(), index.formatVersion());
                String layout = String.format(Locale.ROOT,
                        "index block @ %,d (%,d B), %,d entries, %d restart points, %s keys, %s; %s",
                        index.index().offset(), index.index().size(), index.entries().size(), index.restarts(),
                        p.getIndexKeyIsUserKey() != 0 ? "user" : "internal",
                        p.getIndexValueIsDeltaEncoded() != 0 ? "delta-encoded handles" : "full handles", check);

                fin.println();
                fin.println("  " + head);
                fin.println("    " + props);
                fin.println("    " + layout);
                fin.printf(Locale.ROOT, "      %7s  %-14s %14s %10s%n", "entry", "separator-key", "block-offset",
                        "block-size");
                List<SstIndex.Entry> entries = index.entries();
                for (int i = 0; i < entries.size(); i++) {
                    if (i == INDEX_EXCERPT && entries.size() > 2 * INDEX_EXCERPT) {
                        fin.printf(Locale.ROOT, "      %7s  (%,d more entries)%n", "...",
                                entries.size() - 2 * INDEX_EXCERPT);
                        i = entries.size() - INDEX_EXCERPT;
                    }
                    printEntry(fin, i, entries.get(i));
                }

                dump.println();
                dump.println("==== " + head);
                dump.println(props);
                dump.println(layout);
                dump.printf(Locale.ROOT, "      %7s  %-14s %14s %10s%n", "entry", "separator-key", "block-offset",
                        "block-size");
                for (int i = 0; i < entries.size(); i++) {
                    printEntry(dump, i, entries.get(i));
                }
            }
        }
        System.out.println();
        System.out.print(Files.readString(outDir.resolve("final_sstables.txt")));
    }

    private static void printEntry(PrintStream out, int i, SstIndex.Entry e) {
        String key = e.sequence() < 0 ? e.keyText() : e.keyText() + "@" + e.sequence();
        out.printf(Locale.ROOT, "      %7d  %-14s %,14d %,10d%n", i + 1, key, e.block().offset(), e.block().size());
    }

    /** Index sanity: one entry per data block, blocks contiguous from offset 0, keys ascending. */
    private static String check(SstIndex.Index index, TableProperties p) {
        List<SstIndex.Entry> entries = index.entries();
        if (entries.size() != p.getNumDataBlocks()) {
            return "CHECK FAILED: " + entries.size() + " entries but " + p.getNumDataBlocks() + " data blocks";
        }
        long expected = 0;
        String previous = null;
        for (SstIndex.Entry e : entries) {
            if (e.block().offset() != expected) {
                return "CHECK FAILED: block at " + e.block().offset() + ", expected " + expected;
            }
            expected = e.block().offset() + e.block().size() + SstIndex.BLOCK_TRAILER;
            if (previous != null && previous.compareTo(e.keyText()) >= 0) {
                return "CHECK FAILED: separator " + e.keyText() + " not after " + previous;
            }
            previous = e.keyText();
        }
        return "check OK (1 entry per data block, contiguous, ascending)";
    }

    private static PrintStream printStream(Path path) throws IOException {
        return new PrintStream(new BufferedOutputStream(Files.newOutputStream(path), 1 << 16), false,
                StandardCharsets.UTF_8);
    }

    static long directoryBytes(Path root) throws IOException {
        try (var walk = Files.walk(root)) {
            long total = 0;
            for (Path p : walk.filter(Files::isRegularFile).toList()) {
                total += Files.size(p);
            }
            return total;
        }
    }

    static void deleteDirectory(Path root) throws IOException {
        if (!Files.exists(root)) {
            return;
        }
        try (var walk = Files.walk(root)) {
            for (Path p : walk.sorted(Comparator.reverseOrder()).toList()) {
                Files.deleteIfExists(p);
            }
        }
    }

    record Result(Style style, Path dbPath, long orders, long loadNanos, long settleNanos,
                  SstEventListener.Totals totals, List<SstTables.Sst> finalFiles, long directoryBytes) {

        long sstBytes() {
            long total = 0;
            for (SstTables.Sst f : finalFiles) {
                total += f.size();
            }
            return total;
        }
    }
}
