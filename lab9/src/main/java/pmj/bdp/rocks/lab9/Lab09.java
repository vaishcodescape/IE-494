package pmj.bdp.rocks.lab9;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;

/**
 * Lab 09: compaction in RocksDB (RocksJava).
 *
 * <pre>
 *   java ... Lab09 all  lab8CordersDb dataDir dbDir outputDir
 *   java ... Lab09 dump lab8CordersDb jsonFile
 *   java ... Lab09 load leveled|universal jsonFile dbDir outputDir
 * </pre>
 */
public class Lab09 {

    public static void main(String[] args) throws Exception {
        if (args.length < 1) {
            usage();
            return;
        }
        RocksDB.loadLibrary();
        switch (args[0]) {
            case "all" -> all(Path.of(args[1]), Path.of(args[2]), Path.of(args[3]), Path.of(args[4]));
            case "dump" -> CordersJson.dump(Path.of(args[1]), Path.of(args[2])).print();
            case "load" -> CompactionStudy.run(CompactionStudy.Style.parse(args[1]), Path.of(args[2]),
                    Path.of(args[3]), Path.of(args[4]));
            default -> usage();
        }
    }

    private static void all(Path cordersDb, Path dataDir, Path dbDir, Path outDir)
            throws IOException, RocksDBException, InterruptedException {
        Path json = dataDir.resolve("corders.json");
        CordersJson.dump(cordersDb, json).print();

        List<CompactionStudy.Result> results = new ArrayList<>();
        for (CompactionStudy.Style style : CompactionStudy.Style.values()) {
            results.add(CompactionStudy.run(style, json, dbDir, outDir));
        }
        writeSummary(outDir.resolve("summary.tsv"), results);
    }

    private static void writeSummary(Path path, List<CompactionStudy.Result> results) throws IOException {
        StringBuilder tsv = new StringBuilder("style\torders\tflushes\tcompactions\ttrivial_moves\tmerges"
                + "\tflushed_bytes\tcompaction_read_bytes\tcompaction_written_bytes\twrite_amp"
                + "\tfinal_files\tfinal_levels\tfinal_sst_bytes\tload_s\tsettle_s\n");
        System.out.println();
        System.out.println("======== Leveled vs Universal ========");
        System.out.printf(Locale.ROOT, "%-10s %8s %11s %7s %7s %10s %10s  %-14s %15s%n", "style", "flushes",
                "compactions", "trivial", "merges", "write-amp", "final-sst", "levels", "final-bytes");
        for (CompactionStudy.Result r : results) {
            SstEventListener.Totals t = r.totals();
            String levels = SstTables.levels(r.finalFiles());
            tsv.append(String.format(Locale.ROOT, "%s\t%d\t%d\t%d\t%d\t%d\t%d\t%d\t%d\t%.2f\t%d\t%s\t%d\t%.3f\t%.3f%n",
                    r.style().dir, r.orders(), t.flushes(), t.compactions(), t.trivialMoves(),
                    t.compactions() - t.trivialMoves(), t.flushedBytes(), t.compactionReadBytes(),
                    t.compactionWrittenBytes(), t.writeAmplification(), r.finalFiles().size(), levels,
                    r.sstBytes(), r.loadNanos() / 1e9, r.settleNanos() / 1e9));
            System.out.printf(Locale.ROOT, "%-10s %8d %11d %7d %7d %10.2f %10d  %-14s %,15d%n",
                    r.style().dir, t.flushes(), t.compactions(), t.trivialMoves(),
                    t.compactions() - t.trivialMoves(), t.writeAmplification(), r.finalFiles().size(), levels,
                    r.sstBytes());
        }
        Files.writeString(path, tsv.toString(), StandardCharsets.UTF_8);
        System.out.println("summary: " + path);
    }

    private static void usage() {
        System.err.println("Usage:");
        System.err.println("  Lab09 all  <lab8CordersDb> <dataDir> <dbDir> <outputDir>");
        System.err.println("  Lab09 dump <lab8CordersDb> <jsonFile>");
        System.err.println("  Lab09 load <leveled|universal> <jsonFile> <dbDir> <outputDir>");
    }
}
