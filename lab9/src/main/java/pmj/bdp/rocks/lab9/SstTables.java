package pmj.bdp.rocks.lab9;

import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import org.rocksdb.LiveFileMetaData;
import org.rocksdb.RocksDB;

/** One row per live SSTable: file-name, file-level, file-size, smallest-key, largest-key. */
final class SstTables {

    private SstTables() {
    }

    record Sst(String name, int level, long size, String smallestKey, String largestKey, long entries) {
    }

    static List<Sst> snapshot(RocksDB db) {
        List<Sst> files = new ArrayList<>();
        for (LiveFileMetaData f : db.getLiveFilesMetaData()) {
            files.add(new Sst(fileName(f.fileName()), f.level(), f.size(),
                    text(f.smallestKey()), text(f.largestKey()), f.numEntries()));
        }
        files.sort(Comparator.comparingInt(Sst::level)
                .thenComparing(Sst::smallestKey)
                .thenComparing(Sst::name));
        return files;
    }

    /**
     * Prints the table. A file whose name is in {@code marked} gets {@code mark}
     * in the first column ('*' = compaction input, '+' = written by this job).
     */
    static void print(PrintStream out, String title, List<Sst> files, Set<String> marked, char mark) {
        long total = 0;
        for (Sst f : files) {
            total += f.size();
        }
        out.printf(Locale.ROOT, "  %s: %d files, %,d bytes, %s%n", title, files.size(), total, levels(files));
        if (files.isEmpty()) {
            return;
        }
        out.printf(Locale.ROOT, "      %-11s %-5s %13s  %-12s %s%n",
                "file-name", "level", "file-size", "smallest-key", "largest-key");
        for (Sst f : files) {
            char m = marked.contains(f.name()) ? mark : ' ';
            out.printf(Locale.ROOT, "    %c %-11s L%-4d %,13d  %-12s %s%n",
                    m, f.name(), f.level(), f.size(), f.smallestKey(), f.largestKey());
        }
    }

    /** "L0=2 L6=5" style summary of how many files sit at each level. */
    static String levels(List<Sst> files) {
        Map<Integer, Integer> perLevel = new TreeMap<>();
        for (Sst f : files) {
            perLevel.merge(f.level(), 1, Integer::sum);
        }
        if (perLevel.isEmpty()) {
            return "no levels";
        }
        StringBuilder sb = new StringBuilder();
        perLevel.forEach((level, n) -> sb.append(sb.isEmpty() ? "" : " ").append('L').append(level)
                .append('=').append(n));
        return sb.toString();
    }

    static long sizeOf(List<Sst> files, String name) {
        for (Sst f : files) {
            if (f.name().equals(name)) {
                return f.size();
            }
        }
        return 0;
    }

    /** "/path/to/000123.sst" or "/000123.sst" becomes "000123.sst". */
    static String fileName(String path) {
        int slash = path.lastIndexOf('/');
        return slash < 0 ? path : path.substring(slash + 1);
    }

    private static String text(byte[] key) {
        return key == null ? "(null)" : new String(key, StandardCharsets.US_ASCII);
    }
}
