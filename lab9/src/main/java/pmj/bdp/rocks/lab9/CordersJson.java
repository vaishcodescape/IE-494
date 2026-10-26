package pmj.bdp.rocks.lab9;

import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Locale;

import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;

/**
 * Step 0: dump Lab 08's CORDERS database to corders.json.
 *
 * The file is one JSON array with one order document per line, so it is valid
 * JSON and can still be streamed line by line when it is loaded back.
 */
final class CordersJson {

    static final int KEY_WIDTH = 10;
    private static final String KEY_FIELD = "{\"O_ORDERKEY\":";
    private static final byte[] NEWLINE = {'\n'};
    private static final byte[] COMMA_NEWLINE = {',', '\n'};

    private CordersJson() {
    }

    static DumpReport dump(Path cordersDb, Path jsonFile) throws RocksDBException, IOException {
        Files.createDirectories(jsonFile.toAbsolutePath().getParent());
        long started = System.nanoTime();
        long orders = 0;
        long bytes = 0;
        String firstKey = null;
        String lastKey = null;
        try (Options options = new Options().setCreateIfMissing(false);
             RocksDB db = RocksDB.openReadOnly(options, cordersDb.toString());
             RocksIterator it = db.newIterator();
             OutputStream out = new BufferedOutputStream(Files.newOutputStream(jsonFile), 1 << 20)) {
            out.write('[');
            for (it.seekToFirst(); it.isValid(); it.next()) {
                out.write(orders == 0 ? NEWLINE : COMMA_NEWLINE);
                byte[] value = it.value();
                out.write(value);
                bytes += value.length;
                lastKey = new String(it.key(), StandardCharsets.US_ASCII);
                if (firstKey == null) {
                    firstKey = lastKey;
                }
                orders++;
            }
            out.write(NEWLINE);
            out.write(']');
            out.write(NEWLINE);
        }
        return new DumpReport(cordersDb, jsonFile, orders, bytes, Files.size(jsonFile),
                firstKey, lastKey, System.nanoTime() - started);
    }

    static BufferedReader open(Path jsonFile) throws IOException {
        return new BufferedReader(new InputStreamReader(Files.newInputStream(jsonFile), StandardCharsets.UTF_8),
                1 << 20);
    }

    /** Returns the order document on this line, or null for the array brackets. */
    static String document(String line) {
        if (line.isEmpty() || line.equals("[") || line.equals("]")) {
            return null;
        }
        return line.endsWith(",") ? line.substring(0, line.length() - 1) : line;
    }

    /** 10-digit zero-padded O_ORDERKEY, the same key Lab 08 used. */
    static byte[] key(String document) {
        if (!document.startsWith(KEY_FIELD)) {
            throw new IllegalArgumentException("document does not start with O_ORDERKEY: "
                    + document.substring(0, Math.min(60, document.length())));
        }
        int start = KEY_FIELD.length();
        int end = start;
        while (end < document.length() && Character.isDigit(document.charAt(end))) {
            end++;
        }
        int digits = end - start;
        if (digits == 0 || digits > KEY_WIDTH) {
            throw new IllegalArgumentException("bad O_ORDERKEY in " + document.substring(0, end));
        }
        byte[] key = new byte[KEY_WIDTH];
        int pad = KEY_WIDTH - digits;
        for (int i = 0; i < KEY_WIDTH; i++) {
            key[i] = i < pad ? (byte) '0' : (byte) document.charAt(start + i - pad);
        }
        return key;
    }

    record DumpReport(Path source, Path file, long orders, long valueBytes, long fileBytes,
                      String firstKey, String lastKey, long nanos) {

        void print() {
            System.out.println();
            System.out.println("======== Dump CORDERS -> corders.json ========");
            System.out.println("source rocksdb : " + source);
            System.out.println("json file      : " + file);
            System.out.printf(Locale.ROOT, "orders written : %,d%n", orders);
            System.out.printf(Locale.ROOT, "document bytes : %,d%n", valueBytes);
            System.out.printf(Locale.ROOT, "file size      : %,d bytes (%.1f MiB)%n",
                    fileBytes, fileBytes / (1024.0 * 1024.0));
            System.out.println("key range      : " + firstKey + " .. " + lastKey);
            System.out.printf(Locale.ROOT, "dump time      : %.3f s%n", nanos / 1_000_000_000.0);
        }
    }
}
