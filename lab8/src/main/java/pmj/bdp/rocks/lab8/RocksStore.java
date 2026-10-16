package pmj.bdp.rocks.lab8;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.Locale;

import org.rocksdb.CompressionType;
import org.rocksdb.FlushOptions;
import org.rocksdb.InfoLogLevel;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.rocksdb.WriteBatch;
import org.rocksdb.WriteOptions;

final class RocksStore {

    static final int BATCH_ROWS = 2_000;

    private RocksStore() {
    }

    static CompressionType compression(String name) {
        return switch (name.toLowerCase(Locale.ROOT)) {
            case "none", "no", "nocompression" -> CompressionType.NO_COMPRESSION;
            case "lz4" -> CompressionType.LZ4_COMPRESSION;
            case "zstd" -> CompressionType.ZSTD_COMPRESSION;
            default -> throw new IllegalArgumentException(
                    "compression must be none, lz4, or zstd: " + name);
        };
    }

    static RocksDB open(Path path, CompressionType compression, boolean create) throws RocksDBException, IOException {
        if (create) {
            Files.createDirectories(path);
        }
        try (Options options = new Options()) {
            options.setCreateIfMissing(create);
            options.setCompressionType(compression);
            options.setBottommostCompressionType(compression);
            options.setWriteBufferSize(64L * 1024 * 1024);
            options.setMaxWriteBufferNumber(3);
            options.setTargetFileSizeBase(64L * 1024 * 1024);
            options.setMaxBytesForLevelBase(256L * 1024 * 1024);
            options.setLevelCompactionDynamicLevelBytes(true);
            int cpus = Runtime.getRuntime().availableProcessors();
            options.setIncreaseParallelism(cpus);
            options.setMaxBackgroundJobs(Math.max(2, cpus / 2));
            options.setInfoLogLevel(InfoLogLevel.ERROR_LEVEL);
            return RocksDB.open(options, path.toString());
        }
    }

    static RocksDB openReadOnly(Path path) throws RocksDBException {
        try (Options options = new Options()) {
            options.setCreateIfMissing(false);
            options.setInfoLogLevel(InfoLogLevel.ERROR_LEVEL);
            return RocksDB.openReadOnly(options, path.toString());
        }
    }

    static void flushAndCompact(RocksDB db) throws RocksDBException {
        try (FlushOptions flush = new FlushOptions().setWaitForFlush(true)) {
            db.flush(flush);
        }
        db.compactRange();
    }

    static long directoryBytes(Path root) throws IOException {
        if (!Files.exists(root)) {
            return 0L;
        }
        try (var walk = Files.walk(root)) {
            return walk.filter(Files::isRegularFile).mapToLong(p -> {
                try {
                    return Files.size(p);
                } catch (IOException e) {
                    return 0L;
                }
            }).sum();
        }
    }

    static void deleteDirectory(Path root) throws IOException {
        if (!Files.exists(root)) {
            return;
        }
        try (var walk = Files.walk(root)) {
            walk.sorted(Comparator.reverseOrder()).forEach(p -> {
                try {
                    Files.deleteIfExists(p);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            });
        }
    }

    static Scan scan(RocksDB db) {
        long keys = 0;
        long bytes = 0;
        byte[] firstKey = null;
        byte[] firstValue = null;
        long t0 = System.nanoTime();
        try (RocksIterator it = db.newIterator()) {
            for (it.seekToFirst(); it.isValid(); it.next()) {
                byte[] key = it.key();
                byte[] value = it.value();
                if (firstKey == null) {
                    firstKey = key;
                    firstValue = value;
                }
                keys++;
                bytes += key.length + value.length;
            }
        }
        long nanos = System.nanoTime() - t0;
        return new Scan(keys, bytes, nanos, firstKey, firstValue);
    }

    record Scan(long keys, long bytes, long nanos, byte[] firstKey, byte[] firstValue) {
        double mbPerSec() {
            double seconds = nanos / 1_000_000_000.0;
            if (seconds <= 0) {
                return 0;
            }
            return (bytes / 1_000_000.0) / seconds;
        }
    }

    /** Buffered WriteBatch. Tracks time spent inside RocksDB write calls. */
    static final class Batch implements AutoCloseable {
        private final RocksDB db;
        private final WriteOptions writeOptions;
        private final WriteBatch batch;
        private int pending;
        private long writeNanos;
        private long rows;

        Batch(RocksDB db) {
            this.db = db;
            this.writeOptions = new WriteOptions();
            this.writeOptions.setDisableWAL(true);
            this.writeOptions.setSync(false);
            this.batch = new WriteBatch();
        }

        void put(byte[] key, byte[] value) throws RocksDBException {
            batch.put(key, value);
            rows++;
            if (++pending >= BATCH_ROWS) {
                flush();
            }
        }

        void flush() throws RocksDBException {
            if (pending == 0) {
                return;
            }
            long t0 = System.nanoTime();
            db.write(writeOptions, batch);
            writeNanos += System.nanoTime() - t0;
            batch.clear();
            pending = 0;
        }

        long writeNanos() {
            return writeNanos;
        }

        long rows() {
            return rows;
        }

        @Override
        public void close() {
            batch.close();
            writeOptions.close();
        }
    }
}
