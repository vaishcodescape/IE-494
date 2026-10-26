package pmj.bdp.rocks.lab9;

import java.io.PrintStream;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;

import org.rocksdb.AbstractEventListener;
import org.rocksdb.CompactionJobInfo;
import org.rocksdb.CompactionJobStats;
import org.rocksdb.FlushJobInfo;
import org.rocksdb.RocksDB;

/**
 * Catches every flush and compaction. For each one it prints the live
 * SSTables before (on *Begin) and after (on *Completed) the job.
 *
 * Callbacks run on RocksDB background threads, so every event is written as
 * one synchronized block and exceptions never escape back into JNI.
 */
final class SstEventListener extends AbstractEventListener {

    private final PrintStream out;
    private final long started = System.nanoTime();
    private int events;
    private int flushes;
    private int compactions;
    private int trivialMoves;
    private long flushedBytes;
    private long compactionReadBytes;
    private long compactionWrittenBytes;

    SstEventListener(PrintStream out) {
        super(EnabledEventCallback.ON_FLUSH_BEGIN,
                EnabledEventCallback.ON_FLUSH_COMPLETED,
                EnabledEventCallback.ON_COMPACTION_BEGIN,
                EnabledEventCallback.ON_COMPACTION_COMPLETED);
        this.out = out;
    }

    @Override
    public void onFlushBegin(RocksDB db, FlushJobInfo info) {
        guard("onFlushBegin", () -> {
            List<SstTables.Sst> before = SstTables.snapshot(db);
            // The output file and seqno range are only known once the flush completes.
            header(String.format(Locale.ROOT, "FLUSH BEGIN       job=%d reason=%s",
                    info.getJobId(), info.getFlushReason()));
            SstTables.print(out, "SSTables before", before, Set.of(), ' ');
        });
    }

    @Override
    public void onFlushCompleted(RocksDB db, FlushJobInfo info) {
        guard("onFlushCompleted", () -> {
            List<SstTables.Sst> after = SstTables.snapshot(db);
            String file = SstTables.fileName(info.getFilePath());
            long size = SstTables.sizeOf(after, file);
            flushes++;
            flushedBytes += size;
            header(String.format(Locale.ROOT, "FLUSH COMPLETED   job=%d flush #%d wrote %s (%,d bytes) to L0, seqno %,d..%,d%s",
                    info.getJobId(), flushes, file, size, info.getSmallestSeqno(), info.getLargestSeqno(),
                    info.isTriggeredWritesStop() ? " [writes stopped]"
                            : info.isTriggeredWritesSlowdown() ? " [writes slowed]" : ""));
            SstTables.print(out, "SSTables after", after, Set.of(file), '+');
        });
    }

    @Override
    public void onCompactionBegin(RocksDB db, CompactionJobInfo info) {
        guard("onCompactionBegin", () -> {
            List<SstTables.Sst> before = SstTables.snapshot(db);
            Set<String> inputs = names(info.inputFiles());
            header(String.format(Locale.ROOT, "COMPACTION BEGIN  job=%d reason=%s L%d -> L%d, %d input files",
                    info.jobId(), info.compactionReason(), info.baseInputLevel(), info.outputLevel(),
                    inputs.size()));
            SstTables.print(out, "SSTables before ('*' = input)", before, inputs, '*');
        });
    }

    @Override
    public void onCompactionCompleted(RocksDB db, CompactionJobInfo info) {
        guard("onCompactionCompleted", () -> {
            List<SstTables.Sst> after = SstTables.snapshot(db);
            Set<String> inputs = names(info.inputFiles());
            Set<String> outputs = names(info.outputFiles());
            boolean trivial = inputs.equals(outputs);
            long read;
            long written;
            try (CompactionJobStats stats = info.stats()) {
                read = stats == null ? 0 : stats.totalInputBytes();
                written = stats == null ? 0 : stats.totalOutputBytes();
            }
            compactions++;
            if (trivial) {
                trivialMoves++;
            }
            compactionReadBytes += read;
            compactionWrittenBytes += written;
            header(String.format(Locale.ROOT,
                    "COMPACTION DONE   job=%d compaction #%d L%d -> L%d %s: %d in, %d out, read %,d, wrote %,d bytes",
                    info.jobId(), compactions, info.baseInputLevel(), info.outputLevel(),
                    trivial ? "TRIVIAL MOVE (no rewrite)" : "merge",
                    inputs.size(), outputs.size(), read, written));
            SstTables.print(out, "SSTables after ('+' = output)", after, outputs, '+');
        });
    }

    synchronized Totals totals() {
        return new Totals(flushes, compactions, trivialMoves, flushedBytes, compactionReadBytes,
                compactionWrittenBytes);
    }

    record Totals(int flushes, int compactions, int trivialMoves, long flushedBytes,
                  long compactionReadBytes, long compactionWrittenBytes) {

        /** SST bytes written by flushes and compactions, per byte flushed. */
        double writeAmplification() {
            return flushedBytes == 0 ? 0 : (flushedBytes + compactionWrittenBytes) / (double) flushedBytes;
        }

        void print(PrintStream out) {
            out.printf(Locale.ROOT, "flushes               : %d (%,d bytes written to L0)%n", flushes, flushedBytes);
            out.printf(Locale.ROOT, "compactions           : %d (%d trivial moves, %d merges)%n",
                    compactions, trivialMoves, compactions - trivialMoves);
            out.printf(Locale.ROOT, "compaction read bytes : %,d%n", compactionReadBytes);
            out.printf(Locale.ROOT, "compaction write bytes: %,d%n", compactionWrittenBytes);
            out.printf(Locale.ROOT, "write amplification   : %.2f  ((flush + compaction writes) / flush writes)%n",
                    writeAmplification());
        }
    }

    private void header(String text) {
        events++;
        double t = (System.nanoTime() - started) / 1_000_000_000.0;
        out.println();
        out.printf(Locale.ROOT, "[event %03d  t=%7.3fs] %s%n", events, t, text);
    }

    private synchronized void guard(String callback, Runnable body) {
        try {
            body.run();
            out.flush();
        } catch (Throwable e) {
            out.println("!! " + callback + " failed: " + e);
        }
    }

    private static Set<String> names(List<String> paths) {
        Set<String> names = new HashSet<>();
        for (String p : paths) {
            names.add(SstTables.fileName(p));
        }
        return names;
    }
}
