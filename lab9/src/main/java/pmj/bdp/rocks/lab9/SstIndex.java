package pmj.bdp.rocks.lab9;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Reads the index block of a RocksDB block-based SSTable.
 *
 * RocksJava has no API for the index block, so this decodes the on-disk
 * format directly (RocksDB wiki "Rocksdb BlockBasedTable Format"):
 *
 * <pre>
 *   [data block 1] ... [data block N] [meta blocks] [metaindex block] [index block] [footer]
 *   footer (53 bytes, format_version 1..5):
 *     checksum type (1) | metaindex handle | index handle | padding to 41 bytes
 *     | format_version (fixed32) | magic number (fixed64)
 *   block = contents + trailer (compression type 1 byte + checksum 4 bytes)
 *   index entry = separator key -> BlockHandle(offset, size) of one data block
 * </pre>
 *
 * Tables are written with format_version=5, an uncompressed binary-search
 * index, and the default index_block_restart_interval of 1.
 */
final class SstIndex {

    static final long BLOCK_BASED_MAGIC = 0x88e241b785f4cff7L;
    static final int FOOTER_LENGTH = 53;
    static final int BLOCK_TRAILER = 5;

    private SstIndex() {
    }

    record Handle(long offset, long size) {
    }

    /** One index entry. {@code sequence} is -1 when the index stores user keys. */
    record Entry(byte[] key, long sequence, Handle block) {
        String keyText() {
            return new String(key, StandardCharsets.US_ASCII);
        }
    }

    record Index(int formatVersion, Handle metaindex, Handle index, int restarts, List<Entry> entries) {
    }

    static Index read(Path sst, boolean keyIsUserKey, boolean valueDeltaEncoded) throws IOException {
        try (FileChannel ch = FileChannel.open(sst, StandardOpenOption.READ)) {
            long length = ch.size();
            ByteBuffer footer = read(ch, length - FOOTER_LENGTH, FOOTER_LENGTH);
            long magic = footer.getLong(FOOTER_LENGTH - 8);
            if (magic != BLOCK_BASED_MAGIC) {
                throw new IOException(sst + ": not a block-based table (magic " + Long.toHexString(magic) + ")");
            }
            int formatVersion = footer.getInt(FOOTER_LENGTH - 12);
            if (formatVersion < 1 || formatVersion > 5) {
                throw new IOException(sst + ": footer format_version " + formatVersion + " is not supported");
            }
            footer.position(1); // skip checksum type
            Handle metaindex = new Handle(varint64(footer), varint64(footer));
            Handle index = new Handle(varint64(footer), varint64(footer));

            int size = Math.toIntExact(index.size());
            ByteBuffer block = read(ch, index.offset(), size + BLOCK_TRAILER);
            int compression = block.get(size) & 0xff;
            if (compression != 0) {
                throw new IOException(sst + ": index block is compressed (type " + compression + ")");
            }
            block.limit(size);
            int restarts = block.getInt(size - 4) & 0x7fffffff;
            List<Entry> entries = entries(block, size - 4 - 4 * restarts, keyIsUserKey, valueDeltaEncoded);
            return new Index(formatVersion, metaindex, index, restarts, entries);
        }
    }

    /**
     * Entry layout: shared (varint32) | non_shared (varint32) | [value length (varint32)]
     * | key delta | value. With delta-encoded values (format_version >= 4) there is no
     * value length; an entry that shares no key bytes stores a full BlockHandle, and
     * any other entry stores only the signed size change from the previous block.
     */
    private static List<Entry> entries(ByteBuffer block, int end, boolean keyIsUserKey, boolean valueDeltaEncoded) {
        List<Entry> entries = new ArrayList<>();
        byte[] key = new byte[0];
        Handle previous = null;
        block.position(0);
        while (block.position() < end) {
            int shared = (int) varint64(block);
            int nonShared = (int) varint64(block);
            int valueLength = valueDeltaEncoded ? -1 : (int) varint64(block);
            byte[] next = Arrays.copyOf(key, shared + nonShared);
            block.get(next, shared, nonShared);
            key = next;

            Handle handle;
            if (valueDeltaEncoded && shared > 0 && previous != null) {
                long delta = zigzag(varint64(block));
                handle = new Handle(previous.offset() + previous.size() + BLOCK_TRAILER, previous.size() + delta);
            } else {
                int valueStart = block.position();
                handle = new Handle(varint64(block), varint64(block));
                if (valueLength >= 0) {
                    block.position(valueStart + valueLength);
                }
            }
            previous = handle;

            if (keyIsUserKey) {
                entries.add(new Entry(key, -1, handle));
            } else {
                // Internal key: user key + fixed64 (sequence << 8 | value type).
                int userLength = key.length - 8;
                long packed = ByteBuffer.wrap(key, userLength, 8).order(ByteOrder.LITTLE_ENDIAN).getLong();
                entries.add(new Entry(Arrays.copyOf(key, userLength), packed >>> 8, handle));
            }
        }
        return entries;
    }

    private static ByteBuffer read(FileChannel ch, long offset, int length) throws IOException {
        ByteBuffer buf = ByteBuffer.allocate(length).order(ByteOrder.LITTLE_ENDIAN);
        while (buf.hasRemaining()) {
            if (ch.read(buf, offset + buf.position()) < 0) {
                throw new IOException("unexpected end of file at " + (offset + buf.position()));
            }
        }
        return buf.flip();
    }

    private static long varint64(ByteBuffer buf) {
        long result = 0;
        for (int shift = 0; shift < 64; shift += 7) {
            byte b = buf.get();
            result |= (long) (b & 0x7f) << shift;
            if ((b & 0x80) == 0) {
                return result;
            }
        }
        throw new IllegalStateException("varint longer than 10 bytes");
    }

    private static long zigzag(long n) {
        return (n >>> 1) ^ -(n & 1);
    }
}
