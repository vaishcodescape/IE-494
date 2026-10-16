package pmj.bdp.rocks.lab8;

import java.nio.charset.StandardCharsets;

/**
 * Lexicographic keys that sort in the same order as the numeric TPC-H keys.
 * ORDERS key is the 10-digit O_ORDERKEY.
 * LINEITEM key is O_ORDERKEY + "|" + L_LINENUMBER (L_ORDERKEY in the file).
 */
final class Keys {

    static final int ORDER_WIDTH = 10;
    static final int LINE_WIDTH = 2;

    private Keys() {
    }

    static byte[] orderKey(long orderKey) {
        byte[] key = new byte[ORDER_WIDTH];
        fillDigits(key, 0, ORDER_WIDTH, orderKey);
        return key;
    }

    static byte[] lineKey(long orderKey, int lineNumber) {
        if (lineNumber < 0 || lineNumber > 99) {
            throw new IllegalArgumentException("L_LINENUMBER out of range: " + lineNumber);
        }
        byte[] key = new byte[ORDER_WIDTH + 1 + LINE_WIDTH];
        fillDigits(key, 0, ORDER_WIDTH, orderKey);
        key[ORDER_WIDTH] = '|';
        key[ORDER_WIDTH + 1] = (byte) ('0' + (lineNumber / 10));
        key[ORDER_WIDTH + 2] = (byte) ('0' + (lineNumber % 10));
        return key;
    }

    static String orderKeyText(byte[] lineKey) {
        return new String(lineKey, 0, ORDER_WIDTH, StandardCharsets.US_ASCII);
    }

    static long pack(long orderKey, int lineNumber) {
        return (orderKey << 16) | (lineNumber & 0xffff);
    }

    private static void fillDigits(byte[] dest, int offset, int width, long value) {
        if (value < 0) {
            throw new IllegalArgumentException("negative key " + value);
        }
        for (int i = width - 1; i >= 0; i--) {
            dest[offset + i] = (byte) ('0' + (value % 10));
            value /= 10;
        }
        if (value != 0) {
            throw new IllegalArgumentException("key does not fit in " + width + " digits");
        }
    }
}
