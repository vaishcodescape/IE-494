package pmj.bdp.rocks.lab8;

/**
 * Minimal JSON writer for TPC-H pipe-delimited rows.
 * Numbers are written as JSON numbers; strings are escaped.
 */
final class Json {

    private Json() {
    }

    static String order(String[] f) {
        StringBuilder sb = new StringBuilder(256);
        sb.append('{');
        num(sb, "O_ORDERKEY", f[0]);
        sb.append(',');
        num(sb, "O_CUSTKEY", f[1]);
        sb.append(',');
        str(sb, "O_ORDERSTATUS", f[2]);
        sb.append(',');
        num(sb, "O_TOTALPRICE", f[3]);
        sb.append(',');
        str(sb, "O_ORDERDATE", f[4]);
        sb.append(',');
        str(sb, "O_ORDERPRIORITY", f[5]);
        sb.append(',');
        str(sb, "O_CLERK", f[6]);
        sb.append(',');
        num(sb, "O_SHIPPRIORITY", f[7]);
        sb.append(',');
        str(sb, "O_COMMENT", f[8]);
        sb.append('}');
        return sb.toString();
    }

    static String lineItem(String[] f) {
        StringBuilder sb = new StringBuilder(384);
        sb.append('{');
        num(sb, "L_ORDERKEY", f[0]);
        sb.append(',');
        num(sb, "L_PARTKEY", f[1]);
        sb.append(',');
        num(sb, "L_SUPPKEY", f[2]);
        sb.append(',');
        num(sb, "L_LINENUMBER", f[3]);
        sb.append(',');
        num(sb, "L_QUANTITY", f[4]);
        sb.append(',');
        num(sb, "L_EXTENDEDPRICE", f[5]);
        sb.append(',');
        num(sb, "L_DISCOUNT", f[6]);
        sb.append(',');
        num(sb, "L_TAX", f[7]);
        sb.append(',');
        str(sb, "L_RETURNFLAG", f[8]);
        sb.append(',');
        str(sb, "L_LINESTATUS", f[9]);
        sb.append(',');
        str(sb, "L_SHIPDATE", f[10]);
        sb.append(',');
        str(sb, "L_COMMITDATE", f[11]);
        sb.append(',');
        str(sb, "L_RECEIPTDATE", f[12]);
        sb.append(',');
        str(sb, "L_SHIPINSTRUCT", f[13]);
        sb.append(',');
        str(sb, "L_SHIPMODE", f[14]);
        sb.append(',');
        str(sb, "L_COMMENT", f[15]);
        sb.append('}');
        return sb.toString();
    }

    /** Splice a LINEITEMS array into an order object that ends with '}'. */
    static String embedLineItems(String orderJson, String[] items, int n) {
        StringBuilder sb = new StringBuilder(orderJson.length() + n * 280);
        sb.append(orderJson, 0, orderJson.length() - 1);
        sb.append(",\"LINEITEMS\":[");
        for (int i = 0; i < n; i++) {
            if (i > 0) {
                sb.append(',');
            }
            sb.append(items[i]);
        }
        sb.append("]}");
        return sb.toString();
    }

    private static void num(StringBuilder sb, String name, String raw) {
        sb.append('"').append(name).append("\":").append(raw.trim());
    }

    private static void str(StringBuilder sb, String name, String raw) {
        sb.append('"').append(name).append("\":");
        escape(sb, raw);
    }

    static void escape(StringBuilder sb, String s) {
        sb.append('"');
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            switch (c) {
                case '"' -> sb.append("\\\"");
                case '\\' -> sb.append("\\\\");
                case '\n' -> sb.append("\\n");
                case '\r' -> sb.append("\\r");
                case '\t' -> sb.append("\\t");
                default -> {
                    if (c < 0x20) {
                        sb.append(String.format("\\u%04x", (int) c));
                    } else {
                        sb.append(c);
                    }
                }
            }
        }
        sb.append('"');
    }
}
