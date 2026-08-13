package pmj.bdp.mr.lab2;

import java.util.ArrayList;
import java.util.List;

/**
 * Helper for parsing a row from orders.csv.
 * Handles quoted fields that contain commas (e.g. "1,963.10").
 */
public final class OrderRecord {

    public final String salesChannel;
    public final String orderDate;
    public final int orderQuantity;
    public final double unitPrice;

    private OrderRecord(String salesChannel, String orderDate,
                        int orderQuantity, double unitPrice) {
        this.salesChannel = salesChannel;
        this.orderDate = orderDate;
        this.orderQuantity = orderQuantity;
        this.unitPrice = unitPrice;
    }

    public double orderAmount() {
        return orderQuantity * unitPrice;
    }

    /** Year extracted from OrderDate (formats like 31/5/18 or 2018-05-31). */
    public String year() {
        String d = orderDate.trim();
        if (d.matches("\\d{4}-\\d{2}-\\d{2}")) {
            return d.substring(0, 4);
        }
        // d/M/yy or dd/MM/yyyy
        String[] parts = d.split("/");
        if (parts.length == 3) {
            String y = parts[2].trim();
            if (y.length() == 2) {
                int yy = Integer.parseInt(y);
                return String.valueOf(2000 + yy);
            }
            return y;
        }
        return d;
    }

    public static OrderRecord parse(String line) {
        if (line == null || line.isBlank()) {
            return null;
        }
        // skip header
        if (line.startsWith("OrderNumber") || line.startsWith("\uFEFFOrderNumber")) {
            return null;
        }

        List<String> fields = splitCsv(line);
        // OrderNumber, Sales Channel, ..., Order Quantity, Discount, Unit Cost, Unit Price
        if (fields.size() < 16) {
            return null;
        }

        try {
            String channel = fields.get(1).trim();
            String orderDate = fields.get(4).trim();
            int qty = Integer.parseInt(fields.get(12).trim());
            double unitPrice = parseMoney(fields.get(15));
            return new OrderRecord(channel, orderDate, qty, unitPrice);
        } catch (NumberFormatException e) {
            return null;
        }
    }

    static double parseMoney(String raw) {
        String s = raw.trim().replace("\"", "").replace(",", "").replace("$", "");
        return Double.parseDouble(s);
    }

    /** Split a CSV line, respecting double-quoted fields. */
    static List<String> splitCsv(String line) {
        List<String> fields = new ArrayList<>();
        StringBuilder cur = new StringBuilder();
        boolean inQuotes = false;

        for (int i = 0; i < line.length(); i++) {
            char c = line.charAt(i);
            if (c == '"') {
                inQuotes = !inQuotes;
            } else if (c == ',' && !inQuotes) {
                fields.add(cur.toString());
                cur.setLength(0);
            } else {
                cur.append(c);
            }
        }
        fields.add(cur.toString());
        return fields;
    }
}
