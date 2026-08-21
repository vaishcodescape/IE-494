package pmj.bdp.mr.lab3;

import java.util.ArrayList;
import java.util.List;

/**
 * One row of daily-website-visitors.csv.
 * Numbers may be quoted and contain commas (e.g. "2,146").
 */
public final class VisitorRecord {

    public final int year;
    public final int month;
    public final double firstTimeVisits;
    public final double returningVisits;

    private VisitorRecord(int year, int month,
                          double firstTimeVisits, double returningVisits) {
        this.year = year;
        this.month = month;
        this.firstTimeVisits = firstTimeVisits;
        this.returningVisits = returningVisits;
    }

    /** YYYYMM, e.g. 201409, used as a sortable month timestamp. */
    public long yearMonth() {
        return year * 100L + month;
    }

    public static VisitorRecord parse(String line) {
        if (line == null || line.isBlank()) {
            return null;
        }
        if (line.startsWith("Row") || line.startsWith("\uFEFFRow")) {
            return null;
        }

        List<String> fields = splitCsv(line);
        // Row, Day, Day.Of.Week, Date, Page.Loads, Unique.Visits,
        // First.Time.Visits, Returning.Visits
        if (fields.size() < 8) {
            return null;
        }

        try {
            int[] ymd = parseDate(fields.get(3).trim());
            if (ymd == null) {
                return null;
            }
            double first = parseNumber(fields.get(6));
            double returning = parseNumber(fields.get(7));
            return new VisitorRecord(ymd[0], ymd[1], first, returning);
        } catch (NumberFormatException e) {
            return null;
        }
    }

    /** Date in M/d/yyyy. Returns {year, month, day} or null. */
    static int[] parseDate(String raw) {
        String[] parts = raw.split("/");
        if (parts.length != 3) {
            return null;
        }
        int month = Integer.parseInt(parts[0].trim());
        int day = Integer.parseInt(parts[1].trim());
        int year = Integer.parseInt(parts[2].trim());
        return new int[] {year, month, day};
    }

    static double parseNumber(String raw) {
        String s = raw.trim().replace("\"", "").replace(",", "").replace(" ", "");
        return Double.parseDouble(s);
    }

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
