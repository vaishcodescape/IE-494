package pmj.bdp.spark.lab4;

import java.io.Serializable;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Apache Combined Log Format (see
 * https://httpd.apache.org/docs/2.4/logs.html#accesslog ):
 *
 * host ident user [timestamp] "request" status size "referer" "user-agent"
 */
public final class AccessLog implements Serializable {

    private static final long serialVersionUID = 1L;

    // [12/Dec/2015:18:25:11 +0100] "GET /path HTTP/1.1" 200 4263 "referer" "agent"
    private static final Pattern COMBINED = Pattern.compile(
            "\\[\\d{2}/(\\w{3})/(\\d{4}):[^\\]]+\\]\\s+\"[^\"]*\"\\s+\\d{3}\\s+(\\S+)\\s+\"[^\"]*\"\\s+\"([^\"]*)\""
    );

    public final String month;      // Dec
    public final String year;       // 2015
    public final long bytes;
    public final String userAgent;

    public AccessLog(String month, String year, long bytes, String userAgent) {
        this.month = month;
        this.year = year;
        this.bytes = bytes;
        this.userAgent = userAgent;
    }

    public String yearMonth() {
        return month + "-" + year;
    }

    public int sortKey() {
        return sortKey(yearMonth());
    }

    public static int sortKey(String yearMonth) {
        int dash = yearMonth.lastIndexOf('-');
        String mon = yearMonth.substring(0, dash);
        int year = Integer.parseInt(yearMonth.substring(dash + 1));
        return year * 12 + monthIndex(mon);
    }

    public static AccessLog parse(String line) {
        if (line == null || line.isBlank()) {
            return null;
        }
        Matcher m = COMBINED.matcher(line);
        if (!m.find()) {
            return null;
        }
        long bytes = 0L;
        String sizeTok = m.group(3);
        if (!"-".equals(sizeTok)) {
            try {
                bytes = Long.parseLong(sizeTok);
            } catch (NumberFormatException e) {
                bytes = 0L;
            }
        }
        return new AccessLog(m.group(1), m.group(2), bytes, m.group(4));
    }

    public static String browserFamily(String ua) {
        if (ua == null || ua.isBlank() || "-".equals(ua)) {
            return "Unknown";
        }
        String u = ua.toLowerCase();
        if (u.contains("edg/") || u.contains("edge/")) {
            return "Edge";
        }
        if (u.contains("opr/") || u.contains("opera")) {
            return "Opera";
        }
        if (u.contains("chrome/") || u.contains("crios/")) {
            return "Chrome";
        }
        if (u.contains("firefox/") || u.contains("fxios")) {
            return "Firefox";
        }
        if (u.contains("msie") || u.contains("trident/")) {
            return "Internet Explorer";
        }
        if (u.contains("safari/")) {
            return "Safari";
        }
        if (u.contains("bot") || u.contains("crawler") || u.contains("spider")) {
            return "Bot";
        }
        return "Other";
    }

    private static int monthIndex(String mon) {
        switch (mon) {
            case "Jan": return 1;
            case "Feb": return 2;
            case "Mar": return 3;
            case "Apr": return 4;
            case "May": return 5;
            case "Jun": return 6;
            case "Jul": return 7;
            case "Aug": return 8;
            case "Sep": return 9;
            case "Oct": return 10;
            case "Nov": return 11;
            case "Dec": return 12;
            default: return 0;
        }
    }
}
